package streamload

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// BeginTransaction begins a new transaction with the specified label
func (c *Client) BeginTransaction(label string, tables []string) (*TransactionBeginResponse, error) {
	urlStr := fmt.Sprintf("%s/api/transaction/begin", c.getCurrentFEURL())

	// Always use table as string (StarRocks expects string, not array element)
	tableValue := tables[0]

	req, err := http.NewRequest("POST", urlStr, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Expect", "100-continue")
	req.Header.Set("label", label)
	req.Header.Set("db", c.database)
	req.Header.Set("table", tableValue)
	req.SetBasicAuth(c.username, c.password)

	if c.logger != nil {
		c.logger.Printf("[DEBUG] BeginTransaction Headers: %+v", req.Header)
	}

	resp, err := c.doRequest(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	// Handle 307 Temporary Redirect from FE to BE
	if resp.StatusCode == http.StatusTemporaryRedirect {
		location := resp.Header.Get("Location")
		if location == "" {
			resp.Body.Close()
			return nil, fmt.Errorf("received 307 redirect without Location header")
		}

		resp.Body.Close()

		redirectReq, err := http.NewRequest("POST", location, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create redirect request: %w", err)
		}

		redirectReq.Header.Set("Content-Type", "application/json")
		redirectReq.Header.Set("Expect", "100-continue")
		redirectReq.Header.Set("label", label)
		redirectReq.Header.Set("db", c.database)
		redirectReq.Header.Set("table", tableValue)
		redirectReq.SetBasicAuth(c.username, c.password)

		resp, err = c.httpClient.Do(redirectReq)
		if err != nil {
			return nil, fmt.Errorf("failed to send redirect request: %w", err)
		}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var txnResp TransactionBeginResponse
	if err := json.Unmarshal(body, &txnResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return &txnResp, fmt.Errorf("begin transaction failed with status %d: %s", resp.StatusCode, txnResp.Message)
	}

	return &txnResp, nil
}

// PrepareTransaction pre-commits the current transaction
// Note: This should be called after loading data with LoadTransaction
func (c *Client) PrepareTransaction(label string) (*TransactionPrepareResponse, error) {
	urlStr := fmt.Sprintf("%s/api/transaction/prepare", c.getCurrentFEURL())

	req, err := http.NewRequest("POST", urlStr, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Expect", "100-continue")
	req.Header.Set("label", label)
	req.Header.Set("db", c.database)
	req.SetBasicAuth(c.username, c.password)

	resp, err := c.doRequest(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	// Handle 307 Temporary Redirect from FE to BE
	if resp.StatusCode == http.StatusTemporaryRedirect {
		location := resp.Header.Get("Location")
		if location == "" {
			resp.Body.Close()
			return nil, fmt.Errorf("received 307 redirect without Location header")
		}

		resp.Body.Close()

		redirectReq, err := http.NewRequest("POST", location, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create redirect request: %w", err)
		}

		redirectReq.Header.Set("Content-Type", "application/json")
		redirectReq.Header.Set("Expect", "100-continue")
		redirectReq.Header.Set("label", label)
		redirectReq.Header.Set("db", c.database)
		redirectReq.SetBasicAuth(c.username, c.password)

		resp, err = c.httpClient.Do(redirectReq)
		if err != nil {
			return nil, fmt.Errorf("failed to send redirect request: %w", err)
		}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var prepResp TransactionPrepareResponse
	if err := json.Unmarshal(body, &prepResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return &prepResp, fmt.Errorf("prepare transaction failed with status %d: %s", resp.StatusCode, prepResp.Message)
	}

	if prepResp.Status != "OK" {
		return &prepResp, fmt.Errorf("prepare transaction failed: %s", prepResp.Message)
	}

	return &prepResp, nil
}

// LoadTransaction loads data into a transaction with specified label
func (c *Client) LoadTransaction(label, table string, data io.Reader, opts LoadOptions) (*LoadResponse, error) {
	urlStr := fmt.Sprintf("%s/api/transaction/load", c.getCurrentFEURL())

	// Compress data into buffer to support retry on redirect
	var dataBuf bytes.Buffer
	var err error
	if opts.Compression != CompressionNone {
		compressedReader, err := c.compressData(data, opts.Compression)
		if err != nil {
			return nil, fmt.Errorf("failed to compress data: %w", err)
		}
		if _, err := io.Copy(&dataBuf, compressedReader); err != nil {
			return nil, fmt.Errorf("failed to buffer compressed data: %w", err)
		}
	} else {
		if _, err := io.Copy(&dataBuf, data); err != nil {
			return nil, fmt.Errorf("failed to buffer data: %w", err)
		}
	}

	// Use bytes.NewReader to support seeking for retries
	reader := bytes.NewReader(dataBuf.Bytes())

	if c.logger != nil {
		c.logger.Printf("[DEBUG] LoadTransaction: Data size = %d bytes", dataBuf.Len())
	}

	headers := make(map[string]string)
	for k, v := range c.defaultHeader {
		headers[k] = v
	}
	headers["Expect"] = "100-continue"
	headers["strip_outer_array"] = fmt.Sprintf("%t", opts.StripOuterArray)
	headers["label"] = label
	headers["db"] = c.database
	headers["table"] = table

	if opts.TimeoutStr != "" {
		headers["timeout"] = opts.TimeoutStr
	}

	if opts.Compression != CompressionNone {
		headers["compression"] = string(opts.Compression)
	}

	if opts.Format != "" {
		headers["format"] = string(opts.Format)
	}
	if opts.Columns != "" {
		headers["columns"] = opts.Columns
	}
	if opts.ColumnSeparator != "" {
		headers["column_separator"] = opts.ColumnSeparator
	}
	if opts.RowDelimiter != "" {
		headers["row_delimiter"] = opts.RowDelimiter
	}
	if opts.Where != "" {
		headers["where"] = opts.Where
	}
	if opts.MaxFilterRatio != "" {
		headers["max_filter_ratio"] = opts.MaxFilterRatio
	}
	if len(opts.Partitions) > 0 {
		headers["partitions"] = strings.Join(opts.Partitions, ",")
	}
	if len(opts.TemporaryPartitions) > 0 {
		headers["temporary_partitions"] = strings.Join(opts.TemporaryPartitions, ",")
	}
	if opts.LogRejectedRecordNum != 0 {
		headers["log_rejected_record_num"] = fmt.Sprintf("%d", opts.LogRejectedRecordNum)
	}
	if opts.Timezone != "" {
		headers["timezone"] = opts.Timezone
	}
	if opts.LoadMemLimit > 0 {
		headers["load_mem_limit"] = fmt.Sprintf("%d", opts.LoadMemLimit)
	}
	if opts.StrictMode {
		headers["strict_mode"] = "true"
	}

	req, err := http.NewRequest("PUT", urlStr, reader)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.SetBasicAuth(c.username, c.password)
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := c.doRequest(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	// Handle 307 Temporary Redirect from FE to BE
	if resp.StatusCode == http.StatusTemporaryRedirect {
		location := resp.Header.Get("Location")
		if location == "" {
			resp.Body.Close()
			return nil, fmt.Errorf("received 307 redirect without Location header")
		}

		resp.Body.Close()

		// Reset reader position for retry
		reader.Seek(0, io.SeekStart)

		redirectReq, err := http.NewRequest("PUT", location, reader)
		if err != nil {
			return nil, fmt.Errorf("failed to create redirect request: %w", err)
		}

		redirectReq.SetBasicAuth(c.username, c.password)
		for k, v := range headers {
			redirectReq.Header.Set(k, v)
		}

		resp, err = c.httpClient.Do(redirectReq)
		if err != nil {
			return nil, fmt.Errorf("failed to send redirect request: %w", err)
		}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if c.logger != nil {
		c.logger.Printf("[DEBUG] LoadTransaction: Response body = %s", string(body))
	}

	var loadResp LoadResponse
	if err := json.Unmarshal(body, &loadResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return &loadResp, fmt.Errorf("transaction load failed with status %d: %s", resp.StatusCode, loadResp.Message)
	}

	if loadResp.Status != "OK" {
		return &loadResp, fmt.Errorf("transaction load failed: %s", loadResp.Message)
	}

	return &loadResp, nil
}

// CommitTransaction commits the transaction with the specified label
func (c *Client) CommitTransaction(label string) (*TransactionCommitResponse, error) {
	urlStr := fmt.Sprintf("%s/api/transaction/commit", c.getCurrentFEURL())

	req, err := http.NewRequest("POST", urlStr, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Expect", "100-continue")
	req.Header.Set("label", label)
	req.Header.Set("db", c.database)
	req.SetBasicAuth(c.username, c.password)

	resp, err := c.doRequest(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	// Handle 307 Temporary Redirect from FE to BE
	if resp.StatusCode == http.StatusTemporaryRedirect {
		location := resp.Header.Get("Location")
		if location == "" {
			resp.Body.Close()
			return nil, fmt.Errorf("received 307 redirect without Location header")
		}

		resp.Body.Close()

		redirectReq, err := http.NewRequest("POST", location, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create redirect request: %w", err)
		}

		redirectReq.Header.Set("Content-Type", "application/json")
		redirectReq.Header.Set("Expect", "100-continue")
		redirectReq.Header.Set("label", label)
		redirectReq.Header.Set("db", c.database)
		redirectReq.SetBasicAuth(c.username, c.password)

		resp, err = c.httpClient.Do(redirectReq)
		if err != nil {
			return nil, fmt.Errorf("failed to send redirect request: %w", err)
		}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if c.logger != nil {
		c.logger.Printf("[DEBUG] CommitTransaction: Response body = %s", string(body))
	}

	var commitResp TransactionCommitResponse
	if err := json.Unmarshal(body, &commitResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return &commitResp, fmt.Errorf("commit transaction failed with status %d: %s", resp.StatusCode, commitResp.Message)
	}

	return &commitResp, nil
}

// RollbackTransaction rolls back the transaction with the specified label
func (c *Client) RollbackTransaction(label string) (*TransactionRollbackResponse, error) {
	urlStr := fmt.Sprintf("%s/api/transaction/rollback", c.getCurrentFEURL())

	req, err := http.NewRequest("POST", urlStr, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Expect", "100-continue")
	req.Header.Set("label", label)
	req.Header.Set("db", c.database)
	req.SetBasicAuth(c.username, c.password)

	resp, err := c.doRequest(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	// Handle 307 Temporary Redirect from FE to BE
	if resp.StatusCode == http.StatusTemporaryRedirect {
		location := resp.Header.Get("Location")
		if location == "" {
			resp.Body.Close()
			return nil, fmt.Errorf("received 307 redirect without Location header")
		}

		resp.Body.Close()

		redirectReq, err := http.NewRequest("POST", location, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create redirect request: %w", err)
		}

		redirectReq.Header.Set("Content-Type", "application/json")
		redirectReq.Header.Set("Expect", "100-continue")
		redirectReq.Header.Set("label", label)
		redirectReq.Header.Set("db", c.database)
		redirectReq.SetBasicAuth(c.username, c.password)

		resp, err = c.httpClient.Do(redirectReq)
		if err != nil {
			return nil, fmt.Errorf("failed to send redirect request: %w", err)
		}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var rollbackResp TransactionRollbackResponse
	if err := json.Unmarshal(body, &rollbackResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return &rollbackResp, fmt.Errorf("rollback transaction failed with status %d: %s", resp.StatusCode, rollbackResp.Message)
	}

	return &rollbackResp, nil
}
