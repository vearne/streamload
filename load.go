package streamload

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Load loads data into StarRocks via stream load
func (c *Client) Load(table string, data io.Reader, opts LoadOptions) (*LoadResponse, error) {
	urlStr := fmt.Sprintf("%s/api/%s/%s/_stream_load", c.getCurrentFEURL(), c.database, table)

	headers := make(map[string]string)
	for k, v := range c.defaultHeader {
		headers[k] = v
	}
	headers["Expect"] = "100-continue"
	headers["strip_outer_array"] = fmt.Sprintf("%t", opts.StripOuterArray)

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

	if opts.TimeoutStr != "" {
		headers["timeout"] = opts.TimeoutStr
	}

	if opts.StrictMode {
		headers["strict_mode"] = "true"
	}

	if opts.Compression != CompressionNone {
		headers["compression"] = string(opts.Compression)
	}

	// Compress data into buffer to support retry on redirect
	var dataBuf bytes.Buffer
	var reader io.Reader = data
	var err error
	if opts.Compression != CompressionNone {
		compressedReader, err := c.compressData(data, opts.Compression)
		if err != nil {
			return nil, fmt.Errorf("failed to compress data: %w", err)
		}
		if _, err := io.Copy(&dataBuf, compressedReader); err != nil {
			return nil, fmt.Errorf("failed to buffer compressed data: %w", err)
		}
		reader = &dataBuf
	} else {
		if _, err := io.Copy(&dataBuf, data); err != nil {
			return nil, fmt.Errorf("failed to buffer data: %w", err)
		}
		reader = &dataBuf
	}

	if opts.Label != "" {
		headers["label"] = opts.Label
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

		redirectReq, err := http.NewRequest("PUT", location, &dataBuf)
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

	var loadResp LoadResponse
	if err := json.Unmarshal(body, &loadResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return &loadResp, fmt.Errorf("stream load failed with status %d: %s", resp.StatusCode, loadResp.Message)
	}

	if loadResp.Status != "Success" {
		return &loadResp, fmt.Errorf("stream load failed: %s", loadResp.Message)
	}

	return &loadResp, nil
}
