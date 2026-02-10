package streamload

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/dsnet/compress/bzip2"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// FEEndpoint represents a StarRocks FE endpoint
type FEEndpoint struct {
	Host string
	Port string
}

// CompressionType represents the compression algorithm
type CompressionType string

const (
	CompressionNone  CompressionType = ""
	CompressionGZIP  CompressionType = "GZIP"
	CompressionLZ4   CompressionType = "LZ4_FRAME"
	CompressionZSTD  CompressionType = "ZSTD"
	CompressionBZIP2 CompressionType = "BZIP2"
)

// DataFormat represents the data format
type DataFormat string

const (
	FormatCSV  DataFormat = "csv"
	FormatJSON DataFormat = "json"
)

// LoadOptions represents options for stream load
type LoadOptions struct {
	Format          DataFormat
	Compression     CompressionType
	Columns         string
	ColumnSeparator string
	RowDelimiter    string
	Where           string
	MaxFilterRatio  string
	Timeout         time.Duration
	TimeoutStr      string
	StrictMode      bool
	StripOuterArray bool

	Label               string
	Table               string
	Partitions          []string
	TemporaryPartitions []string

	LogRejectedRecordNum int
	Timezone             string
	LoadMemLimit         int64
}

// Client represents a StarRocks stream load client
type Client struct {
	httpClient     *http.Client
	fes            []FEEndpoint
	currentFEIndex int
	database       string
	username       string
	password       string
	defaultHeader  map[string]string
	logger         *log.Logger
	mu             sync.RWMutex
}

// NewClient creates a new StarRocks stream load client with a single FE endpoint
func NewClient(host, port, database, username, password string) *Client {
	return NewClientWithFEs([]FEEndpoint{{Host: host, Port: port}}, database, username, password)
}

// NewClientWithFEs creates a new StarRocks stream load client with multiple FE endpoints
func NewClientWithFEs(fes []FEEndpoint, database, username, password string) *Client {
	if len(fes) == 0 {
		panic("at least one FE endpoint is required")
	}
	return &Client{
		httpClient: &http.Client{
			Timeout: 30 * time.Minute,
			// Disable automatic redirect following to handle 307 redirects manually
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		fes:            fes,
		currentFEIndex: 0,
		database:       database,
		username:       username,
		password:       password,
		defaultHeader:  make(map[string]string),
		logger:         nil,
	}
}

// getCurrentFEURL returns the current FE URL using round-robin selection
func (c *Client) getCurrentFEURL() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	fe := c.fes[c.currentFEIndex]
	return fmt.Sprintf("http://%s:%s", fe.Host, fe.Port)
}

// nextFE rotates to the next FE endpoint
func (c *Client) nextFE() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.currentFEIndex = (c.currentFEIndex + 1) % len(c.fes)
}

// doRequest executes HTTP request with failover support
func (c *Client) doRequest(req *http.Request) (*http.Response, error) {
	maxRetries := len(c.fes)
	var lastErr error

	for i := 0; i < maxRetries; i++ {
		feURL := c.getCurrentFEURL()
		parsedFEURL, err := url.Parse(feURL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse FE URL: %w", err)
		}

		retryReq := req.Clone(req.Context())
		retryReq.URL.Scheme = parsedFEURL.Scheme
		retryReq.URL.Host = parsedFEURL.Host
		retryReq.URL.Path = parsedFEURL.Path + req.URL.Path

		targetURL := retryReq.URL.String()
		if c.logger != nil {
			c.logger.Printf("[DEBUG] Attempt %d/%d: Connecting to %s", i+1, maxRetries, targetURL)
		}

		resp, err := c.httpClient.Do(retryReq)
		if err == nil {
			if c.logger != nil {
				c.logger.Printf("[DEBUG] Success: Connected to %s", targetURL)
			}
			return resp, nil
		}

		if c.logger != nil {
			c.logger.Printf("[DEBUG] Failed: %s - Error: %v", targetURL, err)
		}
		lastErr = err
		c.nextFE()
	}

	return nil, lastErr
}

// SetHTTPClient sets a custom HTTP client
func (c *Client) SetHTTPClient(client *http.Client) {
	c.httpClient = client
}

// SetDefaultHeader sets a default header for all requests
func (c *Client) SetDefaultHeader(key, value string) {
	c.defaultHeader[key] = value
}

// SetLogger sets a custom logger for debugging
func (c *Client) SetLogger(logger *log.Logger) {
	c.logger = logger
}

// LoadResponse represents the response from StarRocks
type LoadResponse struct {
	Status                    string `json:"Status"`
	Message                   string `json:"Message"`
	NumberTotalRows           int    `json:"NumberTotalRows"`
	NumberLoadedRows          int    `json:"NumberLoadedRows"`
	NumberFilteredRows        int    `json:"NumberFilteredRows"`
	NumberUnselectedRows      int    `json:"NumberUnselectedRows"`
	LoadBytes                 int    `json:"LoadBytes"`
	LoadTimeMs                int    `json:"LoadTimeMs"`
	BeginTxnTimeMs            int    `json:"BeginTxnTimeMs"`
	StreamLoadPlanTimeMs      int    `json:"StreamLoadPlanTimeMs"`
	ReadDataTimeMs            int    `json:"ReadDataTimeMs"`
	WriteDataTimeMs           int    `json:"WriteDataTimeMs"`
	CommittedAndPublishTimeMs int    `json:"CommittedAndPublishTimeMs"`
	ErrorURL                  string `json:"ErrorURL"`
	Timezone                  string `json:"Timezone"`
}

// TransactionBeginResponse represents the response for beginning a transaction
type TransactionBeginResponse struct {
	TxnId   int64  `json:"TxnId"`
	Status  string `json:"Status"`
	Message string `json:"Message"`
}

// TransactionPrepareResponse represents the response for preparing a transaction
type TransactionPrepareResponse struct {
	TxnId                     int64  `json:"TxnId"`
	Status                    string `json:"Status"`
	Message                   string `json:"Message"`
	NumberTotalRows           int    `json:"NumberTotalRows"`
	NumberLoadedRows          int    `json:"NumberLoadedRows"`
	NumberFilteredRows        int    `json:"NumberFilteredRows"`
	NumberUnselectedRows      int    `json:"NumberUnselectedRows"`
	LoadBytes                 int    `json:"LoadBytes"`
	LoadTimeMs                int    `json:"LoadTimeMs"`
	StreamLoadPutTimeMs       int    `json:"StreamLoadPutTimeMs"`
	ReceivedDataTimeMs        int    `json:"ReceivedDataTimeMs"`
	WriteDataTimeMs           int    `json:"WriteDataTimeMs"`
	CommitAndPublishTimeMs    int    `json:"CommitAndPublishTimeMs"`
}

// TransactionCommitResponse represents the response for committing a transaction
type TransactionCommitResponse struct {
	TxnId                     int64  `json:"TxnId"`
	Status                    string `json:"Status"`
	Message                   string `json:"Message"`
	NumberTotalRows           int    `json:"NumberTotalRows"`
	NumberLoadedRows          int    `json:"NumberLoadedRows"`
	NumberFilteredRows        int    `json:"NumberFilteredRows"`
	NumberUnselectedRows      int    `json:"NumberUnselectedRows"`
	LoadBytes                 int    `json:"LoadBytes"`
	LoadTimeMs                int    `json:"LoadTimeMs"`
	StreamLoadPutTimeMs       int    `json:"StreamLoadPutTimeMs"`
	ReceivedDataTimeMs        int    `json:"ReceivedDataTimeMs"`
	WriteDataTimeMs           int    `json:"WriteDataTimeMs"`
	CommitAndPublishTimeMs    int    `json:"CommitAndPublishTimeMs"`
}

// TransactionRollbackResponse represents the response for rolling back a transaction
type TransactionRollbackResponse struct {
	TxnId   int64  `json:"TxnId"`
	Status  string `json:"Status"`
	Message string `json:"Message"`
}

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

// compressData compresses the data reader based on compression type
func (c *Client) compressData(data io.Reader, compression CompressionType) (io.Reader, error) {
	switch compression {
	case CompressionGZIP:
		return c.compressGZIP(data)
	case CompressionLZ4:
		return c.compressLZ4(data)
	case CompressionZSTD:
		return c.compressZSTD(data)
	case CompressionBZIP2:
		return c.compressBZIP2(data)
	default:
		return data, nil
	}
}

// compressGZIP compresses data using GZIP
func (c *Client) compressGZIP(data io.Reader) (io.Reader, error) {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	if _, err := io.Copy(writer, data); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return &buf, nil
}

// compressLZ4 compresses data using LZ4
func (c *Client) compressLZ4(data io.Reader) (io.Reader, error) {
	var buf bytes.Buffer
	writer := lz4.NewWriter(&buf)
	if _, err := io.Copy(writer, data); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return &buf, nil
}

// compressZSTD compresses data using ZSTD
func (c *Client) compressZSTD(data io.Reader) (io.Reader, error) {
	var buf bytes.Buffer
	encoder, err := zstd.NewWriter(&buf)
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(encoder, data); err != nil {
		return nil, err
	}
	if err := encoder.Close(); err != nil {
		return nil, err
	}
	return &buf, nil
}

// compressBZIP2 compresses data using BZIP2
func (c *Client) compressBZIP2(data io.Reader) (io.Reader, error) {
	var buf bytes.Buffer
	writer, err := bzip2.NewWriter(&buf, &bzip2.WriterConfig{})
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(writer, data); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return &buf, nil
}

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
