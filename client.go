package streamload

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

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
