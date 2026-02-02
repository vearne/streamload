# API Documentation

## Package streamload

`streamload` is a Go client library for StarRocks Stream Load protocol.

## Types

### Client

```go
type Client struct {
    httpClient    *http.Client
    baseURL       string
    database      string
    username      string
    password      string
    defaultHeader map[string]string
}
```

#### Functions

**NewClient**

```go
func NewClient(host, port, database, username, password string) *Client
```

Creates a new StarRocks stream load client.

**Parameters:**
- `host`: StarRocks FE host address
- `port`: StarRocks FE port
- `database`: Target database name
- `username`: Username for authentication
- `password`: Password for authentication

**Returns:**
- `*Client`: New client instance with default 30-minute timeout

**SetHTTPClient**

```go
func (c *Client) SetHTTPClient(client *http.Client)
```

Sets a custom HTTP client for the request.

**Parameters:**
- `client`: Custom HTTP client

**SetDefaultHeader**

```go
func (c *Client) SetDefaultHeader(key, value string)
```

Sets a default header for all requests.

**Parameters:**
- `key`: Header name
- `value`: Header value

**Load**

```go
func (c *Client) Load(table string, data io.Reader, opts LoadOptions) (*LoadResponse, error)
```

Loads data into StarRocks via stream load.

**Parameters:**
- `table`: Target table name
- `data`: Data reader containing the data to load
- `opts`: Load options

**Returns:**
- `*LoadResponse`: Response containing load statistics
- `error`: Error if load failed

### LoadOptions
 
```go
type LoadOptions struct {
    Format             DataFormat
    Compression        CompressionType
    Columns            string
    ColumnSeparator     string
    RowDelimiter        string
    Where              string
    MaxFilterRatio     string
    Timeout            time.Duration
    TimeoutStr         string
    StrictMode         bool
    StripOuterArray    bool
    
    Label              string
    Partitions         []string
    TemporaryPartitions []string
    
    LogRejectedRecordNum int
    Timezone           string
    LoadMemLimit       int64
}
```

**Fields:**
- `Format`: Data format (CSV or JSON)
- `Compression`: Compression algorithm (GZIP, LZ4, ZSTD, BZIP2)
- `Columns`: Column mapping expression
- `ColumnSeparator`: Column separator for CSV data
- `RowDelimiter`: Row delimiter for CSV data
- `Where`: Filter condition
- `MaxFilterRatio`: Maximum ratio of filtered rows (e.g., "0.1" for 10%)
- `Timeout`: Request timeout duration
- `TimeoutStr`: Timeout as string (e.g., "300")
- `StrictMode`: Enable strict mode for data validation
- `StripOuterArray`: Strip outer array for JSON format
- `Label`: Label for the load job to prevent duplicate loads
- `Partitions`: Target partitions to load data into
- `TemporaryPartitions`: Temporary partitions to load data into
- `LogRejectedRecordNum`: Maximum number of rejected rows to log (v3.1+)
- `Timezone`: Timezone for the load job (default: Asia/Shanghai)
- `LoadMemLimit`: Maximum memory limit in bytes (default: 2GB)

### LoadResponse

```go
type LoadResponse struct {
    Status                    string
    Message                   string
    NumberTotalRows           int
    NumberLoadedRows          int
    NumberFilteredRows        int
    NumberUnselectedRows      int
    LoadBytes                 int
    LoadTimeMs                int
    BeginTxnTimeMs            int
    StreamLoadPlanTimeMs      int
    ReadDataTimeMs            int
    WriteDataTimeMs           int
    CommittedAndPublishTimeMs int
    ErrorURL                  string
}
```

**Fields:**
- `Status`: Load status ("Success", "Fail", etc.)
- `Message`: Status message
- `NumberTotalRows`: Total rows processed
- `NumberLoadedRows`: Rows successfully loaded
- `NumberFilteredRows`: Rows filtered out
- `NumberUnselectedRows`: Rows unselected
- `LoadBytes`: Bytes loaded
- `LoadTimeMs`: Total load time in milliseconds
- `BeginTxnTimeMs`: Transaction begin time
- `StreamLoadPlanTimeMs`: Planning time
- `ReadDataTimeMs`: Data read time
- `WriteDataTimeMs`: Data write time
- `CommittedAndPublishTimeMs`: Commit and publish time
- `ErrorURL`: URL to error details (if any)

### DataFormat

```go
type DataFormat string

const (
    FormatCSV  DataFormat = "csv"
    FormatJSON DataFormat = "json"
)
```

### CompressionType

```go
type CompressionType string

const (
    CompressionNone   CompressionType = ""
    CompressionGZIP   CompressionType = "GZIP"
    CompressionLZ4    CompressionType = "LZ4_FRAME"
    CompressionZSTD   CompressionType = "ZSTD"
    CompressionBZIP2 CompressionType = "BZIP2"
)
```

All compression algorithms are fully implemented and supported.

## Error Handling

The `Load` function returns an error when:
- HTTP request fails
- Server returns non-200 status code
- Response status is not "Success"
- Data compression fails

All errors are wrapped with context using `fmt.Errorf` with `%w` for error unwrapping.

## Examples

See `example/main.go` for complete usage examples.

### Two-Phase Commit (2PC) Methods

These methods support two-phase commit for data consistency in external systems like Flink and Kafka.

#### BeginTransaction

Begins a new transaction for data loading.

#### PrepareTransaction

Pre-commits current transaction with data.

#### CommitTransaction

Commits the transaction, making data visible.

#### RollbackTransaction

Rolls back the transaction, discarding all changes.

