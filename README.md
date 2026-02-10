# streamload

A Go library for implementing StarRocks Stream Load protocol.

## Features

- Support for CSV and JSON data formats
- **Direct struct loading** (no manual serialization needed)
- Multiple compression algorithms (GZIP, LZ4, ZSTD, BZIP2)
- Custom HTTP client configuration
- Flexible load options (columns, filters, timeouts)
- Label support to prevent duplicate loads
- Partition control (target partitions, temporary partitions)
- Two-phase commit (2PC) support for external systems
- Error handling with detailed response information

## Installation

```bash
go get github.com/vearne/streamload
```

## Usage

### Basic Example

```go
package main

import (
    "strings"
    "github.com/vearne/streamload"
)

func main() {
    // Create a client
    client := streamload.NewClient(
        "localhost",  // StarRocks FE host
        "8030",       // StarRocks FE port
        "test_db",    // Database name
        "root",       // Username
        "password",   // Password
    )

    // Load CSV data
    csvData := `1,Alice,25
2,Bob,30`
    
    resp, err := client.Load("users", strings.NewReader(csvData), streamload.LoadOptions{
        Format:          streamload.FormatCSV,
        Columns:         "id,name,age",
        ColumnSeparator: ",",
    })
    if err != nil {
        panic(err)
    }
    
    fmt.Printf("Loaded %d rows\n", resp.NumberLoadedRows)
}
```

### JSON Format

```go
jsonData := `[{"id": 1, "name": "Alice", "age": 25}]`

resp, err := client.Load("users", strings.NewReader(jsonData), streamload.LoadOptions{
    Format: streamload.FormatJSON,
})
```

### Compression

```go
// GZIP compression
resp, err := client.Load("users", data, streamload.LoadOptions{
    Format:      streamload.FormatCSV,
    Compression: streamload.CompressionGZIP,
})

// LZ4 compression
resp, err := client.Load("users", data, streamload.LoadOptions{
    Format:      streamload.FormatCSV,
    Compression: streamload.CompressionLZ4,
})

// ZSTD compression
resp, err := client.Load("users", data, streamload.LoadOptions{
    Format:      streamload.FormatCSV,
    Compression: streamload.CompressionZSTD,
})

// BZIP2 compression
resp, err := client.Load("users", data, streamload.LoadOptions{
    Format:      streamload.FormatCSV,
    Compression: streamload.CompressionBZIP2,
})
```

### With Filters

```go
resp, err := client.Load("users", data, streamload.LoadOptions{
    Format:          streamload.FormatCSV,
    Where:           "age > 20",
    MaxFilterRatio:  "0.1",
})
```

### Load Structs Directly

#### CSV Format

Load Go structs directly as CSV without manual serialization:

```go
type User struct {
    Id   int    `csv:"id"`
    Name string `csv:"name"`
    Age  int    `csv:"age"`
}

users := []User{
    {Id: 1, Name: "Alice", Age: 25},
    {Id: 2, Name: "Bob", Age: 30},
}

resp, err := client.LoadStructsCSV("users", users, streamload.LoadOptions{
    Label: "unique-label",
})
```

#### JSON Format (with default ZSTD compression)

Load Go structs directly as JSON with automatic ZSTD compression:

```go
type User struct {
    Id   int    `json:"id"`
    Name string `json:"name"`
    Age  int    `json:"age"`
}

users := []User{
    {Id: 1, Name: "Alice", Age: 25},
    {Id: 2, Name: "Bob", Age: 30},
}

resp, err := client.LoadStructsJSON("users", users, streamload.LoadOptions{
    Label: "unique-label",
    // ZSTD compression is enabled by default
})
```

### Custom HTTP Client

```go
import "net/http"
import "time"

customClient := &http.Client{
    Timeout: 5 * time.Minute,
}

client := streamload.NewClient("localhost", "8030", "test_db", "root", "password")
client.SetHTTPClient(customClient)
```

## Load Options

| Option | Type | Description |
|--------|------|-------------|
| Format | DataFormat | Data format (CSV or JSON) |
| Compression | CompressionType | Compression algorithm (GZIP, LZ4, ZSTD, BZIP2) |
| Columns | string | Column mapping |
| ColumnSeparator | string | Column separator for CSV |
| RowDelimiter | string | Row delimiter |
| Where | string | Filter condition |
| MaxFilterRatio | string | Maximum filter ratio |
| Timeout | time.Duration | Request timeout |
| StrictMode | bool | Enable strict mode |
| StripOuterArray | bool | Strip outer array for JSON |

## Response

The `LoadResponse` contains detailed information about the load operation:

```go
type LoadResponse struct {
    Status              string  // Success, Fail, etc.
    Message             string  // Status message
    NumberTotalRows     int     // Total rows processed
    NumberLoadedRows    int     // Rows successfully loaded
    NumberFilteredRows  int     // Rows filtered out
    LoadBytes           int     // Bytes loaded
    LoadTimeMs          int     // Load time in milliseconds
    ErrorURL            string  // URL to error details (if any)
}
```

## License

See LICENSE file.
