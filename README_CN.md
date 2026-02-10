# streamload

一个用于实现 StarRocks Stream Load 协议的 Go 语言库。

## 特性

- 支持 CSV 和 JSON 数据格式
- **直接加载 Go 结构体**（无需手动序列化）
- 多种压缩算法（GZIP、LZ4、ZSTD、BZIP2）
- 自定义 HTTP 客户端配置
- 灵活的加载选项（列、过滤器、超时）
- 标签支持，防止重复加载
- 分区控制（目标分区、临时分区）
- 支持两阶段提交（2PC）以集成外部系统
- 错误处理，提供详细的响应信息

## 安装

```bash
go get github.com/vearne/streamload
```

## 使用方法

### 基本示例

```go
package main

import (
    "strings"
    "github.com/vearne/streamload"
)

func main() {
    // 创建客户端
    client := streamload.NewClient(
        "localhost",  // StarRocks FE 主机
        "8030",       // StarRocks FE 端口
        "test_db",    // 数据库名称
        "root",       // 用户名
        "password",   // 密码
    )

    // 加载 CSV 数据
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
    
    fmt.Printf("已加载 %d 行\n", resp.NumberLoadedRows)
}
```

### JSON 格式

```go
jsonData := `[{"id": 1, "name": "Alice", "age": 25}]`

resp, err := client.Load("users", strings.NewReader(jsonData), streamload.LoadOptions{
    Format: streamload.FormatJSON,
})
```

### 压缩

```go
// GZIP 压缩
resp, err := client.Load("users", data, streamload.LoadOptions{
    Format:      streamload.FormatCSV,
    Compression: streamload.CompressionGZIP,
})

// LZ4 压缩
resp, err := client.Load("users", data, streamload.LoadOptions{
    Format:      streamload.FormatCSV,
    Compression: streamload.CompressionLZ4,
})

// ZSTD 压缩
resp, err := client.Load("users", data, streamload.LoadOptions{
    Format:      streamload.FormatCSV,
    Compression: streamload.CompressionZSTD,
})

// BZIP2 压缩
resp, err := client.Load("users", data, streamload.LoadOptions{
    Format:      streamload.FormatCSV,
    Compression: streamload.CompressionBZIP2,
})
```

### 使用过滤器

```go
resp, err := client.Load("users", data, streamload.LoadOptions{
    Format:          streamload.FormatCSV,
    Where:           "age > 20",
    MaxFilterRatio:  "0.1",
})
```

### 直接加载结构体

#### CSV 格式

无需手动序列化，直接加载 Go 结构体为 CSV 格式：

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

#### JSON 格式（默认启用 ZSTD 压缩）

直接加载 Go 结构体为 JSON 格式，自动启用 ZSTD 压缩：

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
    // 默认已启用 ZSTD 压缩
})
```

### 自定义 HTTP 客户端

```go
import "net/http"
import "time"

customClient := &http.Client{
    Timeout: 5 * time.Minute,
}

client := streamload.NewClient("localhost", "8030", "test_db", "root", "password")
client.SetHTTPClient(customClient)
```

## 加载选项

| 选项 | 类型 | 描述 |
|--------|------|-------------|
| Format | DataFormat | 数据格式（CSV 或 JSON） |
| Compression | CompressionType | 压缩算法（GZIP、LZ4、ZSTD、BZIP2） |
| Columns | string | 列映射 |
| ColumnSeparator | string | CSV 的列分隔符 |
| RowDelimiter | string | 行分隔符 |
| Where | string | 过滤条件 |
| MaxFilterRatio | string | 最大过滤率 |
| Timeout | time.Duration | 请求超时时间 |
| StrictMode | bool | 启用严格模式 |
| StripOuterArray | bool | 对于 JSON 剥离外层数组 |

## 响应

`LoadResponse` 包含有关加载操作的详细信息：

```go
type LoadResponse struct {
    Status              string  // 状态：Success、Fail 等
    Message             string  // 状态消息
    NumberTotalRows     int     // 处理的总行数
    NumberLoadedRows    int     // 成功加载的行数
    NumberFilteredRows  int     // 被过滤掉的行数
    LoadBytes           int     // 加载的字节数
    LoadTimeMs          int     // 加载时间（毫秒）
    ErrorURL            string  // 错误详情的 URL（如果有）
}
```

## 许可证

请参阅 LICENSE 文件。
