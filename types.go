package streamload

import "time"

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
	TxnId                  int64  `json:"TxnId"`
	Status                 string `json:"Status"`
	Message                string `json:"Message"`
	NumberTotalRows        int    `json:"NumberTotalRows"`
	NumberLoadedRows       int    `json:"NumberLoadedRows"`
	NumberFilteredRows     int    `json:"NumberFilteredRows"`
	NumberUnselectedRows   int    `json:"NumberUnselectedRows"`
	LoadBytes              int    `json:"LoadBytes"`
	LoadTimeMs             int    `json:"LoadTimeMs"`
	StreamLoadPutTimeMs    int    `json:"StreamLoadPutTimeMs"`
	ReceivedDataTimeMs     int    `json:"ReceivedDataTimeMs"`
	WriteDataTimeMs        int    `json:"WriteDataTimeMs"`
	CommitAndPublishTimeMs int    `json:"CommitAndPublishTimeMs"`
}

// TransactionCommitResponse represents the response for committing a transaction
type TransactionCommitResponse struct {
	TxnId                  int64  `json:"TxnId"`
	Status                 string `json:"Status"`
	Message                string `json:"Message"`
	NumberTotalRows        int    `json:"NumberTotalRows"`
	NumberLoadedRows       int    `json:"NumberLoadedRows"`
	NumberFilteredRows     int    `json:"NumberFilteredRows"`
	NumberUnselectedRows   int    `json:"NumberUnselectedRows"`
	LoadBytes              int    `json:"LoadBytes"`
	LoadTimeMs             int    `json:"LoadTimeMs"`
	StreamLoadPutTimeMs    int    `json:"StreamLoadPutTimeMs"`
	ReceivedDataTimeMs     int    `json:"ReceivedDataTimeMs"`
	WriteDataTimeMs        int    `json:"WriteDataTimeMs"`
	CommitAndPublishTimeMs int    `json:"CommitAndPublishTimeMs"`
}

// TransactionRollbackResponse represents the response for rolling back a transaction
type TransactionRollbackResponse struct {
	TxnId   int64  `json:"TxnId"`
	Status  string `json:"Status"`
	Message string `json:"Message"`
}
