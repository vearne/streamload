package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/google/uuid"
	"github.com/vearne/streamload"
)

func main() {
	client := streamload.NewClient(
		"10.128.246.182",
		"8030",
		"test",
		"root",
		"8A09-62378A8DF015",
	)

	client.SetLogger(log.New(os.Stdout, "", 0))

	var resp *streamload.LoadResponse
	var err error

	csvData := `1,Alice,25
2,Bob,30
3,Charlie,35
4,David,28
5,Eve,32`

	fmt.Println("=== Example 1: Load CSV data ===")
	resp, err = client.Load("users", strings.NewReader(csvData), streamload.LoadOptions{
		Format:          streamload.FormatCSV,
		Columns:         "id,name,age",
		ColumnSeparator: ",",
	})
	if err != nil {
		log.Printf("CSV load failed: %v\n", err)
	} else {
		fmt.Printf("Load success! Loaded %d rows, %d bytes\n", resp.NumberLoadedRows, resp.LoadBytes)
	}

	jsonData := `[{"id": 6, "name": "Frank", "age": 40}, {"id": 7, "name": "Grace", "age": 35}]`
	fmt.Println("\n=== Example 2: Load JSON data ===")
	resp, err = client.Load("users", strings.NewReader(jsonData), streamload.LoadOptions{
		Format:          streamload.FormatJSON,
		StripOuterArray: true,
	})
	if err != nil {
		log.Printf("JSON load failed: %v\n", err)
	} else {
		fmt.Printf("Load success! Loaded %d rows, %d bytes\n", resp.NumberLoadedRows, resp.LoadBytes)
	}

	// CSV format does not support compression.
	fmt.Println("\n=== Example 3: Load CSV data ===")
	resp, err = client.Load("users", strings.NewReader(csvData), streamload.LoadOptions{
		Format:          streamload.FormatCSV,
		Columns:         "id,name,age",
		ColumnSeparator: ",",
	})
	if err != nil {
		log.Printf("CSV load failed: %v\n", err)
	} else {
		fmt.Printf("Load success! Loaded %d rows, %d bytes\n", resp.NumberLoadedRows, resp.LoadBytes)
	}

	// Example 4: Load JSON with compression (StarRocks only supports compression for JSON)
	fmt.Println("\n=== Example 4: Load JSON with GZIP compression ===")
	resp, err = client.Load("users", strings.NewReader(jsonData), streamload.LoadOptions{
		Format:          streamload.FormatJSON,
		StripOuterArray: true,
		Compression:     streamload.CompressionGZIP,
	})
	if err != nil {
		log.Printf("JSON compressed load failed: %v\n", err)
	} else {
		fmt.Printf("Load success! Loaded %d rows, %d bytes\n", resp.NumberLoadedRows, resp.LoadBytes)
	}

	fmt.Println("\n=== Example 5: Load with Label ===")
	resp, err = client.Load("users", strings.NewReader(csvData), streamload.LoadOptions{
		Format:          streamload.FormatCSV,
		Columns:         "id,name,age",
		ColumnSeparator: ",",
		Label:           uuid.Must(uuid.NewUUID()).String(),
	})
	if err != nil {
		log.Printf("Load with label failed: %v\n", err)
	} else {
		fmt.Printf("Load success! Loaded %d rows\n", resp.NumberLoadedRows)
	}

	fmt.Println("\n=== Example 6: Load with timezone ===")
	resp, err = client.Load("users", strings.NewReader(csvData), streamload.LoadOptions{
		Format:          streamload.FormatCSV,
		Columns:         "id,name,age",
		ColumnSeparator: ",",
		Timezone:        "America/New_York",
	})
	if err != nil {
		log.Printf("Load with timezone failed: %v\n", err)
	} else {
		fmt.Printf("Load success! Loaded %d rows with timezone: %s\n", resp.NumberLoadedRows, resp.Timezone)
	}

	fmt.Println("\n=== Example 7: Two-Phase Commit (2PC) ===")

	label := "streamload-2pc-commit-" + uuid.Must(uuid.NewUUID()).String()
	txnResp, err := client.BeginTransaction(label, []string{"users"})
	if err != nil {
		log.Printf("Begin transaction failed: %v\n", err)
		return
	}
	fmt.Printf("Transaction started: %d\n", txnResp.TxnId)

	// Load data into the transaction
	loadResp, err := client.LoadTransaction(label, "users", strings.NewReader(csvData), streamload.LoadOptions{
		Format:          streamload.FormatCSV,
		Columns:         "id,name,age",
		ColumnSeparator: ",",
	})
	if err != nil {
		log.Printf("Load transaction data failed: %v\n", err)
		return
	}
	fmt.Printf("Data loaded: %d rows, Status: %s\n", loadResp.NumberLoadedRows, loadResp.Status)

	// Prepare transaction (pre-commit)
	prepareResp, err := client.PrepareTransaction(label)
	if err != nil {
		log.Printf("Prepare transaction failed: %v\n", err)
		return
	}
	fmt.Printf("Transaction prepared: TxnId=%d, Status: %s, Loaded %d rows\n", 
		prepareResp.TxnId, prepareResp.Status, prepareResp.NumberLoadedRows)

	// Commit transaction
	commitResp, err := client.CommitTransaction(label)
	if err != nil {
		log.Printf("Commit transaction failed: %v\n", err)
		return
	}
	fmt.Printf("Transaction committed: TxnId=%d, Status: %s\n", 
		commitResp.TxnId, commitResp.Status)

	fmt.Println("\n=== Example 8: Two-Phase Commit with Rollback ===")

	label = "streamload-2pc-rollback-" + uuid.Must(uuid.NewUUID()).String()
	txnResp, err = client.BeginTransaction(label, []string{"users"})
	if err != nil {
		log.Printf("Begin transaction failed: %v\n", err)
		return
	}
	fmt.Printf("Transaction started: %d\n", txnResp.TxnId)

	_, err = client.RollbackTransaction(label)
	if err != nil {
		log.Printf("Rollback transaction failed: %v\n", err)
		return
	}
	fmt.Printf("Transaction rolled back: %d\n", txnResp.TxnId)
}
