package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/vearne/streamload"
)

func main() {
	client := streamload.NewClient(
		"localhost",
		"8030",
		"test_db",
		"root",
		"",
	)

	csvData := `1,Alice,25
2,Bob,30
3,Charlie,35
4,David,28
5,Eve,32`

	jsonData := `[{"id": 6, "name": "Frank", "age": 40}, {"id": 7, "name": "Grace", "age": 35}]`

	fmt.Println("=== Example 1: Load CSV data ===")
	resp, err := client.Load("users", strings.NewReader(csvData), streamload.LoadOptions{
		Format:          streamload.FormatCSV,
		Columns:         "id,name,age",
		ColumnSeparator: ",",
	})
	if err != nil {
		log.Printf("CSV load failed: %v\n", err)
	} else {
		fmt.Printf("Load success! Loaded %d rows, %d bytes\n", resp.NumberLoadedRows, resp.LoadBytes)
	}

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

	fmt.Println("\n=== Example 3: Load with GZIP compression ===")
	resp, err = client.Load("users", strings.NewReader(csvData), streamload.LoadOptions{
		Format:          streamload.FormatCSV,
		Columns:         "id,name,age",
		ColumnSeparator: ",",
		Compression:     streamload.CompressionGZIP,
	})
	if err != nil {
		log.Printf("Compressed load failed: %v\n", err)
	} else {
		fmt.Printf("Load success! Loaded %d rows, %d bytes\n", resp.NumberLoadedRows, resp.LoadBytes)
	}

	fmt.Println("\n=== Example 4: Load with Label ===")
	resp, err = client.Load("users", strings.NewReader(csvData), streamload.LoadOptions{
		Format:          streamload.FormatCSV,
		Columns:         "id,name,age",
		ColumnSeparator: ",",
		Label:           "my-load-label-20240202",
	})
	if err != nil {
		log.Printf("Load with label failed: %v\n", err)
	} else {
		fmt.Printf("Load success! Loaded %d rows\n", resp.NumberLoadedRows)
	}

	fmt.Println("\n=== Example 5: Load into specific partitions ===")
	resp, err = client.Load("users", strings.NewReader(csvData), streamload.LoadOptions{
		Format:          streamload.FormatCSV,
		Columns:         "id,name,age",
		ColumnSeparator: ",",
		Partitions:      []string{"p2024", "p2025"},
	})
	if err != nil {
		log.Printf("Load with partitions failed: %v\n", err)
	} else {
		fmt.Printf("Load success! Loaded %d rows into partitions: %s\n", resp.NumberLoadedRows, strings.Join([]string{"p2024", "p2025"}, ","))
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

	txnResp, err := client.BeginTransaction([]string{"users"})
	if err != nil {
		log.Printf("Begin transaction failed: %v\n", err)
		return
	}
	fmt.Printf("Transaction started: %s\n", txnResp.TxnId)

	prepResp, err := client.PrepareTransaction(txnResp.TxnId, strings.NewReader(csvData), streamload.LoadOptions{
		Format:          streamload.FormatCSV,
		Columns:         "id,name,age",
		ColumnSeparator: ",",
	})
	if err != nil {
		log.Printf("Prepare transaction failed: %v\n", err)
		return
	}
	fmt.Printf("Transaction prepared: %s\n", prepResp.Status)

	commitResp, err := client.CommitTransaction(txnResp.TxnId)
	if err != nil {
		log.Printf("Commit transaction failed: %v\n", err)
		return
	}
	fmt.Printf("Transaction committed: %s, Status: %s\n", commitResp.TxnId, commitResp.Status)

	fmt.Println("\n=== Example 8: Two-Phase Commit with Rollback ===")

	txnResp, err = client.BeginTransaction([]string{"users"})
	if err != nil {
		log.Printf("Begin transaction failed: %v\n", err)
		return
	}
	fmt.Printf("Transaction started: %s\n", txnResp.TxnId)

	prepResp, err = client.PrepareTransaction(txnResp.TxnId, strings.NewReader(csvData), streamload.LoadOptions{
		Format:          streamload.FormatCSV,
		Columns:         "id,name,age",
		ColumnSeparator: ",",
	})
	if err != nil {
		log.Printf("Prepare transaction failed: %v\n", err)
		return
	}
	fmt.Printf("Transaction prepared: %s\n", prepResp.Status)

	_, err = client.RollbackTransaction(txnResp.TxnId)
	if err != nil {
		log.Printf("Rollback transaction failed: %v\n", err)
		return
	}
	fmt.Printf("Transaction rolled back: %s\n", txnResp.TxnId)
}
