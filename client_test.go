package streamload

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/gocarina/gocsv"
)

// TestUser is a test struct for CSV and JSON loading
type TestUser struct {
	Id   int    `csv:"id" json:"id"`
	Name string `csv:"name" json:"name"`
	Age  int    `csv:"age" json:"age"`
}

// TestUserWithTime includes time field for more complex testing
type TestUserWithTime struct {
	Id         int       `csv:"id" json:"id"`
	Name       string    `csv:"name" json:"name"`
	Age        int       `csv:"age" json:"age"`
	CreateDate time.Time `csv:"create_date" json:"create_date"`
}

func TestLoadStructsCSV_MarshalCorrectly(t *testing.T) {
	users := []TestUser{
		{Id: 1, Name: "Alice", Age: 25},
		{Id: 2, Name: "Bob", Age: 30},
		{Id: 3, Name: "Charlie", Age: 35},
	}

	// Test that gocsv marshals correctly
	var buf bytes.Buffer
	if err := gocsv.Marshal(users, &buf); err != nil {
		t.Fatalf("failed to marshal structs to CSV: %v", err)
	}

	csvOutput := buf.String()

	// Check that the header is included
	if !strings.Contains(csvOutput, "id,name,age") {
		t.Errorf("CSV output should contain header, got: %s", csvOutput)
	}

	// Check that data rows are included
	if !strings.Contains(csvOutput, "1,Alice,25") {
		t.Errorf("CSV output should contain first row, got: %s", csvOutput)
	}

	if !strings.Contains(csvOutput, "2,Bob,30") {
		t.Errorf("CSV output should contain second row, got: %s", csvOutput)
	}
}

func TestLoadStructsCSV_EmptySlice(t *testing.T) {
	users := []TestUser{}

	var buf bytes.Buffer
	if err := gocsv.Marshal(users, &buf); err != nil {
		t.Fatalf("failed to marshal empty slice: %v", err)
	}

	// Empty slice should only have header
	csvOutput := buf.String()
	if !strings.Contains(csvOutput, "id,name,age") {
		t.Errorf("CSV output should contain header for empty slice")
	}
}

func TestLoadStructsJSON_MarshalCorrectly(t *testing.T) {
	users := []TestUser{
		{Id: 1, Name: "Alice", Age: 25},
		{Id: 2, Name: "Bob", Age: 30},
	}

	// Test that json.Marshal works correctly
	jsonBytes, err := json.Marshal(users)
	if err != nil {
		t.Fatalf("failed to marshal structs to JSON: %v", err)
	}

	jsonOutput := string(jsonBytes)

	// Check that the JSON is a valid array
	if !strings.HasPrefix(jsonOutput, "[") || !strings.HasSuffix(jsonOutput, "]") {
		t.Errorf("JSON output should be an array, got: %s", jsonOutput)
	}

	// Check that data is included
	if !strings.Contains(jsonOutput, `"id":1`) && !strings.Contains(jsonOutput, `"id": 1`) {
		t.Errorf("JSON output should contain id field, got: %s", jsonOutput)
	}

	if !strings.Contains(jsonOutput, `"name":"Alice"`) && !strings.Contains(jsonOutput, `"name": "Alice"`) {
		t.Errorf("JSON output should contain name field, got: %s", jsonOutput)
	}
}

func TestLoadStructsJSON_EmptySlice(t *testing.T) {
	users := []TestUser{}

	jsonBytes, err := json.Marshal(users)
	if err != nil {
		t.Fatalf("failed to marshal empty slice: %v", err)
	}

	jsonOutput := string(jsonBytes)

	// Empty slice should be []
	if jsonOutput != "[]" {
		t.Errorf("JSON output for empty slice should be [], got: %s", jsonOutput)
	}
}

func TestLoadStructsCSV_WithTimeField(t *testing.T) {
	now := time.Now()
	users := []TestUserWithTime{
		{Id: 1, Name: "Alice", Age: 25, CreateDate: now},
	}

	var buf bytes.Buffer
	if err := gocsv.Marshal(users, &buf); err != nil {
		t.Fatalf("failed to marshal structs with time field: %v", err)
	}

	csvOutput := buf.String()

	// Should contain header with create_date
	if !strings.Contains(csvOutput, "create_date") {
		t.Errorf("CSV output should contain create_date field in header")
	}
}

// Note: Integration tests that actually connect to StarRocks should be added separately
// These tests only verify the marshaling logic
