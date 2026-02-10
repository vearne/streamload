package streamload

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/gocarina/gocsv"
)

// LoadStructsCSV loads a slice of structs as CSV into StarRocks
// The structs parameter should be a slice of structs with csv tags
// Example: []User where User has csv:"field_name" tags
func (c *Client) LoadStructsCSV(table string, structs interface{}, opts LoadOptions) (*LoadResponse, error) {
	// Extract column names from struct tags using reflection
	if opts.Columns == "" {
		columns, err := extractCSVColumns(structs)
		if err != nil {
			return nil, fmt.Errorf("failed to extract columns: %w", err)
		}
		opts.Columns = columns
	}

	// Convert structs to CSV using gocsv
	var buf bytes.Buffer
	if err := gocsv.MarshalWithoutHeaders(structs, &buf); err != nil {
		return nil, fmt.Errorf("failed to marshal structs to CSV: %w", err)
	}

	// Ensure Format is set to CSV
	opts.Format = FormatCSV

	// If ColumnSeparator is not set, use default comma
	if opts.ColumnSeparator == "" {
		opts.ColumnSeparator = ","
	}

	// Call the existing Load method
	return c.Load(table, &buf, opts)
}

// LoadStructsJSON loads a slice of structs as JSON into StarRocks
// The structs parameter should be a slice of structs with json tags
// Example: []User where User has json:"field_name" tags
// By default, enables ZSTD compression and StripOuterArray
func (c *Client) LoadStructsJSON(table string, structs interface{}, opts LoadOptions) (*LoadResponse, error) {
	// Extract column names from struct tags using reflection
	if opts.Columns == "" {
		columns, err := extractJSONColumns(structs)
		if err != nil {
			return nil, fmt.Errorf("failed to extract columns: %w", err)
		}
		opts.Columns = columns
	}

	// Convert structs to JSON using encoding/json
	jsonBytes, err := json.Marshal(structs)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal structs to JSON: %w", err)
	}

	// Set Format to JSON
	opts.Format = FormatJSON

	// If user hasn't set compression, default to ZSTD
	if opts.Compression == CompressionNone {
		opts.Compression = CompressionZSTD
	}

	// Enable StripOuterArray by default (required for JSON arrays)
	opts.StripOuterArray = true

	// Call the existing Load method
	return c.Load(table, bytes.NewReader(jsonBytes), opts)
}

// extractCSVColumns extracts column names from struct csv tags using reflection
func extractCSVColumns(structs interface{}) (string, error) {
	val := reflect.ValueOf(structs)

	// If it's a pointer, get the underlying value
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	// Ensure it's a slice
	if val.Kind() != reflect.Slice {
		return "", fmt.Errorf("structs parameter must be a slice, got %s", val.Kind())
	}

	// Handle empty slice
	if val.Len() == 0 {
		return "", fmt.Errorf("structs slice is empty, cannot extract columns")
	}

	// Get the type of the slice element
	elemType := val.Type().Elem()

	// If the element is a pointer, get the underlying type
	if elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
	}

	// Ensure the element is a struct
	if elemType.Kind() != reflect.Struct {
		return "", fmt.Errorf("slice elements must be structs, got %s", elemType.Kind())
	}

	// Extract column names from csv tags
	var columns []string
	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)

		// Get the csv tag
		csvTag := field.Tag.Get("csv")
		if csvTag == "" {
			// If no csv tag, use the field name in lowercase
			csvTag = strings.ToLower(field.Name)
		}

		// Skip fields with csv:"-" tag
		if csvTag == "-" {
			continue
		}

		columns = append(columns, csvTag)
	}

	if len(columns) == 0 {
		return "", fmt.Errorf("no columns found in struct")
	}

	// Join columns with comma
	return strings.Join(columns, ","), nil
}

// extractJSONColumns extracts column names from struct json tags using reflection
func extractJSONColumns(structs interface{}) (string, error) {
	val := reflect.ValueOf(structs)

	// If it's a pointer, get the underlying value
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	// Ensure it's a slice
	if val.Kind() != reflect.Slice {
		return "", fmt.Errorf("structs parameter must be a slice, got %s", val.Kind())
	}

	// Handle empty slice
	if val.Len() == 0 {
		return "", fmt.Errorf("structs slice is empty, cannot extract columns")
	}

	// Get the type of the slice element
	elemType := val.Type().Elem()

	// If the element is a pointer, get the underlying type
	if elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
	}

	// Ensure the element is a struct
	if elemType.Kind() != reflect.Struct {
		return "", fmt.Errorf("slice elements must be structs, got %s", elemType.Kind())
	}

	// Extract column names from json tags
	var columns []string
	for i := 0; i < elemType.NumField(); i++ {
		field := elemType.Field(i)

		// Get the json tag
		jsonTag := field.Tag.Get("json")
		if jsonTag == "" {
			// If no json tag, use the field name in lowercase
			jsonTag = strings.ToLower(field.Name)
		}

		// Handle json tag with options (e.g., "name,omitempty")
		if idx := strings.Index(jsonTag, ","); idx != -1 {
			jsonTag = jsonTag[:idx]
		}

		// Skip fields with json:"-" tag
		if jsonTag == "-" {
			continue
		}

		columns = append(columns, jsonTag)
	}

	if len(columns) == 0 {
		return "", fmt.Errorf("no columns found in struct")
	}

	// Join columns with comma
	return strings.Join(columns, ","), nil
}
