package streamload

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestLoadOptions_Label(t *testing.T) {
	opts := LoadOptions{
		Label: "test-label-123",
	}
	if opts.Label != "test-label-123" {
		t.Errorf("expected label 'test-label-123', got '%s'", opts.Label)
	}
}

func TestLoadOptions_Partitions(t *testing.T) {
	opts := LoadOptions{
		Partitions: []string{"p1", "p2"},
	}
	if len(opts.Partitions) != 2 {
		t.Errorf("expected 2 partitions, got %d", len(opts.Partitions))
	}
	if strings.Join(opts.Partitions, ",") != "p1,p2" {
		t.Errorf("expected 'p1,p2', got '%s'", strings.Join(opts.Partitions, ","))
	}
}

func TestLoadOptions_TemporaryPartitions(t *testing.T) {
	opts := LoadOptions{
		TemporaryPartitions: []string{"temp1", "temp2"},
	}
	if len(opts.TemporaryPartitions) != 2 {
		t.Errorf("expected 2 temporary partitions, got %d", len(opts.TemporaryPartitions))
	}
}

func TestLoadOptions_LogRejectedRecordNum(t *testing.T) {
	opts := LoadOptions{
		LogRejectedRecordNum: 100,
	}
	if opts.LogRejectedRecordNum != 100 {
		t.Errorf("expected 100, got %d", opts.LogRejectedRecordNum)
	}
}

func TestLoadOptions_Timezone(t *testing.T) {
	opts := LoadOptions{
		Timezone: "Asia/Shanghai",
	}
	if opts.Timezone != "Asia/Shanghai" {
		t.Errorf("expected 'Asia/Shanghai', got '%s'", opts.Timezone)
	}
}

func TestLoadOptions_LoadMemLimit(t *testing.T) {
	opts := LoadOptions{
		LoadMemLimit: 1024 * 1024 * 1024,
	}
	if opts.LoadMemLimit != 1024*1024*1024 {
		t.Errorf("expected %d, got %d", 1024*1024*1024, opts.LoadMemLimit)
	}
}

func TestTransactionBeginResponse_Unmarshal(t *testing.T) {
	jsonData := `{
		"TxnId": "123456",
		"Status": "OK",
		"Message": "Transaction started"
	}`

	var resp TransactionBeginResponse
	err := json.Unmarshal([]byte(jsonData), &resp)
	if err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if resp.TxnId != "123456" {
		t.Errorf("expected TxnId '123456', got '%s'", resp.TxnId)
	}
	if resp.Status != "OK" {
		t.Errorf("expected Status 'OK', got '%s'", resp.Status)
	}
}

func TestTransactionPrepareResponse_Unmarshal(t *testing.T) {
	jsonData := `{
		"TxnId": "123456",
		"Status": "OK",
		"Message": "Transaction prepared"
	}`

	var resp TransactionPrepareResponse
	err := json.Unmarshal([]byte(jsonData), &resp)
	if err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if resp.TxnId != "123456" {
		t.Errorf("expected TxnId '123456', got '%s'", resp.TxnId)
	}
}

func TestTransactionCommitResponse_Unmarshal(t *testing.T) {
	jsonData := `{
		"TxnId": "123456",
		"Status": "Success",
		"Message": "Transaction committed"
	}`

	var resp TransactionCommitResponse
	err := json.Unmarshal([]byte(jsonData), &resp)
	if err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if resp.TxnId != "123456" {
		t.Errorf("expected TxnId '123456', got '%s'", resp.TxnId)
	}
	if resp.Status != "Success" {
		t.Errorf("expected Status 'Success', got '%s'", resp.Status)
	}
}

func TestTransactionRollbackResponse_Unmarshal(t *testing.T) {
	jsonData := `{
		"TxnId": "123456",
		"Status": "Success",
		"Message": "Transaction rolled back"
	}`

	var resp TransactionRollbackResponse
	err := json.Unmarshal([]byte(jsonData), &resp)
	if err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if resp.TxnId != "123456" {
		t.Errorf("expected TxnId '123456', got '%s'", resp.TxnId)
	}
	if resp.Status != "Success" {
		t.Errorf("expected Status 'Success', got '%s'", resp.Status)
	}
}
