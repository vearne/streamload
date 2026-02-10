package streamload

import (
	"reflect"
	"testing"
	"time"
)

// BenchmarkUser is a test struct for benchmarking
type BenchmarkUser struct {
	ID        int       `csv:"id" json:"id"`
	Name      string    `csv:"name" json:"name"`
	Email     string    `csv:"email" json:"email"`
	Age       int       `csv:"age" json:"age"`
	CreatedAt time.Time `csv:"created_at" json:"created_at"`
}

// BenchmarkExtractCSVColumns benchmarks CSV column extraction with cache (warm cache)
func BenchmarkExtractCSVColumns(b *testing.B) {
	users := []BenchmarkUser{
		{ID: 1, Name: "Alice", Email: "alice@example.com", Age: 30, CreatedAt: time.Now()},
	}

	// Warm up cache first
	_, _ = extractCSVColumns(users)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := extractCSVColumns(users)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkExtractJSONColumns benchmarks JSON column extraction with cache (warm cache)
func BenchmarkExtractJSONColumns(b *testing.B) {
	users := []BenchmarkUser{
		{ID: 1, Name: "Alice", Email: "alice@example.com", Age: 30, CreatedAt: time.Now()},
	}

	// Warm up cache first
	_, _ = extractJSONColumns(users)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := extractJSONColumns(users)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkExtractCSVColumns_ColdStart benchmarks first call (no cache)
func BenchmarkExtractCSVColumns_ColdStart(b *testing.B) {
	users := []BenchmarkUser{
		{ID: 1, Name: "Alice", Email: "alice@example.com", Age: 30, CreatedAt: time.Now()},
	}
	
	elemType := reflect.TypeOf(users).Elem()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Clear cache before each iteration to simulate cold start
		csvColumnsCacheMu.Lock()
		delete(csvColumnsCache, elemType)
		csvColumnsCacheMu.Unlock()
		
		b.StartTimer()
		_, err := extractCSVColumns(users)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkExtractJSONColumns_ColdStart benchmarks first call (no cache)
func BenchmarkExtractJSONColumns_ColdStart(b *testing.B) {
	users := []BenchmarkUser{
		{ID: 1, Name: "Alice", Email: "alice@example.com", Age: 30, CreatedAt: time.Now()},
	}
	
	elemType := reflect.TypeOf(users).Elem()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Clear cache before each iteration to simulate cold start
		jsonColumnsCacheMu.Lock()
		delete(jsonColumnsCache, elemType)
		jsonColumnsCacheMu.Unlock()
		
		b.StartTimer()
		_, err := extractJSONColumns(users)
		if err != nil {
			b.Fatal(err)
		}
	}
}
