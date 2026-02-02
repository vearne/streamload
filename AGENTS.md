# streamload - AI Agent Guidelines

## Repository Overview

**streamload** is a Go library for implementing StarRocks Stream Load protocol. This library provides a clean, well-tested API for loading data into StarRocks tables with support for multiple data formats, compression algorithms, partition control, and two-phase commit (2PC) for external system integration.

### Key Characteristics

- **Language**: Go 1.21+
- **Architecture**: Simple client library with no external framework dependencies
- **Dependencies**: Minimal external dependencies (compression libraries only)
- **Design Pattern**: Functional, type-safe, error-oriented API
- **Test Coverage**: Comprehensive test suite (15+ tests)
- **Documentation**: Well-documented with README, API reference, and examples

## Codebase Structure

```
streamload/
├── client.go              # Core client implementation (630+ lines)
├── client_test.go         # Unit tests (200+ lines, 15 tests)
├── example/main.go        # Usage examples (200+ lines, 8 examples)
├── go.mod                # Go module definition
├── go.sum                # Dependency checksums
├── README.md             # User-facing documentation
├── API.md                # API reference documentation
└── LICENSE               # Apache 2.0 License
```

### Architecture Summary

- **Client** (`client.go`): Main API for Stream Load operations
  - `Load()`: Primary method for loading data
  - `BeginTransaction()`: Start a new 2PC transaction
  - `PrepareTransaction()`: Add data to an existing transaction
  - `CommitTransaction()`: Commit a 2PC transaction
  - `RollbackTransaction()`: Rollback a 2PC transaction
  - Helper methods for compression (GZIP, LZ4, ZSTD, BZIP2)

- **Types**:
  - `LoadResponse`: Response from StarRocks with detailed statistics
  - `TransactionBeginResponse`: Response for transaction begin
  - `TransactionPrepareResponse`: Response for transaction prepare
  - `TransactionCommitResponse`: Response for transaction commit
  - `TransactionRollbackResponse`: Response for transaction rollback
  - `LoadOptions`: Configuration options for load operations
  - `CompressionType`: Compression algorithm constants
  - `DataFormat`: Data format constants

## Code Style Guidelines

### Import Organization

```go
import (
	// Standard library imports
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"time"

	// Third-party compression libraries
	bzip2 "github.com/dsnet/compress/bzip2"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)
```

**Rules**:
1. Group imports by purpose with blank line separators
2. Standard library imports first, then third-party
3. Use named imports for clarity (e.g., `bzip2`)

### Naming Conventions

- **Packages**: Lowercase, single word (e.g., `streamload`)
- **Files**: `snake_case.go` for implementation files
- **Constants**: `PascalCase` (e.g., `CompressionGZIP`, `FormatCSV`)
- **Interfaces/Structs**: `PascalCase` (e.g., `LoadResponse`, `Client`)
- **Functions/Methods**: `PascalCase` (e.g., `NewClient`, `Load`)
- **Private fields**: `camelCase` (e.g., `httpClient`, `baseURL`)
- **Exported functions/methods**: `PascalCase`

### Error Handling

```go
if err != nil {
    return nil, fmt.Errorf("descriptive message: %w", err)
}
```

**Rules**:
1. Never use `panic()` for expected errors
2. Always return wrapped errors using `fmt.Errorf` with `%w` verb for error unwrapping
3. Include context in error messages (what operation failed)
4. Check HTTP status codes and response status
5. For 2PC operations, validate transaction state

### Type Safety

- Always use concrete types (no `interface{}` for fields unless necessary)
- Prefer explicit types over type assertions
- Use struct tags for JSON marshaling: `` `json:"FieldName"` ``
- Avoid `as any` or `@ts-ignore` - use proper type checking

### Formatting

```bash
go fmt ./...
go vet ./...
```

**Rules**:
1. Run `go fmt` before committing
2. Run `go vet` to catch potential issues
3. Use `gofmt` standard formatting (4 spaces tabs, 120 char width)
4. Max line length: 120 characters

## Build, Test, and Lint Commands

### Build

```bash
go build ./...
```

**Usage**: Build all packages to ensure compilation

### Testing

```bash
go test -v ./...           # Run all tests with verbose output
go test -race ./...         # Run tests with race detector
go test -cover ./...         # Generate coverage report
```

**Test Organization**:
- Test files: `*_test.go` (e.g., `client_test.go`)
- Test functions: `Test<StructName>_<Scenario>` (e.g., `TestClient_Load`, `TestLoadOptions_Label`)
- Test structure: Table-driven tests preferred for multiple cases

**Test Coverage Goals**:
- Aim for 70%+ coverage for core functionality
- Test both success and failure paths
- Include edge cases (empty data, invalid options, network failures)

### Linting

```bash
go vet ./...     # Go static analysis
golangci-lint run    # If configured
```

**Focus Areas**:
- Unreachable code
- Unused variables and imports
- Incorrect error handling
- Missing error checks
- Type safety issues

## Feature Implementation Status

### ✅ Completed (High Priority)

1. **Core Stream Load API**
   - `Load()` method with full CSV/JSON support
   - All compression algorithms (GZIP, LZ4, ZSTD, BZIP2)
   - Label support for duplicate load prevention
   - Partition control (target and temporary partitions)

2. **Two-Phase Commit (2PC)**
   - `BeginTransaction()` - Start transaction
   - `PrepareTransaction()` - Pre-commit with data
   - `CommitTransaction()` - Commit transaction
   - `RollbackTransaction()` - Rollback transaction

3. **Advanced Load Options**
   - Timezone support
   - LoadMemLimit control
   - LogRejectedRecordNum (v3.1+ feature)
   - Custom HTTP client configuration

### ⚠️ Not Implemented (Lower Priority)

1. **CSV Advanced Parameters**
   - `skip_header` - Skip first N rows (v3.0+)
   - `trim_space` - Trim whitespace around delimiters
   - `enclose` - Field wrapper character
   - `escape` - Escape character

2. **JSON Advanced Parameters**
   - `jsonpaths` - JSON path matching mode
   - `json_root` - Root element path
   - `ignore_json_size` - Skip JSON size validation

3. **Merge Commit Parameters** (v3.4.0+)
   - `enable_merge_commit` - Enable merge commit
   - `merge_commit_async` - Async mode
   - `merge_commit_interval_ms` - Merging window
   - `merge_commit_parallel` - Parallelism control

4. **Partial Update**
   - `partial_update` - Enable partial updates
   - `partial_update_mode` - Row or column mode
   - `merge_condition` - Conditional update condition

5. **Multi-Table Transaction**
   - Support for loading into multiple tables in one transaction (v4.0+)

## Common Tasks and Agent Selection

### When to Use `explore` Agent

Use for:
- Finding code patterns and implementations
- Locating where specific functionality is implemented
- Understanding codebase structure
- Finding usage of types, constants, or functions
- Identifying test patterns

### When to Use `librarian` Agent

Use for:
- Looking up external library documentation (StarRocks API, Go stdlib)
- Finding implementation examples in open-source projects
- Understanding library APIs (compression libraries, multipart)
- Researching best practices

### When to Use `oracle` Agent

Use for:
- Complex architectural decisions
- Debugging tricky issues
- Code refactoring guidance
- Performance optimization strategies
- Security considerations

### When to Use `ultrabrain` Agent

Use for:
- Implementing complex new features (Merge Commit, Partial Update)
- Algorithm design and optimization
- Large-scale refactoring
- Cross-system integration patterns

### When to Use `quick` Agent

Use for:
- Simple bug fixes (typo, import errors)
- Adding missing tests
- Updating documentation
- Small refactoring (renaming, extracting)
- Fixing linting issues

### When to Use `visual-engineering` Agent

**NOT RECOMMENDED** - This is a backend Go library, not a UI project

### When to Use `artistry` Agent

**NOT RECOMMENDED** - This is a standard library with conventional patterns

## Agent-Specific Guidelines

### For `explore` Agent

**Context**:
- This is a pure Go library implementing StarRocks protocol
- Standard Go patterns and conventions are used throughout
- Focus on finding implementations, not reinventing patterns

**Search Strategies**:
1. Use `ast_grep_search` for structural patterns (type definitions, function signatures)
2. Use `grep` for text-based searches (constants, imports, error messages)
3. Look for similar patterns in existing code
4. Search for test patterns to understand testing conventions

**What to Look For**:
- Where compression implementations are located
- How multipart form data is prepared
- HTTP request construction patterns
- Error handling patterns
- Response parsing patterns
- Transaction lifecycle management

### For `librarian` Agent

**Context**:
- The code uses third-party libraries: `github.com/dsnet/compress/bzip2`, `github.com/klauspost/compress/zstd`, `github.com/pierrec/lz4/v4`
- Also uses `mime/multipart` for form data handling
- Uses `net/http` for HTTP client functionality

**Research Focus**:
- StarRocks Stream Load protocol specifications
- Compression library APIs and best practices
- Go multipart/form-data handling
- HTTP header conventions for Stream Load
- Two-phase commit (2PC) protocol details

**Documentation Sources**:
- Official StarRocks documentation: https://docs.starrocks.io
- Go standard library documentation: https://pkg.go.dev/
- Third-party library GitHub repositories

### For All Agents

**General Rules**:

1. **Test-Driven Development**
   - Always create comprehensive tests for new features
   - Test both success and failure scenarios
   - Verify with `go test -race` for concurrent code
   - Mock HTTP responses in tests (use `httptest` or mocking)

2. **Backwards Compatibility**
   - Add new fields to `LoadOptions` struct with default values to maintain compatibility
   - Deprecation warnings should be added before removing functionality
   - Never change method signatures without major version bump

3. **HTTP Protocol Adherence**
   - Use `Expect: 100-continue` header (already implemented)
   - Follow StarRocks API documentation for headers and parameters
   - Set appropriate `Content-Type` and `Content-Encoding` headers
   - Handle HTTP status codes properly (200 OK, 4xx errors, 5xx errors)

4. **Transaction Safety**
   - For 2PC operations, always validate transaction IDs
   - Implement proper error handling for transaction lifecycle
   - Ensure commit/rollback operations are idempotent
   - Handle network failures gracefully (retry with exponential backoff if implementing)

5. **Compression Implementation**
   - Always verify compression is supported before attempting
   - Return wrapped errors with context
   - Test with real compressed data if possible

## Implementation Priorities

### Priority 1: Core Functionality ✅
- Basic Stream Load operations
- All compression algorithms
- Error handling and response parsing
- HTTP client configuration

### Priority 2: Production Features ✅
- Label support for duplicate prevention
- Partition control for data organization
- Two-phase commit (2PC) for external systems
- Timezone and memory limit controls

### Priority 3: Enhanced Features ⚠️
- Merge Commit optimization (v3.4.0+)
- Partial update support
- Multi-table transactions (v4.0.)
- Advanced CSV/JSON parameters

**Note**: Priority 3 features should be implemented with proper research and testing.

### Priority 4: Quality Improvements ⚠️
- Enhanced error logging
- Metrics and observability
- Connection pooling
- Load progress callbacks

## Special Considerations for This Codebase

### StarRocks Protocol Specifics

- **Data Formats**: Only CSV and JSON are officially supported
- **Compression**: All four algorithms (GZIP, LZ4_FRAME, ZSTD, BZIP2) are supported
- **Transaction Size**: Default is 2GB, configurable per load
- **Timeout**: Default 600 seconds (10 minutes), configurable per load

### Go Language Specifics

- **No Framework**: Pure Go implementation, no external dependencies for HTTP
- **Minimal Third-Party**: Only compression libraries
- **Standard Library**: Uses `net/http`, `mime/multipart`, `encoding/json`, `compress/gzip`
- **Context Management**: No context package, pass contexts explicitly if needed

### Compression Library Details

- **github.com/pierrec/lz4/v4**: LZ4 frame compression
- **github.com/klauspost/compress/zstd**: Zstandard compression
- **github.com/dsnet/compress/bzip2**: BZIP2 compression

Each library is used for both:
- Compression (in `compressData()` switch statement)
- Decompression (in future if reading compressed responses)

## Testing Strategy

### Unit Test Structure

```go
func TestClient_Method_Scenario(t *testing.T) {
    client := streamload.NewClient(...)
    
    // Test the scenario
    result, err := client.Method(...)
    
    // Verify results
    if err != nil {
        t.Errorf("expected error, got nil")
    }
    
    // Check response fields
    if result.Field != ExpectedValue {
        t.Errorf("expected %v, got %v", ExpectedValue, result.Field)
    }
}
```

### Test Categories

1. **Constructor Tests** - `NewClient`, `SetHTTPClient`, `SetDefaultHeader`
2. **Load Method Tests** - Success paths, error paths, compression, various options
3. **Transaction Tests** - Begin, Prepare, Commit, Rollback operations
4. **Option Tests** - Verify `LoadOptions` fields are properly set
5. **Compression Tests** - Test each compression algorithm works
6. **Response Tests** - Verify JSON unmarshaling works for all response types

### Integration Tests

**Not Currently Implemented** - Consider adding:
- HTTP server mocking for end-to-end testing
- Connection pool testing
- Retry logic verification
- Performance benchmarks

## File Organization Standards

### Implementation Files

- **client.go**: Core functionality (630+ lines)
  - Types: Client, Response types, Options
  - Constants: CompressionType, DataFormat
  - Load method and HTTP request construction
  - Compression methods: GZIP, LZ4, ZSTD, BZIP2
  - Transaction methods: Begin, Prepare, Commit, Rollback

### Test Files

- **client_test.go**: Comprehensive test coverage (200+ lines)
  - Constructor tests
  - Load method tests with various options
  - Transaction tests
  - Option tests
  - Compression tests
  - Response unmarshaling tests

### Example Files

- **example/main.go**: Usage demonstrations (200+ lines)
  - CSV load example
  - JSON load example
  - Compression examples
  - Label usage example
  - Partition control examples
  - Timezone example
  - 2PC transaction examples (begin, prepare, commit, rollback)
  - Custom HTTP client example

### Documentation Files

- **README.md**: User-facing documentation
  - Feature list and installation instructions
  - Basic usage examples
- **API.md**: Complete API reference
  - All types and methods documented
  - Examples included

## Quick Reference for Common Tasks

### Adding a New Compression Algorithm

1. Add compression type to `CompressionType` enum
2. Implement `compress<Algorithm>` method in `client.go`
3. Add case to `compressData()` switch statement
4. Write unit test in `client_test.go`
5. Update documentation (README.md, API.md)
6. Add example in `example/main.go`
7. Run tests to verify: `go test -v ./...`

### Adding a New Load Option Field

1. Add field to `LoadOptions` struct
2. Update `Load()` method to pass field to HTTP header/query param
3. Write unit test in `client_test.go`
4. Update documentation (README.md, API.md)
5. Add example in `example/main.go`
6. Run tests to verify: `go test -v ./...`

### Fixing a Bug

1. Identify the bug using tests or reproduction
2. Locate the bug in code
3. Understand the root cause
4. Implement fix
5. Write regression test
6. Run all tests to verify fix doesn't break existing functionality

### Refactoring

1. Ensure refactoring maintains all existing tests passing
2. Run tests before and after refactoring
3. Update documentation if behavior changes
4. Consider backwards compatibility for API changes

## Contact and Collaboration

### Repository Maintainer

**Main Maintainer**: vearne
**Repository**: github.com/vearne/streamload
**Issues**: Use GitHub Issues for bug reports and feature requests

### Contribution Guidelines

1. Follow all code style guidelines
2. Add tests for new features (aim for 70%+ coverage)
3. Update documentation for all changes
4. Ensure backward compatibility for API changes
5. Run `go fmt`, `go vet`, and tests before committing
6. Keep pull requests focused and atomic

### Pull Request Checklist

- [ ] Tests added/updated for new functionality
- [ ] Documentation updated
- [ ] Code follows style guidelines
- [ ] `go fmt` and `go vet` pass
- [ ] All existing tests still pass
- [ ] No new dependencies added (or justified in PR description)

## Version Management

### Semantic Versioning

Use semantic versioning: `MAJOR.MINOR.PATCH`

- **MAJOR**: Breaking changes to API
- **MINOR**: New functionality in backwards-compatible way
- **PATCH**: Bug fixes

**Current Version**: 1.0.0

## License

This project is licensed under the Apache License 2.0.

See LICENSE file for full text.

---

**Last Updated**: 2025-02-02
