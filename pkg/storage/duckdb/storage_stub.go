//go:build noduckdb

package duckdb

import (
	"fmt"

	"github.com/adrianolaselva/dataql/pkg/storage"
)

// NewDuckDBStorage returns an error when DuckDB support is not compiled in.
// This is the stub implementation used when building with the "noduckdb" tag.
func NewDuckDBStorage(datasource string) (storage.Storage, error) {
	return nil, fmt.Errorf("DuckDB support is not available in this build (compiled with noduckdb tag)")
}
