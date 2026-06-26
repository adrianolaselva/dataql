// Package source defines the shared contract and helpers for DataQL's remote
// data sources (S3, GCS, Azure Blob, HTTP/URL, stdin). It lets every source
// report errors uniformly and, in a later phase, be resolved through a single
// registry instead of being hard-coded in the core engine.
package source

import "fmt"

// WrapError wraps a source error with consistent context: the source scheme,
// the operation that failed, and the URI involved. It returns nil when err is
// nil, so callers can write `return source.WrapError(...)` unconditionally.
//
// Example: WrapError("gs", "fetch", "gs://bucket/obj", err) ->
//
//	gs fetch "gs://bucket/obj": <cause>
func WrapError(scheme, op, uri string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s %s %q: %w", scheme, op, uri, err)
}
