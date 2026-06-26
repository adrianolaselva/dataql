package source

// Resolver is implemented by every remote source handler (S3, GCS, Azure Blob,
// HTTP/URL). It rewrites a list of inputs, replacing the URIs it owns with local
// file paths it has fetched, and passing through any input it does not own.
// Cleanup removes whatever temporary files the resolver created.
//
// The core engine iterates a slice of Resolvers instead of hard-coding each
// handler, so a new remote source is added by registering its Resolver — no
// change to the engine's dispatch is required.
type Resolver interface {
	// ResolveFiles returns the input list with this source's URIs replaced by
	// local paths. Inputs it does not own are returned unchanged.
	ResolveFiles(files []string) ([]string, error)
	// Cleanup removes any temporary files this resolver created. It is safe to
	// call even when the resolver fetched nothing.
	Cleanup() error
}
