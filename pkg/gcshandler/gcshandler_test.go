package gcshandler_test

import (
	"testing"

	"github.com/adrianolaselva/dataql/pkg/gcshandler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsGCSURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "valid gs url", path: "gs://bucket/object.csv", want: true},
		{name: "valid gs url with nested path", path: "gs://bucket/a/b/c.csv", want: true},
		{name: "gs scheme only", path: "gs://", want: true},
		{name: "local absolute path", path: "/tmp/data.csv", want: false},
		{name: "local relative path", path: "data/file.csv", want: false},
		{name: "http url", path: "http://example.com/file.csv", want: false},
		{name: "https url", path: "https://example.com/file.csv", want: false},
		{name: "s3 url", path: "s3://bucket/object.csv", want: false},
		{name: "empty string", path: "", want: false},
		{name: "uppercase scheme not matched", path: "GS://bucket/object.csv", want: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, gcshandler.IsGCSURL(tt.path))
		})
	}
}

func TestParseGCSURL_Valid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		gcsURL     string
		wantBucket string
		wantObject string
	}{
		{
			name:       "simple bucket and object",
			gcsURL:     "gs://my-bucket/file.csv",
			wantBucket: "my-bucket",
			wantObject: "file.csv",
		},
		{
			name:       "nested object path",
			gcsURL:     "gs://my-bucket/a/b/c.csv",
			wantBucket: "my-bucket",
			wantObject: "a/b/c.csv",
		},
		{
			name:       "object with trailing slash",
			gcsURL:     "gs://my-bucket/folder/",
			wantBucket: "my-bucket",
			wantObject: "folder/",
		},
		{
			name:       "object with query string kept literally",
			gcsURL:     "gs://my-bucket/file.csv?versionId=42",
			wantBucket: "my-bucket",
			wantObject: "file.csv?versionId=42",
		},
		{
			name:       "bucket with dots and hyphens",
			gcsURL:     "gs://my.bucket-name/data/file.parquet",
			wantBucket: "my.bucket-name",
			wantObject: "data/file.parquet",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			loc, err := gcshandler.ParseGCSURL(tt.gcsURL)
			require.NoError(t, err)
			require.NotNil(t, loc)
			assert.Equal(t, tt.wantBucket, loc.Bucket)
			assert.Equal(t, tt.wantObject, loc.Object)
		})
	}
}

func TestParseGCSURL_Invalid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		gcsURL string
	}{
		{name: "empty string", gcsURL: ""},
		{name: "wrong scheme", gcsURL: "s3://bucket/object.csv"},
		{name: "missing object", gcsURL: "gs://bucket"},
		{name: "scheme only", gcsURL: "gs://"},
		{name: "bucket with trailing slash but no object", gcsURL: "gs://bucket/"},
		{name: "local path", gcsURL: "/tmp/file.csv"},
		{name: "http url", gcsURL: "http://example.com/file.csv"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			loc, err := gcshandler.ParseGCSURL(tt.gcsURL)
			require.Error(t, err)
			assert.Nil(t, loc)
			assert.Contains(t, err.Error(), "invalid GCS URL format")
		})
	}
}

func TestNewGCSHandler(t *testing.T) {
	t.Parallel()
	h := gcshandler.NewGCSHandler()
	require.NotNil(t, h)
}

func TestResolveFiles_NoGCSURLsPassThrough(t *testing.T) {
	t.Parallel()

	h := gcshandler.NewGCSHandler()
	input := []string{"/tmp/local.csv", "data/file.parquet", "http://example.com/x.csv"}

	result, err := h.ResolveFiles(input)
	require.NoError(t, err)
	assert.Equal(t, input, result)
}

func TestResolveFiles_EmptyInput(t *testing.T) {
	t.Parallel()

	h := gcshandler.NewGCSHandler()
	result, err := h.ResolveFiles(nil)
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestCleanup_NoTempDir(t *testing.T) {
	t.Parallel()

	h := gcshandler.NewGCSHandler()
	// With no client and no temp dir, Cleanup is a no-op and must not error.
	assert.NoError(t, h.Cleanup())
}
