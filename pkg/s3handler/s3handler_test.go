package s3handler_test

import (
	"testing"

	"github.com/adrianolaselva/dataql/pkg/s3handler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsS3URL(t *testing.T) {
	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "simple s3 url", path: "s3://bucket/key", want: true},
		{name: "nested key", path: "s3://bucket/path/to/file.csv", want: true},
		{name: "bucket only prefix", path: "s3://", want: true},
		{name: "local path", path: "/tmp/file.csv", want: false},
		{name: "relative path", path: "data/file.csv", want: false},
		{name: "http scheme", path: "http://example.com/file", want: false},
		{name: "https scheme", path: "https://example.com/file", want: false},
		{name: "file scheme", path: "file:///tmp/file.csv", want: false},
		{name: "empty string", path: "", want: false},
		{name: "uppercase scheme not matched", path: "S3://bucket/key", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, s3handler.IsS3URL(tt.path))
		})
	}
}

func TestParseS3URL(t *testing.T) {
	tests := []struct {
		name       string
		url        string
		wantErr    bool
		wantBucket string
		wantKey    string
	}{
		{
			name:       "simple url",
			url:        "s3://my-bucket/my-key",
			wantBucket: "my-bucket",
			wantKey:    "my-key",
		},
		{
			name:       "nested key",
			url:        "s3://my-bucket/path/to/file.csv",
			wantBucket: "my-bucket",
			wantKey:    "path/to/file.csv",
		},
		{
			name:       "key with dots and dashes",
			url:        "s3://data-lake/2024/01/report-final.parquet",
			wantBucket: "data-lake",
			wantKey:    "2024/01/report-final.parquet",
		},
		{
			name:    "missing key",
			url:     "s3://bucket-only",
			wantErr: true,
		},
		{
			name:    "missing key with trailing slash",
			url:     "s3://bucket/",
			wantErr: true,
		},
		{
			name:    "wrong scheme",
			url:     "http://bucket/key",
			wantErr: true,
		},
		{
			name:    "empty string",
			url:     "",
			wantErr: true,
		},
		{
			name:    "scheme only",
			url:     "s3://",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loc, err := s3handler.ParseS3URL(tt.url)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, loc)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, loc)
			assert.Equal(t, tt.wantBucket, loc.Bucket)
			assert.Equal(t, tt.wantKey, loc.Key)
		})
	}
}

func TestS3LocationStruct(t *testing.T) {
	loc := s3handler.S3Location{
		Bucket: "b",
		Key:    "k",
		Region: "us-east-1",
	}
	assert.Equal(t, "b", loc.Bucket)
	assert.Equal(t, "k", loc.Key)
	assert.Equal(t, "us-east-1", loc.Region)
}

func TestNewS3Handler(t *testing.T) {
	h := s3handler.NewS3Handler()
	require.NotNil(t, h)
	// Cleanup on a fresh handler with no temp dir should be a no-op without error.
	assert.NoError(t, h.Cleanup())
}

// TestResolveFiles_NonS3PathsPassThrough verifies that local (non-S3) paths are
// returned unchanged and no network/client initialization occurs.
func TestResolveFiles_NonS3PathsPassThrough(t *testing.T) {
	h := s3handler.NewS3Handler()
	input := []string{"/tmp/a.csv", "data/b.json", "./c.parquet"}

	result, err := h.ResolveFiles(input)
	require.NoError(t, err)
	assert.Equal(t, input, result)
}

func TestResolveFiles_Empty(t *testing.T) {
	h := s3handler.NewS3Handler()
	result, err := h.ResolveFiles([]string{})
	require.NoError(t, err)
	assert.Empty(t, result)
}
