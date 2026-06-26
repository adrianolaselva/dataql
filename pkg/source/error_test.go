package source_test

import (
	"errors"
	"testing"

	"github.com/adrianolaselva/dataql/pkg/source"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWrapError_Nil(t *testing.T) {
	assert.Nil(t, source.WrapError("gs", "fetch", "gs://b/o", nil))
}

func TestWrapError_FormatsContext(t *testing.T) {
	cause := errors.New("connection refused")
	err := source.WrapError("gs", "fetch", "gs://bucket/obj", cause)
	require.Error(t, err)
	assert.Equal(t, `gs fetch "gs://bucket/obj": connection refused`, err.Error())
}

func TestWrapError_Unwraps(t *testing.T) {
	cause := errors.New("boom")
	err := source.WrapError("s3", "download", "s3://b/k", cause)
	assert.ErrorIs(t, err, cause, "wrapped error must unwrap to the cause")
}
