package dataql

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/adrianolaselva/dataql/pkg/mqreader"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeReader is a MessageQueueReader whose Stream replays a fixed set of
// messages then (optionally) blocks until ctx is cancelled.
type fakeReader struct {
	msgs      []mqreader.Message
	blockTail bool // keep the channel open after replaying msgs
	closed    bool
}

func (f *fakeReader) Connect(context.Context) error { return nil }
func (f *fakeReader) Peek(context.Context, int) ([]mqreader.Message, error) {
	return f.msgs, nil
}
func (f *fakeReader) GetMetadata(context.Context) (*mqreader.QueueMetadata, error) {
	return &mqreader.QueueMetadata{}, nil
}
func (f *fakeReader) Close() error { f.closed = true; return nil }
func (f *fakeReader) Stream(ctx context.Context) (<-chan mqreader.Message, error) {
	ch := make(chan mqreader.Message)
	go func() {
		defer close(ch)
		for _, m := range f.msgs {
			select {
			case ch <- m:
			case <-ctx.Done():
				return
			}
		}
		if f.blockTail {
			<-ctx.Done() // keep streaming "open" with no further messages
		}
	}()
	return ch, nil
}

func decodeLines(t *testing.T, out *bytes.Buffer) []map[string]any {
	t.Helper()
	var recs []map[string]any
	sc := bufio.NewScanner(out)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var rec map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &rec))
		recs = append(recs, rec)
	}
	return recs
}

func TestStreamFromReader_EmitsEachMessage(t *testing.T) {
	r := &fakeReader{msgs: []mqreader.Message{
		{ID: "1", Body: `{"level":"INFO","n":1}`},
		{ID: "2", Body: `{"level":"ERROR","n":2}`},
		{ID: "3", Body: "plain text"},
	}}
	var out bytes.Buffer
	require.NoError(t, streamFromReader(Params{}, r, &out))

	recs := decodeLines(t, &out)
	require.Len(t, recs, 3)
	assert.Equal(t, "INFO", recs[0]["level"])
	assert.Equal(t, float64(1), recs[0]["n"])
	assert.Equal(t, "1", recs[0]["_id"])
	assert.Equal(t, "plain text", recs[2]["body"], "non-JSON body emitted under 'body'")
}

func TestStreamFromReader_MaxMessages(t *testing.T) {
	r := &fakeReader{blockTail: true, msgs: []mqreader.Message{
		{ID: "1", Body: `{"a":1}`}, {ID: "2", Body: `{"a":2}`},
		{ID: "3", Body: `{"a":3}`}, {ID: "4", Body: `{"a":4}`},
	}}
	var out bytes.Buffer
	require.NoError(t, streamFromReader(Params{MaxMessages: 2}, r, &out))
	assert.Len(t, decodeLines(t, &out), 2)
}

func TestStreamFromReader_Duration(t *testing.T) {
	r := &fakeReader{blockTail: true, msgs: []mqreader.Message{{ID: "1", Body: `{"a":1}`}}}
	var out bytes.Buffer
	start := time.Now()
	require.NoError(t, streamFromReader(Params{Duration: 150 * time.Millisecond}, r, &out))
	assert.Less(t, time.Since(start), 2*time.Second, "should stop near the duration, not hang")
	assert.Len(t, decodeLines(t, &out), 1)
}

func TestStreamFromReader_IdleTimeout(t *testing.T) {
	// No messages at all: should stop after the idle timeout.
	r := &fakeReader{blockTail: true}
	var out bytes.Buffer
	start := time.Now()
	require.NoError(t, streamFromReader(Params{IdleTimeout: 120 * time.Millisecond}, r, &out))
	assert.Less(t, time.Since(start), 2*time.Second)
	assert.Empty(t, decodeLines(t, &out))
}

func TestStreamFromReader_StreamCloses(t *testing.T) {
	// Reader replays then closes the channel (not blockTail): loop returns.
	r := &fakeReader{msgs: []mqreader.Message{{ID: "1", Body: `{"a":1}`}}}
	var out bytes.Buffer
	require.NoError(t, streamFromReader(Params{}, r, &out))
	assert.Len(t, decodeLines(t, &out), 1)
}

func TestStreamFollow_RequiresMQSource(t *testing.T) {
	err := streamFollow(Params{FileInputs: []string{"data.csv"}}, &bytes.Buffer{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "message-queue source")
}

func TestStreamRecord_JSONAndPlain(t *testing.T) {
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	jsonRec := streamRecord(mqreader.Message{ID: "x", Body: `{"k":"v"}`, Timestamp: ts})
	assert.Equal(t, "v", jsonRec["k"])
	assert.Equal(t, "x", jsonRec["_id"])
	assert.Equal(t, "2026-01-02T03:04:05Z", jsonRec["_timestamp"])

	plain := streamRecord(mqreader.Message{Body: "hello"})
	assert.Equal(t, "hello", plain["body"])
	assert.NotContains(t, plain, "_id")
}
