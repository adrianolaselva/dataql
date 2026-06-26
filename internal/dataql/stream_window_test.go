package dataql

import (
	"bytes"
	"testing"
	"time"

	"github.com/adrianolaselva/dataql/pkg/mqreader"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseWindow(t *testing.T) {
	tests := []struct {
		in        string
		wantCount int
		wantDur   time.Duration
		wantErr   bool
	}{
		{"100", 100, 0, false},
		{"1", 1, 0, false},
		{"30s", 0, 30 * time.Second, false},
		{"2m", 0, 2 * time.Minute, false},
		{"0", 0, 0, true},
		{"-5", 0, 0, true},
		{"abc", 0, 0, true},
		{"", 0, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			c, d, err := parseWindow(tt.in)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantCount, c)
			assert.Equal(t, tt.wantDur, d)
		})
	}
}

func TestFlattenMessage(t *testing.T) {
	flat := flattenMessage(mqreader.Message{Body: `{"Level":"INFO","User Id":7,"nested":{"a":1}}`})
	assert.Equal(t, "INFO", flat["level"])
	assert.Equal(t, "7", flat["user_id"])
	assert.Equal(t, `{"a":1}`, flat["nested"])

	plain := flattenMessage(mqreader.Message{Body: "not json"})
	assert.Equal(t, "not json", plain["body"])
}

func TestSanitizeStreamCol(t *testing.T) {
	assert.Equal(t, "user_id", sanitizeStreamCol("User Id"))
	assert.Equal(t, "a_b_c", sanitizeStreamCol("a.b-c"))
	assert.Equal(t, "col", sanitizeStreamCol("***"))
}

func TestRunWindow_RollingAggregate(t *testing.T) {
	r := &fakeReader{blockTail: true, msgs: []mqreader.Message{
		{ID: "1", Body: `{"level":"INFO"}`},
		{ID: "2", Body: `{"level":"ERROR"}`},
		{ID: "3", Body: `{"level":"INFO"}`},
		{ID: "4", Body: `{"level":"INFO"}`},
	}}
	var out bytes.Buffer
	params := Params{
		Window:      "10",
		Interval:    time.Hour, // large so only the final emit (on max-messages) fires
		MaxMessages: 4,
		Query:       "SELECT level, COUNT(*) AS n FROM stream GROUP BY level ORDER BY level",
	}
	require.NoError(t, streamFromReader(params, r, &out))

	recs := decodeLines(t, &out)
	require.Len(t, recs, 2, "two groups: ERROR and INFO")
	// Results ordered by level: ERROR then INFO.
	assert.Equal(t, "ERROR", recs[0]["level"])
	assert.EqualValues(t, 1, toInt(recs[0]["n"]))
	assert.Equal(t, "INFO", recs[1]["level"])
	assert.EqualValues(t, 3, toInt(recs[1]["n"]))
}

func TestRunWindow_CountEviction(t *testing.T) {
	// Window of 2: only the last 2 messages should count.
	msgs := []mqreader.Message{
		{Body: `{"level":"A"}`}, {Body: `{"level":"A"}`},
		{Body: `{"level":"B"}`}, {Body: `{"level":"B"}`},
	}
	r := &fakeReader{blockTail: true, msgs: msgs}
	var out bytes.Buffer
	params := Params{
		Window: "2", Interval: time.Hour, MaxMessages: 4,
		Query: "SELECT COUNT(*) AS total, SUM(CASE WHEN level='A' THEN 1 ELSE 0 END) AS a_count FROM stream",
	}
	require.NoError(t, streamFromReader(params, r, &out))
	recs := decodeLines(t, &out)
	require.Len(t, recs, 1)
	assert.EqualValues(t, 2, toInt(recs[0]["total"]), "only last 2 messages in the window")
	assert.EqualValues(t, 0, toInt(recs[0]["a_count"]), "the two A's were evicted")
}

func TestRunWindow_RequiresQuery(t *testing.T) {
	r := &fakeReader{msgs: []mqreader.Message{{Body: `{"x":1}`}}}
	err := streamFromReader(Params{Window: "10"}, r, &bytes.Buffer{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires a query")
}

func TestRunWindow_InvalidWindow(t *testing.T) {
	r := &fakeReader{msgs: []mqreader.Message{{Body: `{"x":1}`}}}
	err := streamFromReader(Params{Window: "nope", Query: "SELECT 1"}, r, &bytes.Buffer{})
	require.Error(t, err)
}

// toInt coerces a JSON number (float64) or numeric string to int for assertions.
func toInt(v any) int {
	switch t := v.(type) {
	case float64:
		return int(t)
	case int:
		return t
	case int64:
		return int(t)
	default:
		return -1
	}
}
