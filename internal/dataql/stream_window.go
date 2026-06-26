package dataql

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/adrianolaselva/dataql/pkg/mqreader"
	"github.com/adrianolaselva/dataql/pkg/storage"
	"github.com/adrianolaselva/dataql/pkg/storage/duckdb"
)

const streamTable = "stream"

var streamColRegex = regexp.MustCompile(`[^a-z0-9_]+`)

// runWindow maintains a bounded rolling window of recent messages in DuckDB and
// re-runs the user's query over it every interval, emitting the result. Rows
// outside the window (by count or by ingestion time) are evicted.
func runWindow(ctx context.Context, params Params, msgCh <-chan mqreader.Message, out io.Writer) error {
	if strings.TrimSpace(params.Query) == "" {
		return fmt.Errorf("--window requires a query (-q), e.g. -q \"SELECT level, COUNT(*) FROM stream GROUP BY level\"")
	}
	countN, dur, err := parseWindow(params.Window)
	if err != nil {
		return err
	}
	interval := params.Interval
	if interval <= 0 {
		interval = 2 * time.Second
	}

	st, err := duckdb.NewDuckDBStorage("")
	if err != nil {
		return fmt.Errorf("failed to create stream storage: %w", err)
	}
	defer func() { _ = st.Close() }()
	typed, ok := st.(storage.TypedStorage)
	if !ok {
		return fmt.Errorf("stream storage does not support typed columns")
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	idleCh, resetIdle, stopIdle := idleTimer(params.IdleTimeout)
	defer stopIdle()

	var columns []string
	var defs []storage.ColumnDef
	built := false
	seq := 0
	delivered := 0

	finalEmit := func() {
		if built {
			emitQueryRows(st, params.Query, out)
		}
	}

	for {
		select {
		case msg, ok := <-msgCh:
			if !ok {
				finalEmit()
				return nil
			}
			resetIdle()
			rec := flattenMessage(msg)
			seq++
			rec["_seq"] = strconv.Itoa(seq)
			rec["_ingested_at"] = strconv.FormatInt(time.Now().Unix(), 10)

			if !built {
				columns, defs = buildStreamSchema(rec)
				if err := typed.BuildStructureWithTypes(streamTable, defs); err != nil {
					return fmt.Errorf("failed to create stream table: %w", err)
				}
				built = true
			}

			values := make([]any, len(columns))
			for i, c := range columns {
				values[i] = rec[c]
			}
			_ = typed.InsertRowWithCoercion(streamTable, columns, values, defs)
			evictWindow(st, countN, dur, seq)

			delivered++
			if params.MaxMessages > 0 && delivered >= params.MaxMessages {
				finalEmit()
				return nil
			}
		case <-ticker.C:
			if built {
				emitQueryRows(st, params.Query, out)
			}
		case <-idleCh:
			finalEmit()
			return nil
		case <-ctx.Done():
			finalEmit()
			return nil
		}
	}
}

// parseWindow interprets the --window value as a duration (e.g. "30s") or, if
// that fails, a positive message count (e.g. "100").
func parseWindow(spec string) (count int, dur time.Duration, err error) {
	spec = strings.TrimSpace(spec)
	if d, derr := time.ParseDuration(spec); derr == nil {
		if d <= 0 {
			return 0, 0, fmt.Errorf("--window duration must be positive: %q", spec)
		}
		return 0, d, nil
	}
	n, nerr := strconv.Atoi(spec)
	if nerr != nil || n <= 0 {
		return 0, 0, fmt.Errorf("--window must be a positive count (e.g. 100) or a duration (e.g. 30s), got %q", spec)
	}
	return n, 0, nil
}

// buildStreamSchema derives the column order and inferred types for the stream
// table from the first message's flattened record. _seq and _ingested_at are
// forced to BIGINT for reliable eviction math.
func buildStreamSchema(rec map[string]string) ([]string, []storage.ColumnDef) {
	cols := make([]string, 0, len(rec))
	for k := range rec {
		cols = append(cols, k)
	}
	sort.Strings(cols)

	sample := make([]any, len(cols))
	for i, c := range cols {
		sample[i] = rec[c]
	}
	defs := storage.InferColumnTypes(cols, [][]any{sample})
	for i := range defs {
		if defs[i].Name == "_seq" || defs[i].Name == "_ingested_at" {
			defs[i].Type = storage.TypeBigInt
		}
	}
	return cols, defs
}

// evictWindow removes rows outside the window: by count (keep the last N by
// _seq) or by duration (keep rows ingested within the period).
func evictWindow(st storage.Storage, countN int, dur time.Duration, seq int) {
	if countN > 0 {
		cutoff := seq - countN
		if cutoff > 0 {
			closeRows(st.Query(fmt.Sprintf("DELETE FROM %s WHERE _seq <= %d", streamTable, cutoff)))
		}
		return
	}
	if dur > 0 {
		cutoff := time.Now().Add(-dur).Unix()
		closeRows(st.Query(fmt.Sprintf("DELETE FROM %s WHERE _ingested_at < %d", streamTable, cutoff)))
	}
}

// emitQueryRows runs the user's query over the stream table and emits each
// result row as a JSON line.
func emitQueryRows(st storage.Storage, query string, out io.Writer) {
	rows, err := st.Query(query)
	if err != nil {
		// Surface the error as a JSON line so streaming consumers see it without
		// breaking the loop.
		_ = json.NewEncoder(out).Encode(map[string]any{"_error": err.Error()})
		return
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return
	}
	enc := json.NewEncoder(out)
	for rows.Next() {
		cells := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			continue
		}
		rec := make(map[string]any, len(cols))
		for i, c := range cols {
			rec[c] = normalizeCell(cells[i])
		}
		_ = enc.Encode(rec)
	}
}

// flattenMessage turns a message into a flat string record: top-level fields of
// a JSON body become columns; a non-JSON body is kept under "body".
func flattenMessage(msg mqreader.Message) map[string]string {
	rec := map[string]string{}
	var body map[string]any
	if err := json.Unmarshal([]byte(msg.Body), &body); err == nil {
		for k, v := range body {
			rec[sanitizeStreamCol(k)] = stringifyCell(v)
		}
	} else {
		rec["body"] = msg.Body
	}
	return rec
}

func sanitizeStreamCol(name string) string {
	n := strings.ToLower(strings.TrimSpace(name))
	n = strings.ReplaceAll(n, " ", "_")
	n = strings.ReplaceAll(n, "-", "_")
	n = strings.ReplaceAll(n, ".", "_")
	n = streamColRegex.ReplaceAllString(n, "")
	if n == "" {
		n = "col"
	}
	return n
}

func stringifyCell(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case map[string]any, []any:
		b, _ := json.Marshal(t)
		return string(b)
	default:
		return fmt.Sprintf("%v", t)
	}
}

func normalizeCell(v any) any {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
}

func closeRows(rows interface{ Close() error }, _ error) {
	if rows != nil {
		_ = rows.Close()
	}
}
