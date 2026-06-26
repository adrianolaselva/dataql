## Context

MQ reading today: `MessageQueueReader.Peek(ctx, maxMessages)` reads N messages
**without consuming** (Kafka: `FetchMessage` with no commit; SQS:
`ReceiveMessage` with `VisibilityTimeout=0`). `filehandler/mq` peeks once, loads
the rows into DuckDB, and the normal query path runs. Streaming needs the
opposite: keep consuming **new** messages over time and produce output
incrementally.

## Goals / Non-Goals

**Goals:**
- `dataql run -f <queue> --follow` consumes continuously and emits live results.
- Optional windowed SQL over a bounded rolling window.
- Deterministic termination (Ctrl-C, max-messages, duration, idle-timeout).
- No change to one-shot peek behavior.

**Non-Goals:** RabbitMQ/Pulsar (M5); exactly-once; checkpoints; streaming files/DBs.

## Decisions

### D1. Add `Stream` to the reader; keep `Peek`
```go
type MessageQueueReader interface {
    Connect(ctx) error
    Peek(ctx, maxMessages int) ([]Message, error)
    Stream(ctx) (<-chan Message, error)   // NEW: yields new messages until ctx is done
    GetMetadata(ctx) (*QueueMetadata, error)
    Close() error
}
```
- **Kafka**: a consumer group reader using `FetchMessage` + `CommitMessages`
  (offsets advance, so restarts resume). Default `StartOffset` = last for live
  tailing; `--from-start` can read from the beginning.
- **SQS**: `ReceiveMessage` (long poll, `WaitTimeSeconds`) + `DeleteMessage`
  after the message is delivered to the channel (at-least-once).
The channel closes when ctx is cancelled; the reader drains and Closes.

### D2. Two output modes, selected by flags
- **Emit mode** (default, no `--window`): each message → one output record
  (JSONL by default; respects `-t`). If `-q` is given, it runs per micro-batch
  against a transient table and emits the result.
- **Window mode** (`--window`): maintain a bounded DuckDB table `stream`. Append
  each message; evict rows outside the window (count: keep last N; duration: keep
  rows newer than now-d, using an ingestion-time column). Every `--interval`
  (default 2s) run `-q` over `stream` and emit the result. This is the
  rolling-aggregate path (e.g. `SELECT level, COUNT(*) FROM stream GROUP BY level`).

### D3. A dedicated stream engine, not bolted onto the one-shot path
A new `internal/dataql/stream.go` (StreamRunner) owns: the consume loop, the
window table lifecycle, the interval ticker, the emit formatting, and
termination. The existing `Run` path is untouched; `--follow` dispatches to the
StreamRunner. This isolates the complexity and keeps the one-shot path stable.

### D4. Termination via a composed context
`signal.NotifyContext(SIGINT)` for Ctrl-C; `context.WithTimeout` for `--duration`;
a message counter for `--max-messages`; an idle timer reset on each message for
`--idle-timeout`. The first to fire cancels the consume context; the engine
drains the in-flight batch, emits a final window result (in window mode), and
exits 0.

## Risks / Trade-offs

- **At-least-once duplicates** (SQS delete-after-deliver, Kafka commit cadence) →
  documented as the v1 contract; acceptable for tail/sample/aggregate use.
- **Unbounded growth without a window** → emit mode never accumulates (streams
  through); window mode is always bounded by count/duration eviction.
- **Testing needs a live broker** → use the e2e Kafka (and SQS via LocalStack)
  with a producer that emits during the test; bound by `--max-messages`/
  `--duration` so the test is deterministic.
- **Backpressure / slow consumers** → a small buffered channel; if the consumer
  (window/emit) lags, the reader blocks (natural backpressure), which is fine
  for a CLI.

## Migration Plan

- **Phase A**: `Stream` on the interface + Kafka & SQS impls; `--follow` emit
  mode + termination controls; Kafka E2E. Ship + validate.
- **Phase B**: window mode (`--window`, `--interval`) over the bounded table;
  windowed E2E; docs.
Each phase is independently shippable; emit mode is useful on its own.
Rollback: `--follow` is additive; revert the flag/engine without touching peek.

## Open Questions

- Default Kafka start offset for `--follow`: last (true tail) vs first. Lean
  **last** for live tailing, with `--from-start` to opt into history.
- Window duration eviction needs an ingestion timestamp column — add `_ingested_at`
  to the stream table (confirm it doesn't collide with message fields).
