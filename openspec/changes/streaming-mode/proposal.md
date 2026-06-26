## Why

Today DataQL reads message queues in one shot: it peeks N messages, loads them
into DuckDB, and runs a single query. That's great for inspection but can't
power live use — tailing a topic, sampling a stream, or computing rolling
aggregates as data arrives. Streaming is the headline capability for using
DataQL as the "data plane" for agents and pipelines: point it at a topic and get
continuous, SQL-shaped results.

## What Changes

- **`dataql run --follow`**: a streaming mode on `run` that **continuously
  consumes** new messages from a queue/topic (Kafka, SQS) — advancing the
  offset / deleting consumed messages, unlike the non-consuming peek.
- **Continuous emission**: each consumed message is emitted as it arrives
  (JSONL by default), optionally shaped by a per-batch SQL query.
- **Windowed SQL**: with `--window <count|duration>` and `--interval <duration>`,
  DataQL keeps a bounded rolling window of recent messages in DuckDB and
  re-runs the user's `-q` query over that window on each interval, emitting the
  updated result. Rows outside the window are evicted.
- **Termination controls**: graceful `Ctrl-C` (SIGINT, drains the current
  batch), `--max-messages N`, `--duration <d>`, and `--idle-timeout <d>` (stop
  when no messages arrive for a period). Any combination ends the stream cleanly.
- **Reader contract**: add `Stream(ctx) (<-chan Message, error)` to
  `MessageQueueReader`, implemented for Kafka (consumer group, committing
  offsets) and SQS (receive + delete with long polling). RabbitMQ/Pulsar follow
  in milestone 5.

## Capabilities

### New Capabilities
- `streaming`: continuous consumption of a queue/topic with live emission and
  windowed SQL, with deterministic termination controls.

### Modified Capabilities
<!-- None: streaming is additive; one-shot peek behavior is unchanged. -->

## Impact

- `pkg/mqreader` (add `Stream` to the interface), `pkg/mqreader/kafka`,
  `pkg/mqreader/sqs` (streaming implementations).
- `internal/dataql` + `cmd/dataqlctl` (the `--follow` path, window engine,
  termination, signal handling) — likely a new `internal/dataql/stream*.go`.
- New flags on `run`; docs (`docs/data-sources.md`, README) and an MCP note.
- E2E: a Kafka streaming test (and SQS via LocalStack) under `e2e/`.

## Non-goals

- RabbitMQ / Pulsar streaming sources (milestone 5).
- Exactly-once semantics / persistent checkpoints (best-effort at-least-once,
  with committed offsets, is the v1 contract).
- Streaming from non-queue sources (files/DBs).

## Self-contained invariant

Streaming adds no runtime dependency — the Kafka/SQS clients are already embedded
and the window engine uses the embedded DuckDB. The offline smoke is unaffected;
streaming simply requires a reachable broker/queue at run time, like any source.
