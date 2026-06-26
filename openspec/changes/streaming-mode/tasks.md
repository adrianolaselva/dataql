## 1. Phase A — Stream consumption + emit mode

- [x] 1.1 Add `Stream(ctx) (<-chan Message, error)` to `mqreader.MessageQueueReader`; update the stub/registry.
- [x] 1.2 Kafka `Stream`: consumer-group reader using `FetchMessage` + `CommitMessages`; default start offset = last (live tail), `--from-start` opt-in. Close drains and releases.
- [x] 1.3 SQS `Stream`: long-poll `ReceiveMessage` (visibility > 0) + `DeleteMessage` after delivery; channel closes on ctx cancel.
- [x] 1.4 `internal/dataql/stream.go` StreamRunner: consume loop + emit mode (one record per message, JSONL default / honor `-t`; optional per-batch `-q`).
- [x] 1.5 Termination: `signal.NotifyContext(SIGINT)`, `--duration`, `--max-messages`, `--idle-timeout` composed into one cancel; clean exit 0.
- [x] 1.6 `run` flags: `--follow`, `--from-start`, `--max-messages`, `--duration`, `--idle-timeout`; dispatch to StreamRunner when `--follow`.
- [x] 1.7 Unit tests for the StreamRunner (fake reader channel: emit, max-messages, duration, idle-timeout, SIGINT). Kafka E2E (produce during the test, bounded by `--max-messages`).

## 2. Phase B — Windowed SQL

- [ ] 2.1 `--window <count|duration>` + `--interval <duration>`: maintain a bounded `stream` table in DuckDB (append + evict by count or by an `_ingested_at` time column).
- [ ] 2.2 Interval ticker runs `-q` over the window and emits the result; final emit on termination.
- [ ] 2.3 Unit tests for windowing (count eviction, duration eviction, interval emit) + a windowed Kafka E2E (rolling COUNT).

## 3. Docs & wrap-up

- [ ] 3.1 Document `--follow` (emit + window, termination) in `docs/data-sources.md` and the README; add an MCP/agent note (streaming sampling).
- [ ] 3.2 Keep the coverage ratchet green (StreamRunner well-tested); bump baseline.
- [ ] 3.3 `openspec validate streaming-mode --strict`; full gate green; update roadmap (M4 done).
