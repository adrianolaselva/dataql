## ADDED Requirements

### Requirement: Continuous consumption with --follow
`dataql run -f <queue-or-topic> --follow` SHALL continuously consume new
messages from a supported message queue (Kafka, SQS), advancing the offset or
deleting consumed messages so each message is processed once (at-least-once),
and emit results as messages arrive.

#### Scenario: Tail a topic
- **WHEN** a user runs `dataql run -f kafka://broker/topic --follow`
- **THEN** messages produced to the topic appear in the output as they arrive,
  until a termination condition is reached

#### Scenario: Peek behavior is unchanged
- **WHEN** a user runs the same source **without** `--follow`
- **THEN** the existing one-shot peek-and-query behavior is used, unchanged

### Requirement: Emission mode
Without a window, `--follow` SHALL emit one output record per consumed message
(JSONL by default, honoring `-t`). When `-q` is supplied, it SHALL be applied to
each micro-batch and the query result emitted.

#### Scenario: Emit each message
- **WHEN** `--follow` runs with no `--window`
- **THEN** each consumed message is emitted as it arrives without accumulating
  unbounded state

### Requirement: Windowed SQL mode
With `--window <count|duration>` and `--interval <duration>`, DataQL SHALL keep a
bounded rolling window of recent messages and run the user's `-q` query over that
window every interval, emitting the updated result. Messages outside the window
MUST be evicted.

#### Scenario: Rolling aggregate
- **WHEN** `dataql run -f kafka://broker/topic --follow --window 100 --interval 2s -q "SELECT level, COUNT(*) FROM stream GROUP BY level"`
- **THEN** every 2 seconds the aggregate over the last 100 messages is emitted,
  and older messages no longer affect the result

### Requirement: Deterministic termination
The stream SHALL terminate cleanly on any of: SIGINT (Ctrl-C, draining the
current batch), `--max-messages N`, `--duration <d>`, or `--idle-timeout <d>`
(no message for the period). On termination the process MUST exit 0 and release
the broker/queue connection.

#### Scenario: Stop after max messages
- **WHEN** `--follow --max-messages 10` is used
- **THEN** the stream consumes 10 messages, emits their results, and exits

#### Scenario: Stop on idle
- **WHEN** `--follow --idle-timeout 5s` is used and no message arrives for 5s
- **THEN** the stream stops and exits cleanly
