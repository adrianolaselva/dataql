package dataql

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/adrianolaselva/dataql/pkg/filehandler"
	"github.com/adrianolaselva/dataql/pkg/mqreader"
)

// StreamFollow consumes a single message-queue source continuously and emits
// each message as a JSON line, until a termination condition fires (SIGINT,
// --max-messages, --duration, or --idle-timeout). It is the engine behind
// `dataql run --follow` (emit mode).
func StreamFollow(params Params) error {
	return streamFollow(params, os.Stdout)
}

// streamFollow is the testable core: it streams from a reader created for the
// first message-queue input and writes JSON lines to out.
func streamFollow(params Params, out io.Writer) error {
	var mqURL string
	for _, f := range params.FileInputs {
		if filehandler.IsMQURL(f) {
			mqURL = f
			break
		}
	}
	if mqURL == "" {
		return fmt.Errorf("--follow requires a message-queue source (e.g. kafka://broker/topic, sqs://queue)")
	}

	reader, err := mqreader.NewReaderFromURL(mqURL)
	if err != nil {
		return fmt.Errorf("failed to create stream reader: %w", err)
	}
	defer func() { _ = reader.Close() }()

	return streamFromReader(params, reader, out)
}

// streamFromReader sets up termination and the message channel, then dispatches
// to emit mode or windowed mode. Split out so tests can drive it with a fake
// reader.
func streamFromReader(params Params, reader mqreader.MessageQueueReader, out io.Writer) error {
	// Compose termination: SIGINT/SIGTERM, plus an optional overall duration.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if params.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, params.Duration)
		defer cancel()
	}

	msgCh, err := reader.Stream(ctx)
	if err != nil {
		return fmt.Errorf("failed to start stream: %w", err)
	}

	if params.Window != "" {
		return runWindow(ctx, params, msgCh, out)
	}
	return runEmit(ctx, params, msgCh, out)
}

// idleTimer returns an idle-timeout channel and a reset function. When d <= 0
// the channel is nil (never fires) and reset is a no-op.
func idleTimer(d time.Duration) (<-chan time.Time, func(), func()) {
	if d <= 0 {
		return nil, func() {}, func() {}
	}
	t := time.NewTimer(d)
	reset := func() {
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
		t.Reset(d)
	}
	return t.C, reset, func() { t.Stop() }
}

// runEmit emits one JSON line per consumed message until a termination
// condition fires.
func runEmit(ctx context.Context, params Params, msgCh <-chan mqreader.Message, out io.Writer) error {
	enc := json.NewEncoder(out)
	count := 0
	idleCh, resetIdle, stopIdle := idleTimer(params.IdleTimeout)
	defer stopIdle()

	for {
		select {
		case msg, ok := <-msgCh:
			if !ok {
				return nil
			}
			resetIdle()
			if err := enc.Encode(streamRecord(msg)); err != nil {
				return fmt.Errorf("failed to emit message: %w", err)
			}
			count++
			if params.MaxMessages > 0 && count >= params.MaxMessages {
				return nil
			}
		case <-idleCh:
			return nil
		case <-ctx.Done():
			return nil
		}
	}
}

// streamRecord builds the JSON line emitted per message: when the body is a JSON
// object its fields are emitted directly (so downstream stays flat); otherwise
// the raw body is emitted under "body". A few envelope fields are added with an
// underscore prefix to avoid colliding with body fields.
func streamRecord(msg mqreader.Message) map[string]any {
	rec := map[string]any{}
	var body map[string]any
	if err := json.Unmarshal([]byte(msg.Body), &body); err == nil {
		for k, v := range body {
			rec[k] = v
		}
	} else {
		rec["body"] = msg.Body
	}
	if msg.ID != "" {
		rec["_id"] = msg.ID
	}
	if !msg.Timestamp.IsZero() {
		rec["_timestamp"] = msg.Timestamp.Format(time.RFC3339)
	}
	return rec
}
