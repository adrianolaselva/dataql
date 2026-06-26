## Why

The coverage ratchet (milestone 1) holds the line, but many packages sat at 0%
unit coverage — especially the file-format read/export handlers and the
stdin/URL source handlers. To "guarantee everything works across diverse
scenarios", these need real tests, and the E2E suite should assert every shipped
format round-trips through the actual `dataql` binary. This change raises
coverage and broadens E2E without introducing new product behavior.

## What Changes

- Add unit tests (happy path, edge cases, errors, limits, naming) for the
  previously-uncovered packages: `filehandler/{yaml,xml,excel,avro,orc,parquet}`,
  `exportdata/{json,yaml,xml,excel,parquet}`, `stdinhandler`, `urlhandler`.
- Raise the coverage ratchet baseline to lock in the gain (24.1% → 37.4%).
- Add an **E2E format matrix**: run the real binary to query each shipped file
  format from fixtures, asserting correct results.

## Capabilities

### New Capabilities
<!-- None: this implements the existing quality-gates and e2e-harness capabilities. -->

### Modified Capabilities
<!-- None: behavior is unchanged; only tests and the coverage baseline change. -->

## Impact

- New `*_test.go` files under the listed packages (no source changes).
- New E2E format-matrix test under `e2e/`.
- `.coverage-baseline` raised to 37.4%.

## Non-goals

- New features, sources, or connector changes (other milestones).
- GCS/Azure E2E (blocked on connector endpoint support — milestone 3).
- Reaching 90% in one pass — this is one large step on the ratchet.

## Self-contained invariant

Tests-only change; the self-contained invariant is unaffected. The new E2E
format matrix runs the binary against local fixtures with no external services.
