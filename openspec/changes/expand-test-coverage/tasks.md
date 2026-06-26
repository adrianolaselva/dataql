## 1. Unit tests for uncovered packages

- [x] 1.1 `filehandler/yaml` (84.5%) and `filehandler/xml` (81.6%) — import fixtures, edge cases, errors, line limit, collection naming.
- [x] 1.2 `filehandler/excel` (79.7%) and `filehandler/avro` (80.0%).
- [x] 1.3 `filehandler/orc` (82.8%) and `filehandler/parquet` (81.6%).
- [x] 1.4 `exportdata/json` (72.7%), `exportdata/yaml` (73.3%), `exportdata/xml` (73.5%) — round-trip + empty + special chars.
- [x] 1.5 `exportdata/excel` (80.0%) and `exportdata/parquet` (73.7%).
- [x] 1.6 `stdinhandler` (90.0%) and `urlhandler` (90.7%) — stdin via os.Pipe, URL via httptest.

## 2. Lock in coverage

- [x] 2.1 Full suite + `-race` green; lint 0 issues. Total coverage 24.1% → **37.4%**; `.coverage-baseline` raised to 37.4.

## 3. E2E format matrix

- [ ] 3.1 Add an E2E test that runs the real `dataql` binary to query each shipped file format (csv, json, jsonl, parquet, excel, xml, yaml, avro, orc) from `tests/fixtures`, asserting row counts/values.
- [ ] 3.2 Wire it into the suite and `e2e/COVERAGE.md` (mark file-format E2E covered).

## 4. Wrap-up

- [ ] 4.1 `openspec validate expand-test-coverage --strict`; full gate green.
- [ ] 4.2 Follow-up noted: `sqlite.NewSqLiteStorage(":memory:")` opens a pool without `SetMaxOpenConns(1)`/shared-cache, so multi-connection in-memory use can hit independent DBs (surfaced during export round-trip tests). Track for a storage hardening change.
