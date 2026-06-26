## 1. Phase 1 — Safety net (no behavior change)

- [ ] 1.1 Add a `pkg/source` package with a shared error helper `WrapError(scheme, op, uri, err)`.
- [ ] 1.2 Add unit tests for `gcshandler` and `azurehandler` (URL parsing, env/config handling, error paths) — mock or table-drive what can be tested without a live backend.
- [ ] 1.3 Add unit tests for `s3handler` URL/endpoint parsing, and for `dbconnector` + `filehandler/database` (currently 8% / 4%) — connection-string parsing, type mapping, error paths.
- [ ] 1.4 Adopt the error helper in the remote handlers. Full gate green; coverage rises and the ratchet baseline is bumped.

## 2. Phase 2 — Emulator support + E2E

- [ ] 2.1 GCS: honor `STORAGE_EMULATOR_HOST` (create the client with `option.WithoutAuthentication()` when set). Unit-test the wiring.
- [ ] 2.2 Azure: support an emulator endpoint (azurite connection string / explicit endpoint). Unit-test the wiring.
- [ ] 2.3 Add `fake-gcs-server` and `azurite` services to `e2e/docker-compose.yaml` with init/seed; add `e2e/tests/test-gcs.sh` and `test-azure.sh`; wire into `test-all.sh`.
- [ ] 2.4 Update `e2e/COVERAGE.md`: GCS and Azure move from blocked → covered.

## 3. Phase 3 — Source registry

- [ ] 3.1 Define the `Source` interface (`Scheme`, `Matches`, `Fetch`) in `pkg/source`; implement it for s3/gcs/azure/url (and decide on stdin).
- [ ] 3.2 Add a registry and migrate `internal/dataql/dataql.go` to resolve remote URIs via the registry instead of hard-coded handler fields — mechanical, same logic.
- [ ] 3.3 Verify behavior is identical: full unit + E2E suite green; no CLI/output change. Add tests for the registry resolution.
- [ ] 3.4 Document "adding a new source" in `docs/` (the contract + registration steps).

## 4. Wrap-up

- [ ] 4.1 `openspec validate connector-abstraction --strict`; full gate green; coverage ratchet bumped.
- [ ] 4.2 Update the roadmap (M3 done; note GCS/Azure E2E now covered).
