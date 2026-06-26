## 1. Phase 1 — Safety net (no behavior change)

- [x] 1.1 Added `pkg/source` with `WrapError(scheme, op, uri, err)` (100% covered).
- [x] 1.2 Added unit tests for `gcshandler` (25.5%) and `azurehandler` (46.6%) — URL parsing, both Azure URL forms, credential error paths, ResolveFiles pass-through (no network).
- [x] 1.3 Added tests for `s3handler` (18.8%, URL parsing), `dbconnector` (8%→19.2%, type mappers, quoting, not-connected error paths), and `filehandler/database` (4%→52.4%, connection-URL parsing incl. DuckDB forms, sanitizers).
- [x] 1.4 Helper landed and available; **wholesale adoption deferred to Phase 3** (the registry refactor) to avoid churning the error messages the new tests assert. Full gate green; coverage 37.4%→**40.8%**, baseline bumped.

## 2. Phase 2 — Emulator support + E2E

- [x] 2.1 GCS honors `STORAGE_EMULATOR_HOST` (client built with `option.WithoutAuthentication()` when set). **Validated locally end-to-end** against fake-gcs-server (`gs://` query returned correct rows).
- [x] 2.2 Azure works against azurite with **no source change** — `initClient` already creates the client from `AZURE_STORAGE_CONNECTION_STRING`, whose `BlobEndpoint` points at the emulator. (Found: the handler accepts `azure://` URLs, not `az://`; docs say `az://` — a docs/code mismatch to reconcile in Phase 3 or a docs fix.)
- [x] 2.3 Added `fakegcs` + `azurite` to `e2e/docker-compose.yaml`; `tests/test-gcs.sh` (seeds via the no-auth fake-gcs REST API; 2/2 pass locally) and `tests/test-azure.sh` (seeds with the az CLI on CI, skips gracefully without it); wired both into `test-all.sh` + `.env`.
- [x] 2.4 `e2e/COVERAGE.md`: GCS and Azure moved blocked → ✅ covered.

## 3. Phase 3 — Source registry

- [ ] 3.1 Define the `Source` interface (`Scheme`, `Matches`, `Fetch`) in `pkg/source`; implement it for s3/gcs/azure/url (and decide on stdin).
- [ ] 3.2 Add a registry and migrate `internal/dataql/dataql.go` to resolve remote URIs via the registry instead of hard-coded handler fields — mechanical, same logic.
- [ ] 3.3 Verify behavior is identical: full unit + E2E suite green; no CLI/output change. Add tests for the registry resolution.
- [ ] 3.4 Document "adding a new source" in `docs/` (the contract + registration steps).

## 4. Wrap-up

- [ ] 4.1 `openspec validate connector-abstraction --strict`; full gate green; coverage ratchet bumped.
- [ ] 4.2 Update the roadmap (M3 done; note GCS/Azure E2E now covered).
