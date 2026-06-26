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

- [x] 3.1 Defined `source.Resolver` (`ResolveFiles` + `Cleanup`) in `pkg/source` — formalizing the **existing** handler contract (lower-risk than the speculative `Fetch` redesign). s3/gcs/azure/url already satisfy it; stdin (different signature) and compression (special alias logic) stay as dedicated steps.
- [x] 3.2 Migrated `internal/dataql/dataql.go`: the four identical url/s3/gcs/azure blocks collapse into one loop over `[]source.Resolver`; `Close()` cleanup collapses to a loop. Adding a remote source = append to the slice.
- [x] 3.3 Behavior verified identical: full unit suite + `-race` green, lint 0; functional checks pass (local pass-through, stdin, and `gs://` via the registry+emulator all return correct results). Coverage 40.8%→41.0%.
- [x] 3.4 Documented "Adding a remote data source" (the `Resolver` contract + registration + emulator/env guidance) in CONTRIBUTING.md.

## 4. Wrap-up

- [x] 4.1 `openspec validate connector-abstraction --strict` passes; full gate green locally (build, race tests, lint 0); coverage ratchet bumped to 41.0%.
- [ ] 4.2 Update the roadmap (M3 done; GCS/Azure E2E now covered) — on merge.

## Notes / follow-ups

- Docs/code mismatch found: the Azure handler accepts `azure://` URLs but the docs say `az://`. Reconcile (add `az://` alias or fix docs) in a small follow-up.
- DB (`Connector`) and MQ (`mqreader`) sources intentionally not folded into the registry yet (different shape than "download to a file"); a later change can unify them.
- Error helper (`source.WrapError`) is available; adopt it in the handlers when convenient (it would change some error strings the phase-1 tests assert).
