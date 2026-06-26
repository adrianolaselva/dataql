## Context

Source handling today:
- **Remote file sources** (`s3handler`, `gcshandler`, `azurehandler`,
  `urlhandler`, `stdinhandler`) each expose ad-hoc methods and download a URI to
  a local temp file; `internal/dataql/dataql.go` holds one field per handler and
  hard-codes the dispatch.
- **DB connectors** implement `dbconnector.Connector`; the `filehandler/database`
  + `filehandler/mongodb` handlers drive them.
- **Message queues** use `mqreader`.

`dataql.go` (the core, ~14% coverage) is the only place that knows about all
remote handlers. S3 already supports a custom endpoint (`AWS_ENDPOINT_URL`),
which is why S3 has E2E and GCS/Azure don't.

## Goals / Non-Goals

**Goals:**
- One documented `Source` contract + registry for remote file sources; adding a
  source no longer edits the core.
- GCS/Azure work against emulators, with E2E.
- Consistent error wrapping; meaningfully higher coverage on the touched packages.
- Zero user-facing behavior change.

**Non-Goals:**
- Folding DB `Connector` and `mqreader` into the same interface now (they differ
  enough that forcing them is premature; a follow-up can unify).
- New sources / streaming.

## Decisions

### D1. Phase the work; tests before the core refactor
The core engine has ~14% coverage, so a blind refactor is risky. Order:
**(1)** add tests + a shared error helper (no behavior change), **(2)** emulator
support + GCS/Azure E2E (additive), **(3)** introduce the `Source` registry and
migrate `dataql.go` to it — now guarded by the tests and E2E from phases 1–2.
Each phase is independently shippable and revertible.

### D2. `Source` interface scoped to remote *file* sources first
```go
type Source interface {
    Scheme() string                 // e.g. "s3", "gs", "az", "http"
    Matches(uri string) bool        // claims a URI
    Fetch(ctx, uri string) (localPath string, cleanup func(), err error)
}
```
This matches what the five remote handlers already do (download → local path).
The core resolves a URI by asking the registry, then hands the local path to the
existing format `FileHandler`. DB and MQ sources keep their current interfaces
and dispatch (they don't fit "download to a file"); the registry is additive.
Alternative (one mega-interface for files+DB+MQ) rejected — it would force
unnatural shapes and a much riskier rewrite.

### D3. GCS/Azure emulator support via env, mirroring S3
- **GCS**: the Google client honors `STORAGE_EMULATOR_HOST`; create the client
  with `option.WithoutAuthentication()` when it is set, so `fake-gcs-server`
  works. Normal use is unchanged.
- **Azure**: support an emulator by honoring the `AZURE_STORAGE_CONNECTION_STRING`
  (azurite's well-known connection string carries `BlobEndpoint=...`), and/or an
  explicit endpoint env. Normal `account.blob.core.windows.net` use is unchanged.

### D4. Shared error helper
A small `source.WrapError(scheme, op, uri, err)` so every source reports errors
the same way (`gs fetch "gs://...": <cause>`), replacing today's ad-hoc strings.

## Risks / Trade-offs

- **Refactoring the under-tested core (phase 3)** → mitigated by doing phases 1–2
  first; the full unit + E2E suite (incl. the new GCS/Azure/S3 tests) guards the
  migration. Keep the diff mechanical (move dispatch into a registry, same logic).
- **Emulator config could leak into prod paths** → endpoint is read from env only
  when explicitly set; default behavior and the offline smoke are unchanged.
- **fake-gcs-server / azurite flakiness in CI** → keep GCS/Azure E2E under the
  existing advisory `e2e` job until stable, like the other suites.

## Migration Plan

1. Land phase 1 (tests + error helper) — safe, coverage rises.
2. Land phase 2 (emulator support + compose services + test-gcs.sh/test-azure.sh).
3. Land phase 3 (registry + `dataql.go` migration), verifying the full suite stays
   green and behavior is identical. Each phase can be its own commit/PR.
Rollback: phases are independent; revert the offending phase.

## Open Questions

- Exact azurite wiring (connection string vs explicit endpoint) — confirm against
  the azblob SDK version during phase 2.
- Whether to also register `urlhandler`/`stdinhandler` in the registry in phase 3
  or leave them (they're already simple) — decide during implementation.
