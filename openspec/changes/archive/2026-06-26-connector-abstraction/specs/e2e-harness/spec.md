## ADDED Requirements

### Requirement: GCS and Azure Blob have emulator-backed E2E
The E2E suite SHALL cover Google Cloud Storage and Azure Blob Storage against
local emulators (e.g. fake-gcs-server and azurite), querying objects through the
real `dataql` binary. These were previously deferred because the handlers could
not target an emulator.

#### Scenario: Query a GCS object via the emulator
- **WHEN** the E2E suite runs with a GCS emulator and a seeded object
- **THEN** `dataql` reads `gs://<bucket>/<object>` from the emulator and a query
  returns the expected rows

#### Scenario: Query an Azure Blob via the emulator
- **WHEN** the E2E suite runs with azurite and a seeded blob
- **THEN** `dataql` reads the blob and a query returns the expected rows
