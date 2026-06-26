## ADDED Requirements

### Requirement: End-to-end coverage of every shipped file format
The E2E suite SHALL exercise every shipped file format by running the real
`dataql` binary against fixture files and asserting the query results. The
covered formats MUST include CSV, JSON, JSONL, Parquet, Excel, XML, YAML, Avro,
and ORC.

#### Scenario: Each format is queried end-to-end
- **WHEN** the E2E format matrix runs
- **THEN** for each supported format, the binary loads a fixture and a SQL query
  returns the expected rows, proving the format works through the full pipeline

#### Scenario: A format regression is caught
- **WHEN** a change breaks reading of a supported format
- **THEN** the corresponding format-matrix E2E test fails
