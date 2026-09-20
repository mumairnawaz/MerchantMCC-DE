# 18 — Testing Strategy

Status: **PLANNED for this repository's published code** (see
[19-project-status.md](19-project-status.md) — ingestion code, and its tests, are staged
for a later phase, not this documentation-only publication).

## Design principle already committed to

Every ingestion module is designed to separate `fetch()` (network I/O) from
`parse_for_metadata()` (a pure function). This is what makes parsing logic testable without
any live network access — tests run against small embedded fixtures, never a real API call.
This separation is a testing-strategy decision made before any test is written, not an
incidental side effect of the code structure.

## Planned coverage

- Successful parsing of a well-formed response
- Required-field detection
- Empty/malformed-response handling
- Duplicate-key handling, including sources (like ISO 4217) where duplication is expected
  and must not fail validation
- Field-pattern validation (e.g. BIN digit length, ISO country code format)
- Deterministic output (same input → same output) for every pure parsing function

## Planned tooling

pytest, matching the ingestion framework's own design — no heavier testing framework is
planned unless a real gap in this approach appears.
