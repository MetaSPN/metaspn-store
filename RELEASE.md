# metaspn-store v0.1.2

Idempotency and replay-safety update for `metaspn-store`.

## Highlights
- Added idempotent writes for `SignalEnvelope` and `EmissionEnvelope` keyed by stable IDs.
- Added duplicate policy contract: `return_existing` (default), `ignore`, `raise`.
- Added `DuplicateEventError` when duplicate policy is `raise`.
- Replay iterators now suppress duplicate IDs deterministically (first-seen record wins).

## Included APIs
- `write_signal`
- `write_emission`
- `write_snapshot`
- `iter_signals(start, end, entity_ref=None, sources=None)`
- `iter_emissions(start, end, entity_ref=None, emission_types=None)`

## Storage Format
```text
workspace/
  store/
    signals/YYYY-MM-DD.jsonl
    emissions/YYYY-MM-DD.jsonl
    snapshots/<name>__YYYY-MM-DDTHHMMSSZ.json
```

Each JSONL line contains one serialized envelope record.

## Quality and Validation
- Test suite passes (`9 passed`).
- Coverage includes:
  - round-trip write/read
  - replay ordering
  - time window correctness + filters
  - large-file streaming replay (10k+ scale)
  - duplicate writes (same-day and cross-day retry scenarios)
  - mixed duplicate/non-duplicate bulk write behavior

## Notes
- Runtime dependencies are stdlib + `metaspn-schemas`.
- Raw event records remain append-only to preserve reproducibility.
- v0.1 duplicate detection uses a local in-memory ID index hydrated from JSONL partitions.
