# metaspn-store v0.1.0

Initial release of `metaspn-store`, the MetaSPN durability layer for append-only signals/emissions and deterministic replay.

## Highlights
- Filesystem JSONL backend partitioned by UTC day.
- Append-only writes for `SignalEnvelope` and `EmissionEnvelope`.
- Snapshot persistence for point-in-time state capture.
- Streaming replay APIs by time window with optional filters.

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
- Test suite passes (`5 passed`).
- Coverage includes:
  - round-trip write/read
  - replay ordering
  - time window correctness + filters
  - large-file streaming replay (10k+ scale)

## Notes
- Runtime dependencies are stdlib + `metaspn-schemas`.
- Raw event records remain append-only to preserve reproducibility.
