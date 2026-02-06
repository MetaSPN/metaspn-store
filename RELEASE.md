# metaspn-store v0.1.3

M0 ingestion persistence update for `metaspn-store`.

## Highlights
- Added `write_signals(iterable)` and `write_emissions(iterable)` batch helpers.
- Added `ReplayCheckpoint` and checkpoint persistence APIs (`write_checkpoint` / `read_checkpoint`).
- Added `iter_signals_from_checkpoint(...)` for worker resume from partial progress.
- Added `build_signal_checkpoint(...)` for deterministic replay cursor advancement.

## Included APIs
- `write_signal`
- `write_emission`
- `write_signals`
- `write_emissions`
- `write_snapshot`
- `iter_signals(start, end, entity_ref=None, sources=None)`
- `iter_signals_from_checkpoint(end, checkpoint=None, start=None, entity_ref=None, sources=None)`
- `iter_emissions(start, end, entity_ref=None, emission_types=None)`
- `build_signal_checkpoint(processed_signals)`
- `write_checkpoint(name, checkpoint)`
- `read_checkpoint(name)`

## Storage Format
```text
workspace/
  store/
    signals/YYYY-MM-DD.jsonl
    emissions/YYYY-MM-DD.jsonl
    checkpoints/<name>.json
    snapshots/<name>__YYYY-MM-DDTHHMMSSZ.json
```

Each JSONL line contains one serialized envelope record.

## Quality and Validation
- Test suite passes (`12 passed`).
- Coverage includes:
  - round-trip write/read
  - replay ordering
  - time window correctness + filters
  - large-file streaming replay (10k+ scale)
  - duplicate writes (same-day and cross-day retry scenarios)
  - mixed duplicate/non-duplicate bulk write behavior
  - initial ingest from JSONL-derived envelopes
  - replay rerun under duplicate attempts
  - checkpoint-based replay resume after partial processing

## Notes
- Runtime dependencies are stdlib + `metaspn-schemas`.
- Raw event records remain append-only to preserve reproducibility.
- Duplicate detection uses a local in-memory ID index hydrated from JSONL partitions.
