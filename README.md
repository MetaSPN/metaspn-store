# metaspn-store

`metaspn-store` provides a minimal append-only event store for MetaSPN signals and emissions.

## Features
- Filesystem JSONL backend (partitioned by UTC date)
- Append-only writes for signals and emissions
- Idempotent event writes keyed by `signal_id` / `emission_id`
- Snapshot writes for deterministic state rebuild checkpoints
- Streaming replay by time window, entity reference, and source

## Layout
```text
workspace/
  store/
    signals/
      2026-02-05.jsonl
    emissions/
      2026-02-05.jsonl
    snapshots/
      system_state__2026-02-05T120000Z.json
```

## Idempotency Contract
- `write_signal(..., on_duplicate="return_existing")` and `write_emission(..., on_duplicate="return_existing")` are idempotent by stable ID.
- Duplicate policies:
  - `return_existing` (default): do not append; return the path of the first existing record.
  - `ignore`: alias for `return_existing`.
  - `raise`: raise `DuplicateEventError`.
- Duplicate detection scans existing JSONL partitions for matching IDs.
- Replay iterators suppress duplicate IDs and yield the first-seen record deterministically.

## Performance Note
- Duplicate detection in v0.1 is file-scan based for local-first simplicity.
- For large histories with high write throughput, this is a correctness-first tradeoff.
- A manifest/hash index strategy is planned for a future version.

## Release
```bash
python -m pip install -e ".[dev]"
pytest -q
python -m build
python -m twine check dist/*
python -m twine upload dist/*
```
