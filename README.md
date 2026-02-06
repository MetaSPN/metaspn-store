# metaspn-store

`metaspn-store` provides a minimal append-only event store for MetaSPN signals and emissions.

## Features
- Filesystem JSONL backend (partitioned by UTC date)
- Append-only writes for signals and emissions
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

## Release
```bash
python -m pip install -e ".[dev]"
pytest -q
python -m build
python -m twine check dist/*
python -m twine upload dist/*
```
