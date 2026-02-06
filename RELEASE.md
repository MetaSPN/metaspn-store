# metaspn-store v0.1.5

M2 recommendation read-model update for `metaspn-store`.

## Highlights
- Added ranking-input query helper for recommendation runs:
  - `get_top_recommendation_candidates(...)`
- Added recommendation replay helper:
  - `iter_recommendation_signals(...)` with checkpoint-aware deterministic replay
- Added daily digest snapshot read/write utilities:
  - `write_daily_digest_snapshot(...)`
  - `read_daily_digest_snapshot(...)`
- Added read helpers for draft and approval outcome views:
  - `get_latest_draft_signals(...)`
  - `get_latest_approval_outcomes(...)`

## Included APIs
- `write_signal`
- `write_emission`
- `write_signals`
- `write_emissions`
- `write_snapshot`
- `iter_signals(start, end, entity_ref=None, sources=None)`
- `iter_signals_from_checkpoint(end, checkpoint=None, start=None, entity_ref=None, sources=None)`
- `get_recent_signals_by_entity(entity_ref, limit, start=None, end=None, sources=None)`
- `get_recent_signals_by_source(source, limit, start=None, end=None, entity_ref=None)`
- `iter_entity_candidate_signals(start, end, resolved=None, sources=None)`
- `iter_stage_window_signals(stage, start, end, checkpoint=None, entity_ref=None, sources=None, payload_types=None)`
- `iter_recommendation_signals(start, end, checkpoint=None, entity_ref=None, sources=None, payload_types=None)`
- `get_top_recommendation_candidates(start, end, limit, entity_ref=None, sources=None, payload_types=None, score_field="score", unique_by_entity=True)`
- `get_latest_draft_signals(limit, start, end, entity_ref=None, sources=None, payload_types=None)`
- `get_latest_approval_outcomes(limit, start, end, entity_ref=None, emission_types=None)`
- `write_daily_digest_snapshot(day, digest)`
- `read_daily_digest_snapshot(day)`
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
    snapshots/digest__YYYY-MM-DD.json
    snapshots/<name>__YYYY-MM-DDTHHMMSSZ.json
```

Each JSONL line contains one serialized envelope record.

## Quality and Validation
- Test suite passes (`19 passed`).
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
  - recent query deterministic ordering and filter correctness
  - stage-window checkpoint replay for routing runs
  - deterministic recommendation ranking retrieval
  - digest snapshot round-trip reproducibility
  - latest draft/approval read model correctness
  - recommendation replay checkpoint duplicate safety

## Notes
- Runtime dependencies are stdlib + `metaspn-schemas`.
- Raw event records remain append-only to preserve reproducibility.
- Duplicate detection uses a local in-memory ID index hydrated from JSONL partitions.
