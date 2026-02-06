# metaspn-store v0.1.8

Token/promise persistence query update for `metaspn-store`.

## Highlights
- Added token-centric query helper:
  - `get_token_signals(...)` (filter by `token_id` / `project_entity_id`)
- Added promise lifecycle query helpers:
  - `get_promise_signals(...)` (filter by `promise_id` / `status`)
  - `get_promise_outcomes_for_window(...)`
- Added credibility report snapshot persistence:
  - `write_credibility_snapshot(...)`
  - `read_credibility_snapshot(...)`
- Preserved deterministic replay and rerun duplicate safety for token/promise workflows.

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
- `get_last_posts_by_entity(entity_ref, limit, start=None, end=None, sources=None, payload_types=None)`
- `get_ready_candidates(start, end, limit=None, entity_ref=None, sources=None, ready_field="status", ready_value="READY", payload_types=None)`
- `get_outcomes_for_window(start, end, entity_ref=None, emission_types=None)`
- `get_token_signals(start, end, token_id=None, project_entity_id=None, entity_ref=None, sources=None, payload_types=None, limit=None)`
- `get_promise_signals(start, end, promise_id=None, status=None, entity_ref=None, sources=None, payload_types=None, limit=None)`
- `get_promise_outcomes_for_window(start, end, promise_id=None, status=None, entity_ref=None, emission_types=None)`
- `iter_entity_candidate_signals(start, end, resolved=None, sources=None)`
- `iter_stage_window_signals(stage, start, end, checkpoint=None, entity_ref=None, sources=None, payload_types=None)`
- `iter_recommendation_signals(start, end, checkpoint=None, entity_ref=None, sources=None, payload_types=None)`
- `get_top_recommendation_candidates(start, end, limit, entity_ref=None, sources=None, payload_types=None, score_field="score", unique_by_entity=True)`
- `get_latest_draft_signals(limit, start, end, entity_ref=None, sources=None, payload_types=None)`
- `get_latest_approval_outcomes(limit, start, end, entity_ref=None, emission_types=None)`
- `write_daily_digest_snapshot(day, digest)`
- `read_daily_digest_snapshot(day)`
- `iter_learning_signals(start, end, checkpoint=None, entity_ref=None, sources=None, payload_types=None)`
- `iter_learning_emissions(start, end, entity_ref=None, emission_types=None)`
- `get_unresolved_outcome_signals(start, end, entity_ref=None, sources=None, pending_payload_types=None, success_emission_types=None, failure_emission_types=None)`
- `get_expired_outcome_signals(now, start, end, entity_ref=None, sources=None, pending_payload_types=None, success_emission_types=None, failure_emission_types=None, expires_at_field="expires_at")`
- `get_outcome_window_buckets(now, start, end, entity_ref=None, sources=None, pending_payload_types=None, success_emission_types=None, failure_emission_types=None, expires_at_field="expires_at")`
- `write_calibration_snapshot(day, report)`
- `read_calibration_snapshot(day)`
- `write_credibility_snapshot(day, report)`
- `read_credibility_snapshot(day)`
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
    snapshots/calibration__YYYY-MM-DD.json
    snapshots/credibility__YYYY-MM-DD.json
    snapshots/<name>__YYYY-MM-DDTHHMMSSZ.json
```

Each JSONL line contains one serialized envelope record.

## Quality and Validation
- Test suite passes (`28 passed`).
- Coverage includes:
  - deterministic replay and checkpoint resume
  - idempotent duplicate-safe writes/replay
  - ranking/digest read models
  - learning-window pending/expired/success/failure bucket evaluation
  - calibration and credibility snapshot round-trip persistence
  - token/promise query determinism and rerun duplicate safety

## Notes
- Runtime dependencies are stdlib + `metaspn-schemas`.
- Raw event records remain append-only to preserve reproducibility.
- Duplicate detection uses a local in-memory ID index hydrated from JSONL partitions.
