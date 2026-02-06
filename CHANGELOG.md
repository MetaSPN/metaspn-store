# Changelog

All notable changes to this project are documented in this file.

## [0.1.0] - 2026-02-06
- Initial release of `metaspn-store`.
- Filesystem append-only JSONL backend for signals and emissions.
- Snapshot persistence support.
- Streaming replay API with filters by time window, entity reference, and source/type.
- Test coverage for round-trip IO, replay ordering, window filtering, and large-file streaming.

## [0.1.2] - 2026-02-06
- Added idempotent writes for `signal_id` and `emission_id`.
- Added duplicate handling policy on writes: `return_existing` (default), `ignore`, `raise`.
- Added `DuplicateEventError` for explicit duplicate failure mode.
- Replay now suppresses duplicate IDs deterministically (first-seen record wins).
- Added regression tests for same-day/cross-day duplicates and mixed duplicate bulk writes.

## [0.1.3] - 2026-02-06
- Added `write_signals(iterable)` and `write_emissions(iterable)` batch helpers.
- Added `ReplayCheckpoint` plus checkpoint persistence (`write_checkpoint` / `read_checkpoint`).
- Added `iter_signals_from_checkpoint(...)` and `build_signal_checkpoint(...)` for worker resume.
- Added M0 ingestion tests for JSONL-derived ingest, rerun with duplicates, and checkpoint resume.

## [0.1.4] - 2026-02-06
- Added M1 query helpers:
  - `get_recent_signals_by_entity(...)`
  - `get_recent_signals_by_source(...)`
  - `iter_entity_candidate_signals(...)`
  - `iter_stage_window_signals(...)`
- Added deterministic ordering/filter tests for profile/scorer/router usage.
- Added replay + checkpoint stage-window tests validating duplicate-safe rerun behavior.

## [0.1.5] - 2026-02-06
- Added M2 recommendation read-model APIs:
  - `iter_recommendation_signals(...)`
  - `get_top_recommendation_candidates(...)`
  - `get_latest_draft_signals(...)`
  - `get_latest_approval_outcomes(...)`
- Added daily digest snapshot utilities:
  - `write_daily_digest_snapshot(...)`
  - `read_daily_digest_snapshot(...)`
- Added deterministic ranking, digest snapshot round-trip, and recommendation replay checkpoint tests.

## [0.1.6] - 2026-02-06
- Added M3 learning-loop replay helpers:
  - `iter_learning_signals(...)`
  - `iter_learning_emissions(...)`
- Added outcome window query helpers:
  - `get_unresolved_outcome_signals(...)`
  - `get_expired_outcome_signals(...)`
  - `get_outcome_window_buckets(...)`
- Added calibration snapshot utilities:
  - `write_calibration_snapshot(...)`
  - `read_calibration_snapshot(...)`
- Added deterministic tests for incremental learning replay, bucketed outcome evaluation, and calibration snapshot round-trips.
