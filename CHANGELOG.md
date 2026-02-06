# Changelog

All notable changes to this project are documented in this file.

## [0.1.0] - 2026-02-06
- Initial release of `metaspn-store`.
- Filesystem append-only JSONL backend for signals and emissions.
- Snapshot persistence support.
- Streaming replay API with filters by time window, entity reference, and source/type.
- Test coverage for round-trip IO, replay ordering, window filtering, and large-file streaming.

## [0.1.1] - 2026-02-06
- Added idempotent writes for `signal_id` and `emission_id`.
- Added duplicate handling policy on writes: `return_existing` (default), `ignore`, `raise`.
- Added `DuplicateEventError` for explicit duplicate failure mode.
- Replay now suppresses duplicate IDs deterministically (first-seen record wins).
- Added regression tests for same-day/cross-day duplicates and mixed duplicate bulk writes.
