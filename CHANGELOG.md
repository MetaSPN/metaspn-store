# Changelog

All notable changes to this project are documented in this file.

## [0.1.0] - 2026-02-06
- Initial release of `metaspn-store`.
- Filesystem append-only JSONL backend for signals and emissions.
- Snapshot persistence support.
- Streaming replay API with filters by time window, entity reference, and source/type.
- Test coverage for round-trip IO, replay ordering, window filtering, and large-file streaming.
