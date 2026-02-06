# metaspn-store

`metaspn-store` provides a minimal append-only event store for MetaSPN signals and emissions.

## Features
- Filesystem JSONL backend (partitioned by UTC date)
- Append-only writes for signals and emissions
- Idempotent event writes keyed by `signal_id` / `emission_id`
- Batched signal/emission write helpers
- Replay checkpoint utilities for worker resume
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
    checkpoints/
      ingestion_worker.json
    snapshots/
      system_state__2026-02-05T120000Z.json
```

## Idempotency Contract
- `write_signal(..., on_duplicate="return_existing")` and `write_emission(..., on_duplicate="return_existing")` are idempotent by stable ID.
- Duplicate policies:
  - `return_existing` (default): do not append; return the path of the first existing record.
  - `ignore`: alias for `return_existing`.
  - `raise`: raise `DuplicateEventError`.
- Duplicate detection uses a lazily-hydrated in-memory ID index sourced from existing JSONL partitions.
- Replay iterators suppress duplicate IDs and yield the first-seen record deterministically.

## M0 Ingestion Usage
```python
from datetime import datetime, timezone
from metaspn_store import FileSystemStore

store = FileSystemStore("/workspace")

# Batch ingest parsed envelopes
store.write_signals(signal_batch)
store.write_emissions(emission_batch)

# Resume-friendly replay for workers
checkpoint = store.read_checkpoint("ingestion_worker")
to_process = store.iter_signals_from_checkpoint(
    start=datetime(2026, 2, 1, tzinfo=timezone.utc),
    end=datetime.now(timezone.utc),
    checkpoint=checkpoint,
)

processed = process_batch(to_process)
next_checkpoint = store.build_signal_checkpoint(processed)
if next_checkpoint is not None:
    store.write_checkpoint("ingestion_worker", next_checkpoint)
```

## M1 Routing/Profile/Scoring Usage
```python
from datetime import datetime, timezone
from metaspn_store import FileSystemStore

store = FileSystemStore("/workspace")
window_start = datetime(2026, 2, 1, tzinfo=timezone.utc)
window_end = datetime.now(timezone.utc)

# Recent context for profile/scorer
recent_profile = store.get_recent_signals_by_entity(entity_ref=entity_ref, limit=20)
recent_route_source = store.get_recent_signals_by_source(source="route.worker", limit=50)

# Resolver candidate streams
resolved_candidates = store.iter_entity_candidate_signals(
    start=window_start,
    end=window_end,
    resolved=True,
)
unresolved_candidates = store.iter_entity_candidate_signals(
    start=window_start,
    end=window_end,
    resolved=False,
)

# Stage-window replay with checkpoint resume
checkpoint = store.read_checkpoint("route_worker")
batch = list(
    store.iter_stage_window_signals(
        stage="route",
        start=window_start,
        end=window_end,
        checkpoint=checkpoint,
        payload_types=["RouteInput"],
    )
)
next_checkpoint = store.build_signal_checkpoint(batch)
if next_checkpoint is not None:
    store.write_checkpoint("route_worker", next_checkpoint)
```

## M2 Recommendation/Digest Usage
```python
from datetime import datetime, timezone
from metaspn_store import FileSystemStore

store = FileSystemStore("/workspace")
window_start = datetime(2026, 2, 1, tzinfo=timezone.utc)
window_end = datetime.now(timezone.utc)

# Ranking candidates for recommendations
top_candidates = store.get_top_recommendation_candidates(
    start=window_start,
    end=window_end,
    limit=25,
    sources=["score.worker"],
    payload_types=["RecommendationCandidate"],
    score_field="score",
)

# Daily digest snapshots
store.write_daily_digest_snapshot(
    day=window_end,
    digest={"top_candidates": [item.signal_id for item in top_candidates]},
)
digest = store.read_daily_digest_snapshot(window_end)

# Draft/approval read models
latest_drafts = store.get_latest_draft_signals(
    limit=20,
    start=window_start,
    end=window_end,
    sources=["draft.worker"],
)
latest_approval_outcomes = store.get_latest_approval_outcomes(
    limit=20,
    start=window_start,
    end=window_end,
    emission_types=["DraftApproved", "DraftRejected"],
)

# Replay recommendation events with checkpoint safety
checkpoint = store.read_checkpoint("recommend_worker")
events = list(
    store.iter_recommendation_signals(
        start=window_start,
        end=window_end,
        checkpoint=checkpoint,
        sources=["recommend.worker"],
        payload_types=["RecommendationCandidate"],
    )
)
next_checkpoint = store.build_signal_checkpoint(events)
if next_checkpoint is not None:
    store.write_checkpoint("recommend_worker", next_checkpoint)
```

## Release
```bash
python -m pip install -e ".[dev]"
pytest -q
python -m build
python -m twine check dist/*
python -m twine upload dist/*
```
