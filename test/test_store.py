from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from metaspn_schemas import EmissionEnvelope, EntityRef, SignalEnvelope
import pytest

from metaspn_store import DuplicateEventError, FileSystemStore, ReplayCheckpoint


def _ts(day: int, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(2026, 2, day, hour, minute, tzinfo=timezone.utc)


def test_round_trip_signal_and_emission(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    ref = EntityRef(ref_type="entity_id", value="ent-1")
    signal = SignalEnvelope(
        signal_id="s-1",
        timestamp=_ts(5, 10),
        source="ingestor.a",
        payload_type="SocialPostSeen",
        payload={"post_id": "p1"},
        entity_refs=(ref,),
    )
    emission = EmissionEnvelope(
        emission_id="e-1",
        timestamp=_ts(5, 11),
        emission_type="ScoresComputed",
        payload={"score": 0.8},
        caused_by="s-1",
        entity_refs=(ref,),
    )

    store.write_signal(signal)
    store.write_emission(emission)

    read_signals = list(store.iter_signals(_ts(5, 0), _ts(5, 23, 59)))
    read_emissions = list(store.iter_emissions(_ts(5, 0), _ts(5, 23, 59)))

    assert read_signals == [signal]
    assert read_emissions == [emission]


def test_replay_ordering_across_partitions(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)

    signal_ids: list[str] = []
    for idx, ts in enumerate([_ts(5, 23, 59), _ts(6, 0, 1), _ts(6, 12, 0)], start=1):
        signal = SignalEnvelope(
            signal_id=f"s-{idx}",
            timestamp=ts,
            source="ingestor.a",
            payload_type="Synthetic",
            payload={"idx": idx},
        )
        store.write_signal(signal)
        signal_ids.append(signal.signal_id)

    replayed_ids = [s.signal_id for s in store.iter_signals(_ts(5, 0), _ts(6, 23, 59))]
    assert replayed_ids == signal_ids


def test_time_window_entity_ref_and_source_filtering(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    ref_a = EntityRef(ref_type="entity_id", value="ent-a")
    ref_b = EntityRef(ref_type="entity_id", value="ent-b")

    store.write_signal(
        SignalEnvelope(
            signal_id="s-1",
            timestamp=_ts(5, 10, 0),
            source="ingestor.a",
            payload_type="Synthetic",
            payload={},
            entity_refs=(ref_a,),
        )
    )
    store.write_signal(
        SignalEnvelope(
            signal_id="s-2",
            timestamp=_ts(5, 10, 5),
            source="ingestor.b",
            payload_type="Synthetic",
            payload={},
            entity_refs=(ref_b,),
        )
    )
    store.write_signal(
        SignalEnvelope(
            signal_id="s-3",
            timestamp=_ts(5, 11, 0),
            source="ingestor.a",
            payload_type="Synthetic",
            payload={},
            entity_refs=(ref_a,),
        )
    )

    windowed = list(
        store.iter_signals(
            start=_ts(5, 10, 1),
            end=_ts(5, 11, 0),
            entity_ref=ref_a,
            sources=["ingestor.a"],
        )
    )
    assert [s.signal_id for s in windowed] == ["s-3"]


def test_large_file_streaming_replay(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    start = _ts(5, 0, 0)
    total = 12000

    for idx in range(total):
        store.write_signal(
            SignalEnvelope(
                signal_id=f"s-{idx}",
                timestamp=start + timedelta(seconds=idx),
                source="ingestor.bulk",
                payload_type="Bulk",
                payload={"i": idx},
            )
        )

    stream = store.iter_signals(start=start, end=start + timedelta(days=1))
    first = next(stream)
    second = next(stream)
    assert first.signal_id == "s-0"
    assert second.signal_id == "s-1"

    count = 2 + sum(1 for _ in stream)
    assert count == total


def test_snapshot_write_creates_expected_file(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    path = store.write_snapshot(
        name="system_state",
        snapshot_state={"entity_count": 3, "version": "v1"},
        snapshot_time=_ts(5, 12, 0),
    )
    assert path.name == "system_state__2026-02-05T120000Z.json"
    assert path.exists()


def test_duplicate_signal_write_returns_existing_and_replay_has_single_record(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    first = SignalEnvelope(
        signal_id="s-dup",
        timestamp=_ts(5, 10, 0),
        source="ingestor.a",
        payload_type="Synthetic",
        payload={"attempt": 1},
    )
    second = SignalEnvelope(
        signal_id="s-dup",
        timestamp=_ts(6, 10, 0),
        source="ingestor.a",
        payload_type="Synthetic",
        payload={"attempt": 2},
    )

    first_path = store.write_signal(first)
    second_path = store.write_signal(second)

    assert second_path == first_path
    replayed = list(store.iter_signals(_ts(5, 0, 0), _ts(6, 23, 59)))
    assert [item.signal_id for item in replayed] == ["s-dup"]
    assert replayed[0].payload == {"attempt": 1}


def test_duplicate_emission_write_returns_existing_and_replay_has_single_record(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    first = EmissionEnvelope(
        emission_id="e-dup",
        timestamp=_ts(5, 10, 0),
        emission_type="SyntheticOutcome",
        payload={"attempt": 1},
        caused_by="s-1",
    )
    second = EmissionEnvelope(
        emission_id="e-dup",
        timestamp=_ts(6, 10, 0),
        emission_type="SyntheticOutcome",
        payload={"attempt": 2},
        caused_by="s-2",
    )

    first_path = store.write_emission(first)
    second_path = store.write_emission(second)

    assert second_path == first_path
    replayed = list(store.iter_emissions(_ts(5, 0, 0), _ts(6, 23, 59)))
    assert [item.emission_id for item in replayed] == ["e-dup"]
    assert replayed[0].payload == {"attempt": 1}


def test_mixed_bulk_duplicate_and_unique_writes(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)

    for idx in range(20):
        store.write_signal(
            SignalEnvelope(
                signal_id=f"s-{idx}",
                timestamp=_ts(5, 0, 0) + timedelta(minutes=idx),
                source="ingestor.bulk",
                payload_type="Bulk",
                payload={"idx": idx},
            )
        )

    for idx in [0, 1, 2, 2, 3, 10, 10, 19]:
        store.write_signal(
            SignalEnvelope(
                signal_id=f"s-{idx}",
                timestamp=_ts(6, 0, 0) + timedelta(minutes=idx),
                source="ingestor.bulk",
                payload_type="Bulk",
                payload={"idx": idx, "retry": True},
            )
        )

    replayed = list(store.iter_signals(_ts(5, 0, 0), _ts(6, 23, 59)))
    assert len(replayed) == 20
    assert len({item.signal_id for item in replayed}) == 20


def test_on_duplicate_raise_policy(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    signal = SignalEnvelope(
        signal_id="s-raise",
        timestamp=_ts(5, 12, 0),
        source="ingestor.x",
        payload_type="Synthetic",
        payload={},
    )
    store.write_signal(signal)
    with pytest.raises(DuplicateEventError):
        store.write_signal(signal, on_duplicate="raise")


def test_initial_ingest_from_jsonl_derived_envelopes(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    raw_lines = [
        json.dumps(
            {
                "signal_id": "s-j1",
                "timestamp": "2026-02-05T10:00:00Z",
                "source": "ingestor.jsonl",
                "payload_type": "Synthetic",
                "payload": {"k": 1},
                "schema_version": "0.1",
            }
        ),
        json.dumps(
            {
                "signal_id": "s-j2",
                "timestamp": "2026-02-05T10:01:00Z",
                "source": "ingestor.jsonl",
                "payload_type": "Synthetic",
                "payload": {"k": 2},
                "schema_version": "0.1",
            }
        ),
    ]
    signals = [SignalEnvelope.from_dict(json.loads(line)) for line in raw_lines]
    paths = store.write_signals(signals)
    assert len(paths) == 2

    replayed = list(store.iter_signals(_ts(5, 0, 0), _ts(5, 23, 59), sources=["ingestor.jsonl"]))
    assert [signal.signal_id for signal in replayed] == ["s-j1", "s-j2"]


def test_rerun_with_duplicates_stays_deterministic(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    batch_1 = [
        SignalEnvelope(
            signal_id="s-r1",
            timestamp=_ts(5, 10, 0),
            source="ingestor.m0",
            payload_type="Synthetic",
            payload={"attempt": 1},
        ),
        SignalEnvelope(
            signal_id="s-r2",
            timestamp=_ts(5, 10, 1),
            source="ingestor.m0",
            payload_type="Synthetic",
            payload={"attempt": 1},
        ),
    ]
    batch_2 = [
        SignalEnvelope(
            signal_id="s-r1",
            timestamp=_ts(6, 10, 0),
            source="ingestor.m0",
            payload_type="Synthetic",
            payload={"attempt": 2},
        ),
        SignalEnvelope(
            signal_id="s-r3",
            timestamp=_ts(6, 10, 2),
            source="ingestor.m0",
            payload_type="Synthetic",
            payload={"attempt": 1},
        ),
    ]

    store.write_signals(batch_1)
    store.write_signals(batch_2)

    replayed = list(store.iter_signals(_ts(5, 0, 0), _ts(6, 23, 59), sources=["ingestor.m0"]))
    assert [signal.signal_id for signal in replayed] == ["s-r1", "s-r2", "s-r3"]


def test_replay_from_checkpoint_after_partial_processing(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    signals = [
        SignalEnvelope(
            signal_id="s-c1",
            timestamp=_ts(5, 10, 0),
            source="ingestor.cursor",
            payload_type="Synthetic",
            payload={},
        ),
        SignalEnvelope(
            signal_id="s-c2",
            timestamp=_ts(5, 10, 0),
            source="ingestor.cursor",
            payload_type="Synthetic",
            payload={},
        ),
        SignalEnvelope(
            signal_id="s-c3",
            timestamp=_ts(5, 10, 1),
            source="ingestor.cursor",
            payload_type="Synthetic",
            payload={},
        ),
        SignalEnvelope(
            signal_id="s-c4",
            timestamp=_ts(5, 10, 2),
            source="ingestor.cursor",
            payload_type="Synthetic",
            payload={},
        ),
    ]
    store.write_signals(signals)

    first_pass = list(
        store.iter_signals_from_checkpoint(
            start=_ts(5, 0, 0),
            end=_ts(5, 23, 59),
            checkpoint=None,
            sources=["ingestor.cursor"],
        )
    )
    processed_chunk = first_pass[:2]
    checkpoint = store.build_signal_checkpoint(processed_chunk)
    assert isinstance(checkpoint, ReplayCheckpoint)
    store.write_checkpoint("ingestion_worker", checkpoint)

    loaded_checkpoint = store.read_checkpoint("ingestion_worker")
    resumed = list(
        store.iter_signals_from_checkpoint(
            start=_ts(5, 0, 0),
            end=_ts(5, 23, 59),
            checkpoint=loaded_checkpoint,
            sources=["ingestor.cursor"],
        )
    )
    assert [signal.signal_id for signal in resumed] == ["s-c3", "s-c4"]


def test_recent_queries_last_n_by_entity_and_source_are_deterministic(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    ent_a = EntityRef(ref_type="entity_id", value="ent-a")
    ent_b = EntityRef(ref_type="entity_id", value="ent-b")
    signals = [
        SignalEnvelope("s-rq1", _ts(5, 10, 0), "profile.worker", "Synthetic", {}, entity_refs=(ent_a,)),
        SignalEnvelope("s-rq2", _ts(5, 10, 1), "profile.worker", "Synthetic", {}, entity_refs=(ent_a,)),
        SignalEnvelope("s-rq3", _ts(5, 10, 2), "score.worker", "Synthetic", {}, entity_refs=(ent_a,)),
        SignalEnvelope("s-rq4", _ts(5, 10, 3), "profile.worker", "Synthetic", {}, entity_refs=(ent_b,)),
    ]
    store.write_signals(signals)

    recent_entity = store.get_recent_signals_by_entity(entity_ref=ent_a, limit=2)
    assert [signal.signal_id for signal in recent_entity] == ["s-rq3", "s-rq2"]

    recent_source = store.get_recent_signals_by_source(source="profile.worker", limit=3)
    assert [signal.signal_id for signal in recent_source] == ["s-rq4", "s-rq2", "s-rq1"]


def test_entity_candidate_stream_filters_resolved_and_unresolved(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    ent = EntityRef(ref_type="entity_id", value="ent-x")
    store.write_signals(
        [
            SignalEnvelope("s-cand1", _ts(5, 10, 0), "resolver.input", "SocialPostSeen", {}, entity_refs=()),
            SignalEnvelope("s-cand2", _ts(5, 10, 1), "resolver.worker", "EntityResolved", {}, entity_refs=()),
            SignalEnvelope("s-cand3", _ts(5, 10, 2), "resolver.worker", "Synthetic", {}, entity_refs=(ent,)),
        ]
    )

    unresolved = list(store.iter_entity_candidate_signals(start=_ts(5, 0, 0), end=_ts(5, 23, 59), resolved=False))
    resolved = list(store.iter_entity_candidate_signals(start=_ts(5, 0, 0), end=_ts(5, 23, 59), resolved=True))

    assert [signal.signal_id for signal in unresolved] == ["s-cand1"]
    assert [signal.signal_id for signal in resolved] == ["s-cand2", "s-cand3"]


def test_stage_window_replay_with_checkpoint_is_duplicate_safe(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    store.write_signals(
        [
            SignalEnvelope("s-sw1", _ts(5, 10, 0), "route.worker", "RouteInput", {}),
            SignalEnvelope("s-sw2", _ts(5, 10, 0), "route.worker", "RouteInput", {}),
            SignalEnvelope("s-sw3", _ts(5, 10, 1), "route.worker", "RouteInput", {}),
            SignalEnvelope("s-sw4", _ts(5, 10, 2), "score.worker", "RouteInput", {}),
        ]
    )
    # duplicate attempt should stay idempotent
    store.write_signal(SignalEnvelope("s-sw2", _ts(5, 11, 0), "route.worker", "RouteInput", {}))

    first_pass = list(
        store.iter_stage_window_signals(
            stage="route",
            start=_ts(5, 0, 0),
            end=_ts(5, 23, 59),
            payload_types=["RouteInput"],
        )
    )
    assert [signal.signal_id for signal in first_pass] == ["s-sw1", "s-sw2", "s-sw3"]

    checkpoint = store.build_signal_checkpoint(first_pass[:2])
    resumed = list(
        store.iter_stage_window_signals(
            stage="route",
            start=_ts(5, 0, 0),
            end=_ts(5, 23, 59),
            checkpoint=checkpoint,
            payload_types=["RouteInput"],
        )
    )
    assert [signal.signal_id for signal in resumed] == ["s-sw3"]


def test_recommendation_candidate_ranking_is_deterministic(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    ent_a = EntityRef(ref_type="entity_id", value="ent-a")
    ent_b = EntityRef(ref_type="entity_id", value="ent-b")
    ent_c = EntityRef(ref_type="entity_id", value="ent-c")

    store.write_signals(
        [
            SignalEnvelope(
                "s-rec1",
                _ts(5, 9, 0),
                "score.worker",
                "RecommendationCandidate",
                {"score": 0.7},
                entity_refs=(ent_a,),
            ),
            SignalEnvelope(
                "s-rec2",
                _ts(5, 9, 1),
                "score.worker",
                "RecommendationCandidate",
                {"score": 0.9},
                entity_refs=(ent_b,),
            ),
            SignalEnvelope(
                "s-rec3",
                _ts(5, 9, 2),
                "score.worker",
                "RecommendationCandidate",
                {"score": 0.85},
                entity_refs=(ent_c,),
            ),
            # Duplicate entity with lower score should not displace top score for ent-b.
            SignalEnvelope(
                "s-rec4",
                _ts(5, 9, 3),
                "score.worker",
                "RecommendationCandidate",
                {"score": 0.1},
                entity_refs=(ent_b,),
            ),
        ]
    )

    ranked = store.get_top_recommendation_candidates(
        start=_ts(5, 0, 0),
        end=_ts(5, 23, 59),
        limit=3,
        sources=["score.worker"],
        payload_types=["RecommendationCandidate"],
    )
    assert [signal.signal_id for signal in ranked] == ["s-rec2", "s-rec3", "s-rec1"]


def test_daily_digest_snapshot_round_trip(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    digest = {
        "top_candidates": ["ent-a", "ent-b"],
        "drafts_ready": 2,
        "approvals_pending": 1,
    }
    path = store.write_daily_digest_snapshot(day="2026-02-05", digest=digest)
    assert path.name == "digest__2026-02-05.json"

    loaded = store.read_daily_digest_snapshot("2026-02-05")
    assert loaded is not None
    assert loaded["day"] == "2026-02-05"
    assert loaded["digest"] == digest


def test_latest_drafts_and_approval_outcomes_reads(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    ent = EntityRef(ref_type="entity_id", value="ent-draft")
    store.write_signals(
        [
            SignalEnvelope("s-d1", _ts(5, 10, 0), "draft.worker", "DraftGenerated", {}, entity_refs=(ent,)),
            SignalEnvelope("s-d2", _ts(5, 10, 1), "draft.worker", "DraftGenerated", {}, entity_refs=(ent,)),
            SignalEnvelope("s-d3", _ts(5, 10, 2), "draft.worker", "DraftEdited", {}, entity_refs=(ent,)),
        ]
    )
    store.write_emissions(
        [
            EmissionEnvelope("e-a1", _ts(5, 11, 0), "DraftApproved", {"ok": True}, caused_by="s-d1", entity_refs=(ent,)),
            EmissionEnvelope("e-a2", _ts(5, 11, 2), "DraftRejected", {"ok": False}, caused_by="s-d2", entity_refs=(ent,)),
        ]
    )

    drafts = store.get_latest_draft_signals(
        limit=2,
        start=_ts(5, 0, 0),
        end=_ts(5, 23, 59),
        entity_ref=ent,
        sources=["draft.worker"],
    )
    approvals = store.get_latest_approval_outcomes(
        limit=2,
        start=_ts(5, 0, 0),
        end=_ts(5, 23, 59),
        entity_ref=ent,
        emission_types=["DraftApproved", "DraftRejected"],
    )

    assert [signal.signal_id for signal in drafts] == ["s-d3", "s-d2"]
    assert [emission.emission_id for emission in approvals] == ["e-a2", "e-a1"]


def test_recommendation_replay_from_checkpoint_is_duplicate_safe(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    store.write_signals(
        [
            SignalEnvelope("s-rp1", _ts(5, 12, 0), "recommend.worker", "RecommendationCandidate", {"score": 0.3}),
            SignalEnvelope("s-rp2", _ts(5, 12, 0), "recommend.worker", "RecommendationCandidate", {"score": 0.4}),
            SignalEnvelope("s-rp3", _ts(5, 12, 1), "recommend.worker", "RecommendationCandidate", {"score": 0.5}),
        ]
    )
    store.write_signal(
        SignalEnvelope("s-rp2", _ts(5, 12, 5), "recommend.worker", "RecommendationCandidate", {"score": 0.9})
    )

    first_batch = list(
        store.iter_recommendation_signals(
            start=_ts(5, 0, 0),
            end=_ts(5, 23, 59),
            sources=["recommend.worker"],
            payload_types=["RecommendationCandidate"],
        )
    )
    checkpoint = store.build_signal_checkpoint(first_batch[:2])
    resumed = list(
        store.iter_recommendation_signals(
            start=_ts(5, 0, 0),
            end=_ts(5, 23, 59),
            checkpoint=checkpoint,
            sources=["recommend.worker"],
            payload_types=["RecommendationCandidate"],
        )
    )
    assert [signal.signal_id for signal in resumed] == ["s-rp3"]


def test_learning_replay_with_checkpoint_is_deterministic(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    store.write_signals(
        [
            SignalEnvelope(
                "s-l1",
                _ts(5, 13, 0),
                "learning.worker",
                "OutcomePending",
                {"expires_at": "2026-02-05T14:00:00Z"},
            ),
            SignalEnvelope(
                "s-l2",
                _ts(5, 13, 0),
                "learning.worker",
                "OutcomePending",
                {"expires_at": "2026-02-05T15:00:00Z"},
            ),
            SignalEnvelope(
                "s-l3",
                _ts(5, 13, 1),
                "learning.worker",
                "OutcomePending",
                {"expires_at": "2026-02-05T16:00:00Z"},
            ),
        ]
    )
    store.write_signal(
        SignalEnvelope(
            "s-l2",
            _ts(5, 14, 0),
            "learning.worker",
            "OutcomePending",
            {"expires_at": "2026-02-05T17:00:00Z"},
        )
    )

    first = list(
        store.iter_learning_signals(
            start=_ts(5, 0, 0),
            end=_ts(5, 23, 59),
            sources=["learning.worker"],
            payload_types=["OutcomePending"],
        )
    )
    checkpoint = store.build_signal_checkpoint(first[:2])
    resumed = list(
        store.iter_learning_signals(
            start=_ts(5, 0, 0),
            end=_ts(5, 23, 59),
            checkpoint=checkpoint,
            sources=["learning.worker"],
            payload_types=["OutcomePending"],
        )
    )
    assert [signal.signal_id for signal in first] == ["s-l1", "s-l2", "s-l3"]
    assert [signal.signal_id for signal in resumed] == ["s-l3"]


def test_outcome_window_buckets_pending_success_failure_expired(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    store.write_signals(
        [
            SignalEnvelope(
                "s-o1",
                _ts(5, 10, 0),
                "learning.worker",
                "OutcomePending",
                {"expires_at": "2026-02-05T11:00:00Z"},
            ),
            SignalEnvelope(
                "s-o2",
                _ts(5, 10, 1),
                "learning.worker",
                "OutcomePending",
                {"expires_at": "2026-02-05T20:00:00Z"},
            ),
            SignalEnvelope(
                "s-o3",
                _ts(5, 10, 2),
                "learning.worker",
                "OutcomePending",
                {"expires_at": "2026-02-05T20:00:00Z"},
            ),
            SignalEnvelope(
                "s-o4",
                _ts(5, 10, 3),
                "learning.worker",
                "OutcomePending",
                {"expires_at": "2026-02-05T20:00:00Z"},
            ),
        ]
    )
    store.write_emissions(
        [
            EmissionEnvelope("e-o1", _ts(5, 10, 20), "OutcomeSuccess", {"result": "ok"}, caused_by="s-o3"),
            EmissionEnvelope("e-o2", _ts(5, 10, 25), "OutcomeFailure", {"result": "bad"}, caused_by="s-o4"),
        ]
    )

    buckets = store.get_outcome_window_buckets(
        now=_ts(5, 12, 0),
        start=_ts(5, 0, 0),
        end=_ts(5, 23, 59),
        sources=["learning.worker"],
        pending_payload_types=["OutcomePending"],
        success_emission_types=["OutcomeSuccess"],
        failure_emission_types=["OutcomeFailure"],
    )
    assert [signal.signal_id for signal in buckets["pending"]] == ["s-o2"]
    assert [signal.signal_id for signal in buckets["expired"]] == ["s-o1"]
    assert [emission.emission_id for emission in buckets["success"]] == ["e-o1"]
    assert [emission.emission_id for emission in buckets["failure"]] == ["e-o2"]


def test_calibration_snapshot_round_trip(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    report = {
        "model": "draft_v2",
        "accuracy": 0.82,
        "window_days": 30,
    }
    path = store.write_calibration_snapshot(day="2026-02-05", report=report)
    assert path.name == "calibration__2026-02-05.json"

    loaded = store.read_calibration_snapshot("2026-02-05")
    assert loaded is not None
    assert loaded["day"] == "2026-02-05"
    assert loaded["report"] == report


def test_demo_last_posts_by_entity_and_ready_candidates(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    ent = EntityRef(ref_type="entity_id", value="ent-demo")
    store.write_signals(
        [
            SignalEnvelope(
                "s-dp1",
                _ts(5, 9, 0),
                "ingest.social",
                "SocialPostSeen",
                {"text": "a"},
                entity_refs=(ent,),
            ),
            SignalEnvelope(
                "s-dp2",
                _ts(5, 9, 1),
                "score.worker",
                "RecommendationCandidate",
                {"status": "READY", "score": 0.8},
                entity_refs=(ent,),
            ),
            SignalEnvelope(
                "s-dp3",
                _ts(5, 9, 2),
                "score.worker",
                "RecommendationCandidate",
                {"status": "HOLD", "score": 0.9},
                entity_refs=(ent,),
            ),
            SignalEnvelope(
                "s-dp4",
                _ts(5, 9, 3),
                "ingest.social",
                "SocialPostSeen",
                {"text": "b"},
                entity_refs=(ent,),
            ),
            SignalEnvelope(
                "s-dp5",
                _ts(5, 9, 4),
                "score.worker",
                "RecommendationCandidate",
                {"status": "READY", "score": 0.85},
                entity_refs=(ent,),
            ),
        ]
    )

    posts = store.get_last_posts_by_entity(entity_ref=ent, limit=2)
    ready = store.get_ready_candidates(
        start=_ts(5, 0, 0),
        end=_ts(5, 23, 59),
        sources=["score.worker"],
        payload_types=["RecommendationCandidate"],
    )
    assert [signal.signal_id for signal in posts] == ["s-dp4", "s-dp1"]
    assert [signal.signal_id for signal in ready] == ["s-dp5", "s-dp2"]


def test_demo_outcomes_for_window_and_rerun_no_duplicate_artifacts(tmp_path: Path) -> None:
    store = FileSystemStore(tmp_path)
    ent = EntityRef(ref_type="entity_id", value="ent-outcome")
    store.write_signals(
        [
            SignalEnvelope(
                "s-do1",
                _ts(5, 10, 0),
                "score.worker",
                "RecommendationCandidate",
                {"status": "READY", "score": 0.7},
                entity_refs=(ent,),
            ),
            SignalEnvelope(
                "s-do2",
                _ts(5, 10, 1),
                "score.worker",
                "RecommendationCandidate",
                {"status": "READY", "score": 0.6},
                entity_refs=(ent,),
            ),
        ]
    )
    store.write_emissions(
        [
            EmissionEnvelope("e-do1", _ts(5, 11, 0), "OutcomeSuccess", {"ok": True}, caused_by="s-do1", entity_refs=(ent,)),
            EmissionEnvelope("e-do2", _ts(5, 11, 1), "OutcomeFailure", {"ok": False}, caused_by="s-do2", entity_refs=(ent,)),
        ]
    )

    outcomes_1 = store.get_outcomes_for_window(
        start=_ts(5, 0, 0),
        end=_ts(5, 23, 59),
        entity_ref=ent,
        emission_types=["OutcomeSuccess", "OutcomeFailure"],
    )
    ready_1 = store.get_ready_candidates(
        start=_ts(5, 0, 0),
        end=_ts(5, 23, 59),
        entity_ref=ent,
        sources=["score.worker"],
        payload_types=["RecommendationCandidate"],
    )

    digest_payload = {"ready_ids": [signal.signal_id for signal in ready_1]}
    calibration_payload = {"outcomes": [emission.emission_id for emission in outcomes_1]}
    path_digest_1 = store.write_daily_digest_snapshot(day="2026-02-05", digest=digest_payload)
    path_cal_1 = store.write_calibration_snapshot(day="2026-02-05", report=calibration_payload)

    # Simulate rerun of same day.
    outcomes_2 = store.get_outcomes_for_window(
        start=_ts(5, 0, 0),
        end=_ts(5, 23, 59),
        entity_ref=ent,
        emission_types=["OutcomeSuccess", "OutcomeFailure"],
    )
    ready_2 = store.get_ready_candidates(
        start=_ts(5, 0, 0),
        end=_ts(5, 23, 59),
        entity_ref=ent,
        sources=["score.worker"],
        payload_types=["RecommendationCandidate"],
    )
    path_digest_2 = store.write_daily_digest_snapshot(day="2026-02-05", digest=digest_payload)
    path_cal_2 = store.write_calibration_snapshot(day="2026-02-05", report=calibration_payload)

    assert [emission.emission_id for emission in outcomes_1] == [emission.emission_id for emission in outcomes_2]
    assert [signal.signal_id for signal in ready_1] == [signal.signal_id for signal in ready_2]
    assert path_digest_1 == path_digest_2
    assert path_cal_1 == path_cal_2
    assert sorted(path_digest_1.parent.glob("digest__2026-02-05*.json")) == [path_digest_1]
    assert sorted(path_cal_1.parent.glob("calibration__2026-02-05*.json")) == [path_cal_1]
