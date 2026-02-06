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
