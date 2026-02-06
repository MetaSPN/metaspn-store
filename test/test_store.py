from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from metaspn_schemas import EmissionEnvelope, EntityRef, SignalEnvelope

from metaspn_store import FileSystemStore


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
