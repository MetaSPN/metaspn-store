from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator, Literal

from metaspn_schemas import EmissionEnvelope, EntityRef, SignalEnvelope

DuplicatePolicy = Literal["ignore", "return_existing", "raise"]


class DuplicateEventError(ValueError):
    """Raised when a duplicate event ID is written with on_duplicate='raise'."""


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iter_days(start_day: date, end_day: date) -> Iterator[date]:
    current = start_day
    while current <= end_day:
        yield current
        current += timedelta(days=1)


class FileSystemStore:
    """Append-only filesystem store for SignalEnvelope and EmissionEnvelope."""

    def __init__(self, workspace: str | Path) -> None:
        self.workspace = Path(workspace)
        self.store_root = self.workspace / "store"
        self.signals_dir = self.store_root / "signals"
        self.emissions_dir = self.store_root / "emissions"
        self.snapshots_dir = self.store_root / "snapshots"
        self._signal_index: dict[str, Path] | None = None
        self._emission_index: dict[str, Path] | None = None
        self._ensure_dirs()

    def _ensure_dirs(self) -> None:
        self.signals_dir.mkdir(parents=True, exist_ok=True)
        self.emissions_dir.mkdir(parents=True, exist_ok=True)
        self.snapshots_dir.mkdir(parents=True, exist_ok=True)

    def _build_index(self, base_dir: Path, id_field: str) -> dict[str, Path]:
        index: dict[str, Path] = {}
        for partition in sorted(base_dir.glob("*.jsonl")):
            with partition.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    record = json.loads(line)
                    record_id = record.get(id_field)
                    if isinstance(record_id, str) and record_id not in index:
                        index[record_id] = partition
        return index

    def _get_signal_index(self) -> dict[str, Path]:
        if self._signal_index is None:
            self._signal_index = self._build_index(self.signals_dir, "signal_id")
        return self._signal_index

    def _get_emission_index(self) -> dict[str, Path]:
        if self._emission_index is None:
            self._emission_index = self._build_index(self.emissions_dir, "emission_id")
        return self._emission_index

    def _resolve_duplicate(
        self,
        *,
        existing_path: Path | None,
        on_duplicate: DuplicatePolicy,
        id_field: str,
        id_value: str,
    ) -> Path | None:
        if existing_path is None:
            return None
        if on_duplicate in ("ignore", "return_existing"):
            return existing_path
        if on_duplicate == "raise":
            raise DuplicateEventError(f"Duplicate {id_field}={id_value!r} already exists in {existing_path}")
        raise ValueError(f"Unsupported duplicate policy: {on_duplicate!r}")

    def write_signal(
        self,
        signal: SignalEnvelope,
        *,
        on_duplicate: DuplicatePolicy = "return_existing",
    ) -> Path:
        """Append a signal and return the written partition, or existing one for duplicates."""
        if not signal.signal_id:
            raise ValueError("signal_id is required for stable IDs")
        if not signal.schema_version:
            raise ValueError("schema_version is required")

        signal_index = self._get_signal_index()
        existing = signal_index.get(signal.signal_id)
        duplicate_path = self._resolve_duplicate(
            existing_path=existing,
            on_duplicate=on_duplicate,
            id_field="signal_id",
            id_value=signal.signal_id,
        )
        if duplicate_path is not None:
            return duplicate_path

        signal_ts = _ensure_utc(signal.timestamp)
        destination = self.signals_dir / f"{signal_ts.date().isoformat()}.jsonl"
        with destination.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(signal.to_dict(), sort_keys=True, separators=(",", ":")))
            handle.write("\n")
        signal_index[signal.signal_id] = destination
        return destination

    def write_emission(
        self,
        emission: EmissionEnvelope,
        *,
        on_duplicate: DuplicatePolicy = "return_existing",
    ) -> Path:
        """Append an emission and return the written partition, or existing one for duplicates."""
        if not emission.emission_id:
            raise ValueError("emission_id is required for stable IDs")
        if not emission.schema_version:
            raise ValueError("schema_version is required")

        emission_index = self._get_emission_index()
        existing = emission_index.get(emission.emission_id)
        duplicate_path = self._resolve_duplicate(
            existing_path=existing,
            on_duplicate=on_duplicate,
            id_field="emission_id",
            id_value=emission.emission_id,
        )
        if duplicate_path is not None:
            return duplicate_path

        emission_ts = _ensure_utc(emission.timestamp)
        destination = self.emissions_dir / f"{emission_ts.date().isoformat()}.jsonl"
        with destination.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(emission.to_dict(), sort_keys=True, separators=(",", ":")))
            handle.write("\n")
        emission_index[emission.emission_id] = destination
        return destination

    def write_snapshot(
        self,
        name: str,
        snapshot_state: dict[str, Any],
        snapshot_time: datetime | None = None,
    ) -> Path:
        """Write a point-in-time snapshot JSON document."""
        ts = _ensure_utc(snapshot_time or datetime.now(timezone.utc))
        timestamp_token = ts.strftime("%Y-%m-%dT%H%M%SZ")
        destination = self.snapshots_dir / f"{name}__{timestamp_token}.json"
        with destination.open("w", encoding="utf-8") as handle:
            json.dump(snapshot_state, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
        return destination

    def iter_signals(
        self,
        start: datetime,
        end: datetime,
        entity_ref: EntityRef | None = None,
        sources: list[str] | None = None,
    ) -> Iterator[SignalEnvelope]:
        """Stream signals in [start, end], optionally filtered by entity_ref and source."""
        start_utc = _ensure_utc(start)
        end_utc = _ensure_utc(end)
        if end_utc < start_utc:
            raise ValueError("end must be greater than or equal to start")

        source_set = set(sources) if sources else None
        seen_signal_ids: set[str] = set()
        for day in _iter_days(start_utc.date(), end_utc.date()):
            partition = self.signals_dir / f"{day.isoformat()}.jsonl"
            if not partition.exists():
                continue
            with partition.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    record = json.loads(line)
                    signal = SignalEnvelope.from_dict(record)
                    signal_ts = _ensure_utc(signal.timestamp)
                    if signal_ts < start_utc or signal_ts > end_utc:
                        continue
                    if signal.signal_id in seen_signal_ids:
                        continue
                    if source_set is not None and signal.source not in source_set:
                        continue
                    if entity_ref is not None and entity_ref not in signal.entity_refs:
                        continue
                    seen_signal_ids.add(signal.signal_id)
                    yield signal

    def iter_emissions(
        self,
        start: datetime,
        end: datetime,
        entity_ref: EntityRef | None = None,
        emission_types: list[str] | None = None,
    ) -> Iterator[EmissionEnvelope]:
        """Stream emissions in [start, end], optionally filtered by entity_ref and type."""
        start_utc = _ensure_utc(start)
        end_utc = _ensure_utc(end)
        if end_utc < start_utc:
            raise ValueError("end must be greater than or equal to start")

        emission_type_set = set(emission_types) if emission_types else None
        seen_emission_ids: set[str] = set()
        for day in _iter_days(start_utc.date(), end_utc.date()):
            partition = self.emissions_dir / f"{day.isoformat()}.jsonl"
            if not partition.exists():
                continue
            with partition.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    record = json.loads(line)
                    emission = EmissionEnvelope.from_dict(record)
                    emission_ts = _ensure_utc(emission.timestamp)
                    if emission_ts < start_utc or emission_ts > end_utc:
                        continue
                    if emission.emission_id in seen_emission_ids:
                        continue
                    if emission_type_set is not None and emission.emission_type not in emission_type_set:
                        continue
                    if entity_ref is not None and entity_ref not in emission.entity_refs:
                        continue
                    seen_emission_ids.add(emission.emission_id)
                    yield emission
