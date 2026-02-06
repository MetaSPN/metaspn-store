from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Literal

from metaspn_schemas import EmissionEnvelope, EntityRef, SignalEnvelope

DuplicatePolicy = Literal["ignore", "return_existing", "raise"]
RESOLVED_ENTITY_PAYLOAD_TYPES = frozenset({"EntityResolved", "EntityMerged", "EntityAliasAdded"})
DEFAULT_PENDING_OUTCOME_PAYLOAD_TYPES = frozenset(
    {"OutcomePending", "EvaluationRequested", "RecommendationAttempted"}
)
DEFAULT_SUCCESS_EMISSION_TYPES = frozenset({"OutcomeSuccess", "DraftApproved"})
DEFAULT_FAILURE_EMISSION_TYPES = frozenset({"OutcomeFailure", "DraftRejected"})


class DuplicateEventError(ValueError):
    """Raised when a duplicate event ID is written with on_duplicate='raise'."""


@dataclass(frozen=True)
class ReplayCheckpoint:
    """Replay checkpoint keyed by timestamp and IDs seen at that timestamp."""

    last_timestamp: datetime
    seen_ids_at_timestamp: tuple[str, ...] = field(default_factory=tuple)
    schema_version: str = "0.1"

    def __post_init__(self) -> None:
        object.__setattr__(self, "last_timestamp", _ensure_utc(self.last_timestamp))
        unique_seen = tuple(dict.fromkeys(self.seen_ids_at_timestamp))
        object.__setattr__(self, "seen_ids_at_timestamp", unique_seen)

    def to_dict(self) -> dict[str, Any]:
        return {
            "last_timestamp": self.last_timestamp.isoformat().replace("+00:00", "Z"),
            "seen_ids_at_timestamp": list(self.seen_ids_at_timestamp),
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ReplayCheckpoint:
        timestamp_text = data["last_timestamp"]
        if isinstance(timestamp_text, str) and timestamp_text.endswith("Z"):
            timestamp_text = timestamp_text[:-1] + "+00:00"
        return cls(
            last_timestamp=datetime.fromisoformat(timestamp_text),
            seen_ids_at_timestamp=tuple(data.get("seen_ids_at_timestamp", [])),
            schema_version=str(data.get("schema_version", "0.1")),
        )


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _ensure_utc(value)
    if isinstance(value, str):
        text = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            return _ensure_utc(datetime.fromisoformat(text))
        except ValueError:
            return None
    return None


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
        self.checkpoints_dir = self.store_root / "checkpoints"
        self._signal_index: dict[str, Path] | None = None
        self._emission_index: dict[str, Path] | None = None
        self._ensure_dirs()

    def _ensure_dirs(self) -> None:
        self.signals_dir.mkdir(parents=True, exist_ok=True)
        self.emissions_dir.mkdir(parents=True, exist_ok=True)
        self.snapshots_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoints_dir.mkdir(parents=True, exist_ok=True)

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

    def write_signals(
        self,
        signals: Iterable[SignalEnvelope],
        *,
        on_duplicate: DuplicatePolicy = "return_existing",
    ) -> list[Path]:
        """Write a batch of signals using the same duplicate policy."""
        return [self.write_signal(signal, on_duplicate=on_duplicate) for signal in signals]

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

    def write_emissions(
        self,
        emissions: Iterable[EmissionEnvelope],
        *,
        on_duplicate: DuplicatePolicy = "return_existing",
    ) -> list[Path]:
        """Write a batch of emissions using the same duplicate policy."""
        return [self.write_emission(emission, on_duplicate=on_duplicate) for emission in emissions]

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

    def iter_signals_from_checkpoint(
        self,
        *,
        end: datetime,
        checkpoint: ReplayCheckpoint | None = None,
        start: datetime | None = None,
        entity_ref: EntityRef | None = None,
        sources: list[str] | None = None,
    ) -> Iterator[SignalEnvelope]:
        """Replay signals from start/checkpoint without re-yielding already-processed records."""
        effective_start = _ensure_utc(start) if start is not None else datetime(1970, 1, 1, tzinfo=timezone.utc)
        seen_at_checkpoint: set[str] = set()
        checkpoint_ts: datetime | None = None
        if checkpoint is not None:
            checkpoint_ts = _ensure_utc(checkpoint.last_timestamp)
            if checkpoint_ts > effective_start:
                effective_start = checkpoint_ts
            seen_at_checkpoint = set(checkpoint.seen_ids_at_timestamp)

        for signal in self.iter_signals(
            start=effective_start,
            end=end,
            entity_ref=entity_ref,
            sources=sources,
        ):
            signal_ts = _ensure_utc(signal.timestamp)
            if checkpoint_ts is not None and signal_ts == checkpoint_ts and signal.signal_id in seen_at_checkpoint:
                continue
            yield signal

    def get_recent_signals_by_entity(
        self,
        *,
        entity_ref: EntityRef,
        limit: int,
        start: datetime | None = None,
        end: datetime | None = None,
        sources: list[str] | None = None,
    ) -> list[SignalEnvelope]:
        """Return last N signals for an entity in deterministic reverse chronological order."""
        if limit <= 0:
            return []
        end_utc = _ensure_utc(end or datetime.now(timezone.utc))
        start_utc = _ensure_utc(start or datetime(1970, 1, 1, tzinfo=timezone.utc))
        signals = list(
            self.iter_signals(
                start=start_utc,
                end=end_utc,
                entity_ref=entity_ref,
                sources=sources,
            )
        )
        signals.sort(key=lambda signal: (_ensure_utc(signal.timestamp), signal.signal_id), reverse=True)
        return signals[:limit]

    def get_recent_signals_by_source(
        self,
        *,
        source: str,
        limit: int,
        start: datetime | None = None,
        end: datetime | None = None,
        entity_ref: EntityRef | None = None,
    ) -> list[SignalEnvelope]:
        """Return last N signals for a source in deterministic reverse chronological order."""
        if limit <= 0:
            return []
        end_utc = _ensure_utc(end or datetime.now(timezone.utc))
        start_utc = _ensure_utc(start or datetime(1970, 1, 1, tzinfo=timezone.utc))
        signals = list(
            self.iter_signals(
                start=start_utc,
                end=end_utc,
                entity_ref=entity_ref,
                sources=[source],
            )
        )
        signals.sort(key=lambda signal: (_ensure_utc(signal.timestamp), signal.signal_id), reverse=True)
        return signals[:limit]

    def iter_entity_candidate_signals(
        self,
        *,
        start: datetime,
        end: datetime,
        resolved: bool | None = None,
        sources: list[str] | None = None,
    ) -> Iterator[SignalEnvelope]:
        """
        Stream candidate resolver signals.

        - resolved=True: envelopes with entity refs or resolver payload types.
        - resolved=False: envelopes without entity refs and non-resolver payload types.
        - resolved=None: all envelopes in the time range.
        """
        for signal in self.iter_signals(start=start, end=end, sources=sources):
            is_resolved = bool(signal.entity_refs) or signal.payload_type in RESOLVED_ENTITY_PAYLOAD_TYPES
            if resolved is None or is_resolved is resolved:
                yield signal

    def iter_stage_window_signals(
        self,
        *,
        stage: str,
        start: datetime,
        end: datetime,
        checkpoint: ReplayCheckpoint | None = None,
        entity_ref: EntityRef | None = None,
        sources: list[str] | None = None,
        payload_types: list[str] | None = None,
    ) -> Iterator[SignalEnvelope]:
        """
        Replay a stage window for chained workers.

        Signals are read from checkpoint-aware replay, then filtered by:
        - explicit `sources`, if provided
        - source prefix `<stage>.` when `sources` is not provided
        - optional payload type allow-list
        """
        source_set = set(sources) if sources else None
        payload_type_set = set(payload_types) if payload_types else None
        stage_prefix = f"{stage}."
        for signal in self.iter_signals_from_checkpoint(
            start=start,
            end=end,
            checkpoint=checkpoint,
            entity_ref=entity_ref,
            sources=sources,
        ):
            if source_set is None and not (signal.source == stage or signal.source.startswith(stage_prefix)):
                continue
            if payload_type_set is not None and signal.payload_type not in payload_type_set:
                continue
            yield signal

    def iter_recommendation_signals(
        self,
        *,
        start: datetime,
        end: datetime,
        checkpoint: ReplayCheckpoint | None = None,
        entity_ref: EntityRef | None = None,
        sources: list[str] | None = None,
        payload_types: list[str] | None = None,
    ) -> Iterator[SignalEnvelope]:
        """Replay recommendation-scoped signals with optional checkpoint and filters."""
        payload_type_set = set(payload_types) if payload_types else None
        for signal in self.iter_signals_from_checkpoint(
            start=start,
            end=end,
            checkpoint=checkpoint,
            entity_ref=entity_ref,
            sources=sources,
        ):
            if payload_type_set is not None and signal.payload_type not in payload_type_set:
                continue
            yield signal

    def iter_learning_signals(
        self,
        *,
        start: datetime,
        end: datetime,
        checkpoint: ReplayCheckpoint | None = None,
        entity_ref: EntityRef | None = None,
        sources: list[str] | None = None,
        payload_types: list[str] | None = None,
    ) -> Iterator[SignalEnvelope]:
        """Replay learning-loop signal records with optional checkpoint and filters."""
        payload_type_set = set(payload_types) if payload_types else None
        for signal in self.iter_signals_from_checkpoint(
            start=start,
            end=end,
            checkpoint=checkpoint,
            entity_ref=entity_ref,
            sources=sources,
        ):
            if payload_type_set is not None and signal.payload_type not in payload_type_set:
                continue
            yield signal

    def iter_learning_emissions(
        self,
        *,
        start: datetime,
        end: datetime,
        entity_ref: EntityRef | None = None,
        emission_types: list[str] | None = None,
    ) -> Iterator[EmissionEnvelope]:
        """Replay learning-loop emissions with deterministic duplicate-safe behavior."""
        yield from self.iter_emissions(
            start=start,
            end=end,
            entity_ref=entity_ref,
            emission_types=emission_types,
        )

    def get_top_recommendation_candidates(
        self,
        *,
        start: datetime,
        end: datetime,
        limit: int,
        entity_ref: EntityRef | None = None,
        sources: list[str] | None = None,
        payload_types: list[str] | None = None,
        score_field: str = "score",
        unique_by_entity: bool = True,
    ) -> list[SignalEnvelope]:
        """Return top ranked recommendation candidates for a window."""
        if limit <= 0:
            return []

        payload_type_set = set(payload_types) if payload_types else None
        candidates: list[tuple[float, datetime, str, SignalEnvelope]] = []
        for signal in self.iter_signals(start=start, end=end, entity_ref=entity_ref, sources=sources):
            if payload_type_set is not None and signal.payload_type not in payload_type_set:
                continue
            if not isinstance(signal.payload, dict):
                continue
            raw_score = signal.payload.get(score_field, 0.0)
            if not isinstance(raw_score, (int, float)):
                continue
            candidates.append((float(raw_score), _ensure_utc(signal.timestamp), signal.signal_id, signal))

        candidates.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
        if not unique_by_entity:
            return [item[3] for item in candidates[:limit]]

        selected: list[SignalEnvelope] = []
        seen_entity_keys: set[str] = set()
        for _, _, _, signal in candidates:
            if signal.entity_refs:
                primary_ref = signal.entity_refs[0]
                entity_key = f"{primary_ref.ref_type}:{primary_ref.platform}:{primary_ref.value}"
            else:
                entity_key = f"signal:{signal.signal_id}"
            if entity_key in seen_entity_keys:
                continue
            seen_entity_keys.add(entity_key)
            selected.append(signal)
            if len(selected) >= limit:
                break
        return selected

    def get_latest_draft_signals(
        self,
        *,
        limit: int,
        start: datetime,
        end: datetime,
        entity_ref: EntityRef | None = None,
        sources: list[str] | None = None,
        payload_types: list[str] | None = None,
    ) -> list[SignalEnvelope]:
        """Return latest draft-related signals in deterministic descending order."""
        if limit <= 0:
            return []
        payload_type_set = set(payload_types) if payload_types else None
        signals = list(self.iter_signals(start=start, end=end, entity_ref=entity_ref, sources=sources))
        if payload_type_set is not None:
            signals = [signal for signal in signals if signal.payload_type in payload_type_set]
        signals.sort(key=lambda signal: (_ensure_utc(signal.timestamp), signal.signal_id), reverse=True)
        return signals[:limit]

    def get_latest_approval_outcomes(
        self,
        *,
        limit: int,
        start: datetime,
        end: datetime,
        entity_ref: EntityRef | None = None,
        emission_types: list[str] | None = None,
    ) -> list[EmissionEnvelope]:
        """Return latest approval-related emissions in deterministic descending order."""
        if limit <= 0:
            return []
        emissions = list(
            self.iter_emissions(
                start=start,
                end=end,
                entity_ref=entity_ref,
                emission_types=emission_types,
            )
        )
        emissions.sort(key=lambda emission: (_ensure_utc(emission.timestamp), emission.emission_id), reverse=True)
        return emissions[:limit]

    def get_unresolved_outcome_signals(
        self,
        *,
        start: datetime,
        end: datetime,
        entity_ref: EntityRef | None = None,
        sources: list[str] | None = None,
        pending_payload_types: list[str] | None = None,
        success_emission_types: list[str] | None = None,
        failure_emission_types: list[str] | None = None,
    ) -> list[SignalEnvelope]:
        """Return pending outcome signals that have no success/failure emission yet."""
        pending_types = set(pending_payload_types) if pending_payload_types else set(DEFAULT_PENDING_OUTCOME_PAYLOAD_TYPES)
        success_types = set(success_emission_types) if success_emission_types else set(DEFAULT_SUCCESS_EMISSION_TYPES)
        failure_types = set(failure_emission_types) if failure_emission_types else set(DEFAULT_FAILURE_EMISSION_TYPES)
        resolved_caused_by: set[str] = set()

        for emission in self.iter_emissions(
            start=start,
            end=end,
            entity_ref=entity_ref,
            emission_types=sorted(success_types | failure_types),
        ):
            resolved_caused_by.add(emission.caused_by)

        unresolved: list[SignalEnvelope] = []
        for signal in self.iter_signals(
            start=start,
            end=end,
            entity_ref=entity_ref,
            sources=sources,
        ):
            if signal.payload_type not in pending_types:
                continue
            if signal.signal_id in resolved_caused_by:
                continue
            unresolved.append(signal)
        unresolved.sort(key=lambda signal: (_ensure_utc(signal.timestamp), signal.signal_id))
        return unresolved

    def get_expired_outcome_signals(
        self,
        *,
        now: datetime,
        start: datetime,
        end: datetime,
        entity_ref: EntityRef | None = None,
        sources: list[str] | None = None,
        pending_payload_types: list[str] | None = None,
        success_emission_types: list[str] | None = None,
        failure_emission_types: list[str] | None = None,
        expires_at_field: str = "expires_at",
    ) -> list[SignalEnvelope]:
        """Return unresolved outcome signals whose expiration timestamp is before `now`."""
        now_utc = _ensure_utc(now)
        unresolved = self.get_unresolved_outcome_signals(
            start=start,
            end=end,
            entity_ref=entity_ref,
            sources=sources,
            pending_payload_types=pending_payload_types,
            success_emission_types=success_emission_types,
            failure_emission_types=failure_emission_types,
        )
        expired: list[SignalEnvelope] = []
        for signal in unresolved:
            if not isinstance(signal.payload, dict):
                continue
            expires_at = _parse_datetime(signal.payload.get(expires_at_field))
            if expires_at is None:
                continue
            if expires_at < now_utc:
                expired.append(signal)
        expired.sort(key=lambda signal: (_ensure_utc(signal.timestamp), signal.signal_id))
        return expired

    def get_outcome_window_buckets(
        self,
        *,
        now: datetime,
        start: datetime,
        end: datetime,
        entity_ref: EntityRef | None = None,
        sources: list[str] | None = None,
        pending_payload_types: list[str] | None = None,
        success_emission_types: list[str] | None = None,
        failure_emission_types: list[str] | None = None,
        expires_at_field: str = "expires_at",
    ) -> dict[str, list[SignalEnvelope] | list[EmissionEnvelope]]:
        """
        Build deterministic learning-window buckets for evaluator workers.

        Buckets:
        - `pending`: unresolved and non-expired outcome signals
        - `expired`: unresolved and expired outcome signals
        - `success`: outcome success emissions
        - `failure`: outcome failure emissions
        """
        success_types = set(success_emission_types) if success_emission_types else set(DEFAULT_SUCCESS_EMISSION_TYPES)
        failure_types = set(failure_emission_types) if failure_emission_types else set(DEFAULT_FAILURE_EMISSION_TYPES)

        unresolved = self.get_unresolved_outcome_signals(
            start=start,
            end=end,
            entity_ref=entity_ref,
            sources=sources,
            pending_payload_types=pending_payload_types,
            success_emission_types=list(success_types),
            failure_emission_types=list(failure_types),
        )
        expired = self.get_expired_outcome_signals(
            now=now,
            start=start,
            end=end,
            entity_ref=entity_ref,
            sources=sources,
            pending_payload_types=pending_payload_types,
            success_emission_types=list(success_types),
            failure_emission_types=list(failure_types),
            expires_at_field=expires_at_field,
        )
        expired_ids = {signal.signal_id for signal in expired}
        pending = [signal for signal in unresolved if signal.signal_id not in expired_ids]

        success = list(
            self.iter_learning_emissions(
                start=start,
                end=end,
                entity_ref=entity_ref,
                emission_types=sorted(success_types),
            )
        )
        failure = list(
            self.iter_learning_emissions(
                start=start,
                end=end,
                entity_ref=entity_ref,
                emission_types=sorted(failure_types),
            )
        )
        success.sort(key=lambda emission: (_ensure_utc(emission.timestamp), emission.emission_id))
        failure.sort(key=lambda emission: (_ensure_utc(emission.timestamp), emission.emission_id))

        return {
            "pending": pending,
            "expired": expired,
            "success": success,
            "failure": failure,
        }

    def _normalize_day(self, day: date | datetime | str) -> str:
        if isinstance(day, str):
            return day
        if isinstance(day, datetime):
            return _ensure_utc(day).date().isoformat()
        return day.isoformat()

    def write_daily_digest_snapshot(
        self,
        *,
        day: date | datetime | str,
        digest: dict[str, Any],
    ) -> Path:
        """Persist a deterministic daily digest snapshot for recommendation/draft workers."""
        day_token = self._normalize_day(day)
        destination = self.snapshots_dir / f"digest__{day_token}.json"
        payload = {"day": day_token, "digest": digest, "schema_version": "0.1"}
        with destination.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
        return destination

    def read_daily_digest_snapshot(self, day: date | datetime | str) -> dict[str, Any] | None:
        """Read a previously written daily digest snapshot."""
        day_token = self._normalize_day(day)
        source = self.snapshots_dir / f"digest__{day_token}.json"
        if not source.exists():
            return None
        with source.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def write_calibration_snapshot(
        self,
        *,
        day: date | datetime | str,
        report: dict[str, Any],
    ) -> Path:
        """Persist deterministic calibration report snapshot."""
        day_token = self._normalize_day(day)
        destination = self.snapshots_dir / f"calibration__{day_token}.json"
        payload = {"day": day_token, "report": report, "schema_version": "0.1"}
        with destination.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
        return destination

    def read_calibration_snapshot(self, day: date | datetime | str) -> dict[str, Any] | None:
        """Read calibration report snapshot for a specific day."""
        day_token = self._normalize_day(day)
        source = self.snapshots_dir / f"calibration__{day_token}.json"
        if not source.exists():
            return None
        with source.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def build_signal_checkpoint(self, processed_signals: Iterable[SignalEnvelope]) -> ReplayCheckpoint | None:
        """Build a resume checkpoint from an already-processed, ordered signal stream."""
        last_ts: datetime | None = None
        ids_at_last_ts: list[str] = []
        for signal in processed_signals:
            signal_ts = _ensure_utc(signal.timestamp)
            if last_ts is None or signal_ts > last_ts:
                last_ts = signal_ts
                ids_at_last_ts = [signal.signal_id]
                continue
            if signal_ts < last_ts:
                raise ValueError("processed_signals must be in non-decreasing timestamp order")
            if signal.signal_id not in ids_at_last_ts:
                ids_at_last_ts.append(signal.signal_id)
        if last_ts is None:
            return None
        return ReplayCheckpoint(last_timestamp=last_ts, seen_ids_at_timestamp=tuple(ids_at_last_ts))

    def write_checkpoint(self, name: str, checkpoint: ReplayCheckpoint) -> Path:
        destination = self.checkpoints_dir / f"{name}.json"
        with destination.open("w", encoding="utf-8") as handle:
            json.dump(checkpoint.to_dict(), handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
        return destination

    def read_checkpoint(self, name: str) -> ReplayCheckpoint | None:
        source = self.checkpoints_dir / f"{name}.json"
        if not source.exists():
            return None
        with source.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        return ReplayCheckpoint.from_dict(data)

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
