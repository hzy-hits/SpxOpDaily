"""Read-only outcome tracking for deterministic intraday price events.

The tracker never creates alerts or trading instructions.  It accepts already
validated SPX/ES-synchronized samples, stores a compact observation path across
process restarts, and emits allowlisted 5/15/30 minute SPX outcome records.
Return and path-high/path-low fields keep the raw SPX sign (positive means SPX
rose). MFE/MAE use the explicit hypothesis direction: shock continuation, or
the opposite direction for a reclaim.

Raw alert/event identifiers are used only to derive an opaque observation key;
they are never persisted.  State is atomically replaced with mode ``0600`` and
the result JSONL is append-only, locked, fsynced, and de-duplicated by record
key so a crash between the state and journal writes cannot create duplicates.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

from spx_spark.data_platform.ids import make_event_key
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock


STATE_SCHEMA_VERSION = 1
RESULT_SCHEMA_VERSION = 1
DEFAULT_HORIZONS_MINUTES = (5, 15, 30)
ET = ZoneInfo("America/New_York")

EventPhase = Literal["shock", "reclaim", "strategy"]
EventDirection = Literal["up", "down"]


class IntradayOutcomeStoreError(RuntimeError):
    """Raised when persisted outcome state cannot be trusted safely."""


@dataclass(frozen=True)
class IntradayEventOutcomeSettings:
    state_path: str
    results_path: str
    horizons_minutes: tuple[int, ...] = DEFAULT_HORIZONS_MINUTES
    max_source_skew_seconds: float = 5.0
    max_horizon_sample_distance_seconds: float = 30.0
    completed_retention_seconds: int = 7_200

    def __post_init__(self) -> None:
        if not self.state_path or not self.results_path:
            raise ValueError("outcome state and results paths are required")
        if not self.horizons_minutes or any(value <= 0 for value in self.horizons_minutes):
            raise ValueError("outcome horizons must be positive")
        if tuple(sorted(set(self.horizons_minutes))) != self.horizons_minutes:
            raise ValueError("outcome horizons must be unique and increasing")
        if self.max_source_skew_seconds < 0:
            raise ValueError("source skew limit cannot be negative")
        if self.max_horizon_sample_distance_seconds < 0:
            raise ValueError("horizon sample distance cannot be negative")
        if self.completed_retention_seconds < 0:
            raise ValueError("completed observation retention cannot be negative")

    @classmethod
    def from_env(cls) -> "IntradayEventOutcomeSettings":
        data_root = (
            os.getenv("MARKET_DATA_DATA_ROOT") or os.getenv("MAINTENANCE_DATA_ROOT") or "data"
        )
        root = data_root.rstrip("/")
        return cls(
            state_path=os.getenv("ALERT_INTRADAY_OUTCOME_STATE_PATH")
            or f"{root}/latest/intraday_event_outcome_state.json",
            results_path=os.getenv("ALERT_INTRADAY_OUTCOME_RESULTS_PATH")
            or f"{root}/features/intraday_event_outcomes/date={{trading_date}}/outcomes.jsonl",
            completed_retention_seconds=int(
                os.getenv("ALERT_INTRADAY_OUTCOME_RETENTION_SECONDS", "7200")
            ),
        )


@dataclass(frozen=True)
class SynchronizedSPXSample:
    """One SPX price whose SPX and ES source clocks are synchronized upstream."""

    spx: float
    spx_source_at: datetime
    es_source_at: datetime

    @property
    def at(self) -> datetime:
        return max(_as_utc(self.spx_source_at), _as_utc(self.es_source_at))


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("outcome timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def _parse_datetime(value: object) -> datetime:
    if not isinstance(value, str) or not value:
        raise IntradayOutcomeStoreError("invalid timestamp in intraday outcome state")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise IntradayOutcomeStoreError("invalid timestamp in intraday outcome state") from exc
    if parsed.tzinfo is None:
        raise IntradayOutcomeStoreError("naive timestamp in intraday outcome state")
    return parsed.astimezone(timezone.utc)


def _validate_sample(
    sample: SynchronizedSPXSample,
    settings: IntradayEventOutcomeSettings,
) -> tuple[datetime, datetime, datetime]:
    if not math.isfinite(sample.spx) or sample.spx <= 0:
        raise ValueError("SPX outcome sample must be a positive finite number")
    spx_at = _as_utc(sample.spx_source_at)
    es_at = _as_utc(sample.es_source_at)
    if abs((spx_at - es_at).total_seconds()) > settings.max_source_skew_seconds:
        raise ValueError("SPX and ES outcome sample clocks are not synchronized")
    return max(spx_at, es_at), spx_at, es_at


def _empty_state() -> dict[str, object]:
    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "observations": {},
        "updated_at": None,
    }


def _load_state(path: Path) -> dict[str, object]:
    if not path.exists():
        return _empty_state()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise IntradayOutcomeStoreError("intraday outcome state is unreadable") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != STATE_SCHEMA_VERSION:
        raise IntradayOutcomeStoreError("unsupported intraday outcome state schema")
    if not isinstance(payload.get("observations"), dict):
        raise IntradayOutcomeStoreError("invalid intraday outcome observations")
    return payload


def _observation_id(raw_event_id: str, phase: EventPhase) -> str:
    if not raw_event_id:
        raise ValueError("event identifier is required")
    digest = hashlib.sha256(
        b"spx-spark-intraday-outcome-v1\0"
        + raw_event_id.encode("utf-8", errors="strict")
        + b"\0"
        + phase.encode("ascii")
    ).hexdigest()
    return digest[:32]


def _horizon_key(minutes: int) -> str:
    return str(minutes)


def _result_key(observation_id: str, minutes: int) -> str:
    return f"{observation_id}:{minutes}m"


def _new_observation(
    *,
    observation_id: str,
    event_key: str,
    decision_id: str | None,
    phase: EventPhase,
    direction: EventDirection,
    sample: SynchronizedSPXSample,
    settings: IntradayEventOutcomeSettings,
) -> dict[str, object]:
    at, spx_at, es_at = _validate_sample(sample, settings)
    horizons = {
        _horizon_key(minutes): {
            "minutes": minutes,
            "target_at": (at + timedelta(minutes=minutes)).isoformat(),
            "status": "pending",
            "emitted": False,
            "record_key": _result_key(observation_id, minutes),
        }
        for minutes in settings.horizons_minutes
    }
    return {
        "observation_id": observation_id,
        "event_key": event_key,
        "decision_id": decision_id,
        "phase": phase,
        "direction": direction,
        "observed_at": at.isoformat(),
        "start_spx": float(sample.spx),
        "last_spx_source_at": spx_at.isoformat(),
        "last_es_source_at": es_at.isoformat(),
        "samples": [{"at": at.isoformat(), "spx": float(sample.spx)}],
        "horizons": horizons,
    }


def _safe_observations(state: dict[str, object]) -> dict[str, dict[str, object]]:
    raw = state.get("observations")
    if not isinstance(raw, dict):
        raise IntradayOutcomeStoreError("invalid intraday outcome observations")
    observations: dict[str, dict[str, object]] = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not isinstance(value, dict):
            raise IntradayOutcomeStoreError("invalid intraday outcome observation entry")
        observations[key] = value
    return observations


def _sample_path(observation: dict[str, object]) -> list[dict[str, object]]:
    raw_samples = observation.get("samples")
    if not isinstance(raw_samples, list) or not raw_samples:
        raise IntradayOutcomeStoreError("invalid intraday outcome sample path")
    samples: list[dict[str, object]] = []
    previous_at: datetime | None = None
    for raw in raw_samples:
        if not isinstance(raw, dict):
            raise IntradayOutcomeStoreError("invalid intraday outcome sample")
        at = _parse_datetime(raw.get("at"))
        spx = raw.get("spx")
        if not isinstance(spx, int | float) or not math.isfinite(float(spx)) or float(spx) <= 0:
            raise IntradayOutcomeStoreError("invalid SPX value in intraday outcome state")
        if previous_at is not None and at <= previous_at:
            raise IntradayOutcomeStoreError("unordered intraday outcome sample path")
        samples.append({"at": at.isoformat(), "spx": float(spx)})
        previous_at = at
    return samples


def _return_bps(price: float, start_price: float) -> float:
    return (price / start_price - 1.0) * 10_000.0


def _finish_due_horizons(
    observation: dict[str, object],
    *,
    current_at: datetime,
    settings: IntradayEventOutcomeSettings,
) -> None:
    samples = _sample_path(observation)
    start_spx = observation.get("start_spx")
    horizons = observation.get("horizons")
    if not isinstance(start_spx, int | float) or not isinstance(horizons, dict):
        raise IntradayOutcomeStoreError("invalid intraday outcome observation")
    start_price = float(start_spx)
    phase = str(observation.get("phase") or "")
    shock_direction = str(observation.get("direction") or "")
    hypothesis_direction = (
        shock_direction
        if phase in {"shock", "strategy"}
        else "up"
        if shock_direction == "down"
        else "down"
    )

    parsed_samples = [(_parse_datetime(item["at"]), float(item["spx"])) for item in samples]
    for raw_metric in horizons.values():
        if not isinstance(raw_metric, dict):
            raise IntradayOutcomeStoreError("invalid intraday outcome horizon")
        if raw_metric.get("status") != "pending":
            continue
        target_at = _parse_datetime(raw_metric.get("target_at"))
        if current_at < target_at:
            continue

        close_at, close_spx = min(
            parsed_samples,
            key=lambda item: (abs((item[0] - target_at).total_seconds()), item[0]),
        )
        distance_seconds = abs((close_at - target_at).total_seconds())
        path = [item for item in parsed_samples if item[0] <= close_at]
        raw_metric["completed_at"] = current_at.isoformat()
        raw_metric["sample_at"] = close_at.isoformat()
        raw_metric["sample_distance_seconds"] = distance_seconds
        raw_metric["sample_count"] = max(len(path) - 1, 0)
        if distance_seconds > settings.max_horizon_sample_distance_seconds:
            raw_metric.update(
                {
                    "status": "incomplete",
                    "reason": "no_synchronized_sample_near_target",
                    "end_spx": None,
                    "return_bps": None,
                    "mfe_bps": None,
                    "mae_bps": None,
                    "path_high_return_bps": None,
                    "path_low_return_bps": None,
                    "hypothesis_direction": hypothesis_direction,
                }
            )
            continue

        returns = [_return_bps(price, start_price) for _, price in path]
        directional_returns = (
            returns if hypothesis_direction == "up" else [-value for value in returns]
        )
        raw_metric.update(
            {
                "status": "complete",
                "reason": None,
                "end_spx": close_spx,
                "return_bps": _return_bps(close_spx, start_price),
                "mfe_bps": max(directional_returns),
                "mae_bps": min(directional_returns),
                "path_high_return_bps": max(returns),
                "path_low_return_bps": min(returns),
                "hypothesis_direction": hypothesis_direction,
            }
        )

    if horizons and all(
        isinstance(metric, dict) and metric.get("status") in {"complete", "incomplete"}
        for metric in horizons.values()
    ):
        observation.setdefault("terminal_at", current_at.isoformat())


def _prune_emitted_observations(
    observations: dict[str, dict[str, object]],
    *,
    current_at: datetime,
    retention_seconds: int,
) -> bool:
    remove: list[str] = []
    for observation_id, observation in observations.items():
        horizons = observation.get("horizons")
        if not isinstance(horizons, dict):
            raise IntradayOutcomeStoreError("invalid intraday outcome horizons")
        if not horizons or not all(
            isinstance(metric, dict)
            and metric.get("status") in {"complete", "incomplete"}
            and metric.get("emitted") is True
            for metric in horizons.values()
        ):
            continue
        terminal_at = _parse_datetime(observation.get("terminal_at"))
        if (current_at - terminal_at).total_seconds() >= retention_seconds:
            remove.append(observation_id)
    for observation_id in remove:
        observations.pop(observation_id, None)
    return bool(remove)


def _result_record(
    observation: dict[str, object],
    metric: dict[str, object],
) -> dict[str, object]:
    event_key = observation.get("event_key")
    if not isinstance(event_key, str) or not event_key:
        # Backward-compatible recovery for an observation written before the
        # shared data-platform key existed. The legacy observation id is
        # already opaque and therefore safe to use as identity material.
        phase = str(observation.get("phase") or "event")
        event_key = make_event_key(
            f"intraday_price_{phase}",
            _parse_datetime(observation.get("observed_at")),
            str(observation.get("observation_id") or "legacy"),
        )
    return {
        "schema_version": RESULT_SCHEMA_VERSION,
        "record_key": metric["record_key"],
        "observation_id": observation["observation_id"],
        "event_key": event_key,
        "decision_id": observation.get("decision_id"),
        "phase": observation["phase"],
        "direction": observation["direction"],
        "observed_at": observation["observed_at"],
        "horizon_minutes": metric["minutes"],
        "target_at": metric["target_at"],
        "status": metric["status"],
        "reason": metric.get("reason"),
        "sample_at": metric.get("sample_at"),
        "sample_distance_seconds": metric.get("sample_distance_seconds"),
        "sample_count": metric.get("sample_count"),
        "start_spx": observation["start_spx"],
        "end_spx": metric.get("end_spx"),
        "return_bps": metric.get("return_bps"),
        "mfe_bps": metric.get("mfe_bps"),
        "mae_bps": metric.get("mae_bps"),
        "path_high_return_bps": metric.get("path_high_return_bps"),
        "path_low_return_bps": metric.get("path_low_return_bps"),
        "hypothesis_direction": metric.get("hypothesis_direction"),
    }


def _pending_records(state: dict[str, object]) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for observation in _safe_observations(state).values():
        horizons = observation.get("horizons")
        if not isinstance(horizons, dict):
            raise IntradayOutcomeStoreError("invalid intraday outcome horizons")
        for metric in horizons.values():
            if not isinstance(metric, dict):
                raise IntradayOutcomeStoreError("invalid intraday outcome horizon")
            if metric.get("status") in {"complete", "incomplete"} and not metric.get("emitted"):
                records.append(_result_record(observation, metric))
    return records


def _existing_result_keys(path: Path) -> set[str]:
    if not path.exists():
        return set()
    keys: set[str] = set()
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                payload = json.loads(line)
                if not isinstance(payload, dict) or not isinstance(payload.get("record_key"), str):
                    raise IntradayOutcomeStoreError("invalid intraday outcome JSONL record")
                keys.add(payload["record_key"])
    except (OSError, json.JSONDecodeError) as exc:
        raise IntradayOutcomeStoreError("intraday outcome JSONL is unreadable") from exc
    return keys


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written <= 0:
            raise OSError("short write to intraday outcome JSONL")
        offset += written


def _persist_result_records(
    path: Path,
    records: list[dict[str, object]],
) -> tuple[set[str], list[dict[str, object]]]:
    if not records:
        return set(), []
    path.parent.mkdir(parents=True, exist_ok=True)
    appended: list[dict[str, object]] = []
    with exclusive_state_lock(path):
        existing = _existing_result_keys(path)
        descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
        try:
            os.fchmod(descriptor, 0o600)
            for record in records:
                key = record.get("record_key")
                if not isinstance(key, str):
                    raise IntradayOutcomeStoreError("missing intraday outcome record key")
                if key in existing:
                    continue
                encoded = (
                    json.dumps(
                        record,
                        allow_nan=False,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    + "\n"
                ).encode("utf-8")
                _write_all(descriptor, encoded)
                existing.add(key)
                appended.append(record)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    return {
        str(record["record_key"]) for record in records if record["record_key"] in existing
    }, appended


class IntradayEventOutcomeTracker:
    """Persist and score observations without affecting alert or order decisions."""

    def __init__(self, settings: IntradayEventOutcomeSettings) -> None:
        self.settings = settings
        self.state_path = Path(settings.state_path)
        self.results_path = Path(settings.results_path)

    def _results_path_for(self, record: dict[str, object]) -> Path:
        template = self.settings.results_path
        if "{trading_date}" not in template:
            return Path(template)
        observed_at = _parse_datetime(record.get("observed_at"))
        trading_date = observed_at.astimezone(ET).date().isoformat()
        return Path(template.format(trading_date=trading_date))

    def observe_event(
        self,
        *,
        event_id: str,
        phase: EventPhase,
        direction: EventDirection,
        sample: SynchronizedSPXSample,
        event_key: str | None = None,
        decision_id: str | None = None,
    ) -> str:
        if phase not in {"shock", "reclaim", "strategy"}:
            raise ValueError("unsupported intraday outcome event phase")
        if direction not in {"up", "down"}:
            raise ValueError("unsupported intraday outcome direction")
        observation_id = _observation_id(event_id, phase)
        provided_event_key = event_key is not None
        resolved_event_key = event_key or make_event_key(
            f"intraday_price_{phase}" if phase != "strategy" else "intraday_strategy_signal",
            sample.at,
            event_id,
        )
        observation = _new_observation(
            observation_id=observation_id,
            event_key=resolved_event_key,
            decision_id=decision_id,
            phase=phase,
            direction=direction,
            sample=sample,
            settings=self.settings,
        )
        with exclusive_state_lock(self.state_path):
            state = _load_state(self.state_path)
            observations = _safe_observations(state)
            existing = observations.get(observation_id)
            if existing is not None:
                if existing.get("phase") != phase or existing.get("direction") != direction:
                    raise IntradayOutcomeStoreError("conflicting intraday outcome observation")
                existing_event_key = existing.get("event_key")
                if provided_event_key and existing_event_key not in {None, resolved_event_key}:
                    raise IntradayOutcomeStoreError("conflicting intraday outcome event key")
                existing_decision_id = existing.get("decision_id")
                if existing_event_key is None or (
                    existing_decision_id is None and decision_id is not None
                ):
                    existing["event_key"] = resolved_event_key
                    existing["decision_id"] = decision_id
                    state["observations"] = observations
                    state["updated_at"] = sample.at.isoformat()
                    atomic_write_json_secure(self.state_path, state)
                return observation_id
            observations[observation_id] = observation
            state["observations"] = observations
            state["updated_at"] = observation["observed_at"]
            atomic_write_json_secure(self.state_path, state)
        return observation_id

    def observe_sample(self, sample: SynchronizedSPXSample) -> tuple[dict[str, object], ...]:
        current_at, spx_at, es_at = _validate_sample(sample, self.settings)
        changed = False
        with exclusive_state_lock(self.state_path):
            state = _load_state(self.state_path)
            observations = _safe_observations(state)
            changed = _prune_emitted_observations(
                observations,
                current_at=current_at,
                retention_seconds=self.settings.completed_retention_seconds,
            )
            for observation in observations.values():
                horizons = observation.get("horizons")
                if not isinstance(horizons, dict):
                    raise IntradayOutcomeStoreError("invalid intraday outcome horizons")
                if horizons and all(
                    isinstance(metric, dict) and metric.get("status") in {"complete", "incomplete"}
                    for metric in horizons.values()
                ):
                    continue

                last_spx_at = _parse_datetime(observation.get("last_spx_source_at"))
                last_es_at = _parse_datetime(observation.get("last_es_source_at"))
                if spx_at <= last_spx_at or es_at <= last_es_at:
                    continue
                observed_at = _parse_datetime(observation.get("observed_at"))
                if current_at <= observed_at:
                    continue

                samples = _sample_path(observation)
                last_at = _parse_datetime(samples[-1]["at"])
                if current_at <= last_at:
                    continue
                samples.append({"at": current_at.isoformat(), "spx": float(sample.spx)})
                observation["samples"] = samples
                observation["last_spx_source_at"] = spx_at.isoformat()
                observation["last_es_source_at"] = es_at.isoformat()
                _finish_due_horizons(observation, current_at=current_at, settings=self.settings)
                changed = True

            if changed:
                state["observations"] = observations
                state["updated_at"] = current_at.isoformat()
                atomic_write_json_secure(self.state_path, state)
        return self.flush_pending_results()

    def flush_pending_results(self) -> tuple[dict[str, object], ...]:
        with exclusive_state_lock(self.state_path):
            state = _load_state(self.state_path)
            records = _pending_records(state)
        if not records:
            return ()

        grouped: dict[Path, list[dict[str, object]]] = {}
        for record in records:
            grouped.setdefault(self._results_path_for(record), []).append(record)
        persisted_keys: set[str] = set()
        appended: list[dict[str, object]] = []
        for path, path_records in grouped.items():
            path_keys, path_appended = _persist_result_records(path, path_records)
            persisted_keys.update(path_keys)
            appended.extend(path_appended)
        if persisted_keys:
            with exclusive_state_lock(self.state_path):
                state = _load_state(self.state_path)
                changed = False
                for observation in _safe_observations(state).values():
                    horizons = observation.get("horizons")
                    if not isinstance(horizons, dict):
                        raise IntradayOutcomeStoreError("invalid intraday outcome horizons")
                    for metric in horizons.values():
                        if not isinstance(metric, dict):
                            raise IntradayOutcomeStoreError("invalid intraday outcome horizon")
                        if metric.get("record_key") in persisted_keys and not metric.get("emitted"):
                            metric["emitted"] = True
                            changed = True
                if changed:
                    state["updated_at"] = datetime.now(tz=timezone.utc).isoformat()
                    atomic_write_json_secure(self.state_path, state)
        return tuple(appended)
