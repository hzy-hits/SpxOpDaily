"""Deterministic short-horizon attribution for confirmed level decisions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Mapping


DEFAULT_HORIZONS_SECONDS = (30, 60, 180, 300)


@dataclass(frozen=True)
class LevelOutcomeSettings:
    horizons_seconds: tuple[int, ...] = DEFAULT_HORIZONS_SECONDS
    sample_tolerance_seconds: float = 20.0
    no_follow_through_mfe_bps: float = 2.0
    false_confirmation_mae_bps: float = -5.0
    follow_through_end_bps: float = 3.0
    retention_seconds: float = 3600.0


def advance_level_outcomes(
    previous: Mapping[str, object] | None,
    *,
    decision: Mapping[str, object],
    spot: float | None,
    at: datetime,
    confirmed_now: bool,
    settings: LevelOutcomeSettings | None = None,
) -> tuple[dict[str, object], tuple[dict[str, object], ...]]:
    settings = settings or LevelOutcomeSettings()
    now = _utc(at)
    state = dict(previous) if isinstance(previous, Mapping) else {}
    raw_observations = state.get("observations")
    observations = {
        str(key): dict(value)
        for key, value in raw_observations.items()
        if isinstance(key, str) and isinstance(value, Mapping)
    } if isinstance(raw_observations, Mapping) else {}

    event_id = str(decision.get("event_id") or "")
    if confirmed_now and event_id and spot is not None and event_id not in observations:
        observations[event_id] = _new_observation(
            decision,
            spot=float(spot),
            at=now,
            horizons_seconds=settings.horizons_seconds,
        )

    completed: list[dict[str, object]] = []
    for event_id, observation in observations.items():
        _append_sample(observation, spot=spot, at=now)
        completed.extend(_complete_due(event_id, observation, now=now, settings=settings))
    _prune(observations, now=now, retention_seconds=settings.retention_seconds)
    state.update(
        {
            "schema_version": 1,
            "observations": observations,
            "updated_at": now.isoformat(),
        }
    )
    return state, tuple(completed)


def _new_observation(
    decision: Mapping[str, object],
    *,
    spot: float,
    at: datetime,
    horizons_seconds: tuple[int, ...],
) -> dict[str, object]:
    direction = str(decision.get("direction") or "")
    if direction not in {"up", "down"}:
        raise ValueError("confirmed level decision requires up/down direction")
    return {
        "event_id": str(decision["event_id"]),
        "level_kind": str(decision.get("level_kind") or "unknown"),
        "level": float(decision.get("level") or 0.0),
        "thesis": str(decision.get("thesis") or "none"),
        "direction": direction,
        "confirmed_at": at.isoformat(),
        "start_spot": spot,
        "samples": [{"at": at.isoformat(), "spot": spot}],
        "horizons": {
            str(seconds): {
                "seconds": seconds,
                "target_at": (at + timedelta(seconds=seconds)).isoformat(),
                "status": "pending",
            }
            for seconds in horizons_seconds
        },
    }


def _append_sample(observation: dict[str, object], *, spot: float | None, at: datetime) -> None:
    if spot is None:
        return
    samples = observation.get("samples")
    if not isinstance(samples, list):
        samples = []
        observation["samples"] = samples
    last_at = _parse_at(samples[-1].get("at")) if samples and isinstance(samples[-1], dict) else None
    if last_at is not None and at <= last_at:
        return
    samples.append({"at": at.isoformat(), "spot": float(spot)})
    del samples[:-64]


def _complete_due(
    event_id: str,
    observation: dict[str, object],
    *,
    now: datetime,
    settings: LevelOutcomeSettings,
) -> list[dict[str, object]]:
    samples_raw = observation.get("samples")
    horizons = observation.get("horizons")
    if not isinstance(samples_raw, list) or not isinstance(horizons, dict):
        return []
    samples = [
        (_parse_at(row.get("at")), float(row.get("spot")))
        for row in samples_raw
        if isinstance(row, dict)
        and _parse_at(row.get("at")) is not None
        and isinstance(row.get("spot"), int | float)
    ]
    parsed_samples = [(at, spot) for at, spot in samples if at is not None]
    if not parsed_samples:
        return []
    start_spot = float(observation["start_spot"])
    sign = 1.0 if observation.get("direction") == "up" else -1.0
    completed: list[dict[str, object]] = []
    for raw in horizons.values():
        if not isinstance(raw, dict) or raw.get("status") != "pending":
            continue
        target_at = _parse_at(raw.get("target_at"))
        if target_at is None or now < target_at:
            continue
        sample_at, end_spot = min(
            parsed_samples,
            key=lambda row: (abs((row[0] - target_at).total_seconds()), row[0]),
        )
        path = [row for row in parsed_samples if row[0] <= sample_at]
        distance = abs((sample_at - target_at).total_seconds())
        returns = [_return_bps(price, start_spot) for _, price in path]
        directional = [sign * value for value in returns]
        status = (
            "complete"
            if distance <= settings.sample_tolerance_seconds
            else "incomplete"
        )
        end_return = (
            round(_return_bps(end_spot, start_spot), 6) if status == "complete" else None
        )
        mfe = round(max(directional), 6) if status == "complete" else None
        mae = round(min(directional), 6) if status == "complete" else None
        attribution = _attribution(
            sign * end_return if end_return is not None else None,
            mfe,
            mae,
            settings=settings,
        )
        raw.update(
            {
                "status": status,
                "sample_at": sample_at.isoformat(),
                "sample_distance_seconds": distance,
                "end_spot": end_spot if status == "complete" else None,
                "return_bps": end_return,
                "mfe_bps": mfe,
                "mae_bps": mae,
                "attribution": attribution,
                "completed_at": now.isoformat(),
                "emitted": True,
            }
        )
        completed.append(
            {
                "record_key": f"{event_id}:{int(raw['seconds'])}s",
                "event_id": event_id,
                "level_kind": observation.get("level_kind"),
                "level": observation.get("level"),
                "thesis": observation.get("thesis"),
                "direction": observation.get("direction"),
                "confirmed_at": observation.get("confirmed_at"),
                "horizon_seconds": raw["seconds"],
                **{key: raw.get(key) for key in (
                    "status",
                    "sample_at",
                    "sample_distance_seconds",
                    "end_spot",
                    "return_bps",
                    "mfe_bps",
                    "mae_bps",
                    "attribution",
                    "completed_at",
                )},
            }
        )
    return completed


def _attribution(
    directional_end_bps: float | None,
    mfe_bps: float | None,
    mae_bps: float | None,
    *,
    settings: LevelOutcomeSettings,
) -> str:
    if directional_end_bps is None or mfe_bps is None or mae_bps is None:
        return "data_incomplete"
    if mfe_bps < settings.no_follow_through_mfe_bps:
        return "no_follow_through"
    if mae_bps <= settings.false_confirmation_mae_bps and directional_end_bps < 0:
        return "false_confirmation"
    if directional_end_bps >= settings.follow_through_end_bps:
        return "follow_through"
    return "mixed_path"


def _prune(
    observations: dict[str, dict[str, object]],
    *,
    now: datetime,
    retention_seconds: float,
) -> None:
    remove: list[str] = []
    for event_id, observation in observations.items():
        horizons = observation.get("horizons")
        if not isinstance(horizons, dict) or not horizons:
            continue
        completed = [row for row in horizons.values() if isinstance(row, dict)]
        if not completed or not all(row.get("status") != "pending" for row in completed):
            continue
        latest = max(
            (_parse_at(row.get("completed_at")) for row in completed),
            default=None,
        )
        if latest is not None and (now - latest).total_seconds() >= retention_seconds:
            remove.append(event_id)
    for event_id in remove:
        observations.pop(event_id, None)


def _return_bps(price: float, start: float) -> float:
    return (price / start - 1.0) * 10_000.0


def _parse_at(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("outcome timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)
