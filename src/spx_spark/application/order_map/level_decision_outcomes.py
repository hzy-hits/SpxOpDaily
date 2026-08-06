"""Deterministic short-horizon attribution for confirmed level decisions."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Mapping


DEFAULT_HORIZONS_SECONDS = (30, 60, 180, 300, 900, 1800)
MAX_ACTIVE_SAMPLES = 512
MAX_COORDINATE_SKIPS = 512
OUTCOME_STATE_SCHEMA_VERSION = 3
OUTCOME_OBSERVATION_SCHEMA_VERSION = 3

_SPX_COORDINATE_INSTRUMENTS = {
    "official_spx": "index:SPX",
    "chain_implied_spx": "synthetic:SPXW_PARITY",
}
_ES_COORDINATE_KINDS = frozenset({"es_equivalent", "raw_es"})
_ES_INSTRUMENT_ID = "future:ES"


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
    trigger_coordinate_kind: str | None = None,
    trigger_instrument_id: str | None = None,
    trigger_basis_points: float | None = None,
    settings: LevelOutcomeSettings | None = None,
) -> tuple[dict[str, object], tuple[dict[str, object], ...]]:
    settings = settings or LevelOutcomeSettings()
    now = _utc(at)
    state = dict(previous) if isinstance(previous, Mapping) else {}
    raw_observations = state.get("observations")
    observations = (
        {
            str(key): dict(value)
            for key, value in raw_observations.items()
            if isinstance(key, str) and isinstance(value, Mapping)
        }
        if isinstance(raw_observations, Mapping)
        else {}
    )
    for observation in observations.values():
        _migrate_observation(observation)

    coordinate_kind = _coordinate_text(
        trigger_coordinate_kind or decision.get("trigger_coordinate_kind")
    )
    instrument_id = _coordinate_text(
        trigger_instrument_id or decision.get("trigger_instrument_id")
    )
    basis_points = _finite_float(
        trigger_basis_points
        if trigger_basis_points is not None
        else decision.get("trigger_basis_points")
    )

    event_id = str(decision.get("event_id") or "")
    if confirmed_now and event_id and spot is not None and event_id not in observations:
        observations[event_id] = _new_observation(
            decision,
            spot=float(spot),
            at=now,
            horizons_seconds=settings.horizons_seconds,
            trigger_coordinate_kind=coordinate_kind,
            trigger_instrument_id=instrument_id,
            trigger_basis_points=basis_points,
        )

    completed: list[dict[str, object]] = []
    for event_id, observation in observations.items():
        _append_sample(
            observation,
            spot=spot,
            at=now,
            trigger_coordinate_kind=coordinate_kind,
            trigger_instrument_id=instrument_id,
            trigger_basis_points=basis_points,
        )
        completed.extend(_complete_due(event_id, observation, now=now, settings=settings))
    _prune(observations, now=now, retention_seconds=settings.retention_seconds)
    state.update(
        {
            "schema_version": OUTCOME_STATE_SCHEMA_VERSION,
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
    trigger_coordinate_kind: str | None,
    trigger_instrument_id: str | None,
    trigger_basis_points: float | None,
) -> dict[str, object]:
    direction = str(decision.get("direction") or "")
    if direction not in {"up", "down"}:
        raise ValueError("confirmed level decision requires up/down direction")
    normalized_spot, issue, normalization = _canonical_spx_spot(
        spot,
        trigger_coordinate_kind=trigger_coordinate_kind,
        trigger_instrument_id=trigger_instrument_id,
        latched_basis_points=trigger_basis_points,
    )
    coordinate_status = "verified" if normalized_spot is not None else "unverifiable"
    samples = (
        [
            _sample(
                at=at,
                spot=normalized_spot,
                source_spot=spot,
                trigger_coordinate_kind=trigger_coordinate_kind,
                trigger_instrument_id=trigger_instrument_id,
                trigger_basis_points=trigger_basis_points,
                normalization=normalization,
            )
        ]
        if normalized_spot is not None
        else []
    )
    return {
        "schema_version": OUTCOME_OBSERVATION_SCHEMA_VERSION,
        "event_id": str(decision["event_id"]),
        "level_kind": str(decision.get("level_kind") or "unknown"),
        "level": float(decision.get("level") or 0.0),
        "thesis": str(decision.get("thesis") or "none"),
        "direction": direction,
        "confirmed_at": at.isoformat(),
        "trigger_coordinate_kind": trigger_coordinate_kind,
        "trigger_instrument_id": trigger_instrument_id,
        "latched_basis_points": trigger_basis_points,
        "outcome_coordinate_kind": "spx_equivalent",
        "coordinate_status": coordinate_status,
        "coordinate_issue": issue,
        "start_spot": normalized_spot,
        "samples": samples,
        "sample_skip_count": 0,
        "coordinate_skips": [],
        "horizons": {
            str(seconds): {
                "seconds": seconds,
                "target_at": (at + timedelta(seconds=seconds)).isoformat(),
                "status": "pending",
            }
            for seconds in horizons_seconds
        },
    }


def _append_sample(
    observation: dict[str, object],
    *,
    spot: float | None,
    at: datetime,
    trigger_coordinate_kind: str | None,
    trigger_instrument_id: str | None,
    trigger_basis_points: float | None,
) -> None:
    if spot is None:
        return
    if observation.get("coordinate_status") != "verified":
        _record_sample_skip(observation, at=at, reason="outcome_coordinate_unverifiable")
        return
    latched_basis = _finite_float(observation.get("latched_basis_points"))
    normalized_spot, issue, normalization = _canonical_spx_spot(
        float(spot),
        trigger_coordinate_kind=trigger_coordinate_kind,
        trigger_instrument_id=trigger_instrument_id,
        latched_basis_points=latched_basis,
    )
    if normalized_spot is None:
        _record_sample_skip(observation, at=at, reason=issue or "coordinate_mismatch")
        return
    samples = observation.get("samples")
    if not isinstance(samples, list):
        samples = []
        observation["samples"] = samples
    last_at = (
        _parse_at(samples[-1].get("at")) if samples and isinstance(samples[-1], dict) else None
    )
    if last_at is not None and at <= last_at:
        return
    samples.append(
        _sample(
            at=at,
            spot=normalized_spot,
            source_spot=float(spot),
            trigger_coordinate_kind=trigger_coordinate_kind,
            trigger_instrument_id=trigger_instrument_id,
            trigger_basis_points=trigger_basis_points,
            normalization=normalization,
        )
    )
    # The hot worker runs every five seconds.  Keep enough path observations
    # for an exact 30-minute MFE/MAE attribution instead of silently reducing
    # the long horizon to the last five minutes.
    del samples[:-MAX_ACTIVE_SAMPLES]


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
    if observation.get("coordinate_status") != "verified":
        return _complete_unverifiable_due(
            event_id,
            observation,
            horizons=horizons,
            now=now,
        )
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
    start_spot = _finite_float(observation.get("start_spot"))
    if start_spot is None or start_spot <= 0:
        observation["coordinate_status"] = "unverifiable"
        observation["coordinate_issue"] = "outcome_start_spot_invalid"
        return _complete_unverifiable_due(
            event_id,
            observation,
            horizons=horizons,
            now=now,
        )
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
        coordinate_skips = _coordinate_skips(observation)
        nearest_skip = (
            min(
                coordinate_skips,
                key=lambda row: (abs((row[0] - target_at).total_seconds()), row[0]),
            )
            if coordinate_skips
            else None
        )
        coordinate_skip_distance = (
            abs((nearest_skip[0] - target_at).total_seconds())
            if nearest_skip is not None
            else None
        )
        coordinate_skip_is_closer = bool(
            coordinate_skip_distance is not None
            and coordinate_skip_distance <= settings.sample_tolerance_seconds
            and coordinate_skip_distance <= distance
        )
        returns = [_return_bps(price, start_spot) for _, price in path]
        directional = [sign * value for value in returns]
        status = (
            "complete"
            if distance <= settings.sample_tolerance_seconds and not coordinate_skip_is_closer
            else "incomplete"
        )
        data_quality_reason = (
            (
                nearest_skip[1]
                if nearest_skip is not None
                else "coordinate_sample_skipped"
            )
            if coordinate_skip_is_closer
            else None
        )
        end_return = round(_return_bps(end_spot, start_spot), 6) if status == "complete" else None
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
                "coordinate_skip_distance_seconds": (
                    coordinate_skip_distance if coordinate_skip_is_closer else None
                ),
                "end_spot": end_spot if status == "complete" else None,
                "return_bps": end_return,
                "mfe_bps": mfe,
                "mae_bps": mae,
                "attribution": attribution,
                "data_quality_reason": data_quality_reason,
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
                "trigger_coordinate_kind": observation.get("trigger_coordinate_kind"),
                "trigger_instrument_id": observation.get("trigger_instrument_id"),
                "latched_basis_points": observation.get("latched_basis_points"),
                "outcome_coordinate_kind": observation.get("outcome_coordinate_kind"),
                "coordinate_status": observation.get("coordinate_status"),
                "sample_skip_count": observation.get("sample_skip_count", 0),
                "horizon_seconds": raw["seconds"],
                **{
                    key: raw.get(key)
                    for key in (
                        "status",
                        "sample_at",
                        "sample_distance_seconds",
                        "coordinate_skip_distance_seconds",
                        "end_spot",
                        "return_bps",
                        "mfe_bps",
                        "mae_bps",
                        "attribution",
                        "data_quality_reason",
                        "completed_at",
                    )
                },
            }
        )
    return completed


def _complete_unverifiable_due(
    event_id: str,
    observation: dict[str, object],
    *,
    horizons: dict[object, object],
    now: datetime,
) -> list[dict[str, object]]:
    """Censor legacy or coordinate-invalid outcomes instead of emitting returns."""

    completed: list[dict[str, object]] = []
    issue = str(observation.get("coordinate_issue") or "outcome_coordinate_unverifiable")
    for raw in horizons.values():
        if not isinstance(raw, dict) or raw.get("status") != "pending":
            continue
        target_at = _parse_at(raw.get("target_at"))
        if target_at is None or now < target_at:
            continue
        raw.update(
            {
                "status": "incomplete",
                "sample_at": None,
                "sample_distance_seconds": None,
                "end_spot": None,
                "return_bps": None,
                "mfe_bps": None,
                "mae_bps": None,
                "attribution": "data_incomplete",
                "data_quality_reason": issue,
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
                "trigger_coordinate_kind": observation.get("trigger_coordinate_kind"),
                "trigger_instrument_id": observation.get("trigger_instrument_id"),
                "latched_basis_points": observation.get("latched_basis_points"),
                "outcome_coordinate_kind": observation.get("outcome_coordinate_kind"),
                "coordinate_status": observation.get("coordinate_status"),
                "sample_skip_count": observation.get("sample_skip_count", 0),
                "horizon_seconds": raw["seconds"],
                "status": "incomplete",
                "sample_at": None,
                "sample_distance_seconds": None,
                "end_spot": None,
                "return_bps": None,
                "mfe_bps": None,
                "mae_bps": None,
                "attribution": "data_incomplete",
                "data_quality_reason": issue,
                "completed_at": now.isoformat(),
            }
        )
    return completed


def _migrate_observation(observation: dict[str, object]) -> None:
    """Fail closed for v1 observations whose historical coordinate is unknowable."""

    if observation.get("schema_version") == OUTCOME_OBSERVATION_SCHEMA_VERSION:
        observation["coordinate_skips"] = _normalized_coordinate_skip_rows(observation)
        return
    if observation.get("schema_version") == 2:
        observation.update(
            {
                "schema_version": OUTCOME_OBSERVATION_SCHEMA_VERSION,
                "coordinate_skips": _normalized_coordinate_skip_rows(observation),
            }
        )
        return
    legacy_samples = observation.get("samples")
    observation.update(
        {
            "schema_version": OUTCOME_OBSERVATION_SCHEMA_VERSION,
            "trigger_coordinate_kind": None,
            "trigger_instrument_id": None,
            "latched_basis_points": None,
            "outcome_coordinate_kind": "spx_equivalent",
            "coordinate_status": "legacy_unverifiable",
            "coordinate_issue": "legacy_coordinate_contract_missing",
            "legacy_samples_ignored": len(legacy_samples) if isinstance(legacy_samples, list) else 0,
            "sample_skip_count": int(observation.get("sample_skip_count") or 0),
            "coordinate_skips": [],
        }
    )


def _canonical_spx_spot(
    spot: float,
    *,
    trigger_coordinate_kind: str | None,
    trigger_instrument_id: str | None,
    latched_basis_points: float | None,
) -> tuple[float | None, str | None, str | None]:
    """Normalize one verified source observation into the event's SPX coordinate."""

    value = _finite_float(spot)
    if value is None or value <= 0:
        return None, "outcome_spot_invalid", None
    kind = _coordinate_text(trigger_coordinate_kind)
    instrument_id = _coordinate_text(trigger_instrument_id)
    if kind in _SPX_COORDINATE_INSTRUMENTS:
        if instrument_id != _SPX_COORDINATE_INSTRUMENTS[kind]:
            return None, "trigger_coordinate_instrument_mismatch", None
        return value, None, "identity_spx_coordinate"
    if kind in _ES_COORDINATE_KINDS:
        if instrument_id != _ES_INSTRUMENT_ID:
            return None, "trigger_coordinate_instrument_mismatch", None
        if latched_basis_points is None:
            return None, "latched_basis_unavailable_for_es_sample", None
        normalized = value - latched_basis_points
        if not math.isfinite(normalized) or normalized <= 0:
            return None, "normalized_spx_spot_invalid", None
        return normalized, None, "subtract_latched_es_spx_basis"
    return None, "trigger_coordinate_kind_unverifiable", None


def _sample(
    *,
    at: datetime,
    spot: float,
    source_spot: float,
    trigger_coordinate_kind: str | None,
    trigger_instrument_id: str | None,
    trigger_basis_points: float | None,
    normalization: str | None,
) -> dict[str, object]:
    return {
        "at": at.isoformat(),
        "spot": spot,
        "source_spot": source_spot,
        "trigger_coordinate_kind": trigger_coordinate_kind,
        "trigger_instrument_id": trigger_instrument_id,
        "trigger_basis_points": trigger_basis_points,
        "normalization": normalization,
    }


def _record_sample_skip(
    observation: dict[str, object],
    *,
    at: datetime,
    reason: str,
) -> None:
    observation["sample_skip_count"] = int(observation.get("sample_skip_count") or 0) + 1
    observation["last_sample_skip_at"] = at.isoformat()
    observation["last_sample_skip_reason"] = reason
    skips = observation.get("coordinate_skips")
    if not isinstance(skips, list):
        skips = []
        observation["coordinate_skips"] = skips
    skips.append({"at": at.isoformat(), "reason": reason})
    del skips[:-MAX_COORDINATE_SKIPS]


def _coordinate_skips(
    observation: Mapping[str, object],
) -> list[tuple[datetime, str]]:
    parsed: list[tuple[datetime, str]] = []
    for row in observation.get("coordinate_skips") or ():
        if not isinstance(row, Mapping):
            continue
        at = _parse_at(row.get("at"))
        reason = _coordinate_text(row.get("reason"))
        if at is not None and reason is not None:
            parsed.append((at, reason))
    return parsed


def _normalized_coordinate_skip_rows(
    observation: Mapping[str, object],
) -> list[dict[str, str]]:
    rows = [
        {"at": at.isoformat(), "reason": reason}
        for at, reason in _coordinate_skips(observation)
    ]
    if not rows:
        last_at = _parse_at(observation.get("last_sample_skip_at"))
        last_reason = _coordinate_text(observation.get("last_sample_skip_reason"))
        if last_at is not None and last_reason is not None:
            rows.append({"at": last_at.isoformat(), "reason": last_reason})
    return rows[-MAX_COORDINATE_SKIPS:]


def _coordinate_text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    parsed = value.strip()
    return parsed or None


def _finite_float(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


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
