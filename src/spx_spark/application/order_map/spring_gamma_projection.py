"""Fail-closed Spring Gamma v3 projection for order-map reports."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from spx_spark.analytics.options.pricing import finite_float
from spx_spark.application.market_features.spring_gamma_v3_io import (
    latest_spring_gamma_v3_shadow_path,
    validate_spring_gamma_v3_shadow,
)
from spx_spark.application.market_features.state import load_json


STATE_WINDOW_SCHEMA = "spring_gamma_v3_state_window.v1"
PROJECTION_DIAGNOSTIC_SCHEMA = "spring_gamma_v3_projection_diagnostic.v1"
PATH_FALLBACK_SCHEMA = "spring_gamma_v3_path_fallback.v1"
STATE_WINDOW_MINUTES = 15
PATH_FALLBACK_MAX_AGE_SECONDS = 15.0 * 60.0
PROJECTION_FUTURE_TOLERANCE_SECONDS = 5.0
_STATE_ORDER = (
    "TREND_UP",
    "TREND_DOWN",
    "LOW_VOL_RANGE",
    "HIGH_VOL_CHOP",
    "LOW_VOL_PIN",
    "UNCERTAIN",
)


def attach_spring_gamma_v3_shadow(
    payload: dict[str, Any],
    data_root: str | Path,
    *,
    settings: object,
    now: datetime,
) -> None:
    """Mount a current identity-matched shadow and its durable 15m state window."""

    payload.pop("spring_gamma_v3_shadow", None)
    payload.pop("spring_gamma_v3_state_window", None)
    payload.pop("spring_gamma_v3_projection_diagnostic", None)
    payload.pop("spring_gamma_v3_path_fallback", None)
    report_enabled = bool(getattr(settings, "report_enabled", False))
    interval = finite_float(getattr(settings, "prediction_interval_seconds", 0))
    max_age_seconds = max((interval or 0.0) * 2.0, 120.0)
    future_tolerance_seconds = finite_float(
        getattr(settings, "projection_future_tolerance_seconds", None)
    )
    if future_tolerance_seconds is None:
        future_tolerance_seconds = PROJECTION_FUTURE_TOLERANCE_SECONDS
    future_tolerance_seconds = min(max(future_tolerance_seconds, 0.0), 30.0)
    if not report_enabled:
        _set_projection_diagnostic(payload, "disabled", "report_disabled")
        return
    if now.tzinfo is None or now.utcoffset() is None:
        _set_projection_diagnostic(payload, "rejected", "report_clock_not_aware")
        return

    expected_expiry, expected_session_id, expected_session, identity_error = _report_identity(
        payload
    )
    if identity_error is not None:
        _set_projection_diagnostic(payload, "rejected", identity_error)
        return

    latest_candidate = load_json(latest_spring_gamma_v3_shadow_path(data_root))
    if expected_session == "rth":
        payload["spring_gamma_v3_state_window"] = build_spring_gamma_v3_state_window(
            data_root,
            now=now,
            session_id=expected_session_id,
            expiry=expected_expiry,
            future_tolerance_seconds=future_tolerance_seconds,
            latest_candidate=latest_candidate,
        )
    if not latest_candidate:
        _set_projection_diagnostic(payload, "rejected", "latest_projection_missing")
        return
    candidate = latest_candidate
    selection_source = "latest_projection"
    latest_shadow_as_of: str | None = None
    try:
        latest_shadow = validate_spring_gamma_v3_shadow(latest_candidate)
        latest_at = datetime.fromisoformat(str(latest_shadow["as_of"]))
        latest_shadow_as_of = latest_at.isoformat()
    except (TypeError, ValueError):
        latest_at = None
    if (
        latest_at is not None
        and (now - latest_at).total_seconds() < -future_tolerance_seconds
    ):
        fallback = _latest_causal_projection(
            data_root,
            now=now,
            max_age_seconds=max_age_seconds,
            expected_expiry=expected_expiry,
            expected_session_id=expected_session_id,
            expected_session=expected_session,
        )
        if fallback is not None:
            candidate = fallback
            selection_source = "durable_causal_fallback"
    try:
        shadow = validate_spring_gamma_v3_shadow(candidate)
        shadow_as_of = datetime.fromisoformat(str(shadow["as_of"]))
    except (TypeError, ValueError):
        _set_projection_diagnostic(payload, "rejected", "latest_projection_invalid")
        return

    age_seconds = (now - shadow_as_of).total_seconds()
    diagnostic_fields = {
        "shadow_as_of": shadow_as_of.isoformat(),
        "age_seconds": round(age_seconds, 3),
        "max_age_seconds": round(max_age_seconds, 3),
        "future_tolerance_seconds": round(future_tolerance_seconds, 3),
        "selection_source": selection_source,
        **(
            {"latest_shadow_as_of": latest_shadow_as_of}
            if selection_source == "durable_causal_fallback" and latest_shadow_as_of
            else {}
        ),
    }
    if age_seconds < -future_tolerance_seconds:
        _set_projection_diagnostic(
            payload,
            "rejected",
            "projection_future_beyond_tolerance",
            **diagnostic_fields,
        )
        return
    if age_seconds > max_age_seconds:
        _set_projection_diagnostic(
            payload,
            "rejected",
            "projection_stale",
            **diagnostic_fields,
        )
        return
    if str(shadow.get("expiry") or "") != expected_expiry:
        _set_projection_diagnostic(payload, "rejected", "expiry_mismatch", **diagnostic_fields)
        return
    if str(shadow.get("session_id") or "") != expected_session_id:
        _set_projection_diagnostic(payload, "rejected", "session_id_mismatch", **diagnostic_fields)
        return
    if expected_session and str(shadow.get("session") or "") != expected_session:
        _set_projection_diagnostic(
            payload, "rejected", "session_kind_mismatch", **diagnostic_fields
        )
        return

    payload["spring_gamma_v3_shadow"] = shadow
    if not _rolling_path_usable(shadow, now=now):
        path_fallback = _latest_causal_rolling_path(
            data_root,
            now=now,
            expected_expiry=expected_expiry,
            expected_session_id=expected_session_id,
            expected_session=expected_session,
        )
        if path_fallback is not None:
            payload["spring_gamma_v3_path_fallback"] = path_fallback
    _set_projection_diagnostic(
        payload,
        "attached",
        (
            "projection_durable_causal_fallback"
            if selection_source == "durable_causal_fallback"
            else "projection_future_within_tolerance"
            if age_seconds < 0
            else "projection_current"
        ),
        **diagnostic_fields,
    )


def build_spring_gamma_v3_state_window(
    data_root: str | Path,
    *,
    now: datetime,
    session_id: str,
    expiry: str,
    future_tolerance_seconds: float = PROJECTION_FUTURE_TOLERANCE_SECONDS,
    latest_candidate: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Summarize durable one-minute RTH states over the report's last 15 minutes."""

    window_start = now - timedelta(minutes=STATE_WINDOW_MINUTES)
    future_cutoff = now + timedelta(seconds=max(future_tolerance_seconds, 0.0))
    candidates = _load_window_candidates(
        data_root,
        window_start=window_start,
        future_cutoff=future_cutoff,
        latest_candidate=latest_candidate,
    )
    observations: list[tuple[datetime, str]] = []
    prediction_keys: set[tuple[str, datetime]] = set()
    for candidate in candidates:
        try:
            record = validate_spring_gamma_v3_shadow(candidate)
            observed_at = datetime.fromisoformat(str(record["as_of"]))
        except (TypeError, ValueError):
            continue
        prediction_id = str(record.get("prediction_id") or "")
        prediction_key = (prediction_id, observed_at)
        if prediction_key in prediction_keys:
            continue
        if observed_at > future_cutoff:
            continue
        effective_at = min(observed_at, now)
        if not window_start < effective_at <= now:
            continue
        if str(record.get("session") or "") != "rth":
            continue
        if str(record.get("session_id") or "") != session_id:
            continue
        if str(record.get("expiry") or "") != expiry:
            continue
        market_state = record.get("rth_market_state")
        if (
            not isinstance(market_state, dict)
            or market_state.get("schema_version") != "market_state_5m.v1"
        ):
            continue
        state = str(market_state.get("state") or "").strip().upper()
        if not state:
            continue
        prediction_keys.add(prediction_key)
        observations.append((observed_at, state))

    observations.sort(key=lambda item: item[0])
    counts = Counter(state for _, state in observations)
    buckets_by_state: dict[str, set[str]] = defaultdict(set)
    all_buckets: set[str] = set()
    max_future_skew_seconds = 0.0
    for observed_at, state in observations:
        effective_at = min(observed_at, now)
        utc_at = effective_at.astimezone(timezone.utc)
        bucket_at = utc_at.replace(
            minute=(utc_at.minute // 5) * 5,
            second=0,
            microsecond=0,
        )
        bucket = bucket_at.isoformat()
        buckets_by_state[state].add(bucket)
        all_buckets.add(bucket)
        max_future_skew_seconds = max(
            max_future_skew_seconds,
            (observed_at - now).total_seconds(),
        )

    unknown_states = sorted(set(counts).difference(_STATE_ORDER))
    states = [state for state in (*_STATE_ORDER, *unknown_states) if counts[state]]
    latest_state = observations[-1][1] if observations else None
    latest_state_as_of = observations[-1][0].isoformat() if observations else None
    return {
        "schema_version": STATE_WINDOW_SCHEMA,
        "session_id": session_id,
        "session": "rth",
        "expiry": expiry,
        "window_start": window_start.isoformat(),
        "window_end": now.isoformat(),
        "window_minutes": STATE_WINDOW_MINUTES,
        "sample_count": len(observations),
        "states": states,
        "counts": {state: counts[state] for state in states},
        "five_minute_slot_count": len(all_buckets),
        "five_minute_slot_counts": {state: len(buckets_by_state[state]) for state in states},
        "latest_state": latest_state,
        "latest_state_as_of": latest_state_as_of,
        "future_tolerance_seconds": round(max(future_tolerance_seconds, 0.0), 3),
        "max_future_skew_seconds": round(max(max_future_skew_seconds, 0.0), 3),
        "source": "durable_spring_gamma_v3_predictions",
        "action_authority": "none",
        "actionable": False,
    }


def _report_identity(
    payload: dict[str, Any],
) -> tuple[str, str, str, str | None]:
    expected_expiry = str(payload.get("expiry") or "")
    if not expected_expiry:
        return "", "", "", "report_expiry_missing"
    frame = (
        payload.get("minute_market_frame")
        if isinstance(payload.get("minute_market_frame"), dict)
        else {}
    )
    expected_sessions = {
        value
        for value in (
            str(frame.get("session_id") or ""),
            str(payload.get("trading_date") or ""),
        )
        if value
    }
    if not expected_sessions:
        return expected_expiry, "", "", "report_session_id_missing"
    if len(expected_sessions) != 1:
        return expected_expiry, "", "", "report_session_identity_conflict"
    expected_session_id = next(iter(expected_sessions))
    diagnostics = frame.get("diagnostics")
    frame_diagnostics = diagnostics if isinstance(diagnostics, dict) else {}
    segment = str(frame.get("session") or frame_diagnostics.get("segment") or "").strip().lower()
    expected_session = (
        "rth"
        if segment == "rth"
        else "gth"
        if segment in {"asia", "europe", "us_premarket", "curb", "gth"}
        else ""
    )
    if segment and not expected_session:
        return expected_expiry, expected_session_id, "", "report_segment_unknown"
    return expected_expiry, expected_session_id, expected_session, None


def _load_window_candidates(
    data_root: str | Path,
    *,
    window_start: datetime,
    future_cutoff: datetime,
    latest_candidate: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    day = window_start.astimezone(timezone.utc).date()
    final_day = future_cutoff.astimezone(timezone.utc).date()
    root = Path(data_root).expanduser()
    while day <= final_day:
        path = (
            root / "features" / "spring_gamma_v3" / f"date={day.isoformat()}" / "predictions.jsonl"
        )
        try:
            with path.open(encoding="utf-8") as handle:
                for line in handle:
                    try:
                        candidate = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(candidate, dict):
                        candidates.append(candidate)
        except OSError:
            pass
        day += timedelta(days=1)
    if isinstance(latest_candidate, dict):
        candidates.append(latest_candidate)
    return candidates


def _latest_causal_projection(
    data_root: str | Path,
    *,
    now: datetime,
    max_age_seconds: float,
    expected_expiry: str,
    expected_session_id: str,
    expected_session: str,
) -> dict[str, Any] | None:
    """Select the newest durable projection valid at the report's fixed clock."""

    candidates = _load_window_candidates(
        data_root,
        window_start=now - timedelta(seconds=max_age_seconds),
        future_cutoff=now,
        latest_candidate=None,
    )
    eligible: list[tuple[datetime, dict[str, Any]]] = []
    for candidate in candidates:
        try:
            shadow = validate_spring_gamma_v3_shadow(candidate)
            observed_at = datetime.fromisoformat(str(shadow["as_of"]))
        except (TypeError, ValueError):
            continue
        age_seconds = (now - observed_at).total_seconds()
        if age_seconds < 0 or age_seconds > max_age_seconds:
            continue
        if str(shadow.get("expiry") or "") != expected_expiry:
            continue
        if str(shadow.get("session_id") or "") != expected_session_id:
            continue
        if expected_session and str(shadow.get("session") or "") != expected_session:
            continue
        eligible.append((observed_at, shadow))
    return max(eligible, key=lambda item: item[0])[1] if eligible else None


def _latest_causal_rolling_path(
    data_root: str | Path,
    *,
    now: datetime,
    expected_expiry: str,
    expected_session_id: str,
    expected_session: str,
) -> dict[str, Any] | None:
    """Preserve a recent path modifier when the current strict frame drops out."""

    candidates = _load_window_candidates(
        data_root,
        window_start=now - timedelta(seconds=PATH_FALLBACK_MAX_AGE_SECONDS),
        future_cutoff=now,
        latest_candidate=None,
    )
    eligible: list[tuple[datetime, dict[str, Any], dict[str, Any]]] = []
    for candidate in candidates:
        try:
            shadow = validate_spring_gamma_v3_shadow(candidate)
            observed_at = datetime.fromisoformat(str(shadow["as_of"]))
        except (TypeError, ValueError):
            continue
        age_seconds = (now - observed_at).total_seconds()
        if age_seconds < 0 or age_seconds > PATH_FALLBACK_MAX_AGE_SECONDS:
            continue
        if str(shadow.get("expiry") or "") != expected_expiry:
            continue
        if str(shadow.get("session_id") or "") != expected_session_id:
            continue
        if expected_session and str(shadow.get("session") or "") != expected_session:
            continue
        path = _rolling_path(shadow)
        if not _rolling_path_usable(shadow, now=now):
            continue
        eligible.append((observed_at, shadow, path))
    if not eligible:
        return None

    observed_at, shadow, source_path = max(eligible, key=lambda item: item[0])
    path = dict(source_path)
    latest_bar_end = _aware_datetime(path.get("latest_bar_end"))
    if latest_bar_end is None:  # Guarded by _rolling_path_usable.
        return None
    source_shadow_lag_seconds = max((now - observed_at).total_seconds(), 0.0)
    source_bar_lag_seconds = max((now - latest_bar_end).total_seconds(), 0.0)
    source_input_quality = str(path.get("input_quality") or "strict")
    path.update(
        {
            "confidence": "low",
            "input_quality": "stale_fallback",
            "source_input_quality": source_input_quality,
            "source_as_of": observed_at.isoformat(),
            "source_latest_bar_end": latest_bar_end.isoformat(),
            "source_shadow_lag_seconds": round(source_shadow_lag_seconds, 3),
            "source_bar_lag_seconds": round(source_bar_lag_seconds, 3),
            "source_lag_seconds": round(
                max(source_shadow_lag_seconds, source_bar_lag_seconds),
                3,
            ),
            "fallback_reason": "current_path_unavailable",
            "action_authority": "none",
        }
    )
    return {
        "schema_version": PATH_FALLBACK_SCHEMA,
        "prediction_id": shadow.get("prediction_id"),
        "session_id": expected_session_id,
        "expiry": expected_expiry,
        "source_as_of": observed_at.isoformat(),
        "source_latest_bar_end": latest_bar_end.isoformat(),
        "maximum_age_seconds": PATH_FALLBACK_MAX_AGE_SECONDS,
        "rolling_path_percentiles": path,
        "reason": "current_path_unavailable_recent_causal_path_retained",
        "action_authority": "none",
        "actionable": False,
        "automatic_ordering": False,
    }


def _rolling_path(shadow: dict[str, Any]) -> dict[str, Any]:
    market_state = shadow.get("rth_market_state")
    if not isinstance(market_state, dict):
        return {}
    lineage = market_state.get("input_lineage")
    if not isinstance(lineage, dict):
        return {}
    diagnostics = lineage.get("diagnostics")
    if not isinstance(diagnostics, dict):
        return {}
    path = diagnostics.get("rolling_path_percentiles")
    return dict(path) if isinstance(path, dict) else {}


def _rolling_path_usable(
    shadow: dict[str, Any],
    *,
    now: datetime,
) -> bool:
    path = _rolling_path(shadow)
    if str(path.get("status") or "") not in {"ready", "provisional"}:
        return False
    if (finite_float(path.get("sample_count")) or 0) < 5:
        return False
    dip = path.get("dip")
    rally = path.get("rally")
    values_usable = bool(
        isinstance(dip, dict)
        and finite_float(dip.get("shrunk_percentile")) is not None
        and isinstance(rally, dict)
        and finite_float(rally.get("shrunk_percentile")) is not None
    )
    if not values_usable:
        return False
    observed_at = _aware_datetime(shadow.get("as_of"))
    latest_bar_end = _aware_datetime(path.get("latest_bar_end"))
    if observed_at is None or latest_bar_end is None:
        return False
    if latest_bar_end > observed_at:
        return False
    bar_age_seconds = (now - latest_bar_end).total_seconds()
    return 0 <= bar_age_seconds <= PATH_FALLBACK_MAX_AGE_SECONDS


def _aware_datetime(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None and parsed.utcoffset() is not None else None


def _set_projection_diagnostic(
    payload: dict[str, Any],
    status: str,
    reason: str,
    **fields: object,
) -> None:
    payload["spring_gamma_v3_projection_diagnostic"] = {
        "schema_version": PROJECTION_DIAGNOSTIC_SCHEMA,
        "status": status,
        "reason": reason,
        **fields,
    }
