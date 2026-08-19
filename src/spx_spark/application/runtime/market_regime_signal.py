"""Publish advisory-only online regime and same-day range research context."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import signal
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from time import monotonic

from spx_spark.config import StorageSettings
from spx_spark.application.runtime.market_regime_context import (
    build_research_context_document,
)
from spx_spark.application.runtime.market_regime_denoising import (
    DENOISING_FORWARD_CONTRACT_HASH,
    WALL_HAZARD_CONTRACT_HASH,
    advance_denoising_forward,
)
from spx_spark.application.runtime.market_regime_observation import (
    CROSS_INDEX_FEATURE_SET_VERSION,
    ES_FEATURE_WEIGHTS,
    FEATURE_SCHEMA_VERSION,
    OBSERVATION_COMPONENT_WEIGHTS,
    build_feature_observation,
)
from spx_spark.application.runtime.market_regime_range import (
    MarketRegimeFreshnessPolicy,
    build_intraday_extreme_ranges,
    causal_spx_session_minutes,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.settings import current_app_settings
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock, read_json_object


UTC = timezone.utc
SCHEMA_VERSION = "market_regime_signal.experimental.v2"
MODEL_SCHEMA_VERSION = "online_gaussian_hmm_3state_cross_index.v4"
RANGE_SCHEMA_VERSION = "same_day_range.experimental.v2"
STATE_NAMES = ("state_00", "state_01", "state_02")
STATE_HINTS = {
    "state_00": "down_pressure",
    "state_01": "balanced",
    "state_02": "up_pressure",
}
TRANSITION = (
    (0.94, 0.055, 0.005),
    (0.04, 0.92, 0.04),
    (0.005, 0.055, 0.94),
)
EMISSION_MEANS = (-0.75, 0.0, 0.75)
EMISSION_SIGMAS = (0.55, 0.45, 0.55)
P10_Z = 1.2815515655446004
FUTURE_TOLERANCE_SECONDS = 2.0
HMM_CLOSE_SHIFT_FRACTION = 0.25
HMM_ADJUSTED_RANGE_VERSION = "hmm-adjusted-close:k0p25:v1"

_MODEL_SPEC = {
    "schema_version": MODEL_SCHEMA_VERSION,
    "states": STATE_NAMES,
    "transition": TRANSITION,
    "emission_means": EMISSION_MEANS,
    "emission_sigmas": EMISSION_SIGMAS,
    "feature_schema_version": FEATURE_SCHEMA_VERSION,
    "feature_weights": ES_FEATURE_WEIGHTS,
    "observation_component_weights": OBSERVATION_COMPONENT_WEIGHTS,
    "cash_index_features": (
        "relative_to_spx_15m_bps",
        "dispersion_15m_bps",
        "breadth_15m",
    ),
    "globex_index_features": (
        "relative_to_es_15m_bps",
        "dispersion_15m_bps",
        "breadth_15m",
    ),
    "cross_index_session_policy": "rth_cash_gth_globex_futures_percent_vs_es",
    "observation_owner": "session_selected_cross_index",
    "prior_rth_schema_version": "prior_rth_context.v2",
    "parameter_mode": "fixed_bootstrap",
    "inference": "causal_forward_filter",
    "observation_cadence": "one_update_per_market_frame_id",
}
MODEL_VERSION = (
    "sha256:"
    + hashlib.sha256(
        json.dumps(_MODEL_SPEC, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
)


@dataclass(frozen=True, slots=True)
class SignalPaths:
    market: Path
    options: Path
    spx_minutes: Path
    latest_state: Path
    prior_rth_context: Path
    output: Path
    state: Path

    @classmethod
    def from_data_root(cls, data_root: str | Path) -> "SignalPaths":
        latest = Path(data_root).expanduser() / "latest"
        return cls(
            market=latest / "minute_market_frame.json",
            options=latest / "option_structure_frame.json",
            spx_minutes=latest / "spx_standardized_minutes.json",
            latest_state=latest / "state.json",
            prior_rth_context=latest / "prior_rth_context.json",
            output=latest / "experimental_research_signals.json",
            state=latest / "experimental_research_signals.state.json",
        )


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def _mapping(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, Mapping) else {}


def _parse_at(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def _canonical_hash(value: object) -> str:
    encoded = json.dumps(
        value,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _local_frame_freshness(
    value: Mapping[str, object],
    *,
    now: datetime,
    max_age_seconds: float,
    label: str,
) -> tuple[bool, str | None]:
    as_of = _parse_at(value.get("as_of"))
    if as_of is None:
        return False, f"{label}_as_of_missing"
    age_seconds = (now - as_of).total_seconds()
    if age_seconds < 0.0:
        return False, f"{label}_from_future"
    if age_seconds > max_age_seconds:
        return False, f"{label}_stale"
    return True, None


def _causal_prior_rth_context(
    prior: Mapping[str, object],
    *,
    now: datetime,
) -> Mapping[str, object]:
    as_of = _parse_at(prior.get("as_of"))
    if as_of is not None and as_of <= now:
        return prior
    reason = (
        "prior_rth_context_as_of_missing"
        if as_of is None
        else "prior_rth_context_from_future"
    )
    return {
        "schema_version": prior.get("schema_version"),
        "status": "unavailable",
        "as_of": prior.get("as_of"),
        "for_trading_date": prior.get("for_trading_date"),
        "session_date": prior.get("session_date"),
        "reasons": [reason],
        "cross_index": {
            "status": "unavailable",
            "reason_codes": [reason],
        },
    }


def _session_date(market: Mapping[str, object], now: datetime) -> date | None:
    raw = market.get("session_id")
    if isinstance(raw, str):
        try:
            candidate = date.fromisoformat(raw)
        except ValueError:
            candidate = None
        if candidate is not None and DEFAULT_MARKET_CALENDAR.session(candidate) is not None:
            return candidate
    return DEFAULT_MARKET_CALENDAR.spx_session_date_for(now, retain_completed=True)


def _expected_move(options: Mapping[str, object]) -> float | None:
    value = _number(_mapping(options.get("volatility")).get("expected_move_points_0dte"))
    return value if value is not None and value > 0.0 else None


def _online_posterior(score: float, prior: Sequence[float]) -> tuple[float, float, float]:
    predicted = tuple(
        sum(float(prior[source]) * TRANSITION[source][target] for source in range(3))
        for target in range(3)
    )
    log_weights = []
    for probability, mean, sigma in zip(predicted, EMISSION_MEANS, EMISSION_SIGMAS, strict=True):
        log_likelihood = -math.log(sigma) - 0.5 * ((score - mean) / sigma) ** 2
        log_weights.append(math.log(max(probability, 1e-15)) + log_likelihood)
    maximum = max(log_weights)
    weights = [math.exp(value - maximum) for value in log_weights]
    total = sum(weights)
    return tuple(value / total for value in weights)  # type: ignore[return-value]


def _validated_prior(value: object) -> tuple[float, float, float] | None:
    if not isinstance(value, list) or len(value) != 3:
        return None
    probabilities = [_number(item) for item in value]
    if any(item is None or item < 0.0 for item in probabilities):
        return None
    resolved = [float(item) for item in probabilities if item is not None]
    total = math.fsum(resolved)
    if total <= 0.0:
        return None
    return tuple(item / total for item in resolved)  # type: ignore[return-value]


def _regime_projection(
    *,
    market: Mapping[str, object],
    options: Mapping[str, object],
    prior_rth_context: Mapping[str, object],
    previous: Mapping[str, object],
    now: datetime,
    max_input_age_seconds: float,
    session_day: date | None,
) -> tuple[dict[str, object], dict[str, object]]:
    option_frame_fresh, option_frame_reason = _local_frame_freshness(
        options,
        now=now,
        max_age_seconds=max_input_age_seconds,
        label="option_frame",
    )
    observation = build_feature_observation(
        market,
        options if option_frame_fresh else {},
        prior_rth_context,
        session_day=session_day,
    )
    if observation is not None and option_frame_reason is not None:
        degradation_reasons = list(observation.get("degradation_reasons") or ())
        observation["degradation_reasons"] = sorted(
            set([*degradation_reasons, option_frame_reason])
        )
    market_as_of = _parse_at(market.get("as_of"))
    previous_state = _mapping(previous.get("online_state"))
    retained_state: dict[str, object] = {
        "model_version": MODEL_VERSION,
        "session_id": previous_state.get("session_id"),
        "last_observation_id": previous_state.get("last_observation_id"),
        "last_as_of": previous_state.get("last_as_of"),
        "observation_count": int(previous_state.get("observation_count") or 0),
        "posterior": previous_state.get("posterior") or [1 / 3, 1 / 3, 1 / 3],
        "dominant_state": previous_state.get("dominant_state"),
        "dwell_observations": int(previous_state.get("dwell_observations") or 0),
    }
    reasons: list[str] = []
    if market_as_of is None:
        reasons.append("market_as_of_missing")
    elif (now - market_as_of).total_seconds() > max_input_age_seconds:
        reasons.append("market_frame_stale")
    elif market_as_of > now:
        reasons.append("market_frame_from_future")
    if str(market.get("quality") or "unavailable") == "unavailable":
        reasons.append("market_frame_unavailable")
    if observation is None:
        reasons.append("direction_features_missing")
    if session_day is None:
        reasons.append("session_date_unavailable")
    if reasons:
        return (
            {
                "status": "unavailable",
                "quality": "unavailable",
                "reasons": sorted(set(reasons)),
                "model_schema_version": MODEL_SCHEMA_VERSION,
                "model_version": MODEL_VERSION,
                "posterior": None,
                "dominant_state": None,
                "action_authority": "none",
            },
            retained_state,
        )

    assert market_as_of is not None and observation is not None and session_day is not None
    session_id = session_day.isoformat()
    same_session = (
        previous_state.get("model_version") == MODEL_VERSION
        and previous_state.get("session_id") == session_id
    )
    prior_raw = previous_state.get("posterior") if same_session else None
    prior = _validated_prior(prior_raw) or (1 / 3, 1 / 3, 1 / 3)
    observation_id = str(observation["observation_id"])
    last_as_of = _parse_at(previous_state.get("last_as_of")) if same_session else None
    duplicate = same_session and previous_state.get("last_observation_id") == observation_id
    out_of_order = last_as_of is not None and market_as_of < last_as_of
    if duplicate:
        posterior = prior
        advanced = False
    elif out_of_order:
        return (
            {
                "status": "unavailable",
                "quality": "unavailable",
                "reasons": ["observation_out_of_order"],
                "model_schema_version": MODEL_SCHEMA_VERSION,
                "model_version": MODEL_VERSION,
                "posterior": None,
                "dominant_state": None,
                "action_authority": "none",
            },
            retained_state,
        )
    else:
        posterior = _online_posterior(float(observation["direction_score"]), prior)
        advanced = True

    probabilities = dict(zip(STATE_NAMES, posterior, strict=True))
    dominant = max(STATE_NAMES, key=probabilities.__getitem__)
    entropy = -sum(value * math.log(value) for value in posterior if value > 0.0)
    count = int(previous_state.get("observation_count") or 0) if same_session else 0
    previous_dominant = previous_state.get("dominant_state") if same_session else None
    dwell = int(previous_state.get("dwell_observations") or 0) if same_session else 0
    if advanced:
        count += 1
        dwell = dwell + 1 if previous_dominant == dominant else 1
    state = {
        "model_version": MODEL_VERSION,
        "session_id": session_id,
        "last_observation_id": observation_id,
        "last_as_of": market_as_of.isoformat(),
        "observation_count": count,
        "posterior": list(posterior),
        "dominant_state": dominant,
        "dwell_observations": max(dwell, 1),
    }
    observation_reasons = observation.get("degradation_reasons")
    reasons = ["fixed_bootstrap_parameters_unvalidated"]
    if isinstance(observation_reasons, list):
        reasons.extend(str(value) for value in observation_reasons if isinstance(value, str))
    if market.get("quality") != "ready":
        reasons.append("market_frame_degraded")
    return (
        {
            "status": "available",
            "quality": "degraded",
            "reasons": sorted(set(reasons)),
            "model_schema_version": MODEL_SCHEMA_VERSION,
            "model_version": MODEL_VERSION,
            "parameter_mode": "fixed_bootstrap",
            "inference": "causal_forward_filter",
            "state_hints": STATE_HINTS,
            "posterior": {key: round(value, 10) for key, value in probabilities.items()},
            "dominant_state": dominant,
            "max_state_probability": round(probabilities[dominant], 10),
            "entropy_nats": round(entropy, 10),
            "normalized_entropy": round(entropy / math.log(3.0), 10),
            "observation_count": count,
            "dwell_observations": max(dwell, 1),
            "advanced": advanced,
            "as_of": market_as_of.isoformat(),
            "observation": observation,
            "action_authority": "none",
        },
        state,
    )


def _selected_price(row: Mapping[str, object]) -> float | None:
    selected = _mapping(row.get("selected"))
    value = _number(selected.get("price"))
    return value if value is not None and value > 0.0 else None


def _live_es_price_and_close(
    latest_state: Mapping[str, object],
    *,
    now: datetime,
    max_input_age_seconds: float,
) -> tuple[float | None, float | None]:
    rows = latest_state.get("best_quotes")
    if not isinstance(rows, list):
        return None, None
    candidates: list[tuple[int, datetime, float, float]] = []
    for value in rows:
        row = _mapping(value)
        instrument = _mapping(row.get("instrument"))
        canonical_id = str(instrument.get("canonical_id") or row.get("instrument_id") or "")
        if not canonical_id.startswith("future:ES") or row.get("quality") != "live":
            continue
        price = _number(row.get("mid")) or _number(row.get("effective_price"))
        close = _number(row.get("close"))
        source_at = (
            _parse_at(row.get("quote_time"))
            or _parse_at(row.get("trade_time"))
        )
        transport_at = _parse_at(row.get("last_update_at")) or _parse_at(
            row.get("received_at")
        )
        source_age_seconds = (now - source_at).total_seconds() if source_at is not None else None
        transport_age_seconds = (
            (now - transport_at).total_seconds() if transport_at is not None else None
        )
        if (
            price is not None
            and price > 0.0
            and close is not None
            and close > 0.0
            and source_at is not None
            and transport_at is not None
            and source_age_seconds is not None
            and transport_age_seconds is not None
            and -FUTURE_TOLERANCE_SECONDS
            <= source_age_seconds
            <= max_input_age_seconds
            and 0.0 <= transport_age_seconds <= max_input_age_seconds
            and source_at <= transport_at + timedelta(seconds=FUTURE_TOLERANCE_SECONDS)
        ):
            candidates.append(
                (1 if row.get("provider") == "ibkr" else 0, source_at, price, close)
            )
    if not candidates:
        return None, None
    _, _, price, close = max(candidates)
    return price, close


def _observed_edge(
    spx_minutes: Mapping[str, object],
    *,
    session_day: date,
    edge: str,
    now: datetime,
) -> tuple[float, datetime] | None:
    session = DEFAULT_MARKET_CALENDAR.session(session_day)
    if session is None:
        return None
    candidates = causal_spx_session_minutes(
        spx_minutes,
        session_day=session_day,
        now=now,
    )
    if not candidates:
        return None
    if edge == "open":
        sample = candidates[0]
        return (
            (sample.price, sample.minute)
            if abs((sample.minute - session.open_at.astimezone(UTC)).total_seconds()) <= 120
            else None
        )
    sample = candidates[-1]
    return (
        (sample.price, sample.minute)
        if abs((sample.minute - session.close_at.astimezone(UTC)).total_seconds()) <= 120
        else None
    )


def _unavailable_range(reason: str, *, target_at: datetime | None) -> dict[str, object]:
    return {
        "status": "unavailable",
        "quality": "unavailable",
        "reason": reason,
        "p10": None,
        "p50": None,
        "p90": None,
        "target_at": target_at.isoformat() if target_at is not None else None,
    }


def _normal_range(
    *,
    anchor: float,
    sigma_points: float,
    source: str,
    semantics: str,
    target_at: datetime,
) -> dict[str, object]:
    width = P10_Z * sigma_points
    return {
        "status": "available",
        "quality": "degraded",
        "source": source,
        "semantics": semantics,
        "p10": round(anchor - width, 4),
        "p50": round(anchor, 4),
        "p90": round(anchor + width, 4),
        "target_at": target_at.isoformat(),
    }


def _range_projection(
    *,
    market: Mapping[str, object],
    options: Mapping[str, object],
    spx_minutes: Mapping[str, object],
    latest_state: Mapping[str, object],
    prior_rth_context: Mapping[str, object],
    session_day: date | None,
    now: datetime,
    freshness_policy: MarketRegimeFreshnessPolicy,
) -> dict[str, object]:
    if session_day is None:
        missing = _unavailable_range("session_date_unavailable", target_at=None)
        return {
            "schema_version": RANGE_SCHEMA_VERSION,
            "session_date": None,
            "open": missing,
            "close": missing,
            "high": missing,
            "low": missing,
        }
    session = DEFAULT_MARKET_CALENDAR.session(session_day)
    assert session is not None
    observed_open = _observed_edge(
        spx_minutes,
        session_day=session_day,
        edge="open",
        now=now,
    )
    observed_close = _observed_edge(
        spx_minutes,
        session_day=session_day,
        edge="close",
        now=now,
    )
    if observed_open is not None:
        price, at = observed_open
        open_range = {
            "status": "observed",
            "quality": "ready",
            "source": "spx_standardized_minutes",
            "semantics": "observed_rth_open_degenerate",
            "p10": price,
            "p50": price,
            "p90": price,
            "target_at": at.isoformat(),
        }
    elif now >= session.open_at.astimezone(UTC):
        open_range = _unavailable_range(
            "official_rth_open_observation_missing",
            target_at=session.open_at.astimezone(UTC),
        )
    else:
        es_price, es_close = _live_es_price_and_close(
            latest_state,
            now=now,
            max_input_age_seconds=freshness_policy.live_input_max_age_seconds,
        )
        prior_spx_close = _number(prior_rth_context.get("close"))
        prior_for_date = prior_rth_context.get("for_trading_date")
        es_gap_anchor = (
            prior_spx_close + es_price - es_close
            if (
                es_price is not None
                and es_close is not None
                and prior_spx_close is not None
                and prior_for_date == session_day.isoformat()
            )
            else None
        )
        option_fresh, _option_reason = _local_frame_freshness(
            options,
            now=now,
            max_age_seconds=freshness_policy.live_input_max_age_seconds,
            label="option_frame",
        )
        option_expiry_ok = options.get("front_expiry") == session_day.strftime("%Y%m%d")
        expected_move = (
            _expected_move(options) if option_fresh and option_expiry_ok else None
        )
        if es_gap_anchor is None or expected_move is None:
            open_range = _unavailable_range(
                "open_proxy_inputs_missing",
                target_at=session.open_at.astimezone(UTC),
            )
        else:
            to_open = max((session.open_at.astimezone(UTC) - now).total_seconds(), 0.0)
            to_close = max((session.close_at.astimezone(UTC) - now).total_seconds(), 1.0)
            sigma = expected_move * math.sqrt(min(to_open / to_close, 1.0))
            open_range = _normal_range(
                anchor=es_gap_anchor,
                sigma_points=sigma,
                source="expected_move_with_prior_spx_close_plus_es_gap",
                semantics="experimental_time_scaled_normal_proxy_not_calibrated",
                target_at=session.open_at.astimezone(UTC),
            )

    if now >= session.close_at.astimezone(UTC) and observed_close is not None:
        price, at = observed_close
        close_range = {
            "status": "observed",
            "quality": "ready",
            "source": "spx_standardized_minutes",
            "semantics": "observed_rth_close_degenerate",
            "p10": price,
            "p50": price,
            "p90": price,
            "target_at": at.isoformat(),
        }
    else:
        option_as_of = _parse_at(options.get("as_of"))
        option_fresh, _option_reason = _local_frame_freshness(
            options,
            now=now,
            max_age_seconds=freshness_policy.live_input_max_age_seconds,
            label="option_frame",
        )
        expiry = str(options.get("front_expiry") or "")
        expiry_ok = bool(expiry) and expiry == session_day.strftime("%Y%m%d")
        density = _mapping(options.get("density"))
        p10 = _number(density.get("p10"))
        p50 = _number(density.get("median")) or _number(density.get("p50"))
        p90 = _number(density.get("p90"))
        density_ok = (
            density.get("quality") in {"ok", "ready", "live"}
            and p10 is not None
            and p50 is not None
            and p90 is not None
            and p10 < p50 < p90
        )
        if option_fresh and expiry_ok and density_ok:
            close_range = {
                "status": "available",
                "quality": "ready",
                "source": "option_structure_frame.density",
                "semantics": "risk_neutral_terminal_not_physical",
                "p10": p10,
                "p50": p50,
                "p90": p90,
                "target_at": session.close_at.astimezone(UTC).isoformat(),
                "expiry": expiry or None,
                "as_of": option_as_of.isoformat() if option_as_of else None,
                "expected_move_points": _expected_move(options),
            }
        else:
            reason = (
                "front_expiry_mismatch"
                if not expiry_ok
                else "fresh_risk_neutral_density_unavailable"
            )
            close_range = _unavailable_range(
                reason,
                target_at=session.close_at.astimezone(UTC),
            )
    high_range, low_range = build_intraday_extreme_ranges(
        options=options,
        spx_minutes=spx_minutes,
        session_day=session_day,
        now=now,
        freshness_policy=freshness_policy,
    )
    return {
        "schema_version": RANGE_SCHEMA_VERSION,
        "session_date": session_day.isoformat(),
        "open": open_range,
        "close": close_range,
        "high": high_range,
        "low": low_range,
    }


def build_signal(
    *,
    market: Mapping[str, object],
    options: Mapping[str, object],
    spx_minutes: Mapping[str, object],
    latest_state: Mapping[str, object],
    prior_rth_context: Mapping[str, object],
    previous: Mapping[str, object],
    now: datetime,
    freshness_policy: MarketRegimeFreshnessPolicy,
) -> dict[str, object]:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    now = now.astimezone(UTC)
    session_day = _session_date(market, now)
    causal_prior_rth_context = _causal_prior_rth_context(
        prior_rth_context,
        now=now,
    )
    regime, online_state = _regime_projection(
        market=market,
        options=options,
        prior_rth_context=causal_prior_rth_context,
        previous=previous,
        now=now,
        max_input_age_seconds=freshness_policy.live_input_max_age_seconds,
        session_day=session_day,
    )
    ranges = _range_projection(
        market=market,
        options=options,
        spx_minutes=spx_minutes,
        latest_state=latest_state,
        prior_rth_context=causal_prior_rth_context,
        session_day=session_day,
        now=now,
        freshness_policy=freshness_policy,
    )
    denoising_forward, denoising_forward_state = advance_denoising_forward(
        market,
        options,
        previous,
        now=now,
        session_day=session_day,
    )
    source_times = [
        parsed
        for parsed in (
            _parse_at(market.get("as_of")),
            _parse_at(options.get("as_of")),
            _parse_at(latest_state.get("as_of")),
            _parse_at(prior_rth_context.get("as_of")),
        )
        if parsed is not None
    ]
    fingerprint = _canonical_hash(
        {
            "market": market,
            "options": options,
            "spx_minutes": spx_minutes,
            "latest_state": latest_state,
            "prior_rth_context": prior_rth_context,
        }
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "experimental": True,
        "generated_at": now.isoformat(),
        "as_of": max(source_times).isoformat() if source_times else None,
        "session_date": session_day.isoformat() if session_day else None,
        "input_fingerprint": fingerprint,
        "sources": {
            "market_frame_id": market.get("frame_id"),
            "market_as_of": market.get("as_of"),
            "option_frame_id": options.get("frame_id"),
            "option_as_of": options.get("as_of"),
        },
        "regime": regime,
        "today_range": ranges,
        "online_state": online_state,
        "denoising_forward": denoising_forward,
        "denoising_forward_state": denoising_forward_state,
        "readiness": "experimental_context_only",
        "action_authority": "none",
        "automatic_ordering": False,
    }


def produce_once(
    *,
    paths: SignalPaths,
    now: datetime,
    freshness_policy: MarketRegimeFreshnessPolicy,
    market: Mapping[str, object] | None = None,
    options: Mapping[str, object] | None = None,
) -> dict[str, object]:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    evaluated_at = now.astimezone(UTC)
    market = dict(market) if market is not None else read_json_object(paths.market)
    options = dict(options) if options is not None else read_json_object(paths.options)
    spx_minutes = read_json_object(paths.spx_minutes)
    latest_state = read_json_object(paths.latest_state)
    prior_rth_context = read_json_object(paths.prior_rth_context)
    with exclusive_state_lock(paths.state, timeout_seconds=5.0):
        previous = read_json_object(paths.state)
        fingerprint = _canonical_hash(
            {
                "market": market,
                "options": options,
                "spx_minutes": spx_minutes,
                "latest_state": latest_state,
                "prior_rth_context": prior_rth_context,
            }
        )
        evaluation_fingerprint = _canonical_hash(
            {
                "input_fingerprint": fingerprint,
                "evaluated_at": evaluated_at.isoformat(),
                "freshness_policy": {
                    "live_input_max_age_seconds": (
                        freshness_policy.live_input_max_age_seconds
                    ),
                    "standardized_spx_minute_max_age_seconds": (
                        freshness_policy.standardized_spx_minute_max_age_seconds
                    ),
                },
                "evaluator_versions": {
                    "signal_schema": SCHEMA_VERSION,
                    "model": MODEL_VERSION,
                    "range_schema": RANGE_SCHEMA_VERSION,
                    "research_context_schema": "research_context.v2",
                    "hmm_adjusted_range": HMM_ADJUSTED_RANGE_VERSION,
                    "denoising_forward": DENOISING_FORWARD_CONTRACT_HASH,
                    "wall_hazard": WALL_HAZARD_CONTRACT_HASH,
                },
            }
        )
        if (
            previous.get("schema_version") == "research_context.state.v2"
            and previous.get("evaluation_fingerprint") == evaluation_fingerprint
        ):
            cached = _mapping(previous.get("wire_document"))
            if cached.get("schema_version") == "research_context.v2":
                if read_json_object(paths.output) != cached:
                    atomic_write_json_secure(paths.output, cached)
                return cached
        payload = build_signal(
            market=market,
            options=options,
            spx_minutes=spx_minutes,
            latest_state=latest_state,
            prior_rth_context=prior_rth_context,
            previous=previous,
            now=evaluated_at,
            freshness_policy=freshness_policy,
        )
        payload["evaluation_fingerprint"] = evaluation_fingerprint
        wire = build_research_context_document(
            signal=payload,
            market=market,
            prior_rth_context=prior_rth_context,
            available_at=evaluated_at,
            regime_feature_set_version=FEATURE_SCHEMA_VERSION,
            cross_index_feature_set_version=CROSS_INDEX_FEATURE_SET_VERSION,
            model_version=MODEL_VERSION,
            state_names=STATE_NAMES,
            hmm_adjusted_model_version=HMM_ADJUSTED_RANGE_VERSION,
            hmm_close_shift_fraction=HMM_CLOSE_SHIFT_FRACTION,
            p10_z=P10_Z,
            market_frame_max_age_seconds=(
                freshness_policy.live_input_max_age_seconds
            ),
        )
        wire["denoising_forward"] = payload["denoising_forward"]
        state_payload = {
            "schema_version": "research_context.state.v2",
            "input_fingerprint": payload["input_fingerprint"],
            "evaluation_fingerprint": evaluation_fingerprint,
            "online_state": payload["online_state"],
            "denoising_forward_state": payload["denoising_forward_state"],
            "wire_document": wire,
        }
        atomic_write_json_secure(paths.state, state_payload)
        atomic_write_json_secure(paths.output, wire)
        return wire


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish an experimental online regime signal.")
    parser.add_argument("--data-root", type=Path)
    parser.add_argument("--market-path", type=Path)
    parser.add_argument("--options-path", type=Path)
    parser.add_argument("--spx-minutes-path", type=Path)
    parser.add_argument("--latest-state-path", type=Path)
    parser.add_argument("--prior-rth-context-path", type=Path)
    parser.add_argument("--output-path", type=Path)
    parser.add_argument("--state-path", type=Path)
    parser.add_argument("--interval-seconds", type=float, default=5.0)
    parser.add_argument("--max-input-age-seconds", type=float)
    parser.add_argument("--spx-minute-max-age-seconds", type=float)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--now", help="Aware ISO timestamp for deterministic --once replay.")
    parser.add_argument("--json", action="store_true", help="Print each produced projection.")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    storage = StorageSettings.from_env()
    market_data_settings = current_app_settings().market_data
    defaults = SignalPaths.from_data_root(args.data_root or storage.data_root)
    paths = SignalPaths(
        market=args.market_path or defaults.market,
        options=args.options_path or defaults.options,
        spx_minutes=args.spx_minutes_path or defaults.spx_minutes,
        latest_state=args.latest_state_path or defaults.latest_state,
        prior_rth_context=args.prior_rth_context_path or defaults.prior_rth_context,
        output=args.output_path or defaults.output,
        state=args.state_path or defaults.state,
    )
    if args.interval_seconds <= 0:
        raise ValueError("--interval-seconds must be positive")
    max_input_age_seconds = (
        float(args.max_input_age_seconds)
        if args.max_input_age_seconds is not None
        else float(storage.latest_stale_after_seconds)
    )
    if max_input_age_seconds <= 0:
        raise ValueError("--max-input-age-seconds must be positive")
    spx_minute_max_age_seconds = (
        float(args.spx_minute_max_age_seconds)
        if args.spx_minute_max_age_seconds is not None
        else market_data_settings.standardized_minute_max_age_seconds
    )
    if spx_minute_max_age_seconds <= 0:
        raise ValueError("--spx-minute-max-age-seconds must be positive")
    freshness_policy = MarketRegimeFreshnessPolicy(
        live_input_max_age_seconds=max_input_age_seconds,
        standardized_spx_minute_max_age_seconds=spx_minute_max_age_seconds,
    )
    fixed_now = _parse_at(args.now) if args.now else None
    if args.now and fixed_now is None:
        raise ValueError("--now must be an aware ISO-8601 timestamp")
    if fixed_now is not None and not args.once:
        raise ValueError("--now requires --once")
    stop = threading.Event()
    for signal_number in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signal_number, lambda *_args: stop.set())
    while not stop.is_set():
        started = monotonic()
        payload = produce_once(
            paths=paths,
            now=fixed_now or datetime.now(tz=UTC),
            freshness_policy=freshness_policy,
        )
        if args.json:
            print(json.dumps(payload, allow_nan=False, sort_keys=True), flush=True)
        if args.once:
            break
        stop.wait(max(args.interval_seconds - (monotonic() - started), 0.0))
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
