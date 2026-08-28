"""Promoted candidate-level edge model and sole manual-authority gate.

The runtime artifact is deliberately JSON and linear: training may use
scikit-learn, but production inference stays deterministic, auditable, and
free of pickle/joblib execution. Rules still enumerate legal structures and
protect data/quote/risk invariants; a promoted model alone may authorize a
manual card.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
import json
import math
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "strategy_edge_model.v1"
FEATURE_VERSION = "strategy_edge_features.v1"
ARTIFACT_RELATIVE_PATH = ("research", "strategy_edge_model.v1.json")
_PREAVERAGE_SETUP = "PREAVERAGE15_PULLBACK"
_PREAVERAGE_CONTRACT_HASH = (
    "sha256:fc276ff1d44bf4a150ff18889c445a6eaa68b12131b93b4c191765617fc1fb27"
)
_WALL_HAZARD_SETUP = "WALL_BREAKOUT_HAZARD"
_WALL_HAZARD_CONTRACT_HASH = (
    "sha256:ff0e0d1204b97af334ec3d65679bc0dcfdb9e4b3084912e650af6caef05494a2"
)
_RTH_LEVEL_CONFIRMATION_SETUP = "RTH_LEVEL_CONFIRMATION"
_RTH_LEVEL_CONFIRMATION_CONTRACT_HASH = (
    "sha256:7d576327f9f0bf9fe23c993392efd76ec512e34e671826550ea33d38ff7f0a6c"
)
_CLOSE_CONVERGENCE_SETUP = "CLOSE_CONVERGENCE_60M"
_CLOSE_CONVERGENCE_CONTRACT_HASH = (
    "sha256:095333c301d7317da804792c243002c4dd36116e982970ee391b1c4dbd926732"
)
_IRON_CONDOR_SETUP = "IRON_CONDOR_DELTA"
_IRON_CONDOR_CONTRACT_HASH = (
    "sha256:2a8a220ed3dee489ccb2373954ade3cdf2a5390f46ee3e9e46d6871299e2e680"
)
_GTH_MINUTE_GATE_POLICY = "strategy_policy.bootstrap.v48"
_GTH_MINUTE_GATE_CONTRACT_HASH = (
    "sha256:72e0036694a079ed8e6a18b930662a12979d827db3ca27ab2b95a76bfd60884f"
)
_GTH_MINUTE_GATE_SOURCES = frozenset(
    {"gth_level_manual_candidate", "gth_dip_reclaim_evidence"}
)
_GTH_MINUTE_GATE_SETUPS = frozenset({"FAILED_BREAK_RECLAIM", "TREND_PULLBACK"})
_GTH_MINUTE_MAX_OPPOSING_5M_ATR = 0.50
_GTH_MINUTE_MAX_DEBIT_FRACTION = 0.45
_GTH_MINUTE_MAX_RISK_USD = 1000.0

# Stable feature order shared by offline training and runtime inference.
FEATURE_NAMES: tuple[str, ...] = (
    "direction_sign",
    "is_vertical",
    "is_butterfly",
    "width_points",
    "max_loss_points",
    "max_gain_points",
    "debit_fraction_of_width",
    "quote_spread_fraction",
    "long_abs_delta",
    "short_abs_delta",
    "iv_skew",
    "breakeven_distance_atr",
    "target_distance_atr",
    "stop_distance_atr",
    "target_stop_ratio",
    "return_1m_atr_directional",
    "return_5m_atr_directional",
    "return_15m_atr_directional",
    "return_60m_atr_directional",
    "momentum_accel_1v5",
    "momentum_accel_5v15",
    "distance_to_vwap_atr_directional",
    "efficiency_ratio_30m",
    "vwap_crosses_30m",
    "vwap_slope_atr_directional",
    "breadth_directional",
    "direction_score_directional",
    "expected_move_atr",
    "atm_iv_0dte",
    "atm_iv_change_5m",
    "atm_iv_change_15m",
    "vix_return_15m_directional",
    "put_wall_distance_atr_directional",
    "call_wall_distance_atr_directional",
    "zero_gamma_distance_atr_directional",
    "flip_distance_atr_directional",
    "minutes_to_close_scaled",
    "session_rth",
    "session_gth",
    "path_trend",
    "path_transition",
    "path_balanced",
    "path_aligned",
    "event_pre",
    "event_post",
    "shock_active",
    "pin_stable",
    "pin_migrating",
    "setup_momentum",
    "setup_gth_scan",
    "setup_failed_break",
    "setup_pullback",
    "setup_event_settlement",
    "setup_stable_pin",
)

_DEFAULT_THRESHOLDS = {
    "min_expected_pnl_points": 0.25,
    "min_expected_pnl_lcb_points": 0.10,
    "min_p_profit": 0.58,
    "max_p_stop_first_5m": 0.30,
    "min_return_on_risk": 0.08,
}


@dataclass(frozen=True, slots=True)
class EdgeAuthorityResult:
    passed: list[dict[str, Any]]
    rejected: list[dict[str, Any]]


def apply_strategy_edge_authority(
    candidates: Sequence[Mapping[str, Any]],
    facts: Mapping[str, Any],
    regime: Mapping[str, Any],
    *,
    data_root: str | Path | None,
    now: datetime,
) -> EdgeAuthorityResult:
    """Gate candidates by promoted models or explicit unvalidated manual lanes.

    ``data_root is None`` is reserved for pure unit/replay fixtures that do not
    model deployment state. Production model-backed lanes fail closed when the
    artifact is absent, unpromoted, stale, malformed, or out of domain. The
    Pre-average, wall hazard, confirmed RTH levels, close convergence, the RTH
    iron condor, and the user-authorized v48 GTH minute gate are explicit
    manual-policy exceptions and are always labeled forward-unvalidated.
    """

    if not candidates:
        return EdgeAuthorityResult(passed=[], rejected=[])
    if data_root is None:
        return EdgeAuthorityResult(
            passed=[dict(candidate) for candidate in candidates],
            rejected=[],
        )

    scored: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    model_candidates: list[Mapping[str, Any]] = []
    for candidate in candidates:
        setup = candidate.get("setup_kind")
        if _is_gth_minute_gate_candidate(candidate):
            minute_reasons = _gth_minute_gate_reasons(candidate, facts)
            if minute_reasons:
                rejected.append(
                    _reject(
                        candidate,
                        *minute_reasons,
                        model_payload={
                            "status": "gth_minute_gate_rejected",
                            "policy_version": _GTH_MINUTE_GATE_POLICY,
                            "evidence_contract_hash": _GTH_MINUTE_GATE_CONTRACT_HASH,
                        },
                    )
                )
                continue
            authorized = {
                **dict(candidate),
                "authorization_policy": _GTH_MINUTE_GATE_POLICY,
                "evidence_contract_hash": _GTH_MINUTE_GATE_CONTRACT_HASH,
                "evidence_status": "forward_unvalidated_user_override",
            }
            scored.append(
                _attach_model_payload(
                    authorized,
                    {
                        "status": "explicit_policy_authority_unvalidated",
                        "policy_version": _GTH_MINUTE_GATE_POLICY,
                        "evidence_contract_hash": _GTH_MINUTE_GATE_CONTRACT_HASH,
                        "evidence_status": "forward_unvalidated_user_override",
                        "gate_kind": "gth_minute_confirmation",
                        "return_1m_points": _number(
                            _map(facts.get("path")).get("return_1m_points")
                        ),
                        "return_5m_points": _number(
                            _map(facts.get("path")).get("return_5m_points")
                        ),
                    },
                )
            )
            continue
        if setup not in {
            _PREAVERAGE_SETUP,
            _WALL_HAZARD_SETUP,
            _RTH_LEVEL_CONFIRMATION_SETUP,
            _CLOSE_CONVERGENCE_SETUP,
            _IRON_CONDOR_SETUP,
        }:
            model_candidates.append(candidate)
            continue
        authority_contracts = {
            _PREAVERAGE_SETUP: (
                "strategy_policy.bootstrap.v40",
                _PREAVERAGE_CONTRACT_HASH,
                "preaverage_policy_authority_invalid",
            ),
            _WALL_HAZARD_SETUP: (
                "strategy_policy.bootstrap.v56",
                _WALL_HAZARD_CONTRACT_HASH,
                "wall_hazard_policy_authority_invalid",
            ),
            _RTH_LEVEL_CONFIRMATION_SETUP: (
                "strategy_policy.bootstrap.v56",
                _RTH_LEVEL_CONFIRMATION_CONTRACT_HASH,
                "rth_level_confirmation_policy_authority_invalid",
            ),
            _CLOSE_CONVERGENCE_SETUP: (
                "strategy_policy.bootstrap.v56",
                _CLOSE_CONVERGENCE_CONTRACT_HASH,
                "close_convergence_policy_authority_invalid",
            ),
            _IRON_CONDOR_SETUP: (
                "strategy_policy.bootstrap.v56",
                _IRON_CONDOR_CONTRACT_HASH,
                "iron_condor_policy_authority_invalid",
            ),
        }
        expected_policy, expected_hash, failure = authority_contracts[str(setup)]
        if (
            candidate.get("authorization_policy") != expected_policy
            or candidate.get("evidence_contract_hash") != expected_hash
            or candidate.get("evidence_status") != "forward_unvalidated_user_override"
        ):
            rejected.append(_reject(candidate, failure))
            continue
        scored.append(
            _attach_model_payload(
                candidate,
                {
                    "status": "explicit_policy_authority_unvalidated",
                    "policy_version": expected_policy,
                    "evidence_contract_hash": expected_hash,
                    "evidence_status": "forward_unvalidated_user_override",
                    "hazard_probability": candidate.get("hazard_probability"),
                    "hazard_oos": dict(_map(candidate.get("hazard_oos"))),
                    "close_convergence": dict(_map(candidate.get("close_convergence"))),
                    "convergence_risk": dict(_map(candidate.get("convergence_risk"))),
                },
            )
        )

    artifact, artifact_reason = load_strategy_edge_artifact(data_root)
    if artifact is None:
        rejected.extend(
            _reject(
                candidate,
                artifact_reason or "strategy_edge_model_unavailable",
                model_payload={"status": "unavailable"},
            )
            for candidate in model_candidates
        )
        return EdgeAuthorityResult(passed=scored, rejected=rejected)

    for candidate in model_candidates:
        result, reasons = score_candidate_with_edge_model(
            candidate,
            facts,
            regime,
            artifact=artifact,
            now=now,
        )
        if reasons:
            rejected.append(_reject(result, *reasons))
        else:
            scored.append(result)

    scored.sort(key=_edge_sort_key, reverse=True)
    rejected.sort(key=_edge_sort_key, reverse=True)
    return EdgeAuthorityResult(passed=scored, rejected=rejected)


def _is_gth_minute_gate_candidate(candidate: Mapping[str, Any]) -> bool:
    return (
        str(candidate.get("source") or "") in _GTH_MINUTE_GATE_SOURCES
        and str(candidate.get("setup_kind") or "") in _GTH_MINUTE_GATE_SETUPS
        and str(candidate.get("strategy_type") or "").endswith("_DEBIT_VERTICAL")
    )


def _gth_minute_gate_reasons(
    candidate: Mapping[str, Any], facts: Mapping[str, Any]
) -> list[str]:
    """Authorize fresh confirmed GTH evidence with only live minute direction.

    The upstream selector owns source causality, expiry, exact BBO and payoff
    geometry. This gate deliberately does not inherit a long-horizon trend
    state or require a promoted historical model.
    """

    if str(_map(facts.get("session")).get("mode") or "").lower() != "gth":
        return ["gth_minute_gate_session_mismatch"]
    direction = str(candidate.get("direction") or "").upper()
    sign = 1.0 if direction == "UP" else -1.0 if direction == "DOWN" else 0.0
    if sign == 0.0:
        return ["gth_minute_gate_direction_unavailable"]
    path = _map(facts.get("path"))
    ret1 = _number(path.get("return_1m_points"))
    ret5 = _number(path.get("return_5m_points"))
    atr = _number(path.get("atr_5m"))
    if ret1 is None or ret5 is None or atr is None or atr <= 0.0:
        return ["gth_minute_path_unavailable"]
    reasons: list[str] = []
    if sign * ret1 <= 0.0:
        reasons.append("gth_1m_direction_not_confirmed")
    if sign * ret5 < -_GTH_MINUTE_MAX_OPPOSING_5M_ATR * atr:
        reasons.append("gth_5m_move_strongly_opposes_direction")
    economics = _map(candidate.get("economics"))
    debit_fraction = _number(economics.get("debit_fraction_of_width"))
    max_loss = _number(economics.get("max_loss_points"))
    if debit_fraction is None or debit_fraction > _GTH_MINUTE_MAX_DEBIT_FRACTION:
        reasons.append("gth_minute_debit_fraction_above_max")
    if max_loss is None or max_loss * 100.0 > _GTH_MINUTE_MAX_RISK_USD:
        reasons.append("gth_minute_defined_risk_above_max")
    return reasons


def load_strategy_edge_artifact(
    data_root: str | Path,
) -> tuple[Mapping[str, Any] | None, str | None]:
    path = Path(data_root).expanduser().joinpath(*ARTIFACT_RELATIVE_PATH)
    try:
        stat = path.stat()
    except OSError:
        return None, "strategy_edge_model_artifact_missing"
    artifact = _load_artifact_cached(str(path), stat.st_mtime_ns, stat.st_size)
    if artifact is None:
        return None, "strategy_edge_model_artifact_invalid"
    return artifact, None


@lru_cache(maxsize=16)
def _load_artifact_cached(
    path_text: str,
    mtime_ns: int,
    size: int,
) -> Mapping[str, Any] | None:
    del mtime_ns, size
    try:
        payload = json.loads(Path(path_text).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if payload.get("schema_version") != SCHEMA_VERSION:
        return None
    if payload.get("feature_version") != FEATURE_VERSION:
        return None
    if tuple(payload.get("feature_names") or ()) != FEATURE_NAMES:
        return None
    if not isinstance(payload.get("models"), dict):
        return None
    return payload


def score_candidate_with_edge_model(
    candidate: Mapping[str, Any],
    facts: Mapping[str, Any],
    regime: Mapping[str, Any],
    *,
    artifact: Mapping[str, Any],
    now: datetime,
) -> tuple[dict[str, Any], list[str]]:
    """Return an annotated candidate plus zero or more authority failures."""

    model_key = edge_model_key(candidate, facts)
    raw_model = _map(_map(artifact.get("models")).get(model_key))
    if not raw_model:
        return _attach_model_payload(
            candidate,
            {
                "status": "model_unavailable",
                "model_key": model_key,
                "artifact_version": artifact.get("artifact_version"),
            },
        ), ["strategy_edge_model_bucket_unavailable"]
    if raw_model.get("promoted") is not True:
        return _attach_model_payload(
            candidate,
            {
                "status": "not_promoted",
                "model_key": model_key,
                "artifact_version": artifact.get("artifact_version"),
                "promotion": dict(_map(raw_model.get("promotion"))),
            },
        ), ["strategy_edge_model_not_promoted"]

    generated_at = _time(artifact.get("generated_at"))
    valid_days = _number(artifact.get("valid_days"))
    observed_now = _utc(now)
    if generated_at is None:
        return _attach_model_payload(
            candidate,
            {"status": "artifact_timestamp_missing", "model_key": model_key},
        ), ["strategy_edge_model_artifact_invalid"]
    if valid_days is not None and valid_days > 0:
        age_days = (observed_now - generated_at).total_seconds() / 86_400.0
        if age_days > valid_days:
            return _attach_model_payload(
                candidate,
                {
                    "status": "artifact_stale",
                    "model_key": model_key,
                    "artifact_age_days": round(age_days, 4),
                    "valid_days": valid_days,
                },
            ), ["strategy_edge_model_artifact_stale"]

    features = candidate_edge_features(candidate, facts, regime, now=observed_now)
    try:
        standardized = _standardize(features, raw_model)
        expected = _linear(raw_model, "pnl", standardized)
        p_profit = _sigmoid(_linear(raw_model, "profit", standardized))
        p_stop = _sigmoid(_linear(raw_model, "stop_first_5m", standardized))
    except (KeyError, TypeError, ValueError, OverflowError):
        return _attach_model_payload(
            candidate,
            {"status": "model_invalid", "model_key": model_key},
        ), ["strategy_edge_model_artifact_invalid"]

    residual_q10 = _number(raw_model.get("residual_q10_points"))
    max_loss = _number(_map(candidate.get("economics")).get("max_loss_points"))
    if residual_q10 is None or max_loss is None or max_loss <= 0:
        return _attach_model_payload(
            candidate,
            {"status": "model_invalid", "model_key": model_key},
        ), ["strategy_edge_model_artifact_invalid"]

    lower_bound = expected + residual_q10
    return_on_risk = lower_bound / max_loss
    max_abs_z = max((abs(value) for value in standardized), default=0.0)
    domain_limit = _number(raw_model.get("max_abs_z")) or 4.0
    in_domain = max_abs_z <= domain_limit
    thresholds = {**_DEFAULT_THRESHOLDS, **dict(_map(raw_model.get("thresholds")))}
    payload = {
        "status": "scored",
        "model_key": model_key,
        "model_version": raw_model.get("model_version"),
        "artifact_version": artifact.get("artifact_version"),
        "feature_version": FEATURE_VERSION,
        "expected_pnl_points": round(expected, 6),
        "expected_pnl_lcb_points": round(lower_bound, 6),
        "p_profit": round(p_profit, 6),
        "p_stop_first_5m": round(p_stop, 6),
        "return_on_risk": round(return_on_risk, 6),
        "max_abs_z": round(max_abs_z, 6),
        "domain_limit": round(domain_limit, 6),
        "model_coverage": "in_domain" if in_domain else "out_of_domain",
        "thresholds": thresholds,
        "trained_through": raw_model.get("trained_through"),
        "holdout_metrics": dict(_map(raw_model.get("holdout_metrics"))),
    }
    result = _attach_model_payload(candidate, payload)

    failures: list[str] = []
    if not in_domain:
        failures.append("strategy_edge_model_out_of_domain")
    if expected < float(thresholds["min_expected_pnl_points"]):
        failures.append("strategy_edge_expected_pnl_below_threshold")
    if lower_bound < float(thresholds["min_expected_pnl_lcb_points"]):
        failures.append("strategy_edge_lower_bound_below_threshold")
    if p_profit < float(thresholds["min_p_profit"]):
        failures.append("strategy_edge_profit_probability_below_threshold")
    if p_stop > float(thresholds["max_p_stop_first_5m"]):
        failures.append("strategy_edge_early_stop_probability_above_threshold")
    if return_on_risk < float(thresholds["min_return_on_risk"]):
        failures.append("strategy_edge_return_on_risk_below_threshold")
    return result, failures


def candidate_edge_features(
    candidate: Mapping[str, Any],
    facts: Mapping[str, Any],
    regime: Mapping[str, Any],
    *,
    now: datetime,
) -> dict[str, float]:
    """Build the causal feature vector used by training and runtime."""

    del now
    strategy_type = str(candidate.get("strategy_type") or "").upper()
    setup = str(candidate.get("setup_kind") or "").upper()
    direction = str(candidate.get("direction") or "").upper()
    sign = 1.0 if direction == "UP" else -1.0 if direction == "DOWN" else 0.0
    economics = _map(candidate.get("economics"))
    quote = _map(candidate.get("quote"))
    path = _map(facts.get("path"))
    volatility = _map(facts.get("volatility"))
    structure = _map(facts.get("structure"))
    spot = _number(_map(facts.get("spot")).get("spx")) or 0.0
    atr = _number(path.get("atr_5m")) or 0.0
    scale = atr if atr > 1e-9 else 1.0
    width = _number(economics.get("width_points")) or _number(candidate.get("width")) or 0.0
    loss = _number(economics.get("max_loss_points")) or 0.0
    gain = _number(economics.get("max_gain_points")) or 0.0
    ask = _number(quote.get("ask")) or 0.0
    bid = _number(quote.get("bid")) or 0.0
    debit_fraction = _number(economics.get("debit_fraction_of_width")) or (
        loss / width if width > 0 else 0.0
    )
    long_leg, short_leg = _directional_legs(candidate)
    long_delta = abs(_number(long_leg.get("delta")) or 0.0)
    short_delta = abs(_number(short_leg.get("delta")) or 0.0)
    long_iv = _number(long_leg.get("implied_vol")) or 0.0
    short_iv = _number(short_leg.get("implied_vol")) or 0.0
    breakeven = _number(economics.get("breakeven_spx"))
    target = _number(candidate.get("target_spx"))
    invalidation = candidate.get("invalidation_spx")
    stop = _number(invalidation)
    target_distance = _directional_distance(spot, target, sign)
    stop_distance = -_directional_distance(spot, stop, sign)
    target_stop_ratio = (
        max(target_distance, 0.0) / max(stop_distance, 1e-9)
        if target is not None and stop is not None
        else 0.0
    )
    ret1 = _number(path.get("return_1m_points")) or 0.0
    ret5 = _number(path.get("return_5m_points")) or 0.0
    ret15 = _number(path.get("impulse_15m_points")) or 0.0
    ret60 = _number(path.get("return_60m_points")) or 0.0
    breadth = _number(path.get("breadth_above_vwap"))
    flip = _flip_mid(structure.get("flip_zone"))
    session_mode = str(_map(facts.get("session")).get("mode") or "").lower()
    path_state = str(regime.get("path_state") or "").upper()
    path_direction = str(regime.get("path_direction") or "").upper()
    event_state = str(regime.get("event_state") or "").upper()
    shock_state = str(_map(facts.get("shock")).get("state") or "NONE").upper()
    terminal_state = str(regime.get("terminal_state") or "").upper()

    values = {
        "direction_sign": sign,
        "is_vertical": float(strategy_type.endswith("_DEBIT_VERTICAL")),
        "is_butterfly": float(strategy_type.endswith("_BUTTERFLY")),
        "width_points": width,
        "max_loss_points": loss,
        "max_gain_points": gain,
        "debit_fraction_of_width": debit_fraction,
        "quote_spread_fraction": max(ask - bid, 0.0) / max(loss, 1e-9),
        "long_abs_delta": long_delta,
        "short_abs_delta": short_delta,
        "iv_skew": long_iv - short_iv,
        "breakeven_distance_atr": _directional_distance(spot, breakeven, sign) / scale,
        "target_distance_atr": target_distance / scale,
        "stop_distance_atr": stop_distance / scale,
        "target_stop_ratio": target_stop_ratio,
        "return_1m_atr_directional": sign * ret1 / scale,
        "return_5m_atr_directional": sign * ret5 / scale,
        "return_15m_atr_directional": sign * ret15 / scale,
        "return_60m_atr_directional": sign * ret60 / scale,
        "momentum_accel_1v5": sign * (ret1 - ret5 / 5.0) / scale,
        "momentum_accel_5v15": sign * (ret5 - ret15 / 3.0) / scale,
        "distance_to_vwap_atr_directional": sign
        * (_number(path.get("distance_to_vwap_points")) or 0.0)
        / scale,
        "efficiency_ratio_30m": _number(path.get("efficiency_ratio_30m")) or 0.0,
        "vwap_crosses_30m": _number(path.get("vwap_crosses_30m")) or 0.0,
        "vwap_slope_atr_directional": sign * (_number(path.get("vwap_slope")) or 0.0) / scale,
        "breadth_directional": _breadth_directional(breadth, sign),
        "direction_score_directional": sign * (_number(path.get("direction_score")) or 0.0),
        "expected_move_atr": (_number(volatility.get("expected_move_points")) or 0.0) / scale,
        "atm_iv_0dte": _number(volatility.get("atm_iv_0dte")) or 0.0,
        "atm_iv_change_5m": _number(volatility.get("atm_iv_change_5m")) or 0.0,
        "atm_iv_change_15m": _number(volatility.get("atm_iv_change_15m")) or 0.0,
        "vix_return_15m_directional": -sign
        * (_number(volatility.get("vix_return_15m_pct")) or 0.0),
        "put_wall_distance_atr_directional": _directional_distance(
            spot, _number(structure.get("put_wall")), sign
        )
        / scale,
        "call_wall_distance_atr_directional": _directional_distance(
            spot, _number(structure.get("call_wall")), sign
        )
        / scale,
        "zero_gamma_distance_atr_directional": _directional_distance(
            spot, _number(structure.get("zero_gamma")), sign
        )
        / scale,
        "flip_distance_atr_directional": _directional_distance(spot, flip, sign) / scale,
        "minutes_to_close_scaled": (_number(facts.get("minutes_to_close")) or 0.0) / 390.0,
        "session_rth": float(session_mode == "rth"),
        "session_gth": float(session_mode == "gth"),
        "path_trend": float(path_state == "TREND"),
        "path_transition": float(path_state == "TRANSITION"),
        "path_balanced": float(path_state == "BALANCED"),
        "path_aligned": float(direction in {"UP", "DOWN"} and path_direction == direction),
        "event_pre": float(event_state == "SCHEDULED_EVENT_RISK"),
        "event_post": float(event_state == "POST_EVENT_DISCOVERY"),
        "shock_active": float(shock_state in {"ACTIVE", "POST_SHOCK_DISCOVERY"}),
        "pin_stable": float(terminal_state == "PIN_STABLE"),
        "pin_migrating": float(terminal_state == "PIN_MIGRATING"),
        "setup_momentum": float(setup == "ES_VOLUME_MOMENTUM"),
        "setup_gth_scan": float(setup in {"GTH_WIDTH_SCAN", "GTH_DELTA_SCAN"}),
        "setup_failed_break": float(setup == "FAILED_BREAK_RECLAIM"),
        "setup_pullback": float(setup == "TREND_PULLBACK"),
        "setup_event_settlement": float(setup == "EVENT_SETTLEMENT_THRESHOLD"),
        "setup_stable_pin": float(setup == "STABLE_PIN"),
    }
    return {name: _finite(values.get(name, 0.0)) for name in FEATURE_NAMES}


def feature_vector(features: Mapping[str, float]) -> list[float]:
    return [_finite(features.get(name, 0.0)) for name in FEATURE_NAMES]


def edge_model_key(candidate: Mapping[str, Any], facts: Mapping[str, Any]) -> str:
    session = str(_map(facts.get("session")).get("mode") or "unknown").lower()
    strategy = str(candidate.get("strategy_type") or "").upper()
    family = (
        "vertical"
        if strategy.endswith("_DEBIT_VERTICAL")
        else ("butterfly" if strategy.endswith("_BUTTERFLY") else "other")
    )
    return f"{session}|{family}"


def _standardize(features: Mapping[str, float], model: Mapping[str, Any]) -> list[float]:
    values = feature_vector(features)
    mean = model.get("feature_mean")
    scale = model.get("feature_scale")
    if not isinstance(mean, list) or not isinstance(scale, list):
        raise ValueError("feature standardizer missing")
    if len(mean) != len(FEATURE_NAMES) or len(scale) != len(FEATURE_NAMES):
        raise ValueError("feature standardizer length mismatch")
    standardized = []
    for value, center, divisor in zip(values, mean, scale, strict=True):
        center_value = float(center)
        scale_value = float(divisor)
        if not math.isfinite(scale_value) or scale_value <= 0:
            raise ValueError("invalid feature scale")
        standardized.append((value - center_value) / scale_value)
    return standardized


def _linear(model: Mapping[str, Any], name: str, values: Sequence[float]) -> float:
    payload = _map(model.get(name))
    coefficients = payload.get("coef")
    intercept = _number(payload.get("intercept"))
    if not isinstance(coefficients, list) or len(coefficients) != len(values):
        raise ValueError("linear coefficient length mismatch")
    if intercept is None:
        raise ValueError("linear intercept missing")
    result = intercept + sum(
        float(weight) * value for weight, value in zip(coefficients, values, strict=True)
    )
    if not math.isfinite(result):
        raise ValueError("linear result is non-finite")
    return result


def _sigmoid(value: float) -> float:
    if value >= 0:
        factor = math.exp(-min(value, 700.0))
        return 1.0 / (1.0 + factor)
    factor = math.exp(max(value, -700.0))
    return factor / (1.0 + factor)


def _edge_sort_key(candidate: Mapping[str, Any]) -> tuple[float, float, float]:
    model = _map(_map(candidate.get("edge")).get("strategy_edge"))
    loss = _number(_map(candidate.get("economics")).get("max_loss_points")) or 0.0
    lower = _number(model.get("expected_pnl_lcb_points"))
    expected = _number(model.get("expected_pnl_points"))
    stop = _number(model.get("p_stop_first_5m"))
    return (
        (lower / loss) if lower is not None and loss > 0 else -999.0,
        expected if expected is not None else -999.0,
        -(stop if stop is not None else 1.0),
    )


def _attach_model_payload(
    candidate: Mapping[str, Any], payload: Mapping[str, Any]
) -> dict[str, Any]:
    edge = dict(_map(candidate.get("edge")))
    edge["strategy_edge"] = dict(payload)
    if payload.get("status") == "scored":
        edge["edge_status"] = "promoted_model_pass"
    elif payload.get("status") == "explicit_policy_authority_unvalidated":
        edge["edge_status"] = "explicit_manual_policy_unvalidated"
    return {**dict(candidate), "edge": edge}


def _reject(
    candidate: Mapping[str, Any],
    *reasons: str,
    model_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    row = (
        _attach_model_payload(candidate, model_payload)
        if model_payload is not None
        else dict(candidate)
    )
    failed = list(row.get("failed_gates") or ())
    existing = [str(value) for value in row.get("rejection_reasons") or ()]
    for reason in reasons:
        failed.append(
            {
                "gate": reason,
                "actual": None,
                "threshold": "promoted_positive_edge",
            }
        )
        existing.append(reason)
    edge = dict(_map(row.get("edge")))
    edge["edge_status"] = "model_rejected"
    return {
        **row,
        "edge": edge,
        "failed_gates": failed,
        "rejection_reasons": list(dict.fromkeys(existing)),
    }


def _directional_legs(
    candidate: Mapping[str, Any],
) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    long_leg = _map(candidate.get("long"))
    short_leg = _map(candidate.get("short"))
    if long_leg or short_leg:
        return long_leg, short_leg
    raw = candidate.get("legs")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return {}, {}
    legs = [_map(item) for item in raw if _map(item)]
    if not legs:
        return {}, {}
    return legs[0], legs[1] if len(legs) > 1 else {}


def _directional_distance(spot: float, level: float | None, sign: float) -> float:
    if level is None or sign == 0:
        return 0.0
    return sign * (level - spot)


def _breadth_directional(value: float | None, sign: float) -> float:
    if value is None or sign == 0:
        return 0.0
    return sign * (value - 0.5)


def _flip_mid(value: object) -> float | None:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        levels = [_number(item) for item in value[:2]]
        if len(levels) == 2 and None not in levels:
            return (float(levels[0]) + float(levels[1])) / 2.0
    mapped = _map(value)
    low, high = _number(mapped.get("low")), _number(mapped.get("high"))
    return (low + high) / 2.0 if low is not None and high is not None else None


def _map(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    if not isinstance(value, int | float) or isinstance(value, bool):
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def _finite(value: object) -> float:
    number = _number(value)
    return number if number is not None else 0.0


def _time(value: object) -> datetime | None:
    if not isinstance(value, str | datetime):
        return None
    try:
        parsed = (
            value
            if isinstance(value, datetime)
            else datetime.fromisoformat(value.replace("Z", "+00:00"))
        )
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("edge scoring time must be timezone-aware")
    return value.astimezone(timezone.utc)
