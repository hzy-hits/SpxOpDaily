"""Causal ICT liquidity events and their non-authoritative candidate filter."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR


ICT_FILTER_FIELDS = ("ict_liquidity", "ict_alignment", "ict_decision_modifier")
_LEVEL_PRIORITY = {"ONH": 0, "ONL": 0, "OR15H": 1, "OR15L": 1}


@dataclass(frozen=True, slots=True)
class IctLiquidityPolicy:
    minimum_penetration_points: float = 0.50
    minimum_penetration_atr: float = 0.10
    maximum_extension_atr: float = 1.00
    reclaim_bars: int = 3
    mss_lookback_bars: int = 5
    mss_deadline_bars: int = 5
    mss_buffer_points: float = 0.25
    displacement_atr: float = 0.80
    latest_signal_minutes_to_close: float = 60.0
    conflict_modifier_cap: float = 0.05
    opportunity_ttl_seconds: float = 300.0


DEFAULT_ICT_LIQUIDITY_POLICY = IctLiquidityPolicy()


def spx_bars(value: object, basis: float | None) -> list[dict[str, Any]]:
    if not isinstance(value, list) or basis is None:
        return []
    rows = []
    for raw in value:
        bar = _map(raw)
        converted = {
            key: _spx_level(bar.get(key), basis)
            for key in ("open", "high", "low", "close")
        }
        if None not in converted.values():
            rows.append({"bar_start": bar.get("bar_start"), **converted})
    return sorted(rows, key=lambda row: str(row.get("bar_start") or ""))


def build_ict_liquidity_fact(
    raw_bars: object,
    *,
    session_ranges: Mapping[str, Any],
    opening_high: float | None,
    opening_low: float | None,
    basis: float | None,
    session_date: str,
    decision_at: datetime,
    policy: IctLiquidityPolicy = DEFAULT_ICT_LIQUIDITY_POLICY,
) -> dict[str, Any]:
    base = {
        "status": "unavailable",
        "stage": None,
        "direction": None,
        "level_names": [],
        "action_authority": "filter_only",
        "automatic_ordering": False,
        "reason": "ict_minute_path_or_levels_unavailable",
    }
    try:
        trading_date = date.fromisoformat(session_date)
    except (TypeError, ValueError):
        return base
    bars = _spx_recent_minute_bars(raw_bars, basis=basis, decision_at=decision_at)
    session = DEFAULT_MARKET_CALENDAR.session(trading_date)
    if session is None or len(bars) < 10:
        return base
    overnight = _map(session_ranges.get("overnight"))
    levels = (
        ("ONH", _spx_level(overnight.get("high"), basis), "high", session.open_at),
        ("ONL", _spx_level(overnight.get("low"), basis), "low", session.open_at),
        ("OR15H", opening_high, "high", session.open_at + timedelta(minutes=15)),
        ("OR15L", opening_low, "low", session.open_at + timedelta(minutes=15)),
    )
    latest_signal_at = session.close_at - timedelta(
        minutes=policy.latest_signal_minutes_to_close
    )
    events = [
        event
        for name, level, side, available_at in levels
        if level is not None
        and (
            event := _level_event(
                bars,
                level_name=name,
                level=float(level),
                side=side,
                available_at=available_at,
                latest_signal_at=latest_signal_at,
                policy=policy,
            )
        )
    ]
    if not events:
        return {**base, "status": "none", "reason": "no_causal_liquidity_sweep"}
    selected = max(
        events,
        key=lambda event: (
            event["signal_at"],
            -_LEVEL_PRIORITY[event["level_name"]],
        ),
    )
    cohort = [
        event
        for event in events
        if event["direction"] == selected["direction"]
        and abs((event["signal_at"] - selected["signal_at"]).total_seconds()) <= 300.0
    ]
    level_names = sorted(
        {event["level_name"] for event in cohort},
        key=lambda name: (_LEVEL_PRIORITY[name], name),
    )
    signal_at = selected["signal_at"]
    valid_until = signal_at + timedelta(seconds=policy.opportunity_ttl_seconds)
    return {
        "status": "active" if signal_at <= decision_at < valid_until else "expired",
        "stage": selected["stage"],
        "direction": selected["direction"],
        "level_names": level_names,
        "level": selected["level"],
        "sweep_extreme": selected["sweep_extreme"],
        "sweep_at": selected["sweep_at"].isoformat(),
        "mss_at": selected["mss_at"].isoformat() if selected["mss_at"] else None,
        "signal_at": signal_at.isoformat(),
        "valid_until": valid_until.isoformat(),
        "atr_at_sweep": round(selected["atr_at_sweep"], 4),
        "action_authority": "filter_only",
        "automatic_ordering": False,
        "reason": "causal_sweep_reclaim_mss_displacement_filter",
    }


def attach_ict_liquidity_filters(
    setups: Sequence[Mapping[str, Any]],
    *,
    ict_liquidity: Mapping[str, Any],
    policy: IctLiquidityPolicy = DEFAULT_ICT_LIQUIDITY_POLICY,
) -> list[dict[str, Any]]:
    return [
        _attach_filter(setup, ict_liquidity=ict_liquidity, policy=policy)
        for setup in setups
    ]


def ict_filter_payload(source: Mapping[str, Any]) -> dict[str, Any]:
    return {key: source.get(key) for key in ICT_FILTER_FIELDS}


def apply_ict_liquidity_modifier(
    candidate: Mapping[str, Any],
    facts: Mapping[str, Any],
    *,
    policy: IctLiquidityPolicy = DEFAULT_ICT_LIQUIDITY_POLICY,
) -> dict[str, Any]:
    context = _map(candidate.get("ict_liquidity"))
    if (
        str(_map(facts.get("session")).get("mode") or "").lower() != "rth"
        or not str(candidate.get("strategy_type") or "").endswith("_DEBIT_VERTICAL")
        or context.get("action_authority") != "filter_only"
    ):
        return dict(candidate)
    base = float(candidate.get("selection_score") or 0.0)
    requested = _number(candidate.get("ict_decision_modifier")) or 0.0
    modifier = min(max(requested, -policy.conflict_modifier_cap), 0.0)
    return {
        **dict(candidate),
        "selection_score_pre_ict": round(base, 4),
        "ict_decision_modifier": round(modifier, 4),
        "selection_score": round(base + modifier, 4),
    }


def _spx_recent_minute_bars(
    value: object,
    *,
    basis: float | None,
    decision_at: datetime,
) -> list[dict[str, Any]]:
    if not isinstance(value, list) or basis is None:
        return []
    rows = []
    for raw in value:
        bar = _map(raw)
        available_at = _time(bar.get("available_at"))
        converted = {
            key: _spx_level(bar.get(key), basis)
            for key in ("open", "high", "low", "close")
        }
        if available_at is None or available_at > decision_at or None in converted.values():
            continue
        rows.append(
            {"bar_start": bar.get("bar_start"), "available_at": available_at, **converted}
        )
    return sorted(rows, key=lambda row: row["available_at"])


def _level_event(
    bars: list[dict[str, Any]],
    *,
    level_name: str,
    level: float,
    side: str,
    available_at: datetime,
    latest_signal_at: datetime,
    policy: IctLiquidityPolicy,
) -> dict[str, Any] | None:
    penetration_index: int | None = None
    events: list[dict[str, Any]] = []
    for index, bar in enumerate(bars):
        bar_available = bar["available_at"]
        if bar_available <= available_at:
            continue
        if bar_available > latest_signal_at:
            break
        atr = _atr_before(bars, index)
        if atr is None or atr <= 0.0:
            continue
        if penetration_index is not None and index - penetration_index > policy.reclaim_bars:
            penetration_index = None
        if penetration_index is None:
            extension = (
                float(bar["high"]) - level if side == "high" else level - float(bar["low"])
            )
            minimum = max(policy.minimum_penetration_points, policy.minimum_penetration_atr * atr)
            if minimum <= extension <= policy.maximum_extension_atr * atr:
                penetration_index = index
        if penetration_index is None:
            continue
        reclaimed = float(bar["close"]) < level if side == "high" else float(bar["close"]) > level
        if not reclaimed:
            continue
        direction = "DOWN" if side == "high" else "UP"
        relevant = bars[penetration_index : index + 1]
        extreme = (
            max(float(row["high"]) for row in relevant)
            if direction == "DOWN"
            else min(float(row["low"]) for row in relevant)
        )
        event = {
            "level_name": level_name,
            "level": level,
            "direction": direction,
            "penetration_index": penetration_index,
            "sweep_index": index,
            "sweep_extreme": extreme,
            "sweep_at": bar_available,
            "atr_at_sweep": atr,
        }
        events.append(_confirmations(event, bars, policy=policy))
        penetration_index = None
    return max(events, key=lambda event: event["signal_at"]) if events else None


def _confirmations(
    event: dict[str, Any],
    bars: list[dict[str, Any]],
    *,
    policy: IctLiquidityPolicy,
) -> dict[str, Any]:
    penetration_index = int(event["penetration_index"])
    sweep_index = int(event["sweep_index"])
    start = max(0, penetration_index - policy.mss_lookback_bars)
    reference_rows = bars[start:penetration_index]
    if len(reference_rows) < 3:
        return {**event, "stage": "SWEEP_RECLAIMED", "mss_at": None, "signal_at": event["sweep_at"]}
    direction = str(event["direction"])
    reference = (
        max(float(bar["high"]) for bar in reference_rows)
        if direction == "UP"
        else min(float(bar["low"]) for bar in reference_rows)
    )
    mss_index = None
    deadline = min(len(bars), sweep_index + policy.mss_deadline_bars + 1)
    for index in range(sweep_index, deadline):
        crossed = (
            float(bars[index]["close"]) >= reference + policy.mss_buffer_points
            if direction == "UP"
            else float(bars[index]["close"]) <= reference - policy.mss_buffer_points
        )
        if crossed:
            mss_index = index
            break
    if mss_index is None:
        return {**event, "stage": "SWEEP_RECLAIMED", "mss_at": None, "signal_at": event["sweep_at"]}
    displacement = False
    sign = 1.0 if direction == "UP" else -1.0
    for index in range(sweep_index, mss_index + 1):
        atr = _atr_before(bars, index)
        body = sign * (float(bars[index]["close"]) - float(bars[index]["open"]))
        if atr is not None and body >= policy.displacement_atr * atr:
            displacement = True
            break
    return {
        **event,
        "stage": "MSS_DISPLACEMENT_CONFIRMED" if displacement else "MSS_CONFIRMED",
        "mss_at": bars[mss_index]["available_at"],
        "signal_at": bars[mss_index]["available_at"],
    }


def _atr_before(
    bars: list[dict[str, Any]], index: int, *, lookback: int = 14
) -> float | None:
    if index < 3:
        return None
    true_ranges = []
    for current in range(max(0, index - lookback), index):
        bar = bars[current]
        previous_close = float(bars[current - 1]["close"]) if current > 0 else float(bar["open"])
        true_ranges.append(
            max(
                float(bar["high"]) - float(bar["low"]),
                abs(float(bar["high"]) - previous_close),
                abs(float(bar["low"]) - previous_close),
            )
        )
    return sum(true_ranges) / len(true_ranges) if len(true_ranges) >= min(10, lookback) else None


def _attach_filter(
    setup: Mapping[str, Any],
    *,
    ict_liquidity: Mapping[str, Any],
    policy: IctLiquidityPolicy,
) -> dict[str, Any]:
    row = dict(setup)
    if ict_liquidity.get("status") != "active":
        return row
    setup_direction = str(row.get("direction") or "").upper()
    ict_direction = str(ict_liquidity.get("direction") or "").upper()
    if setup_direction not in {"UP", "DOWN"} or ict_direction not in {"UP", "DOWN"}:
        return row
    confirmed = ict_liquidity.get("stage") == "MSS_DISPLACEMENT_CONFIRMED"
    alignment = (
        "CONFIRMS"
        if confirmed and setup_direction == ict_direction
        else "CONFLICTS"
        if confirmed
        else "OBSERVE_ONLY"
    )
    return {
        **row,
        "ict_liquidity": dict(ict_liquidity),
        "ict_alignment": alignment,
        "ict_decision_modifier": -policy.conflict_modifier_cap if alignment == "CONFLICTS" else 0.0,
    }


def _spx_level(value: object, basis: float | None) -> float | None:
    number = _number(value)
    return number - basis if number is not None and basis is not None else None


def _map(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    return None


def _time(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
