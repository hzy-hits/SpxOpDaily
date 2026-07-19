"""Signal definitions and JSONL feature-store loaders for the 0DTE level backtest.

Normalizes the control/proxy signal sets (confirmed level transitions,
pricing-outcome prefills, GTH dip reclaims) plus persisted production
``trade_ready`` decisions into point-in-time coordinates. See
``odte_level_backtest`` for the simulation and aggregation pipeline.
"""

from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Iterator, Mapping, NamedTuple, Sequence
from zoneinfo import ZoneInfo

from spx_spark.strategy_contract import parse_aware_time, strategy_contract_issues

logger = logging.getLogger(__name__)
SET_CONFIRMED = "confirmed"
SET_PREFILL = "prefill"
SET_GTH_DIP = "gth_dip"
SET_TRADE_READY = "trade_ready"
SET_ORDER = (SET_CONFIRMED, SET_PREFILL, SET_GTH_DIP, SET_TRADE_READY)

VARIANT_NAKED = "naked"
VARIANT_SPREAD5 = "spread5"
VARIANT_SPREAD10 = "spread10"
VARIANT_SPREAD_WALL = "spread_wall"
VARIANTS = (VARIANT_NAKED, VARIANT_SPREAD5, VARIANT_SPREAD10, VARIANT_SPREAD_WALL)
SPREAD_VARIANTS = (VARIANT_SPREAD5, VARIANT_SPREAD10, VARIANT_SPREAD_WALL)
SPREAD_WIDTHS = {VARIANT_SPREAD5: 5.0, VARIANT_SPREAD10: 10.0}

DEFAULT_BASIS_POINTS = 45.0
FOLLOW_THROUGH_DELAY = timedelta(seconds=15)
MAX_ENTRY_QUOTE_AGE = timedelta(seconds=30)
MAX_ENTRY_LEG_SKEW = timedelta(seconds=5)
MAX_MARK_QUOTE_AGE = timedelta(seconds=30)
MAX_MARK_LEG_SKEW = timedelta(seconds=5)
MAX_UNDERLIER_QUOTE_AGE = timedelta(seconds=30)
TIME_STOP_DELAY = timedelta(minutes=15)
MAX_HOLD = timedelta(minutes=35)
PROFIT_TARGET_MULTIPLE = 1.30
INVALIDATION_BUFFER_POINTS = 3.0
FT_GATE_SECONDS = 15
FT_GATE_POINTS = 2.0
FT_GATE_EM_FRACTION = 0.05
DELTA_MIN = 0.35
DELTA_MAX = 0.70
DELTA_TARGET = 0.50
POINTS_PER_CONTRACT = 100.0
PROVIDERS = ("schwab", "ibkr")
WALL_KEYS = ("put_wall", "flip_low", "flip_high", "call_wall")

TRAILING_ACTIVATION_FRACTION = 0.15  # arm trailing stop at +15% unrealized (mid)
TRAILING_GIVEBACK_FRACTION = 1.0 / 3.0  # exit after giving back 1/3 of peak gain
WIDE_INVALIDATION_EM_FRACTION = 0.15
GTH_TIME_STOP = timedelta(minutes=360)
GTH_MAX_HOLD = timedelta(minutes=370)

# production gth_dip._spread_structure defaults (config/runtime.yaml gth_spread_*)
SPREAD_WALL_MIN_WIDTH = 15.0
SPREAD_WALL_MAX_WIDTH = 75.0
SPREAD_WALL_DEFAULT_WIDTH = 50.0
SPREAD_WALL_EM_FRACTION = 0.5
NEW_YORK = ZoneInfo("America/New_York")
EXIT_CLOCK_ET_HHMM = (9, 45)
EXPIRY_CLOSE_ET_HHMM = (16, 0)
SATURATION_FRACTION = 0.85  # sat85: take profit at 85% of spread width
TRAIL33_ARM_FRACTION = 0.50  # trail33: arm at 50% of spread width


@dataclass(frozen=True)
class Profile:
    """Exit-rule configuration; baseline reproduces the production rules."""

    name: str
    # None => fixed INVALIDATION_BUFFER_POINTS; else max(buffer, frac * expected_move)
    invalidation_em_fraction: float | None = None
    # "fixed" (1.3x) | "trailing" | "sat85" | "trail33" | "clock" (no profit rule)
    profit_target_mode: str = "fixed"
    gth_time_stop: timedelta | None = None  # overrides TIME_STOP_DELAY for GTH signals
    gth_max_hold: timedelta | None = None  # overrides MAX_HOLD for GTH signals
    gth_clock_exit: bool = False  # GTH stop anchored to expiry-date 09:45 ET
    gth_only: bool = False  # evaluate GTH signals only under this profile
    spread_only: bool = False  # skip the naked variant under this profile
    set_names: tuple[str, ...] | None = None  # None => every set


PROFILE_BASELINE = "baseline"
PROFILE_WIDE_INVALIDATION = "wide_invalidation"
PROFILE_TRAILING_TP = "trailing_tp"
PROFILE_GTH_360 = "gth_360"
PROFILE_SAT85 = "sat85"
PROFILE_TRAIL33 = "trail33"
PROFILE_CLOCK = "clock"
_GTH_EVAL_SETS = (SET_CONFIRMED, SET_GTH_DIP)
PROFILES = (
    Profile(name=PROFILE_BASELINE),
    Profile(name=PROFILE_WIDE_INVALIDATION, invalidation_em_fraction=WIDE_INVALIDATION_EM_FRACTION),
    Profile(name=PROFILE_TRAILING_TP, profit_target_mode="trailing"),
    Profile(name=PROFILE_GTH_360, gth_time_stop=GTH_TIME_STOP, gth_max_hold=GTH_MAX_HOLD),
    Profile(
        name=PROFILE_SAT85,
        profit_target_mode="sat85",
        gth_clock_exit=True,
        gth_only=True,
        spread_only=True,
        set_names=_GTH_EVAL_SETS,
    ),
    Profile(
        name=PROFILE_TRAIL33,
        profit_target_mode="trail33",
        gth_clock_exit=True,
        gth_only=True,
        spread_only=True,
        set_names=_GTH_EVAL_SETS,
    ),
    Profile(
        name=PROFILE_CLOCK,
        profit_target_mode="clock",
        gth_clock_exit=True,
        gth_only=True,
        spread_only=True,
        set_names=_GTH_EVAL_SETS,
    ),
)


class OptionTick(NamedTuple):
    at: datetime
    bid: float | None
    ask: float | None
    mid: float | None


class UnderlierTick(NamedTuple):
    at: datetime
    price: float


@dataclass(frozen=True)
class Signal:
    """One alert event normalized into SPX-equivalent coordinates."""

    set_name: str
    key: str
    at: datetime
    direction: str  # "up" | "down"
    level: float  # invalidation anchor (SPX-equivalent; raw ES for gth_dip)
    strike: float | None  # None until delta-selected (S3)
    expiry: date | None
    entry_at: datetime
    level_kind: str | None = None
    thesis: str | None = None
    walls: tuple[float, ...] = ()
    wall_map: dict = field(default_factory=dict)  # kind -> SPX-coord wall (S1 only)
    expected_move_points: float | None = None
    entry_px: float | None = None  # optional externally recorded ask; None => quote lookup
    entry_limit: float | None = None  # persisted production limit; fill only at ask <= limit
    entry_expires_at: datetime | None = None  # exclusive production entry-window end
    entry_provider: str | None = None  # persisted provider; never re-selected with hindsight
    decision_spot: float | None = None  # persisted point-in-time spot at evaluation
    target_level: float | None = None  # exact persisted production target
    recorded_time_stop_at: datetime | None = None
    basis_points: float = 0.0  # subtracted from future:ES to get SPX-equivalent
    underlier_instrument: str = "index:SPX"
    invalidation_level: float | None = None  # defaults to level
    invalidation_buffer: float = INVALIDATION_BUFFER_POINTS
    target_mode: str = "wall"  # "wall" (S1) | "formula" (S2/S3)
    trend_regime: str | None = None
    session_bucket: str | None = None
    first_touch_at: datetime | None = None
    contract_id: str | None = None
    horizons: dict = field(default_factory=dict)
    recorded_short_strike: float | None = None  # S3 production spread short leg
    recorded_spread_width: float | None = None  # S3 production spread width


@dataclass(frozen=True)
class Trade:
    set_name: str
    profile: str
    key: str
    at: str
    play: str | None
    direction: str
    level: float
    level_kind: str | None
    contract_id: str | None
    short_contract_id: str | None
    variant: str
    entry_time: str
    entry_px: float
    exit_time: str
    exit_px: float
    exit_reason: str
    pnl_points: float
    pnl_usd: float
    mfe_points: float | None
    mae_points: float | None
    underlier_source: str
    trend_regime: str | None
    session_bucket: str | None
    ft_pass_15s2p: bool | None
    entry_price_source: str
    h60_ret: float | None
    h300_ret: float | None
    h900_ret: float | None


@dataclass(frozen=True)
class Skip:
    set_name: str
    profile: str
    key: str
    variant: str
    reason: str


def _parse_ts(value: object) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _float(value: object) -> float | None:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _iter_jsonl(path: Path) -> Iterator[dict]:
    """Yield parsed JSON objects, skipping blank or malformed lines defensively."""
    try:
        handle = path.open("r", encoding="utf-8")
    except OSError:
        logger.warning("jsonl unreadable: %s", path)
        return
    with handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("skipping malformed line in %s", path)
                continue
            if isinstance(record, dict):
                yield record


def round_strike(level: float) -> float:
    """Round to the nearest 5-point SPXW strike, ties away from zero."""
    return math.copysign(math.floor(abs(level) / 5.0 + 0.5) * 5.0, level)


def right_for(direction: str) -> str:
    return "C" if direction == "up" else "P"


def spread_strikes(direction: str, strike: float, width: float) -> tuple[float, float]:
    """Return (long, short) strikes for a debit vertical spread."""
    return (strike, strike + width) if direction == "up" else (strike, strike - width)


def wall_spread_structure(
    *,
    direction: str,
    long_strike: float,
    wall_map: dict,
    expected_move_points: float | None,
    min_width: float = SPREAD_WALL_MIN_WIDTH,
    max_width: float = SPREAD_WALL_MAX_WIDTH,
    default_width: float = SPREAD_WALL_DEFAULT_WIDTH,
) -> tuple[float, float, str]:
    """Production gth_dip._spread_structure rule, mirrored for down signals.

    Up (calls): nearest 5-rounded wall above the long strike among
    (flip_high, call_wall), width clamped to [min, max]; falls back to
    round5(0.5*EM) clamped, then the default width. Down (puts) mirrors with
    (flip_low, put_wall) below. Returns (short_strike, width, anchor).
    """
    dir_sign = 1 if direction == "up" else -1
    kinds = ("flip_high", "call_wall") if dir_sign == 1 else ("flip_low", "put_wall")
    walls = sorted(
        dir_sign * round_strike(float(wall_map[kind]))
        for kind in kinds
        if isinstance(wall_map.get(kind), (int, float))
    )
    for oriented in walls:  # ascending in the profitable direction
        strike = oriented * dir_sign
        width = (strike - long_strike) * dir_sign
        if width < min_width:
            continue
        short = strike if width <= max_width else long_strike + dir_sign * max_width
        return short, abs(short - long_strike), "structure_wall"
    if expected_move_points is not None and expected_move_points > 0:
        width = round_strike(SPREAD_WALL_EM_FRACTION * expected_move_points)
        width = min(max(width, min_width), max_width)
        return long_strike + dir_sign * width, width, "expected_move"
    return long_strike + dir_sign * default_width, default_width, "default"


def next_exit_clock(
    at: datetime,
    expiry: date | None = None,
    hhmm: tuple[int, int] = EXIT_CLOCK_ET_HHMM,
) -> datetime:
    """The expiry session's 09:45 America/New_York exit clock in UTC.

    The historical name is retained for API compatibility. The result never
    rolls to the following day when ``at`` is already past the clock.
    """
    aware = at if at.tzinfo else at.replace(tzinfo=timezone.utc)
    session_date = expiry or aware.astimezone(NEW_YORK).date()
    local = datetime.combine(
        session_date,
        time(hour=hhmm[0], minute=hhmm[1]),
        tzinfo=NEW_YORK,
    )
    return local.astimezone(timezone.utc)


def expiry_close_at(expiry: date) -> datetime:
    """Return the SPX expiry session's 16:00 America/New_York close in UTC."""
    local = datetime.combine(
        expiry,
        time(hour=EXPIRY_CLOSE_ET_HHMM[0], minute=EXPIRY_CLOSE_ET_HHMM[1]),
        tzinfo=NEW_YORK,
    )
    return local.astimezone(timezone.utc)


def contract_id_for(expiry: date, strike: float, right: str) -> str:
    return f"option:SPX:SPXW:{expiry:%Y%m%d}:{strike:g}:{right}"


def nearest_wall(level: float, walls: Sequence[float], dir_sign: int) -> float | None:
    """Nearest wall strictly on the profitable side of the trigger level."""
    candidates = [wall for wall in walls if (wall - level) * dir_sign > 0]
    if not candidates:
        return None
    return min(candidates, key=lambda wall: abs(wall - level))


def formula_target(level: float, dir_sign: int, expected_move_points: float | None) -> float | None:
    if expected_move_points is None:
        return None
    return level + dir_sign * max(5.0, expected_move_points * 0.15)


def hour_bucket(at: datetime) -> str:
    """RTH buckets in America/New_York local time; everything else is GTH."""
    aware = at if at.tzinfo else at.replace(tzinfo=timezone.utc)
    local = aware.astimezone(NEW_YORK)
    minutes = local.hour * 60 + local.minute
    if 9 * 60 + 30 <= minutes < 10 * 60 + 30:
        return "rth_open"
    if 10 * 60 + 30 <= minutes < 15 * 60:
        return "rth_midday"
    if 15 * 60 <= minutes < 16 * 60:
        return "rth_close"
    return "gth"


# ---------------------------------------------------------------------------
# Signal loaders
# ---------------------------------------------------------------------------


def _confirmed_signal(record: dict, session_date: date) -> Signal | None:
    at = _parse_ts(record.get("at"))
    direction = record.get("direction")
    level_raw = _float(record.get("level"))
    if at is None or direction not in ("up", "down") or level_raw is None:
        return None
    kind = record.get("trigger_coordinate_kind")
    raw_basis = _float(record.get("trigger_basis_points"))
    if kind == "official_spx":
        basis = 0.0
        underlier_instrument = "index:SPX"
    elif kind == "chain_implied_spx":
        if record.get("trigger_instrument_id") not in {None, "synthetic:SPXW_PARITY"}:
            return None
        basis = 0.0
        underlier_instrument = "synthetic:SPXW_PARITY"
    elif kind == "es_equivalent":
        if record.get("trigger_instrument_id") not in {None, "future:ES"}:
            return None
        basis = raw_basis if raw_basis is not None else DEFAULT_BASIS_POINTS
        underlier_instrument = "future:ES"
    else:
        return None
    # es_equivalent records carry level/levels in raw ES coordinates; spx_level
    # is the SPX-coordinate equivalent. Normalize everything to SPX coordinates.
    spx_level = _float(record.get("spx_level"))
    if spx_level is None:
        spx_level = level_raw - basis if kind == "es_equivalent" else level_raw
    levels = record.get("levels") or {}
    walls: list[float] = []
    wall_map: dict[str, float] = {}
    for wall_key in WALL_KEYS:
        wall = _float(levels.get(wall_key))
        if wall is not None:
            adjusted = wall - basis if kind == "es_equivalent" else wall
            walls.append(adjusted)
            wall_map[wall_key] = adjusted
    strike = round_strike(spx_level)
    right = right_for(direction)
    return Signal(
        set_name=SET_CONFIRMED,
        key=str(record.get("event_id")),
        at=at,
        direction=direction,
        level=spx_level,
        strike=strike,
        expiry=session_date,
        entry_at=at + FOLLOW_THROUGH_DELAY,
        level_kind=record.get("level_kind"),
        thesis=record.get("thesis"),
        walls=tuple(walls),
        wall_map=wall_map,
        basis_points=basis,
        underlier_instrument=underlier_instrument,
        invalidation_level=spx_level,
        target_mode="wall",
        contract_id=contract_id_for(session_date, strike, right),
    )


def load_confirmed_signals(features_root: Path) -> list[Signal]:
    """S1: first transition into the confirmed phase, deduped by event_id."""
    signals: list[Signal] = []
    seen: set[str] = set()
    for path in sorted(features_root.glob("level_decision_audit/date=*/transitions.jsonl")):
        try:
            session_date = date.fromisoformat(path.parent.name.removeprefix("date="))
        except ValueError:
            continue
        for record in _iter_jsonl(path):
            if record.get("current_phase") != "confirmed":
                continue
            if record.get("previous_phase") == "confirmed":
                continue
            event_id = record.get("event_id")
            if not event_id or event_id in seen:
                continue
            signal = _confirmed_signal(record, session_date)
            if signal is None:
                continue
            seen.add(event_id)
            signals.append(signal)
    return signals


def _prefill_signal(record: dict) -> Signal | None:
    if record.get("touched") is not True:
        return None
    contract_id = record.get("contract_id")
    parts = str(contract_id or "").split(":")
    if len(parts) != 6:
        return None
    try:
        expiry = date.fromisoformat(f"{parts[3][:4]}-{parts[3][4:6]}-{parts[3][6:]}")
        strike = float(parts[4])
    except ValueError:
        return None
    right = parts[5]
    if right not in ("C", "P"):
        return None
    direction = "up" if right == "C" else "down"
    first_touch_at = _parse_ts(record.get("first_touch_at"))
    if first_touch_at is None:
        return None
    kind = record.get("trigger_coordinate_kind")
    if kind == "es_equivalent":
        # ES-equivalent outcomes persist ``trigger_target`` in raw ES
        # coordinates. Comparing ES-45 with ``spx_level`` silently introduces
        # a variable basis error, so keep both the trigger and the path in ES.
        level = _float(record.get("trigger_target"))
        underlier_instrument = "future:ES"
    elif kind == "chain_implied_spx":
        # Chain parity is SPX-scaled but is not the official index. Preserve its
        # native synthetic path and fail closed when that path is absent.
        if record.get("trigger_instrument_id") != "synthetic:SPXW_PARITY":
            return None
        level = _float(record.get("trigger_target")) or _float(record.get("spx_level"))
        underlier_instrument = "synthetic:SPXW_PARITY"
    elif kind == "official_spx":
        level = _float(record.get("spx_level")) or _float(record.get("trigger_target"))
        underlier_instrument = "index:SPX"
    else:
        return None
    if level is None:
        return None
    return Signal(
        set_name=SET_PREFILL,
        key=str(record.get("key") or contract_id),
        at=first_touch_at,
        direction=direction,
        level=level,
        strike=strike,
        expiry=expiry,
        # The production gate can only be known after the full hold interval.
        # The simulator re-reads the ask at/after this timestamp; historical
        # prefill prices are deliberately excluded from executable PnL.
        entry_at=first_touch_at + FOLLOW_THROUGH_DELAY,
        level_kind=record.get("level_kind"),
        thesis=record.get("play"),
        expected_move_points=_float(record.get("expected_move_points")),
        entry_px=None,
        basis_points=0.0,
        underlier_instrument=underlier_instrument,
        invalidation_level=level,
        target_mode="formula",
        trend_regime=record.get("trend_regime"),
        session_bucket=record.get("session_bucket"),
        first_touch_at=first_touch_at,
        contract_id=str(contract_id),
        horizons=record.get("horizons") or {},
    )


def load_prefill_signals(features_root: Path) -> list[Signal]:
    """S2: deduped, follow-through-only observational proxy records.

    Recorded ``prefill_ask`` values precede the production follow-through gate
    and are never treated as fills. Entry is repriced from the quote lake after
    the hold interval in :func:`evaluate_signal`. Repeated snapshots of one
    production semantic key are collapsed to its earliest valid first touch;
    this proxy is never counted as the production strategy cohort.
    """
    by_semantic_key: dict[str, Signal] = {}
    for path in sorted(features_root.glob("pricing_outcomes/date=*/outcomes.jsonl")):
        for record in _iter_jsonl(path):
            signal = _prefill_signal(record)
            semantic_key = str(record.get("key") or "")
            if signal is None or not semantic_key:
                continue
            previous = by_semantic_key.get(semantic_key)
            if previous is None or signal.at < previous.at:
                by_semantic_key[semantic_key] = signal
    return sorted(by_semantic_key.values(), key=lambda signal: (signal.at, signal.key))


def _trade_ready_signal(record: dict) -> Signal | None:
    """Normalize one complete persisted production entry decision.

    Every execution-relevant field comes from the decision itself. Malformed
    rows fail closed instead of rebuilding a contract, target, or entry window
    from later market data.
    """
    if record.get("status") != SET_TRADE_READY:
        return None
    intent_id = str(record.get("intent_id") or "")
    evaluated_at = _parse_ts(record.get("evaluated_at"))
    expires_at = _parse_ts(record.get("expires_at"))
    direction = str(record.get("direction") or "")
    contract_id = str(record.get("contract_id") or "")
    parts = contract_id.split(":")
    if (
        not intent_id
        or evaluated_at is None
        or expires_at is None
        or expires_at <= evaluated_at
        or direction not in {"up", "down"}
        or len(parts) != 6
        or parts[:3] != ["option", "SPX", "SPXW"]
    ):
        return None
    try:
        expiry = date.fromisoformat(f"{parts[3][:4]}-{parts[3][4:6]}-{parts[3][6:]}")
        strike = float(parts[4])
    except ValueError:
        return None
    right = parts[5]
    if right != right_for(direction) or strike <= 0:
        return None
    provider = str(record.get("provider") or "").lower()
    entry_limit = _float(record.get("entry_limit"))
    trigger = _float(record.get("trigger_level"))
    invalidation = _float(record.get("invalidation_spx"))
    target = _float(record.get("target_spx"))
    decision_spot = _float(record.get("spx_spot"))
    if (
        provider not in PROVIDERS
        or entry_limit is None
        or entry_limit <= 0
        or trigger is None
        or invalidation is None
        or target is None
        or decision_spot is None
    ):
        return None
    dir_sign = 1 if direction == "up" else -1
    if dir_sign * (trigger - invalidation) <= 0 or dir_sign * (target - trigger) <= 0:
        return None
    recorded_time_stop_at = _parse_ts(record.get("time_stop_at"))
    if recorded_time_stop_at is not None and recorded_time_stop_at <= evaluated_at:
        return None
    return Signal(
        set_name=SET_TRADE_READY,
        key=intent_id,
        at=evaluated_at,
        direction=direction,
        level=trigger,
        strike=strike,
        expiry=expiry,
        entry_at=evaluated_at,
        level_kind="production_trade_ready",
        thesis=str(record.get("play") or record.get("thesis") or "") or None,
        entry_limit=entry_limit,
        entry_expires_at=expires_at,
        entry_provider=provider,
        decision_spot=decision_spot,
        target_level=target,
        recorded_time_stop_at=recorded_time_stop_at,
        invalidation_level=invalidation,
        invalidation_buffer=0.0,
        target_mode="recorded",
        session_bucket=hour_bucket(evaluated_at),
        contract_id=contract_id,
        # Outcome horizons, when present on other feature sets, are deliberately
        # excluded from production entry decisions to prevent label leakage.
        horizons={},
    )


def load_trade_ready_signals(features_root: Path) -> list[Signal]:
    """Load unique terminal ``trade_ready`` decisions, deduped by intent id."""
    signals: list[Signal] = []
    seen: set[str] = set()
    for path in sorted(features_root.glob("trade_intents/date=*/events.jsonl")):
        for record in _iter_jsonl(path):
            if record.get("status") != SET_TRADE_READY:
                continue
            intent_id = str(record.get("intent_id") or "")
            if not intent_id or intent_id in seen:
                continue
            # Mark the first terminal decision as seen even when malformed; a
            # later duplicate must not repair it with information unavailable
            # at the original decision time.
            seen.add(intent_id)
            signal = _trade_ready_signal(record)
            if signal is not None:
                signals.append(signal)
    return sorted(signals, key=lambda signal: (signal.at, signal.key))


def trade_intent_coverage(
    features_root: Path,
    *,
    cutoff_at: datetime,
    last_complete_date: date,
) -> dict[str, object]:
    """Count persisted intent telemetry without coercing ``observing``.

    Counts are raw evaluation-record coverage, not a gate pass rate: repeated
    blocked evaluations remain repeated records and ``observing`` is kept as a
    separate non-decision state.
    """
    statuses = ("observing", "blocked", SET_TRADE_READY)
    by_status = {status: 0 for status in statuses}
    event_ids = {status: set() for status in statuses}
    intent_ids: set[str] = set()
    invalid_timestamp_records = 0
    other_status_records = 0
    for path in sorted(features_root.glob("trade_intents/date=*/events.jsonl")):
        for record in _iter_jsonl(path):
            evaluated_at = _parse_ts(record.get("evaluated_at"))
            if evaluated_at is None:
                invalid_timestamp_records += 1
                continue
            if evaluated_at >= cutoff_at or evaluated_at.date() > last_complete_date:
                continue
            status = str(record.get("status") or "")
            if status not in by_status:
                other_status_records += 1
                continue
            by_status[status] += 1
            event_id = str(record.get("event_id") or "")
            if event_id:
                event_ids[status].add(event_id)
            intent_id = str(record.get("intent_id") or "")
            if status == SET_TRADE_READY and intent_id:
                intent_ids.add(intent_id)
    return {
        "evaluation_records": sum(by_status.values()) + other_status_records,
        "records_by_status": by_status,
        "distinct_event_ids_by_status": {
            status: len(event_ids[status]) for status in statuses
        },
        "distinct_trade_ready_intent_ids": len(intent_ids),
        "invalid_timestamp_records": invalid_timestamp_records,
        "other_status_records": other_status_records,
        "observing_semantics": (
            "non-decision telemetry; excluded from pass/block rates and from trade PnL"
        ),
    }


def load_gth_dip_signals(features_root: Path) -> list[Signal]:
    """S3: GTH dip-reclaim confirmations.

    New production records persist the exact debit spread shown to the operator.
    Those strikes are copied verbatim. Legacy records without ``spread`` remain
    usable for the explicitly hypothetical naked/fixed-width variants, but the
    production ``spread_wall`` variant fails closed instead of rebuilding legs
    from a later delta snapshot.
    """
    signals: list[Signal] = []
    for path in sorted(features_root.glob("gth_dip_reclaim/date=*/events.jsonl")):
        for record in _iter_jsonl(path):
            confirmed_at = _parse_ts(record.get("confirmed_at"))
            trough = _float(record.get("trough"))
            es = _float(record.get("es"))
            if confirmed_at is None or trough is None or es is None:
                continue
            session = record.get("session_date")
            try:
                expiry = date.fromisoformat(str(session)) if session else confirmed_at.date()
            except ValueError:
                expiry = confirmed_at.date()
            direction = record.get("direction") or "up"
            spread = record.get("spread")
            long_strike: float | None = None
            short_strike: float | None = None
            spread_width: float | None = None
            spread_right = right_for(direction)
            confirmed_contract = bool(
                record.get("block_reasons") == []
                and not strategy_contract_issues(
                    record,
                    require_valid_until=True,
                    require_actionable_coordinate=True,
                )
                and parse_aware_time(record.get("valid_until")) is not None
                and parse_aware_time(record.get("valid_until")) > confirmed_at
                and isinstance(record.get("coordinate"), Mapping)
                and record["coordinate"].get("kind") == "raw_es"
                and record["coordinate"].get("instrument_id") == "future:ES"
            )
            if isinstance(spread, Mapping) and confirmed_contract:
                candidate_long = _float(spread.get("long_strike"))
                candidate_short = _float(spread.get("short_strike"))
                candidate_width = _float(spread.get("width_points"))
                candidate_right = str(spread.get("right") or spread_right).upper()
                dir_sign = 1 if direction == "up" else -1
                calculated_width = (
                    dir_sign * (candidate_short - candidate_long)
                    if candidate_long is not None and candidate_short is not None
                    else None
                )
                if (
                    candidate_long is not None
                    and candidate_short is not None
                    and calculated_width is not None
                    and calculated_width > 0
                    and candidate_right == spread_right
                    and (
                        candidate_width is None
                        or math.isclose(candidate_width, calculated_width, abs_tol=1e-6)
                    )
                ):
                    long_strike = candidate_long
                    short_strike = candidate_short
                    spread_width = calculated_width
                else:
                    logger.warning(
                        "invalid recorded production spread for GTH event %s",
                        record.get("event_id"),
                    )
            elif isinstance(spread, Mapping):
                logger.warning(
                    "legacy/noncompliant GTH spread excluded from production variant for %s",
                    record.get("event_id"),
                )
            signals.append(
                Signal(
                    set_name=SET_GTH_DIP,
                    key=str(record.get("event_id")),
                    at=confirmed_at,
                    direction=direction,
                    level=es,
                    strike=long_strike,
                    expiry=expiry,
                    entry_at=confirmed_at,
                    level_kind="gth_dip_reclaim",
                    thesis=record.get("kind"),
                    expected_move_points=_float(record.get("expected_move_points")),
                    basis_points=0.0,  # raw ES: invalidation compares against the ES trough
                    underlier_instrument="future:ES",
                    invalidation_level=trough,
                    invalidation_buffer=0.0,
                    target_mode="formula",
                    session_bucket="gth",
                    contract_id=(
                        contract_id_for(expiry, long_strike, spread_right)
                        if long_strike is not None
                        else None
                    ),
                    recorded_short_strike=short_strike,
                    recorded_spread_width=spread_width,
                )
            )
    return signals
