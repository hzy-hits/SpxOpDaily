"""Deterministic, symmetric 0DTE call-path confirmation.

The module is deliberately advisory.  It freezes option structure before a
price transition, confirms the transition with fresh SPX/ES source pairs, and
persists a short-lived conditional call bias.  It never places an order and it
never interprets a Gamma sign as market direction.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Mapping, Protocol

from spx_spark.config import env_float, env_int
from spx_spark.marketdata import MarketDataQuality, OptionRight, Provider, as_utc
from spx_spark.options_map import OptionsMap, is_spxw_option
from spx_spark.settings import settings_value
from spx_spark.storage import LatestState


FLIP_RECLAIM_CALL_KIND = "flip_reclaim_call"
CALL_WALL_BREAKOUT_CALL_KIND = "call_wall_breakout_call"
STRATEGY_KINDS = frozenset({FLIP_RECLAIM_CALL_KIND, CALL_WALL_BREAKOUT_CALL_KIND})
SIGNED_GEX_SIGN_METHOD = "call_positive_put_negative_oi_proxy_not_dealer_position"
SIGNED_GEX_INTRADAY_SIGN_METHOD = (
    "call_positive_put_negative_oi_plus_volume_proxy_not_dealer_position"
)
STATE_SCHEMA_VERSION = 1


def signed_gex_sign_method(weighting: str | None) -> str:
    return (
        SIGNED_GEX_INTRADAY_SIGN_METHOD if weighting == "oi_plus_volume" else SIGNED_GEX_SIGN_METHOD
    )


class PriceSampleLike(Protocol):
    at: datetime
    spx: float
    es: float
    spx_source_at: datetime | None
    es_source_at: datetime | None


@dataclass(frozen=True)
class IntradayStrategySettings:
    level_buffer_points: float = 3.0
    confirm_samples: int = 2
    confirm_window_seconds: int = 60
    source_event_ttl_seconds: int = 300
    bias_ttl_seconds: int = 300
    cooldown_seconds: int = 900
    structure_grace_seconds: int = 30
    retry_seconds: int = 30
    level_drift_tolerance_points: float = 5.0
    es_confirm_ratio: float = 0.50
    es_hold_tolerance_bps: float = 1.0
    # Flip-reclaim reuses the shock machine's reclaim-hold thresholds (one override point).
    reclaim_hold_fraction: float = 0.55
    es_reclaim_hold_fraction: float = 0.35

    @classmethod
    def from_env(cls) -> "IntradayStrategySettings":
        return cls(
            level_buffer_points=env_float(
                "ALERT_INTRADAY_CALL_LEVEL_BUFFER_POINTS",
                float(settings_value("intraday_strategy.level_buffer_points")),
            ),
            confirm_samples=env_int(
                "ALERT_INTRADAY_CALL_CONFIRM_SAMPLES",
                int(settings_value("intraday_strategy.confirm_samples")),
            ),
            confirm_window_seconds=env_int(
                "ALERT_INTRADAY_CALL_CONFIRM_WINDOW_SECONDS",
                int(settings_value("intraday_strategy.confirm_window_seconds")),
            ),
            source_event_ttl_seconds=env_int(
                "ALERT_INTRADAY_CALL_SOURCE_TTL_SECONDS",
                int(settings_value("intraday_strategy.source_event_ttl_seconds")),
            ),
            bias_ttl_seconds=env_int(
                "ALERT_INTRADAY_CALL_BIAS_TTL_SECONDS",
                int(settings_value("intraday_strategy.bias_ttl_seconds")),
            ),
            cooldown_seconds=env_int(
                "ALERT_INTRADAY_CALL_COOLDOWN_SECONDS",
                int(settings_value("intraday_strategy.cooldown_seconds")),
            ),
            structure_grace_seconds=env_int(
                "ALERT_INTRADAY_CALL_STRUCTURE_GRACE_SECONDS",
                int(settings_value("intraday_strategy.structure_grace_seconds")),
            ),
            retry_seconds=env_int(
                "ALERT_INTRADAY_CALL_RETRY_SECONDS",
                int(settings_value("intraday_strategy.retry_seconds")),
            ),
            level_drift_tolerance_points=env_float(
                "ALERT_INTRADAY_CALL_LEVEL_DRIFT_TOLERANCE_POINTS",
                float(settings_value("intraday_strategy.level_drift_tolerance_points")),
            ),
            es_confirm_ratio=env_float(
                "ALERT_INTRADAY_CALL_ES_CONFIRM_RATIO",
                float(settings_value("intraday_strategy.es_confirm_ratio")),
            ),
            es_hold_tolerance_bps=env_float(
                "ALERT_INTRADAY_CALL_ES_HOLD_TOLERANCE_BPS",
                float(settings_value("intraday_strategy.es_hold_tolerance_bps")),
            ),
            reclaim_hold_fraction=env_float(
                "ALERT_INTRADAY_RECLAIM_HOLD_FRACTION",
                float(settings_value("intraday_shock.reclaim_hold_fraction")),
            ),
            es_reclaim_hold_fraction=env_float(
                "ALERT_INTRADAY_RECLAIM_ES_HOLD_FRACTION",
                float(settings_value("intraday_shock.es_reclaim_hold_fraction")),
            ),
        )


@dataclass(frozen=True)
class IntradayStructure:
    valid: bool
    reason: str | None
    expiry: str | None
    flip_low: float | None
    flip_high: float | None
    zero_gamma: float | None
    call_wall: float | None
    put_wall: float | None
    gamma_state: str
    net_gex: float | None
    abs_gex: float | None
    net_gamma_ratio: float | None
    gex_quality: str | None
    gex_weighting: str | None
    wall_method: str | None
    observed_at: datetime
    flip_source_fresh: bool = True
    call_wall_source_fresh: bool = True
    flip_source_max_age_seconds: float | None = None
    call_wall_source_max_age_seconds: float | None = None

    def to_state_dict(self, *, stable_count: int) -> dict[str, object]:
        payload = asdict(self)
        payload["observed_at"] = as_utc(self.observed_at).isoformat()
        payload["stable_count"] = stable_count
        payload["signed_gex_sign_method"] = signed_gex_sign_method(self.gex_weighting)
        payload["dealer_position_sign"] = "unknown"
        return payload


@dataclass(frozen=True)
class IntradayPathSignal:
    kind: str
    event_id: str
    source_event_id: str | None
    level: float
    invalidation_level: float
    confirmed_at: datetime
    expires_at: datetime
    gamma_state: str
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class IntradayPathDecision:
    status: str
    play: str | None
    event_id: str | None
    source_event_id: str | None
    level: float | None
    invalidation_level: float | None
    confirmed_at: str | None
    expires_at: str | None
    gamma_state: str
    signed_gex_proxy_ratio: float | None
    signed_gex_sign_method: str
    dealer_position_sign: str
    reasons: tuple[str, ...]
    blocks: tuple[str, ...]

    @property
    def conditional_call_bias(self) -> bool:
        return self.status == "confirmed" and bool(
            self.play in STRATEGY_KINDS
            or (self.play or "").startswith("level_decision:")
        )

    @property
    def flip_reclaim_call(self) -> bool:
        return self.status == "confirmed" and self.play == FLIP_RECLAIM_CALL_KIND

    @property
    def call_wall_breakout_call(self) -> bool:
        return self.status == "confirmed" and self.play == CALL_WALL_BREAKOUT_CALL_KIND

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["reasons"] = list(self.reasons)
        payload["blocks"] = list(self.blocks)
        payload["conditional_call_bias"] = self.conditional_call_bias
        return payload


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return as_utc(parsed)


def _finite(value: object) -> float | None:
    if not isinstance(value, int | float):
        return None
    number = float(value)
    if number != number or number in {float("inf"), float("-inf")}:
        return None
    return number


def _source_at(sample: PriceSampleLike, field: str) -> datetime:
    value = getattr(sample, field)
    return as_utc(value or sample.at)


def _level_same(left: object, right: float | None, tolerance: float) -> bool:
    old = _finite(left)
    if old is None or right is None:
        return old is None and right is None
    return abs(old - right) <= tolerance


def structure_from_options_map(
    options_map: OptionsMap,
    *,
    session_date: str,
    observed_at: datetime,
    state: LatestState | None = None,
    source_max_age_seconds: float = 120.0,
) -> IntradayStructure:
    """Extract strict same-day OI structure and label the signed proxy honestly."""

    exact_expiry = session_date.replace("-", "")
    front = next((row for row in options_map.expiries if row.expiry == exact_expiry), None)
    source = options_map.underlier.source
    warning_text = " ".join(options_map.warnings).lower()
    reason: str | None = None
    if front is None:
        reason = "exact_same_day_expiry_unavailable"
    elif source != "index:SPX":
        reason = "official_spx_underlier_unavailable"
    elif "underlier_mismatch" in warning_text:
        reason = "underlier_mismatch"
    elif front.gex_quality != "open_interest_gex":
        reason = "open_interest_gex_unavailable"

    flip_low: float | None = None
    flip_high: float | None = None
    if front is not None and front.gamma_flip_zone is not None:
        flip_low, flip_high = sorted(map(float, front.gamma_flip_zone))

    structure_observed_at = getattr(options_map, "as_of", None)
    if not isinstance(structure_observed_at, datetime):
        structure_observed_at = observed_at
    eligible: list[tuple[float, OptionRight, float]] = []
    if state is not None and front is not None:
        for quote in state.best_quotes:
            strike = _finite(quote.instrument.strike)
            gamma = _finite(quote.greeks.gamma) if quote.greeks is not None else None
            age_ms = quote.quote_age_ms(structure_observed_at)
            if (
                (quote.instrument.expiry or "") != exact_expiry
                or not is_spxw_option(quote)
                or strike is None
                or quote.instrument.right is None
                or quote.provider != Provider.IBKR
                or quote.market_data_type != 1
                or quote.quality not in {MarketDataQuality.LIVE, MarketDataQuality.STALE}
                or _finite(quote.open_interest) is None
                or float(quote.open_interest or 0.0) <= 0
                or gamma is None
                or gamma <= 0
                or age_ms is None
                or age_ms < 0
                or age_ms > source_max_age_seconds * 1000.0
            ):
                continue
            eligible.append((strike, quote.instrument.right, age_ms / 1000.0))

    flip_ages: list[float] = []
    if flip_low is not None and flip_high is not None:
        by_strike: dict[float, dict[OptionRight, float]] = {}
        for strike, right, age in eligible:
            by_strike.setdefault(strike, {})[right] = age
        paired = [
            (strike, max(sides[OptionRight.CALL], sides[OptionRight.PUT]))
            for strike, sides in by_strike.items()
            if OptionRight.CALL in sides and OptionRight.PUT in sides
        ]
        used_strikes: set[float] = set()
        for level in dict.fromkeys((flip_low, flip_high)):
            nearby = [
                row for row in paired if row[0] not in used_strikes and abs(row[0] - level) <= 10.0
            ]
            if nearby:
                selected = min(nearby, key=lambda row: (abs(row[0] - level), row[1]))
                used_strikes.add(selected[0])
                flip_ages.append(selected[1])
    flip_level_count = (
        len(set((flip_low, flip_high))) if flip_low is not None and flip_high is not None else 0
    )
    flip_source_fresh = flip_level_count > 0 and len(flip_ages) == flip_level_count

    call_wall_ages = (
        [
            age
            for strike, right, age in eligible
            if front is not None
            and front.call_wall is not None
            and right == OptionRight.CALL
            and abs(strike - front.call_wall) <= 0.1
        ]
        if front is not None
        else []
    )
    call_wall_source_fresh = bool(call_wall_ages)
    if state is None:
        # Pure unit callers may construct IntradayStructure directly. Extracting
        # a live strategy structure without source quotes must fail closed.
        flip_source_fresh = False
        call_wall_source_fresh = False
    if reason is None and not flip_source_fresh and not call_wall_source_fresh:
        reason = "key_structure_quotes_stale_or_unavailable"
    return IntradayStructure(
        valid=reason is None,
        reason=reason,
        expiry=front.expiry if front is not None else None,
        flip_low=flip_low,
        flip_high=flip_high,
        zero_gamma=front.zero_gamma if front is not None else None,
        call_wall=(
            front.call_wall if front is not None and front.wall_method == "oi_gex" else None
        ),
        put_wall=front.put_wall if front is not None else None,
        gamma_state=front.gamma_state if front is not None else "unknown",
        net_gex=front.net_gex if front is not None else None,
        abs_gex=front.abs_gex if front is not None else None,
        net_gamma_ratio=front.net_gamma_ratio if front is not None else None,
        gex_quality=front.gex_quality if front is not None else None,
        gex_weighting=front.gex_weighting if front is not None else None,
        wall_method=front.wall_method if front is not None else None,
        observed_at=structure_observed_at,
        flip_source_fresh=flip_source_fresh,
        call_wall_source_fresh=call_wall_source_fresh,
        flip_source_max_age_seconds=max(flip_ages) if flip_ages else None,
        call_wall_source_max_age_seconds=max(call_wall_ages) if call_wall_ages else None,
    )


def unavailable_structure(*, observed_at: datetime, reason: str) -> IntradayStructure:
    return IntradayStructure(
        valid=False,
        reason=reason,
        expiry=None,
        flip_low=None,
        flip_high=None,
        zero_gamma=None,
        call_wall=None,
        put_wall=None,
        gamma_state="unknown",
        net_gex=None,
        abs_gex=None,
        net_gamma_ratio=None,
        gex_quality=None,
        gex_weighting=None,
        wall_method=None,
        observed_at=observed_at,
    )


def _empty_call_state() -> dict[str, object]:
    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "structure_snapshot": None,
        "flip_watch": None,
        "wall_watch": None,
        "active_bias": None,
        "last_signal": None,
        "last_spx_source_at": None,
        "last_es_source_at": None,
        "last_down_event_id": None,
        "structure_invalid_since": None,
        "updated_at": None,
    }


def _safe_call_state(monitor_state: Mapping[str, object]) -> dict[str, object]:
    raw = monitor_state.get("call_strategy")
    if not isinstance(raw, dict) or raw.get("schema_version") != STATE_SCHEMA_VERSION:
        return _empty_call_state()
    return dict(raw)


def _recent_down_event(
    monitor_state: Mapping[str, object], now: datetime, ttl_seconds: int
) -> dict[str, object] | None:
    rows: list[tuple[datetime, dict[str, object]]] = []
    for key in ("active_event", "last_event"):
        raw = monitor_state.get(key)
        if not isinstance(raw, dict) or raw.get("direction") != "down" or not raw.get("event_id"):
            continue
        at = (
            _parse_datetime(raw.get("reclaim_confirmed_at"))
            or _parse_datetime(raw.get("extreme_at"))
            or _parse_datetime(raw.get("anchor_at"))
        )
        if at is None or not (-5 <= (now - at).total_seconds() <= ttl_seconds):
            continue
        rows.append((at, dict(raw)))
    return max(rows, key=lambda item: item[0])[1] if rows else None


def _update_structure_snapshot(
    call_state: dict[str, object],
    *,
    structure: IntradayStructure,
    sample: PriceSampleLike,
    settings: IntradayStrategySettings,
) -> dict[str, object] | None:
    if not structure.valid:
        return None
    current_raw = call_state.get("structure_snapshot")
    current = dict(current_raw) if isinstance(current_raw, dict) else None
    if current is None:
        return structure.to_state_dict(stable_count=1)

    frozen_wall = _finite(current.get("call_wall"))
    crossed_frozen_wall = (
        frozen_wall is not None and sample.spx > frozen_wall + settings.level_buffer_points
    )
    same = (
        current.get("expiry") == structure.expiry
        and _level_same(
            current.get("flip_low"), structure.flip_low, settings.level_drift_tolerance_points
        )
        and _level_same(
            current.get("flip_high"), structure.flip_high, settings.level_drift_tolerance_points
        )
        and _level_same(
            current.get("call_wall"), structure.call_wall, settings.level_drift_tolerance_points
        )
    )
    if crossed_frozen_wall:
        # The live wall ladder is side constrained and normally jumps to the
        # next strike after a real breakout.  Keep the pre-break wall frozen.
        current["observed_at"] = as_utc(structure.observed_at).isoformat()
        current["gamma_state"] = structure.gamma_state
        current["net_gex"] = structure.net_gex
        current["abs_gex"] = structure.abs_gex
        current["net_gamma_ratio"] = structure.net_gamma_ratio
        return current
    if same:
        return structure.to_state_dict(stable_count=int(current.get("stable_count") or 0) + 1)
    return structure.to_state_dict(stable_count=1)


def _new_bias(
    *,
    play: str,
    source_event_id: str | None,
    level: float,
    invalidation_level: float,
    sample: PriceSampleLike,
    gamma_state: str,
    gex_weighting: str | None,
    expiry: str,
    reasons: tuple[str, ...],
    ttl_seconds: int,
) -> dict[str, object]:
    minute = as_utc(sample.at).strftime("%Y%m%dT%H%M%S")
    event_id = f"spx_call:{play}:{level:.2f}:{minute}"
    return {
        "status": "confirmed",
        "play": play,
        "event_id": event_id,
        "source_event_id": source_event_id,
        "expiry": expiry,
        "level": level,
        "invalidation_level": invalidation_level,
        "confirmed_at": as_utc(sample.at).isoformat(),
        "expires_at": (as_utc(sample.at) + timedelta(seconds=ttl_seconds)).isoformat(),
        "gamma_state": gamma_state,
        "reasons": list(reasons),
        "signed_gex_sign_method": signed_gex_sign_method(gex_weighting),
        "dealer_position_sign": "unknown",
    }


def _bias_is_active(raw: object, *, sample: PriceSampleLike) -> bool:
    if not isinstance(raw, dict) or raw.get("status") != "confirmed":
        return False
    expires_at = _parse_datetime(raw.get("expires_at"))
    invalidation = _finite(raw.get("invalidation_level"))
    if expires_at is None or as_utc(sample.at) > expires_at or invalidation is None:
        return False
    return sample.spx >= invalidation


def _decision(
    call_state: Mapping[str, object],
    *,
    structure: IntradayStructure,
    blocks: tuple[str, ...] = (),
) -> IntradayPathDecision:
    raw = call_state.get("active_bias")
    bias = raw if isinstance(raw, dict) else None
    if bias is not None and bias.get("status") == "confirmed":
        return IntradayPathDecision(
            status="confirmed",
            play=str(bias.get("play") or "") or None,
            event_id=str(bias.get("event_id") or "") or None,
            source_event_id=str(bias.get("source_event_id") or "") or None,
            level=_finite(bias.get("level")),
            invalidation_level=_finite(bias.get("invalidation_level")),
            confirmed_at=str(bias.get("confirmed_at") or "") or None,
            expires_at=str(bias.get("expires_at") or "") or None,
            gamma_state=str(bias.get("gamma_state") or structure.gamma_state),
            signed_gex_proxy_ratio=structure.net_gamma_ratio,
            signed_gex_sign_method=str(
                bias.get("signed_gex_sign_method")
                or signed_gex_sign_method(structure.gex_weighting)
            ),
            dealer_position_sign="unknown",
            reasons=tuple(str(row) for row in bias.get("reasons", ()) if row),
            blocks=blocks,
        )
    watches = [call_state.get("flip_watch"), call_state.get("wall_watch")]
    watching = any(isinstance(row, dict) and row.get("status") == "watch" for row in watches)
    return IntradayPathDecision(
        status="watch" if watching else "neutral",
        play=None,
        event_id=None,
        source_event_id=None,
        level=None,
        invalidation_level=None,
        confirmed_at=None,
        expires_at=None,
        gamma_state=structure.gamma_state,
        signed_gex_proxy_ratio=structure.net_gamma_ratio,
        signed_gex_sign_method=signed_gex_sign_method(structure.gex_weighting),
        dealer_position_sign="unknown",
        reasons=(),
        blocks=blocks,
    )


def _signal_from_bias(bias: Mapping[str, object]) -> IntradayPathSignal | None:
    confirmed_at = _parse_datetime(bias.get("confirmed_at"))
    expires_at = _parse_datetime(bias.get("expires_at"))
    level = _finite(bias.get("level"))
    invalidation = _finite(bias.get("invalidation_level"))
    kind = str(bias.get("play") or "")
    event_id = str(bias.get("event_id") or "")
    if (
        kind not in STRATEGY_KINDS
        or not event_id
        or confirmed_at is None
        or expires_at is None
        or level is None
        or invalidation is None
    ):
        return None
    return IntradayPathSignal(
        kind=kind,
        event_id=event_id,
        source_event_id=str(bias.get("source_event_id") or "") or None,
        level=level,
        invalidation_level=invalidation,
        confirmed_at=confirmed_at,
        expires_at=expires_at,
        gamma_state=str(bias.get("gamma_state") or "unknown"),
        reasons=tuple(str(row) for row in bias.get("reasons", ()) if row),
    )


def advance_intraday_strategy(
    monitor_state: Mapping[str, object],
    sample: PriceSampleLike,
    structure: IntradayStructure,
    settings: IntradayStrategySettings,
) -> tuple[dict[str, object], IntradayPathDecision, tuple[IntradayPathSignal, ...]]:
    """Advance frozen-level watches using one already validated live SPX/ES pair."""

    state = dict(monitor_state)
    call_state = _safe_call_state(state)
    prior_snapshot_raw = call_state.get("structure_snapshot")
    prior_snapshot = dict(prior_snapshot_raw) if isinstance(prior_snapshot_raw, dict) else None
    now = as_utc(sample.at)
    spx_source_at = _source_at(sample, "spx_source_at")
    es_source_at = _source_at(sample, "es_source_at")
    previous_spx_at = _parse_datetime(call_state.get("last_spx_source_at"))
    previous_es_at = _parse_datetime(call_state.get("last_es_source_at"))
    if (
        previous_spx_at is not None
        and previous_es_at is not None
        and (spx_source_at <= previous_spx_at or es_source_at <= previous_es_at)
    ):
        state["call_strategy"] = call_state
        return state, _decision(call_state, structure=structure), ()
    call_state["last_spx_source_at"] = spx_source_at.isoformat()
    call_state["last_es_source_at"] = es_source_at.isoformat()
    blocks: list[str] = []
    if not structure.valid:
        blocks.append(structure.reason or "invalid_option_structure")
        invalid_since = _parse_datetime(call_state.get("structure_invalid_since"))
        if invalid_since is None:
            invalid_since = now
            call_state["structure_invalid_since"] = now.isoformat()
        if not _bias_is_active(call_state.get("active_bias"), sample=sample):
            call_state["active_bias"] = None
        if (now - invalid_since).total_seconds() > settings.structure_grace_seconds:
            call_state["structure_snapshot"] = None
            call_state["flip_watch"] = None
            call_state["wall_watch"] = None
            call_state["active_bias"] = None
        call_state["updated_at"] = now.isoformat()
        state["call_strategy"] = call_state
        return state, _decision(call_state, structure=structure, blocks=tuple(blocks)), ()

    call_state["structure_invalid_since"] = None

    snapshot = _update_structure_snapshot(
        call_state, structure=structure, sample=sample, settings=settings
    )
    call_state["structure_snapshot"] = snapshot
    active_bias = call_state.get("active_bias")
    if not _bias_is_active(active_bias, sample=sample):
        call_state["active_bias"] = None

    recent_event = _recent_down_event(state, now, settings.source_event_ttl_seconds)

    # Freeze the flip band while the down shock exists, before a reclaim can
    # cause the live map to move with price.
    flip_watch_raw = call_state.get("flip_watch")
    flip_watch = dict(flip_watch_raw) if isinstance(flip_watch_raw, dict) else None
    if recent_event is not None:
        source_id = str(recent_event["event_id"])
        first_seen = call_state.get("last_down_event_id") != source_id
        event_extreme = _finite(recent_event.get("extreme_spx"))
        freeze_snapshot = (
            prior_snapshot
            if isinstance(prior_snapshot, dict)
            and int(prior_snapshot.get("stable_count") or 0) >= 2
            and prior_snapshot.get("flip_source_fresh") is True
            else None
        )
        snapshot_flip_low = (
            _finite(freeze_snapshot.get("flip_low")) if freeze_snapshot is not None else None
        )
        if (
            first_seen
            and snapshot_flip_low is not None
            and _finite(freeze_snapshot.get("flip_high")) is not None
        ):
            flip_watch = {
                "status": "watch",
                "source_event_id": source_id,
                "flip_low": snapshot_flip_low,
                "flip_high": _finite(freeze_snapshot.get("flip_high")),
                "crossed_frozen_flip": bool(
                    event_extreme is not None and event_extreme < snapshot_flip_low
                ),
                "armed_at": None,
                "confirm_count": 0,
                "last_spx_source_at": None,
                "last_es_source_at": None,
                "last_es": None,
            }
        call_state["last_down_event_id"] = source_id
    elif flip_watch is not None and flip_watch.get("status") != "confirmed":
        flip_watch = None

    flip_bias: dict[str, object] | None = None
    if flip_watch is not None and recent_event is not None:
        flip_low = _finite(flip_watch.get("flip_low"))
        flip_high = _finite(flip_watch.get("flip_high"))
        reclaim_at = _parse_datetime(recent_event.get("reclaim_confirmed_at"))
        event_extreme = _finite(recent_event.get("extreme_spx"))
        if event_extreme is not None and flip_low is not None and event_extreme < flip_low:
            flip_watch["crossed_frozen_flip"] = True
        if flip_low is None or flip_high is None:
            flip_watch = None
        elif flip_watch.get("status") == "confirmed":
            if sample.spx < flip_low - settings.level_buffer_points:
                flip_watch = None
            reclaim_at = None
        elif reclaim_at is not None and flip_watch.get("crossed_frozen_flip") is not True:
            flip_watch = None
            reclaim_at = None
        elif reclaim_at is not None:
            if sample.spx < flip_low - settings.level_buffer_points:
                flip_watch = None
                reclaim_at = None
        if not structure.flip_source_fresh:
            reclaim_at = None
        if flip_watch is not None and reclaim_at is not None:
            armed_at = _parse_datetime(flip_watch.get("armed_at"))
            if armed_at is None:
                # Confirmation pairs must arrive after the reclaim transition.
                flip_watch["armed_at"] = now.isoformat()
                flip_watch["last_spx_source_at"] = _source_at(sample, "spx_source_at").isoformat()
                flip_watch["last_es_source_at"] = _source_at(sample, "es_source_at").isoformat()
                flip_watch["last_es"] = sample.es
                flip_watch["confirm_count"] = 0
            elif (now - armed_at).total_seconds() > settings.source_event_ttl_seconds:
                flip_watch = None
            else:
                spx_at = _source_at(sample, "spx_source_at")
                es_at = _source_at(sample, "es_source_at")
                last_spx_at = _parse_datetime(flip_watch.get("last_spx_source_at"))
                last_es_at = _parse_datetime(flip_watch.get("last_es_source_at"))
                fresh_pair = (
                    last_spx_at is not None
                    and last_es_at is not None
                    and spx_at > last_spx_at
                    and es_at > last_es_at
                )
                if fresh_pair:
                    previous_es = _finite(flip_watch.get("last_es")) or sample.es
                    es_floor = previous_es * (1.0 - settings.es_hold_tolerance_bps / 10_000.0)
                    holds = (
                        sample.spx >= flip_high + settings.level_buffer_points
                        and sample.es >= es_floor
                        and float(recent_event.get("spx_recovery_fraction") or 0.0)
                        >= settings.reclaim_hold_fraction
                        and float(recent_event.get("es_recovery_fraction") or 0.0)
                        >= settings.es_reclaim_hold_fraction
                    )
                    count = int(flip_watch.get("confirm_count") or 0) + 1 if holds else 0
                    flip_watch["confirm_count"] = count
                    flip_watch["last_spx_source_at"] = spx_at.isoformat()
                    flip_watch["last_es_source_at"] = es_at.isoformat()
                    flip_watch["last_es"] = sample.es
                    active = call_state.get("active_bias")
                    same_active = (
                        isinstance(active, dict)
                        and active.get("play") == FLIP_RECLAIM_CALL_KIND
                        and _level_same(active.get("level"), flip_high, 0.01)
                    )
                    if count >= settings.confirm_samples and not same_active:
                        flip_watch["status"] = "confirmed"
                        flip_bias = _new_bias(
                            play=FLIP_RECLAIM_CALL_KIND,
                            source_event_id=str(recent_event.get("event_id") or "") or None,
                            level=flip_high,
                            invalidation_level=flip_low - settings.level_buffer_points,
                            sample=sample,
                            gamma_state=structure.gamma_state,
                            gex_weighting=structure.gex_weighting,
                            expiry=str(structure.expiry or ""),
                            reasons=(
                                "down_shock_reclaim_confirmed",
                                "two_fresh_spx_es_pairs_held_above_frozen_flip",
                                "gamma_is_context_not_direction",
                            ),
                            ttl_seconds=settings.bias_ttl_seconds,
                        )

    # Freeze the pre-break call wall.  Never replace it with the next wall
    # while SPX is trading above it.
    wall_watch_raw = call_state.get("wall_watch")
    wall_watch = dict(wall_watch_raw) if isinstance(wall_watch_raw, dict) else None
    frozen_wall = _finite(snapshot.get("call_wall")) if isinstance(snapshot, dict) else None
    if (
        wall_watch is None
        and frozen_wall is not None
        and snapshot.get("call_wall_source_fresh") is True
        and sample.spx <= frozen_wall
    ):
        wall_watch = {
            "status": "watch",
            "level": frozen_wall,
            "pre_cross_structure_samples": int(snapshot.get("stable_count") or 0),
            "crossed_at": None,
            "confirm_count": 0,
            "pre_cross_spx": sample.spx,
            "pre_cross_es": sample.es,
            "last_spx_source_at": _source_at(sample, "spx_source_at").isoformat(),
            "last_es_source_at": _source_at(sample, "es_source_at").isoformat(),
        }
    wall_bias: dict[str, object] | None = None
    if wall_watch is not None:
        level = _finite(wall_watch.get("level"))
        crossed_at = _parse_datetime(wall_watch.get("crossed_at"))
        current_wall = _finite(structure.call_wall)
        if (
            crossed_at is None
            and structure.call_wall_source_fresh
            and current_wall is not None
            and level is not None
            and sample.spx < level + settings.level_buffer_points
            and abs(current_wall - level) > 0.1
        ):
            # Before a cross, the wall is not frozen yet. Follow a genuine OI
            # structure change instead of later declaring a breakout through
            # a provisional wall that the live map already replaced.
            if sample.spx <= current_wall:
                wall_watch = {
                    "status": "watch",
                    "level": current_wall,
                    "pre_cross_structure_samples": 1,
                    "crossed_at": None,
                    "confirm_count": 0,
                    "pre_cross_spx": sample.spx,
                    "pre_cross_es": sample.es,
                    "last_spx_source_at": _source_at(sample, "spx_source_at").isoformat(),
                    "last_es_source_at": _source_at(sample, "es_source_at").isoformat(),
                }
                level = current_wall
                crossed_at = None
            else:
                wall_watch = None
                level = None
        if level is None:
            wall_watch = None
        elif crossed_at is not None and sample.spx < level - settings.level_buffer_points:
            # A failed breakout resets the watch; a new stable pre-break wall
            # can arm on a later sample.
            wall_watch = None
        elif not structure.call_wall_source_fresh:
            pass
        elif wall_watch.get("status") == "confirmed":
            pass
        elif crossed_at is None and sample.spx >= level + settings.level_buffer_points:
            wall_watch["crossed_at"] = now.isoformat()
            wall_watch["confirm_count"] = 0
            wall_watch["last_spx_source_at"] = _source_at(sample, "spx_source_at").isoformat()
            wall_watch["last_es_source_at"] = _source_at(sample, "es_source_at").isoformat()
        elif crossed_at is None:
            wall_watch["pre_cross_spx"] = sample.spx
            wall_watch["pre_cross_es"] = sample.es
            wall_watch["pre_cross_structure_samples"] = max(
                int(wall_watch.get("pre_cross_structure_samples") or 0),
                int(snapshot.get("stable_count") or 0),
            )
            wall_watch["last_spx_source_at"] = _source_at(sample, "spx_source_at").isoformat()
            wall_watch["last_es_source_at"] = _source_at(sample, "es_source_at").isoformat()
        elif (now - crossed_at).total_seconds() > settings.confirm_window_seconds:
            wall_watch = None
        else:
            spx_at = _source_at(sample, "spx_source_at")
            es_at = _source_at(sample, "es_source_at")
            last_spx_at = _parse_datetime(wall_watch.get("last_spx_source_at"))
            last_es_at = _parse_datetime(wall_watch.get("last_es_source_at"))
            fresh_pair = (
                last_spx_at is not None
                and last_es_at is not None
                and spx_at > last_spx_at
                and es_at > last_es_at
            )
            if fresh_pair:
                pre_spx = _finite(wall_watch.get("pre_cross_spx")) or level
                pre_es = _finite(wall_watch.get("pre_cross_es")) or sample.es
                spx_move_bps = (sample.spx / pre_spx - 1.0) * 10_000.0
                es_move_bps = (sample.es / pre_es - 1.0) * 10_000.0
                holds = (
                    sample.spx >= level + settings.level_buffer_points
                    and es_move_bps > 0
                    and es_move_bps >= spx_move_bps * settings.es_confirm_ratio
                )
                count = int(wall_watch.get("confirm_count") or 0) + 1 if holds else 0
                wall_watch["confirm_count"] = count
                wall_watch["last_spx_source_at"] = spx_at.isoformat()
                wall_watch["last_es_source_at"] = es_at.isoformat()
                active = call_state.get("active_bias")
                same_active = (
                    isinstance(active, dict)
                    and active.get("play") == CALL_WALL_BREAKOUT_CALL_KIND
                    and _level_same(active.get("level"), level, 0.01)
                )
                if count >= settings.confirm_samples and not same_active:
                    wall_watch["status"] = "confirmed"
                    wall_bias = _new_bias(
                        play=CALL_WALL_BREAKOUT_CALL_KIND,
                        source_event_id=None,
                        level=level,
                        invalidation_level=level - settings.level_buffer_points,
                        sample=sample,
                        gamma_state=structure.gamma_state,
                        gex_weighting=structure.gex_weighting,
                        expiry=str(structure.expiry or ""),
                        reasons=(
                            "frozen_pre_break_call_wall_crossed",
                            "two_fresh_spx_es_pairs_accepted_above_wall",
                            "gamma_is_context_not_direction",
                        ),
                        ttl_seconds=settings.bias_ttl_seconds,
                    )

    call_state["flip_watch"] = flip_watch
    call_state["wall_watch"] = wall_watch
    existing = call_state.get("active_bias")
    chosen = wall_bias or flip_bias
    previous_signal = call_state.get("last_signal")
    if chosen is not None and isinstance(previous_signal, dict):
        previous_at = _parse_datetime(previous_signal.get("confirmed_at"))
        previous_play = str(previous_signal.get("play") or "")
        in_cooldown = (
            previous_at is not None
            and 0 <= (now - previous_at).total_seconds() < settings.cooldown_seconds
        )
        upgrades_reclaim = (
            chosen.get("play") == CALL_WALL_BREAKOUT_CALL_KIND
            and previous_play == FLIP_RECLAIM_CALL_KIND
        )
        if in_cooldown and not upgrades_reclaim:
            chosen = None
    if chosen is not None:
        # Breakout is the stronger continuation state and supersedes reclaim.
        if not isinstance(existing, dict) or existing.get("event_id") != chosen.get("event_id"):
            call_state["active_bias"] = chosen
            call_state["last_signal"] = {
                **chosen,
                "delivered": False,
                "last_attempt_at": None,
            }

    signals: tuple[IntradayPathSignal, ...] = ()
    last_signal = call_state.get("last_signal")
    if isinstance(last_signal, dict) and last_signal.get("delivered") is not True:
        attempted_at = _parse_datetime(last_signal.get("last_attempt_at"))
        due = attempted_at is None or (now - attempted_at).total_seconds() >= settings.retry_seconds
        active = call_state.get("active_bias")
        if (
            due
            and isinstance(active, dict)
            and active.get("event_id") == last_signal.get("event_id")
        ):
            signal = _signal_from_bias(last_signal)
            if signal is not None:
                signals = (signal,)

    call_state["updated_at"] = now.isoformat()
    state["call_strategy"] = call_state
    return state, _decision(call_state, structure=structure, blocks=tuple(blocks)), signals


def mark_strategy_alert_attempts(
    monitor_state: Mapping[str, object],
    *,
    event_ids: set[str],
    at: datetime,
    delivered: bool,
) -> dict[str, object]:
    state = dict(monitor_state)
    call_state = _safe_call_state(state)
    raw = call_state.get("last_signal")
    if not isinstance(raw, dict) or str(raw.get("event_id") or "") not in event_ids:
        return state
    last_signal = dict(raw)
    last_signal["last_attempt_at"] = as_utc(at).isoformat()
    if delivered:
        last_signal["delivered"] = True
        last_signal["delivered_at"] = as_utc(at).isoformat()
    call_state["last_signal"] = last_signal
    state["call_strategy"] = call_state
    return state


def confirmed_call_bias(
    monitor_state: Mapping[str, object] | None,
    *,
    now: datetime,
) -> dict[str, object] | None:
    if not isinstance(monitor_state, Mapping):
        return None
    call_state = _safe_call_state(monitor_state)
    raw = call_state.get("active_bias")
    if not isinstance(raw, dict) or raw.get("status") != "confirmed":
        return None
    expires_at = _parse_datetime(raw.get("expires_at"))
    if expires_at is None or as_utc(now) > expires_at:
        return None
    play = str(raw.get("play") or "")
    if (
        play not in STRATEGY_KINDS
        or _finite(raw.get("level")) is None
        or _finite(raw.get("invalidation_level")) is None
        or not str(raw.get("expiry") or "")
    ):
        return None
    return dict(raw)
