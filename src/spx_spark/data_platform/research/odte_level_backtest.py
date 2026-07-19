"""Backtest 0DTE level-alert signals as traded via SPX/SPXW options.

Evaluates three control/proxy signal sets — confirmed level transitions (S1),
pricing-outcome prefills (S2) and GTH dip reclaims (S3) — plus the persisted
production ``trade_ready`` cohort. Writes trades.csv, artifact.json and a
Chinese report.md.

Relevant data conventions:
- ``index:SPX`` rows from schwab populate ``mid``; ibkr leaves ``mid`` NULL but
  fills ``last``/``effective_price``; ``future:ES`` populates ``mid`` for both
  providers. Underlier price therefore uses ``COALESCE(mid, last, effective_price)``.
- S2 recorded ``prefill_ask`` values precede the production follow-through
  gate. They are never fills; passing events are repriced after the full hold.
- ``trade_ready`` uses its recorded provider, contract, limit and exclusive
  expiry window. It fills only when a contemporaneous ask is at/below the
  recorded limit and never reconstructs entry fields from later data.
- New S3 events persist production debit-spread strikes. ``spread_wall`` copies
  those legs exactly and fails closed for legacy records without them.
- S1 ``es_equivalent`` records keep ``level``/``levels`` in raw ES coordinates;
  ``spx_level`` is the SPX-coordinate equivalent (level - basis). Everything is
  normalized to SPX coordinates before simulation.
"""

from __future__ import annotations

import logging
from bisect import bisect_left, bisect_right
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Sequence

from .odte_level_aggregate import (
    _stats as _stats,
    aggregate as aggregate,
    build_artifact,
    write_outputs,
)
from .odte_level_quotes import QuoteStore, pick_provider
from .odte_level_signals import (
    FT_GATE_EM_FRACTION,
    FT_GATE_POINTS,
    FT_GATE_SECONDS,
    MAX_ENTRY_LEG_SKEW,
    MAX_ENTRY_QUOTE_AGE,
    MAX_HOLD,
    MAX_MARK_LEG_SKEW,
    MAX_MARK_QUOTE_AGE,
    MAX_UNDERLIER_QUOTE_AGE,
    POINTS_PER_CONTRACT,
    PROFILES,
    PROFIT_TARGET_MULTIPLE,
    SATURATION_FRACTION,
    SET_CONFIRMED,
    SET_GTH_DIP,
    SET_ORDER,
    SET_PREFILL,
    SET_TRADE_READY,
    SPREAD_WIDTHS,
    TIME_STOP_DELAY,
    TRAIL33_ARM_FRACTION,
    TRAILING_ACTIVATION_FRACTION,
    TRAILING_GIVEBACK_FRACTION,
    VARIANT_NAKED,
    VARIANT_SPREAD_WALL,
    VARIANTS,
    OptionTick,
    Profile,
    Signal,
    Skip,
    Trade,
    UnderlierTick,
    _float,
    contract_id_for,
    expiry_close_at,
    formula_target,
    load_confirmed_signals,
    load_gth_dip_signals,
    load_prefill_signals,
    load_trade_ready_signals,
    nearest_wall,
    next_exit_clock,
    right_for,
    spread_strikes,
    trade_intent_coverage,
    wall_spread_structure,
)
from .strategy_readiness import build_strategy_readiness

logger = logging.getLogger(__name__)


def _tick_mid(tick: OptionTick) -> float | None:
    if tick.mid is not None:
        return tick.mid
    if tick.bid is not None and tick.ask is not None:
        return (tick.bid + tick.ask) / 2.0
    return None


def _first_tick_at_or_after(
    series: Sequence[OptionTick], times: Sequence[datetime], at: datetime
) -> OptionTick | None:
    index = bisect_left(times, at)
    if index >= len(series):
        return None
    return series[index]


def _tick_at_or_before(
    series: Sequence, times: Sequence[datetime], at: datetime, *, fallback_first: bool
):
    index = bisect_right(times, at) - 1
    if index < 0:
        return series[0] if fallback_first and series else None
    return series[index]


# ---------------------------------------------------------------------------
# Trade simulation
# ---------------------------------------------------------------------------


def follow_through_pass(
    underlier: Sequence[UnderlierTick],
    first_touch_at: datetime | None,
    dir_sign: int,
    *,
    trigger_level: float | None = None,
    expected_move_points: float | None = None,
    seconds: int = FT_GATE_SECONDS,
    min_points: float = FT_GATE_POINTS,
    em_fraction: float = FT_GATE_EM_FRACTION,
) -> bool | None:
    """Evaluate the production follow-through gate at the end of its hold.

    Production measures spot against the confirmed trigger level, not against
    the first observed price at touch. The required distance is
    ``max(min_points, em_fraction * expected_move_points)``. The legacy
    touch-to-touch calculation remains available when ``trigger_level`` is not
    supplied so callers do not silently break, but executable S2 evaluation
    always supplies it.
    """
    if first_touch_at is None or not underlier:
        return None
    if trigger_level is not None and expected_move_points is None:
        # Production blocks trade readiness when the expected move is absent.
        return None
    times = [tick.at for tick in underlier]
    gate_at = first_touch_at + timedelta(seconds=seconds)
    end_tick = _tick_at_or_before(underlier, times, gate_at, fallback_first=False)
    if (
        end_tick is None
        or end_tick.at < first_touch_at
        or gate_at - end_tick.at > MAX_UNDERLIER_QUOTE_AGE
    ):
        return None
    anchor = trigger_level
    if anchor is None:
        start_tick = _tick_at_or_before(underlier, times, first_touch_at, fallback_first=False)
        if start_tick is None:
            index = bisect_left(times, first_touch_at)
            if (
                index >= len(underlier)
                or underlier[index].at - first_touch_at > MAX_UNDERLIER_QUOTE_AGE
            ):
                return None
            start_tick = underlier[index]
        anchor = start_tick.price
    threshold = max(
        min_points,
        (expected_move_points or 0.0) * em_fraction,
    )
    return dir_sign * (end_tick.price - anchor) >= threshold


def simulate_trade(
    signal: Signal,
    variant: str,
    long_series: Sequence[OptionTick],
    short_series: Sequence[OptionTick] | None,
    underlier: Sequence[UnderlierTick],
    profile: Profile = PROFILES[0],
    *,
    spread_width: float | None = None,
    ft_pass: bool | None = None,
    short_contract_id: str | None = None,
) -> Trade | Skip:
    """Simulate one signal/variant/profile against in-memory quote series.

    Exit rules are checked per long-leg tick in order: invalidation,
    target_wall, profit-taking (fixed 1.3x / trailing / sat85 / trail33 per
    profile; clock has none), time_stop, then an end_of_data fallback.
    Stop-style exits pay the bid (long bid, or long bid - short ask for
    spreads); the fixed profit target exits at the triggering mid. GTH signals
    (future:ES underlier) use the profile's GTH time-stop/max-hold overrides,
    or expiry-date 09:45 America/New_York when the profile sets gth_clock_exit.
    """
    dir_sign = 1 if signal.direction == "up" else -1
    requested_entry_at = signal.entry_at
    production_entry = signal.set_name == SET_TRADE_READY
    if production_entry and variant != VARIANT_NAKED:
        return Skip(signal.set_name, profile.name, signal.key, variant, "not_applicable")
    gth = signal.underlier_instrument == "future:ES"
    expiry_close: datetime | None = None
    exit_clock: datetime | None = None
    if gth:
        if signal.expiry is None:
            return Skip(signal.set_name, profile.name, signal.key, variant, "no_expiry")
        expiry_close = expiry_close_at(signal.expiry)
        if requested_entry_at >= expiry_close:
            return Skip(
                signal.set_name, profile.name, signal.key, variant, "entry_after_expiry_close"
            )
        if profile.gth_clock_exit:
            exit_clock = next_exit_clock(requested_entry_at, signal.expiry)
            if requested_entry_at >= exit_clock:
                return Skip(
                    signal.set_name,
                    profile.name,
                    signal.key,
                    variant,
                    "entry_after_exit_clock",
                )
    long_times = [tick.at for tick in long_series]
    entry_tick: OptionTick | None
    recorded_entry_px = signal.entry_px if signal.set_name != SET_PREFILL else None

    if production_entry:
        entry_limit = signal.entry_limit
        entry_expires_at = signal.entry_expires_at
        invalidation = signal.invalidation_level
        target = signal.target_level
        if (
            entry_limit is None
            or entry_limit <= 0
            or entry_expires_at is None
            or invalidation is None
            or target is None
        ):
            return Skip(
                signal.set_name,
                profile.name,
                signal.key,
                variant,
                "recorded_entry_fields_unavailable",
            )
        if requested_entry_at >= entry_expires_at:
            return Skip(
                signal.set_name, profile.name, signal.key, variant, "entry_window_expired"
            )
        entry_tick = next(
            (
                tick
                for tick in long_series[bisect_left(long_times, requested_entry_at) :]
                if tick.at < entry_expires_at
                and tick.ask is not None
                and tick.ask > 0
                and tick.ask <= entry_limit
            ),
            None,
        )
        boundary_at = entry_tick.at if entry_tick is not None else entry_expires_at
        pre_entry_prices: list[UnderlierTick] = []
        if signal.decision_spot is not None:
            pre_entry_prices.append(
                UnderlierTick(at=requested_entry_at, price=signal.decision_spot)
            )
        pre_entry_prices.extend(
            tick
            for tick in underlier
            if requested_entry_at <= tick.at <= boundary_at
        )
        pre_entry_prices.sort(key=lambda tick: tick.at)
        for spot_tick in pre_entry_prices:
            spot = spot_tick.price
            if (dir_sign == 1 and spot <= invalidation) or (
                dir_sign == -1 and spot >= invalidation
            ):
                return Skip(
                    signal.set_name,
                    profile.name,
                    signal.key,
                    variant,
                    "invalidation_before_entry",
                )
            if (dir_sign == 1 and spot >= target) or (dir_sign == -1 and spot <= target):
                return Skip(
                    signal.set_name,
                    profile.name,
                    signal.key,
                    variant,
                    "target_before_entry",
                )
        if (
            not pre_entry_prices
            or boundary_at - pre_entry_prices[-1].at > MAX_UNDERLIER_QUOTE_AGE
        ):
            return Skip(
                signal.set_name,
                profile.name,
                signal.key,
                variant,
                "pre_entry_underlier_unavailable",
            )
        if entry_tick is None:
            return Skip(
                signal.set_name, profile.name, signal.key, variant, "entry_limit_not_reached"
            )
        assert entry_tick.ask is not None  # established by the executable-limit scan
        long_entry_px = entry_tick.ask
        entry_source = "lake_ask_at_or_below_recorded_limit"
        entry_at = entry_tick.at
    elif recorded_entry_px is None:
        entry_tick = _first_tick_at_or_after(long_series, long_times, requested_entry_at)
        if entry_tick is None or entry_tick.at - requested_entry_at > MAX_ENTRY_QUOTE_AGE:
            return Skip(signal.set_name, profile.name, signal.key, variant, "no_quote")
        if entry_tick.ask is None:
            return Skip(signal.set_name, profile.name, signal.key, variant, "no_quote")
        long_entry_px = entry_tick.ask
        entry_source = "lake_ask"
        entry_at = entry_tick.at
    else:
        entry_tick = _first_tick_at_or_after(long_series, long_times, requested_entry_at)
        long_entry_px = recorded_entry_px
        entry_source = "recorded_ask"
        if entry_tick is None:
            return Skip(signal.set_name, profile.name, signal.key, variant, "no_path")
        entry_at = requested_entry_at

    width = spread_width if spread_width is not None else SPREAD_WIDTHS.get(variant)
    short_times: list[datetime] = []
    short_entry_tick: OptionTick | None = None
    if width is not None:
        short_series = short_series or []
        short_times = [tick.at for tick in short_series]
        short_entry_tick = (
            _first_tick_at_or_after(short_series, short_times, requested_entry_at)
            if recorded_entry_px is None
            else _tick_at_or_before(
                short_series, short_times, requested_entry_at, fallback_first=False
            )
        )
        if short_entry_tick is None or short_entry_tick.bid is None:
            return Skip(signal.set_name, profile.name, signal.key, variant, "no_short_leg")
        if recorded_entry_px is None:
            if short_entry_tick.at - requested_entry_at > MAX_ENTRY_QUOTE_AGE:
                return Skip(signal.set_name, profile.name, signal.key, variant, "no_short_leg")
            # Price both legs from information available at one execution time.
            # This removes the old look-ahead where a future first short tick was
            # reused for earlier long-leg marks.
            entry_at = max(entry_tick.at, short_entry_tick.at)  # type: ignore[union-attr]
            entry_tick = _tick_at_or_before(long_series, long_times, entry_at, fallback_first=False)
            short_entry_tick = _tick_at_or_before(
                short_series, short_times, entry_at, fallback_first=False
            )
            if (
                entry_tick is None
                or short_entry_tick is None
                or entry_tick.at < requested_entry_at
                or short_entry_tick.at < requested_entry_at
                or entry_at - requested_entry_at > MAX_ENTRY_QUOTE_AGE
                or entry_at - entry_tick.at > MAX_ENTRY_QUOTE_AGE
                or entry_at - short_entry_tick.at > MAX_ENTRY_QUOTE_AGE
            ):
                return Skip(
                    signal.set_name, profile.name, signal.key, variant, "no_synchronized_entry"
                )
            if abs(entry_tick.at - short_entry_tick.at) > MAX_ENTRY_LEG_SKEW:
                return Skip(signal.set_name, profile.name, signal.key, variant, "entry_leg_skew")
            if entry_tick.ask is None or short_entry_tick.bid is None:
                return Skip(
                    signal.set_name, profile.name, signal.key, variant, "no_synchronized_entry"
                )
            long_entry_px = entry_tick.ask
        elif requested_entry_at - short_entry_tick.at > MAX_ENTRY_LEG_SKEW:
            return Skip(signal.set_name, profile.name, signal.key, variant, "entry_leg_skew")
        entry_px = long_entry_px - short_entry_tick.bid
        if entry_px <= 0 or entry_px > width:
            return Skip(signal.set_name, profile.name, signal.key, variant, "invalid_spread_debit")
    else:
        entry_px = long_entry_px

    if expiry_close is not None and entry_at >= expiry_close:
        return Skip(signal.set_name, profile.name, signal.key, variant, "entry_after_expiry_close")
    if exit_clock is not None and entry_at >= exit_clock:
        return Skip(signal.set_name, profile.name, signal.key, variant, "entry_after_exit_clock")
    if (
        production_entry
        and signal.recorded_time_stop_at is not None
        and entry_at >= signal.recorded_time_stop_at
    ):
        return Skip(signal.set_name, profile.name, signal.key, variant, "entry_after_time_stop")

    def short_mark(at: datetime) -> OptionTick | None:
        mark = _tick_at_or_before(short_series, short_times, at, fallback_first=False)
        if mark is None or mark.at > at:
            return None
        age = at - mark.at
        if age > MAX_MARK_QUOTE_AGE or age > MAX_MARK_LEG_SKEW:
            return None
        return mark

    inv_level = signal.invalidation_level if signal.invalidation_level is not None else signal.level
    inv_buffer = signal.invalidation_buffer
    if profile.invalidation_em_fraction is not None and inv_buffer > 0:
        # wide_invalidation: scale the buffer with expected move; fall back to the
        # fixed buffer when EM is missing. S3's trough rule (buffer 0) is untouched.
        if signal.expected_move_points is not None:
            inv_buffer = max(
                inv_buffer, profile.invalidation_em_fraction * signal.expected_move_points
            )
    if signal.target_mode == "wall":
        target = nearest_wall(signal.level, signal.walls, dir_sign)
    elif signal.target_mode == "recorded":
        target = signal.target_level
    else:
        target = formula_target(signal.level, dir_sign, signal.expected_move_points)
    underlier_times = [tick.at for tick in underlier]
    if production_entry and signal.recorded_time_stop_at is not None:
        stop_at = signal.recorded_time_stop_at
        hold_until = min(entry_at + MAX_HOLD, stop_at + MAX_MARK_QUOTE_AGE)
    elif gth and profile.gth_clock_exit:
        assert exit_clock is not None  # established by the GTH clock preflight
        stop_at = exit_clock
        hold_until = stop_at + MAX_MARK_QUOTE_AGE
    else:
        time_stop = (profile.gth_time_stop or TIME_STOP_DELAY) if gth else TIME_STOP_DELAY
        max_hold = (profile.gth_max_hold or MAX_HOLD) if gth else MAX_HOLD
        stop_at = entry_at + time_stop
        hold_until = entry_at + max_hold
    if expiry_close is not None:
        stop_at = min(stop_at, expiry_close)
        hold_until = min(hold_until, expiry_close)
    hold_until = min(hold_until, stop_at + MAX_MARK_QUOTE_AGE)

    path_mids: list[float] = []
    peak_mid: float | None = None
    trailing_armed = False
    exit_px: float | None = None
    exit_time: datetime | None = None
    exit_reason: str | None = None
    last_tick: OptionTick | None = None
    last_short_tick: OptionTick | None = None

    for tick in long_series[bisect_left(long_times, entry_at) :]:
        if tick.at > hold_until:
            break
        long_mid = _tick_mid(tick)
        if width is not None:
            short_tick = short_mark(tick.at)
            if short_tick is None:
                continue
            short_mid = _tick_mid(short_tick)
            pos_mid = (
                long_mid - short_mid if long_mid is not None and short_mid is not None else None
            )
            pos_stop = (  # long bid - short ask
                tick.bid - short_tick.ask
                if tick.bid is not None and short_tick.ask is not None
                else None
            )
        else:
            pos_mid = long_mid
            pos_stop = tick.bid
        if pos_stop is not None:
            last_tick = tick
            last_short_tick = short_tick if width is not None else None
        if pos_mid is not None:
            path_mids.append(pos_mid)
        if tick.at >= stop_at and pos_stop is not None:
            exit_px, exit_time, exit_reason = pos_stop, tick.at, "time_stop"
            break
        underlier_tick = _tick_at_or_before(
            underlier, underlier_times, tick.at, fallback_first=False
        )
        underlier_is_fresh = (
            underlier_tick is not None
            and underlier_tick.at <= tick.at
            and tick.at - underlier_tick.at <= MAX_UNDERLIER_QUOTE_AGE
        )
        if underlier_is_fresh and pos_stop is not None:
            spot = underlier_tick.price
            if (dir_sign == 1 and spot <= inv_level - inv_buffer) or (
                dir_sign == -1 and spot >= inv_level + inv_buffer
            ):
                exit_px, exit_time, exit_reason = pos_stop, tick.at, "invalidation"
                break
            if target is not None and (
                (dir_sign == 1 and spot >= target) or (dir_sign == -1 and spot <= target)
            ):
                exit_px, exit_time, exit_reason = pos_stop, tick.at, "target_wall"
                break
        if profile.profit_target_mode == "trailing":
            # arm at +15% unrealized (mid), then exit (bid) once the position
            # gives back 1/3 of the peak unrealized gain
            if pos_mid is not None and entry_px > 0:
                peak_mid = pos_mid if peak_mid is None else max(peak_mid, pos_mid)
                if pos_mid >= (1.0 + TRAILING_ACTIVATION_FRACTION) * entry_px:
                    trailing_armed = True
                giveback = (peak_mid - entry_px) * TRAILING_GIVEBACK_FRACTION
                if trailing_armed and pos_mid <= peak_mid - giveback and pos_stop is not None:
                    exit_px, exit_time, exit_reason = pos_stop, tick.at, "trailing_tp"
                    break
        elif profile.profit_target_mode == "sat85" and width is not None:
            # spread saturation: take profit once the spread is worth >= 85% of width
            if pos_mid is not None and pos_mid >= SATURATION_FRACTION * width:
                if pos_stop is not None:
                    exit_px, exit_time, exit_reason = pos_stop, tick.at, "saturation"
                    break
        elif profile.profit_target_mode == "trail33" and width is not None:
            # arm once the spread is worth >= 50% of width, then exit (bid) after
            # giving back 1/3 of the peak unrealized gain
            if pos_mid is not None:
                peak_mid = pos_mid if peak_mid is None else max(peak_mid, pos_mid)
                if pos_mid >= TRAIL33_ARM_FRACTION * width:
                    trailing_armed = True
                giveback = (peak_mid - entry_px) * TRAILING_GIVEBACK_FRACTION
                if trailing_armed and pos_mid <= peak_mid - giveback and pos_stop is not None:
                    exit_px, exit_time, exit_reason = pos_stop, tick.at, "trailing_tp"
                    break
        elif profile.profit_target_mode == "clock":
            pass  # clock profile: invalidation + clock stop only, no profit rule
        elif pos_mid is not None and pos_mid >= PROFIT_TARGET_MULTIPLE * entry_px:
            exit_px, exit_time, exit_reason = pos_mid, tick.at, "profit_target"
            break

    if exit_px is None:
        if last_tick is None or stop_at - last_tick.at > MAX_MARK_QUOTE_AGE:
            return Skip(
                signal.set_name,
                profile.name,
                signal.key,
                variant,
                "no_fresh_exit_quote",
            )
        if width is not None:
            short_tick = last_short_tick
            if short_tick is None:
                return Skip(
                    signal.set_name,
                    profile.name,
                    signal.key,
                    variant,
                    "no_fresh_spread_path",
                )
            fallback = (
                last_tick.bid - short_tick.ask
                if last_tick.bid is not None and short_tick.ask is not None
                else None
            )
        else:
            fallback = last_tick.bid
        if fallback is None:
            return Skip(signal.set_name, profile.name, signal.key, variant, "no_path")
        exit_px, exit_time, exit_reason = fallback, last_tick.at, "end_of_data"

    pnl_points = exit_px - entry_px
    horizons = signal.horizons or {}

    def _hz(key: str) -> float | None:
        return _float((horizons.get(key) or {}).get("return_fraction"))

    return Trade(
        set_name=signal.set_name,
        profile=profile.name,
        key=signal.key,
        at=signal.at.isoformat(),
        play=signal.thesis,
        direction=signal.direction,
        level=signal.level,
        level_kind=signal.level_kind,
        contract_id=signal.contract_id,
        short_contract_id=short_contract_id,
        variant=variant,
        entry_time=entry_at.isoformat(),
        entry_px=round(entry_px, 4),
        exit_time=exit_time.isoformat(),
        exit_px=round(exit_px, 4),
        exit_reason=exit_reason,
        pnl_points=round(pnl_points, 4),
        pnl_usd=round(pnl_points * POINTS_PER_CONTRACT, 2),
        mfe_points=round(max(path_mids) - entry_px, 4) if path_mids else None,
        mae_points=round(min(path_mids) - entry_px, 4) if path_mids else None,
        underlier_source=(
            signal.underlier_instrument
            + (f"-{signal.basis_points:g}" if signal.basis_points else "")
        ),
        trend_regime=signal.trend_regime,
        session_bucket=signal.session_bucket,
        ft_pass_15s2p=ft_pass,
        entry_price_source=entry_source,
        h60_ret=_hz("60"),
        h300_ret=_hz("300"),
        h900_ret=_hz("900"),
    )


def evaluate_signal(
    store: QuoteStore, signal: Signal, profiles: Sequence[Profile] = PROFILES
) -> tuple[list[Trade], list[Skip]]:
    """Resolve strike/provider/legs once, then simulate all profiles x variants.

    Quote series are loaded a single time (shared QuoteStore cache); for GTH
    signals the load window covers the longest profile horizon (gth_360 needs
    entry+370min; clock profiles use expiry-date 09:45 America/New_York, and no
    GTH path may cross the expiry 16:00 ET close). Profiles may restrict sets
    (set_names), GTH-only signals (gth_only) or spread variants (spread_only).
    S2 signals enter only after passing the production follow-through gate.
    S3 ``spread_wall`` uses the two strikes persisted by production and is
    unavailable for legacy events that did not persist a spread.
    ``trade_ready`` is naked-only and replays its persisted provider/contract
    and limit window; counterfactual spread construction is not applicable.
    """
    right = right_for(signal.direction)
    active_profiles = [
        profile
        for profile in profiles
        if (profile.set_names is None or signal.set_name in profile.set_names)
        and (not profile.gth_only or signal.underlier_instrument == "future:ES")
    ]
    if not active_profiles:
        return [], []

    def skip_variants(reason: str) -> list[Skip]:
        rows: list[Skip] = []
        for profile in active_profiles:
            for variant in VARIANTS:
                if profile.spread_only and variant == VARIANT_NAKED:
                    continue
                variant_reason = (
                    "not_applicable"
                    if signal.set_name == SET_TRADE_READY and variant != VARIANT_NAKED
                    else reason
                )
                if signal.set_name == SET_PREFILL and variant == VARIANT_SPREAD_WALL:
                    variant_reason = "not_applicable"
                rows.append(
                    Skip(signal.set_name, profile.name, signal.key, variant, variant_reason)
                )
        return rows

    ft_pass: bool | None = None
    touch = signal.first_touch_at or signal.at
    if signal.set_name == SET_PREFILL:
        gate_at = touch + timedelta(seconds=FT_GATE_SECONDS)
        gate_raw = store.underlier_series(
            instrument_id=signal.underlier_instrument,
            start=touch - MAX_UNDERLIER_QUOTE_AGE,
            end=gate_at,
        )
        gate_underlier = [
            UnderlierTick(at=tick.at, price=tick.price - signal.basis_points) for tick in gate_raw
        ]
        ft_pass = follow_through_pass(
            gate_underlier,
            touch,
            1 if signal.direction == "up" else -1,
            trigger_level=signal.level,
            expected_move_points=signal.expected_move_points,
        )
        if ft_pass is not True:
            reason = "follow_through_failed" if ft_pass is False else "follow_through_unavailable"
            return [], skip_variants(reason)
        # Reprice only after the complete hold. This intentionally discards the
        # pricing-outcome prefill, which was observed before the production gate.
        signal = replace(signal, entry_at=gate_at, entry_px=None)

    t0 = signal.entry_at
    if signal.strike is None or signal.expiry is None:
        if signal.expiry is None:
            return [], skip_variants("no_expiry")
        strike = store.select_delta_strike(expiry=signal.expiry, right=right, t0=t0)
        if strike is None:
            return [], skip_variants("no_delta_candidate")
        signal = replace(
            signal, strike=strike, contract_id=contract_id_for(signal.expiry, strike, right)
        )
    is_gth = signal.underlier_instrument == "future:ES"
    if is_gth and t0 >= expiry_close_at(signal.expiry):
        return [], skip_variants("entry_after_expiry_close")
    load_end = t0 + MAX_HOLD
    if is_gth:
        session_close = expiry_close_at(signal.expiry)
        horizons = [
            (
                t0 + (p.gth_max_hold or MAX_HOLD)
                if not p.gth_clock_exit
                else next_exit_clock(t0, signal.expiry)
            )
            for p in active_profiles
        ]
        load_end = min(
            max(max(horizons), t0 + MAX_ENTRY_QUOTE_AGE) + MAX_MARK_QUOTE_AGE,
            session_close,
        )
    provider = signal.entry_provider
    if signal.set_name != SET_TRADE_READY:
        provider = pick_provider(
            store,
            expiry=signal.expiry,
            strike=signal.strike,
            right=right,
            t0=t0,
            quote_side="ask",
        )
    long_series = (
        store.option_series(
            provider=provider,
            expiry=signal.expiry,
            strike=signal.strike,
            right=right,
            start=t0 - timedelta(minutes=5),
            end=load_end,
        )
        if provider
        else []
    )
    raw_underlier = store.underlier_series(
        instrument_id=signal.underlier_instrument,
        start=min(touch, t0) - timedelta(minutes=10),
        end=load_end + timedelta(minutes=1),
    )
    underlier = [
        UnderlierTick(at=tick.at, price=tick.price - signal.basis_points) for tick in raw_underlier
    ]
    # resolve short strikes per spread variant; spread_wall follows the
    # S1 wall-derived rule. S3 must use its persisted production legs exactly.
    leg_specs: dict[str, tuple[float, float]] = {}  # variant -> (short_strike, width)
    for variant in VARIANTS:
        if signal.set_name == SET_TRADE_READY:
            continue
        width = SPREAD_WIDTHS.get(variant)
        if width is not None:
            _, short_strike = spread_strikes(signal.direction, signal.strike, width)
            leg_specs[variant] = (short_strike, width)
    if signal.set_name == SET_CONFIRMED:
        short_strike, wall_width, _ = wall_spread_structure(
            direction=signal.direction,
            long_strike=signal.strike,
            wall_map=signal.wall_map,
            expected_move_points=signal.expected_move_points,
        )
        leg_specs[VARIANT_SPREAD_WALL] = (short_strike, wall_width)
    elif (
        signal.set_name == SET_GTH_DIP
        and signal.recorded_short_strike is not None
        and signal.recorded_spread_width is not None
    ):
        leg_specs[VARIANT_SPREAD_WALL] = (
            signal.recorded_short_strike,
            signal.recorded_spread_width,
        )

    short_legs: dict[str, tuple[list[OptionTick], str | None]] = {}
    for variant, (short_strike, width) in leg_specs.items():
        short_contract_id = contract_id_for(signal.expiry, short_strike, right)
        short_provider = pick_provider(
            store,
            expiry=signal.expiry,
            strike=short_strike,
            right=right,
            t0=t0,
            quote_side="bid",
        )
        short_series = (
            store.option_series(
                provider=short_provider,
                expiry=signal.expiry,
                strike=short_strike,
                right=right,
                start=t0 - timedelta(minutes=5),
                end=load_end,
            )
            if short_provider
            else []
        )
        short_legs[variant] = (short_series, short_contract_id)

    trades: list[Trade] = []
    skips: list[Skip] = []
    for profile in active_profiles:
        for variant in VARIANTS:
            if profile.spread_only and variant == VARIANT_NAKED:
                continue
            if signal.set_name == SET_TRADE_READY and variant != VARIANT_NAKED:
                skips.append(
                    Skip(signal.set_name, profile.name, signal.key, variant, "not_applicable")
                )
                continue
            if variant == VARIANT_SPREAD_WALL and variant not in leg_specs:
                reason = (
                    "no_recorded_production_spread"
                    if signal.set_name == SET_GTH_DIP
                    else "not_applicable"
                )
                skips.append(Skip(signal.set_name, profile.name, signal.key, variant, reason))
                continue
            spec = leg_specs.get(variant)
            short_series, short_contract_id = short_legs.get(variant, (None, None))
            result = simulate_trade(
                signal,
                variant,
                long_series,
                short_series,
                underlier,
                profile,
                spread_width=spec[1] if spec else None,
                ft_pass=ft_pass,
                short_contract_id=short_contract_id,
            )
            (trades if isinstance(result, Trade) else skips).append(result)
    return trades, skips


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


_FEATURE_PARTITION_GLOBS = (
    "level_decision_health/date=*",
    "level_decision_audit/date=*",
    "pricing_outcomes/date=*",
    "gth_dip_reclaim/date=*",
    "trade_intents/date=*",
)


def _cutoff_for(as_of: date | datetime | None, *, now: datetime) -> datetime:
    """Return an exclusive UTC cutoff aligned to complete sessions.

    A date means "through this full UTC session". A datetime is an exact
    knowledge cutoff; its current UTC date is deliberately excluded below so a
    weekly report never labels an intraday partition as a complete trading day.
    With no explicit value, the most recent UTC midnight is used.
    """
    if isinstance(as_of, datetime):
        return (
            as_of.astimezone(timezone.utc) if as_of.tzinfo else as_of.replace(tzinfo=timezone.utc)
        )
    if isinstance(as_of, date):
        return datetime.combine(as_of + timedelta(days=1), time.min, tzinfo=timezone.utc)
    return datetime.combine(now.date(), time.min, tzinfo=timezone.utc)


def _feature_partition_dates(features_root: Path) -> set[date]:
    sessions: set[date] = set()
    for pattern in _FEATURE_PARTITION_GLOBS:
        for path in features_root.glob(pattern):
            try:
                sessions.add(date.fromisoformat(path.name.removeprefix("date=")))
            except ValueError:
                continue
    return sessions


def run(
    features_root: Path,
    data_root: Path,
    output_dir: Path,
    *,
    as_of: date | datetime | None = None,
) -> Path:
    """Load complete-session signals and write trades/artifact/report.

    ``as_of`` is explicit and reproducible: a date includes that full UTC date;
    a datetime is an exclusive event cutoff but only earlier full UTC dates are
    admitted. The default likewise excludes today's possibly incomplete data.
    """
    features_root = Path(features_root).expanduser().resolve()
    data_root = Path(data_root).expanduser().resolve()
    generated_at = datetime.now(timezone.utc)
    cutoff_at = _cutoff_for(as_of, now=generated_at)
    last_complete_date = cutoff_at.date() - timedelta(days=1)
    strategy_readiness = build_strategy_readiness(
        features_root,
        cutoff_at=cutoff_at,
        generated_at=generated_at,
    )
    readiness_sessions = strategy_readiness.get("sessions")
    readiness_details = (
        readiness_sessions.get("details")
        if isinstance(readiness_sessions, dict)
        else None
    )
    complete_session_dates: set[date] = set()
    if isinstance(readiness_details, list):
        for detail in readiness_details:
            if not isinstance(detail, dict) or detail.get("complete") is not True:
                continue
            try:
                session_date = date.fromisoformat(str(detail.get("session_date") or ""))
            except ValueError:
                continue
            if session_date <= last_complete_date:
                complete_session_dates.add(session_date)

    observed_partition_dates = sorted(
        session
        for session in _feature_partition_dates(features_root)
        if session <= last_complete_date and session.weekday() < 5
    )
    signal_sets = {
        SET_CONFIRMED: load_confirmed_signals(features_root),
        SET_PREFILL: load_prefill_signals(features_root),
        SET_GTH_DIP: load_gth_dip_signals(features_root),
        SET_TRADE_READY: load_trade_ready_signals(features_root),
    }
    for set_name, signals in signal_sets.items():
        signal_sets[set_name] = [
            signal
            for signal in signals
            if signal.at < cutoff_at and signal.at.date() <= last_complete_date
            and (signal.expiry or signal.at.date()) in complete_session_dates
        ]
    signal_counts = {name: len(signals) for name, signals in signal_sets.items()}
    intent_coverage = trade_intent_coverage(
        features_root,
        cutoff_at=cutoff_at,
        last_complete_date=last_complete_date,
    )
    intent_coverage["replay_eligible_trade_ready_signals"] = signal_counts[SET_TRADE_READY]
    intent_coverage["scope"] = {
        "kind": "observed_feature_partitions",
        "dates": [session.isoformat() for session in observed_partition_dates],
        "note": "telemetry scope; the executable backtest cohort uses readiness-complete sessions",
    }
    logger.info("signal counts: %s", signal_counts)

    store = QuoteStore(data_root)
    trades: list[Trade] = []
    skips: list[Skip] = []
    try:
        for set_name in SET_ORDER:
            for signal in signal_sets[set_name]:
                signal_trades, signal_skips = evaluate_signal(store, signal)
                trades.extend(signal_trades)
                skips.extend(signal_skips)
    finally:
        store.close()
    logger.info("trades=%d skips=%d", len(trades), len(skips))

    skip_summary: dict[str, int] = {}
    for skip in skips:
        skip_summary[skip.reason] = skip_summary.get(skip.reason, 0) + 1
    logger.info("skip reasons: %s", skip_summary)

    sessions = sorted(session.isoformat() for session in complete_session_dates)
    observed_partitions = [session.isoformat() for session in observed_partition_dates]
    artifact = build_artifact(
        generated_at=generated_at,
        features_root=features_root,
        data_root=data_root,
        sessions=sessions,
        observed_partitions=observed_partitions,
        cutoff_at=cutoff_at,
        as_of=as_of,
        signal_counts=signal_counts,
        intent_coverage=intent_coverage,
        signal_sets=signal_sets,
        trades=trades,
        skips=skips,
        strategy_readiness=strategy_readiness,
    )
    return write_outputs(output_dir, artifact, trades)
