"""Causal executable-quote simulation for 0DTE level signals."""

from __future__ import annotations

from bisect import bisect_left
from datetime import datetime, timedelta
from typing import Sequence

from .odte_level_timing import (
    first_tick_at_or_after as _first_tick_at_or_after,
    option_tick_mid as _tick_mid,
    tick_at_or_before as _tick_at_or_before,
)
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
    RTH_ANALYSIS_START_ET_HHMM,
    RTH_EXIT_CLOCK_ET_HHMM,
    SATURATION_FRACTION,
    SET_GTH_LEVEL_CANDIDATE,
    SET_PREFILL,
    SET_TRADE_READY,
    SPREAD_WIDTHS,
    TIME_STOP_DELAY,
    TRAIL33_ARM_FRACTION,
    TRAILING_ACTIVATION_FRACTION,
    TRAILING_GIVEBACK_FRACTION,
    VARIANT_NAKED,
    VARIANT_SPREAD_WALL,
    OptionTick,
    Profile,
    Signal,
    Skip,
    Trade,
    UnderlierTick,
    _float,
    expiry_close_at,
    next_exit_clock,
    pre_entry_path_reason,
    replay_boundaries,
)


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


def _two_sided(tick: OptionTick | None) -> bool:
    return bool(
        tick is not None
        and tick.bid is not None
        and tick.ask is not None
        and 0 <= tick.bid < tick.ask
    )


def _first_synchronized_spread_entry(
    long_series: Sequence[OptionTick],
    short_series: Sequence[OptionTick],
    *,
    requested_at: datetime,
    expires_at: datetime,
    entry_limit: float,
    width: float,
) -> tuple[datetime, OptionTick, OptionTick] | None:
    """Return the first causal two-leg natural ask available after operator latency."""

    long_times = [tick.at for tick in long_series]
    short_times = [tick.at for tick in short_series]
    event_times = {requested_at}
    event_times.update(tick.at for tick in long_series if requested_at < tick.at < expires_at)
    event_times.update(tick.at for tick in short_series if requested_at < tick.at < expires_at)
    for observed_at in sorted(event_times):
        long_tick = _tick_at_or_before(
            long_series,
            long_times,
            observed_at,
            fallback_first=False,
        )
        short_tick = _tick_at_or_before(
            short_series,
            short_times,
            observed_at,
            fallback_first=False,
        )
        if not _two_sided(long_tick) or not _two_sided(short_tick):
            continue
        assert long_tick is not None and short_tick is not None
        if (
            observed_at - long_tick.at > MAX_ENTRY_QUOTE_AGE
            or observed_at - short_tick.at > MAX_ENTRY_QUOTE_AGE
            or abs(long_tick.at - short_tick.at) > MAX_ENTRY_LEG_SKEW
        ):
            continue
        assert long_tick.ask is not None and short_tick.bid is not None
        debit = long_tick.ask - short_tick.bid
        if 0 < debit <= entry_limit and debit <= width:
            return observed_at, long_tick, short_tick
    return None


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
    long_provider: str | None = None,
    short_provider: str | None = None,
    entry_latency_seconds: int = 0,
) -> Trade | Skip:
    """Simulate one signal/variant/profile against in-memory quote series.

    Exit rules are checked per long-leg tick in order: invalidation,
    target_wall, profit-taking (fixed 1.3x / trailing / sat85 / trail33 per
    profile; clock has none), time_stop, then an end_of_data fallback.
    Stop-style exits pay the bid (long bid, or long bid - short ask for
    spreads); fixed profit targets trigger on mid but exit at the executable
    bid (or long bid minus short ask). GTH signals
    (future:ES underlier) use the profile's GTH time-stop/max-hold overrides,
    or expiry-date 09:45 America/New_York when the profile sets gth_clock_exit.
    The RTH 13:00 profile accepts entries in [09:45, 13:00) New York time and
    uses the first fresh executable bid at/after 13:00 as its clock exit.
    """
    if type(entry_latency_seconds) is not int or entry_latency_seconds < 0:
        raise ValueError("entry_latency_seconds must be a non-negative int")
    dir_sign = 1 if signal.direction == "up" else -1
    inv_level, inv_buffer, target = replay_boundaries(signal, profile, dir_sign)
    requested_entry_at = signal.entry_at + timedelta(seconds=entry_latency_seconds)
    production_entry = signal.set_name == SET_TRADE_READY
    production_spread_entry = signal.set_name == SET_GTH_LEVEL_CANDIDATE
    recorded_entry = production_entry or production_spread_entry
    if production_entry and variant != VARIANT_NAKED:
        return Skip(signal.set_name, profile.name, signal.key, variant, "not_applicable")
    if production_spread_entry and variant != VARIANT_SPREAD_WALL:
        return Skip(signal.set_name, profile.name, signal.key, variant, "not_applicable")
    gth = signal.underlier_instrument == "future:ES"
    expiry_close: datetime | None = None
    exit_clock: datetime | None = None
    if gth or profile.rth_clock_exit:
        if signal.expiry is None:
            return Skip(signal.set_name, profile.name, signal.key, variant, "no_expiry")
        expiry_close = expiry_close_at(signal.expiry)
        if requested_entry_at >= expiry_close:
            return Skip(
                signal.set_name, profile.name, signal.key, variant, "entry_after_expiry_close"
            )
        if profile.rth_clock_exit:
            opened_at = next_exit_clock(
                requested_entry_at,
                signal.expiry,
                hhmm=RTH_ANALYSIS_START_ET_HHMM,
            )
            exit_clock = next_exit_clock(
                requested_entry_at,
                signal.expiry,
                hhmm=RTH_EXIT_CLOCK_ET_HHMM,
            )
            if requested_entry_at < opened_at:
                return Skip(
                    signal.set_name,
                    profile.name,
                    signal.key,
                    variant,
                    "entry_before_rth_window",
                )
            if requested_entry_at >= exit_clock:
                return Skip(
                    signal.set_name,
                    profile.name,
                    signal.key,
                    variant,
                    "entry_after_exit_clock",
                )
        elif profile.gth_clock_exit:
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
    width = spread_width if spread_width is not None else SPREAD_WIDTHS.get(variant)
    short_series = short_series or []
    short_times = [tick.at for tick in short_series]
    entry_tick: OptionTick | None = None
    short_entry_tick: OptionTick | None = None
    entry_long_ask: float | None = None
    entry_short_bid: float | None = None
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
            return Skip(signal.set_name, profile.name, signal.key, variant, "entry_window_expired")
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
        reason = pre_entry_path_reason(
            signal,
            underlier,
            entry_at=boundary_at,
            dir_sign=dir_sign,
            invalidation_level=invalidation,
            invalidation_buffer=0.0,
            target=target,
        )
        if reason is not None:
            return Skip(signal.set_name, profile.name, signal.key, variant, reason)
        if entry_tick is None:
            return Skip(
                signal.set_name, profile.name, signal.key, variant, "entry_limit_not_reached"
            )
        assert entry_tick.ask is not None  # established by the executable-limit scan
        long_entry_px = entry_tick.ask
        entry_long_ask = entry_tick.ask
        entry_source = "lake_ask_at_or_below_recorded_limit"
        entry_at = entry_tick.at
    elif production_spread_entry:
        if (
            recorded_entry_px is None
            or width is None
            or signal.entry_limit is None
            or signal.entry_expires_at is None
            or signal.recorded_time_stop_at is None
            or signal.invalidation_level is None
            or signal.target_level is None
            or signal.entry_provider != "ibkr"
            or requested_entry_at >= signal.entry_expires_at
            or recorded_entry_px > signal.entry_limit
        ):
            return Skip(
                signal.set_name,
                profile.name,
                signal.key,
                variant,
                "recorded_entry_fields_unavailable",
            )
        if entry_latency_seconds == 0:
            decision_sides = signal.decision_leg_sides
            entry_long_ask = decision_sides[1] if decision_sides is not None else None
            entry_short_bid = decision_sides[2] if decision_sides is not None else None
            if (
                entry_long_ask is None
                or entry_short_bid is None
                or not 0 < entry_long_ask - entry_short_bid <= signal.entry_limit
                or not abs((entry_long_ask - entry_short_bid) - recorded_entry_px) <= 1e-6
            ):
                return Skip(
                    signal.set_name,
                    profile.name,
                    signal.key,
                    variant,
                    "recorded_entry_leg_quotes_unavailable",
                )
            entry_at = requested_entry_at
            entry_px = entry_long_ask - entry_short_bid
            entry_source = "recorded_ibkr_long_ask_short_bid"
        else:
            synchronized = _first_synchronized_spread_entry(
                long_series,
                short_series,
                requested_at=requested_entry_at,
                expires_at=signal.entry_expires_at,
                entry_limit=signal.entry_limit,
                width=width,
            )
            if synchronized is None:
                return Skip(
                    signal.set_name,
                    profile.name,
                    signal.key,
                    variant,
                    "entry_limit_not_reached_after_latency",
                )
            entry_at, entry_tick, short_entry_tick = synchronized
            assert entry_tick.ask is not None and short_entry_tick.bid is not None
            entry_long_ask = entry_tick.ask
            entry_short_bid = short_entry_tick.bid
            entry_px = entry_long_ask - entry_short_bid
            entry_source = "lake_ibkr_long_ask_short_bid_after_latency"
            reason = pre_entry_path_reason(
                signal,
                underlier,
                entry_at=entry_at,
                dir_sign=dir_sign,
                invalidation_level=signal.invalidation_level,
                invalidation_buffer=0.0,
                target=signal.target_level,
            )
            if reason is not None:
                return Skip(signal.set_name, profile.name, signal.key, variant, reason)
    elif recorded_entry_px is None:
        entry_tick = _first_tick_at_or_after(long_series, long_times, requested_entry_at)
        if entry_tick is None or entry_tick.at - requested_entry_at > MAX_ENTRY_QUOTE_AGE:
            return Skip(signal.set_name, profile.name, signal.key, variant, "no_quote")
        if entry_tick.ask is None:
            return Skip(signal.set_name, profile.name, signal.key, variant, "no_quote")
        long_entry_px = entry_tick.ask
        entry_long_ask = entry_tick.ask
        entry_source = "lake_ask"
        entry_at = entry_tick.at
    else:
        entry_tick = _first_tick_at_or_after(long_series, long_times, requested_entry_at)
        long_entry_px = recorded_entry_px
        entry_source = "recorded_ask"
        if entry_tick is None:
            return Skip(signal.set_name, profile.name, signal.key, variant, "no_path")
        entry_at = requested_entry_at

    if width is not None and production_spread_entry:
        pass  # exact natural ask was fixed above from its two executable leg sides
    elif width is not None:
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
            entry_long_ask = entry_tick.ask
        elif requested_entry_at - short_entry_tick.at > MAX_ENTRY_LEG_SKEW:
            return Skip(signal.set_name, profile.name, signal.key, variant, "entry_leg_skew")
        entry_long_ask = long_entry_px
        entry_px = long_entry_px - short_entry_tick.bid
        entry_short_bid = short_entry_tick.bid
        if entry_px <= 0 or entry_px > width:
            return Skip(signal.set_name, profile.name, signal.key, variant, "invalid_spread_debit")
    else:
        entry_px = long_entry_px
        entry_long_ask = long_entry_px

    if not recorded_entry:
        reason = pre_entry_path_reason(
            signal,
            underlier,
            entry_at=entry_at,
            dir_sign=dir_sign,
            invalidation_level=inv_level,
            invalidation_buffer=inv_buffer,
            target=target,
        )
        if reason is not None:
            return Skip(signal.set_name, profile.name, signal.key, variant, reason)
    if expiry_close is not None and entry_at >= expiry_close:
        return Skip(signal.set_name, profile.name, signal.key, variant, "entry_after_expiry_close")
    if exit_clock is not None and entry_at >= exit_clock:
        return Skip(signal.set_name, profile.name, signal.key, variant, "entry_after_exit_clock")
    if (
        recorded_entry
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

    underlier_times = [tick.at for tick in underlier]
    if profile.rth_clock_exit:
        assert exit_clock is not None  # established by the RTH clock preflight
        stop_at = exit_clock
        hold_until = stop_at + MAX_MARK_QUOTE_AGE
    elif recorded_entry and signal.recorded_time_stop_at is not None:
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
        elif (
            pos_mid is not None
            and pos_mid >= PROFIT_TARGET_MULTIPLE * entry_px
            and pos_stop is not None
        ):
            exit_px, exit_time, exit_reason = pos_stop, tick.at, "profit_target"
            break

    if exit_px is None:
        if profile.rth_clock_exit and (last_tick is None or last_tick.at < stop_at):
            return Skip(
                signal.set_name,
                profile.name,
                signal.key,
                variant,
                "no_fresh_exit_quote_after_rth_exit",
            )
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

    assert exit_time is not None
    exit_long_tick = _tick_at_or_before(
        long_series,
        long_times,
        exit_time,
        fallback_first=False,
    )
    exit_short_tick = short_mark(exit_time) if width is not None else None
    exit_long_bid = exit_long_tick.bid if exit_long_tick is not None else None
    exit_short_ask = exit_short_tick.ask if exit_short_tick is not None else None
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
        long_provider=long_provider,
        short_provider=short_provider if variant != VARIANT_NAKED else None,
        entry_latency_seconds=entry_latency_seconds,
        executable_sides=(
            round(entry_long_ask, 4) if entry_long_ask is not None else None,
            round(entry_short_bid, 4) if entry_short_bid is not None else None,
            round(exit_long_bid, 4) if exit_long_bid is not None else None,
            round(exit_short_ask, 4) if exit_short_ask is not None else None,
        ),
    )
