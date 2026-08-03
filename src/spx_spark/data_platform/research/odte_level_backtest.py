"""Backtest 0DTE level-alert signals as traded via SPX/SPXW options.

Evaluates confirmed/prefill controls, the current GTH runtime candidate lane,
and the persisted production ``trade_ready`` cohort. Writes trades.csv,
artifact.json and a Chinese report.md.

Relevant data conventions:
- ``index:SPX`` rows from schwab populate ``mid``; ibkr leaves ``mid`` NULL but
  fills ``last``/``effective_price``; ``future:ES`` populates ``mid`` for both
  providers. Underlier price therefore uses ``COALESCE(mid, last, effective_price)``.
- S2 recorded ``prefill_ask`` values precede the production follow-through
  gate. They are never fills; passing events are repriced after the full hold.
- ``trade_ready`` uses its recorded provider, contract, limit and exclusive
  expiry window. It fills only when a contemporaneous ask is at/below the
  recorded limit and never reconstructs entry fields from later data.
- Current GTH events freeze the runtime debit-spread legs, provider, decision
  Ask-Bid and TTL. Legacy dip-reclaim events are excluded from execution replay.
- S1 ``es_equivalent`` records keep ``level``/``levels`` in raw ES coordinates;
  ``spx_level`` is the SPX-coordinate equivalent (level - basis). Everything is
  normalized to SPX coordinates before simulation.
"""

from __future__ import annotations

import logging
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
from .odte_level_opportunities import (
    LATENCY_SENSITIVITY_SECONDS,
    ReplayKey,
    ReplayResult,
    build_opportunity_artifacts,
)
from .odte_level_quotes import QuoteStore, pick_provider
from .odte_level_session_cohorts import (
    readiness_session_cohorts as _readiness_session_cohorts,
    uses_put_session_cohort as _uses_put_session_cohort,
)
from .odte_level_simulation import follow_through_pass, simulate_trade
from .odte_level_timing import (
    in_rth_1300_entry_window as _in_rth_1300_entry_window,
)
from .odte_level_signals import (
    FT_GATE_SECONDS,
    MAX_ENTRY_QUOTE_AGE,
    MAX_HOLD,
    MAX_MARK_QUOTE_AGE,
    MAX_UNDERLIER_QUOTE_AGE,
    PROFILES,
    RTH_EXIT_CLOCK_ET_HHMM,
    SET_CONFIRMED,
    SET_GTH_LEVEL_CANDIDATE,
    SET_ORDER,
    SET_PREFILL,
    SET_TRADE_READY,
    SPREAD_WIDTHS,
    VARIANT_NAKED,
    VARIANT_SPREAD_WALL,
    VARIANTS,
    OptionTick,
    Profile,
    Signal,
    Skip,
    Trade,
    UnderlierTick,
    contract_id_for,
    expiry_close_at,
    load_confirmed_signals,
    load_gth_level_candidate_signals,
    load_prefill_signals,
    load_trade_ready_signals,
    next_exit_clock,
    right_for,
    spread_strikes,
    trade_intent_coverage,
    wall_spread_structure,
)
from .strategy_readiness import build_strategy_readiness

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Trade simulation
# ---------------------------------------------------------------------------


def evaluate_signal(
    store: QuoteStore,
    signal: Signal,
    profiles: Sequence[Profile] = PROFILES,
    *,
    entry_latency_seconds: int = 0,
) -> tuple[list[Trade], list[Skip]]:
    """Resolve strike/provider/legs once, then simulate all profiles x variants.

    Quote series are loaded a single time (shared QuoteStore cache); for GTH
    signals the load window covers the longest profile horizon (gth_360 needs
    entry+370min; clock profiles use expiry-date 09:45 America/New_York, and no
    GTH path may cross the expiry 16:00 ET close). The RTH clock profile loads
    the same exact contracts through 13:00 ET. Profiles may restrict sets
    (set_names), GTH/RTH windows, or spread variants (spread_only).
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
        and (not profile.rth_only or _in_rth_1300_entry_window(signal))
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
                if signal.set_name == SET_GTH_LEVEL_CANDIDATE and variant != VARIANT_SPREAD_WALL:
                    variant_reason = "not_applicable"
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
    rth_clock_profiles = [profile for profile in active_profiles if profile.rth_clock_exit]
    if rth_clock_profiles:
        rth_exit = next_exit_clock(
            t0,
            signal.expiry,
            hhmm=RTH_EXIT_CLOCK_ET_HHMM,
        )
        load_end = max(load_end, rth_exit + MAX_MARK_QUOTE_AGE)
        load_end = min(load_end, expiry_close_at(signal.expiry))
    provider = signal.entry_provider
    if signal.set_name not in {SET_TRADE_READY, SET_GTH_LEVEL_CANDIDATE}:
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
        if signal.set_name in {SET_TRADE_READY, SET_GTH_LEVEL_CANDIDATE}:
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
        signal.set_name == SET_GTH_LEVEL_CANDIDATE
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
        # A vertical is one coherent market observation. Never synthesize a
        # spread from a long leg on one provider and a short leg on another.
        short_provider = provider
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
            if signal.set_name == SET_GTH_LEVEL_CANDIDATE and variant != VARIANT_SPREAD_WALL:
                skips.append(
                    Skip(signal.set_name, profile.name, signal.key, variant, "not_applicable")
                )
                continue
            if variant == VARIANT_SPREAD_WALL and variant not in leg_specs:
                reason = (
                    "no_recorded_production_spread"
                    if signal.set_name == SET_GTH_LEVEL_CANDIDATE
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
                long_provider=provider,
                short_provider=provider if spec else None,
                entry_latency_seconds=entry_latency_seconds,
            )
            (trades if isinstance(result, Trade) else skips).append(result)
    return trades, skips


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def _opportunity_result(
    signal: Signal,
    trades: Sequence[Trade],
    skips: Sequence[Skip],
) -> ReplayResult:
    variant = VARIANT_NAKED if signal.set_name == SET_TRADE_READY else VARIANT_SPREAD_WALL
    profile = PROFILES[0].name
    for row in trades:
        if row.profile == profile and row.variant == variant:
            return row
    for row in skips:
        if row.profile == profile and row.variant == variant:
            return row
    return Skip(signal.set_name, profile, signal.key, variant, "replay_result_unavailable")


_FEATURE_PARTITION_GLOBS = (
    "level_decision_health/date=*",
    "level_decision_audit/date=*",
    "pricing_outcomes/date=*",
    "gth_level_manual_candidates/date=*",
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
    complete_session_dates, put_complete_session_dates = _readiness_session_cohorts(
        strategy_readiness,
        last_complete_date=last_complete_date,
    )

    observed_partition_dates = sorted(
        session
        for session in _feature_partition_dates(features_root)
        if session <= last_complete_date and session.weekday() < 5
    )
    signal_sets = {
        SET_CONFIRMED: load_confirmed_signals(features_root),
        SET_PREFILL: load_prefill_signals(features_root),
        SET_GTH_LEVEL_CANDIDATE: load_gth_level_candidate_signals(features_root),
        SET_TRADE_READY: load_trade_ready_signals(features_root),
    }
    for set_name, signals in signal_sets.items():
        signal_sets[set_name] = [
            signal
            for signal in signals
            if signal.at < cutoff_at
            and signal.at.date() <= last_complete_date
            and (signal.expiry or signal.at.date())
            in (
                put_complete_session_dates
                if _uses_put_session_cohort(signal)
                else complete_session_dates
            )
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
        "note": (
            "telemetry scope; executable backtest signals use their readiness-complete "
            "GTH/global or RTH Put session cohort"
        ),
    }
    logger.info("signal counts: %s", signal_counts)

    store = QuoteStore(data_root)
    trades: list[Trade] = []
    skips: list[Skip] = []
    opportunity_results: dict[ReplayKey, ReplayResult] = {}
    try:
        for set_name in SET_ORDER:
            for signal in signal_sets[set_name]:
                signal_trades, signal_skips = evaluate_signal(store, signal)
                trades.extend(signal_trades)
                skips.extend(signal_skips)
                if signal.set_name not in {SET_TRADE_READY, SET_GTH_LEVEL_CANDIDATE}:
                    continue
                opportunity_results[(signal.set_name, signal.key, 0)] = _opportunity_result(
                    signal,
                    signal_trades,
                    signal_skips,
                )
                for latency_seconds in LATENCY_SENSITIVITY_SECONDS[1:]:
                    delayed_trades, delayed_skips = evaluate_signal(
                        store,
                        signal,
                        profiles=(PROFILES[0],),
                        entry_latency_seconds=latency_seconds,
                    )
                    opportunity_results[(signal.set_name, signal.key, latency_seconds)] = (
                        _opportunity_result(signal, delayed_trades, delayed_skips)
                    )
    finally:
        store.close()
    logger.info("trades=%d skips=%d", len(trades), len(skips))

    skip_summary: dict[str, int] = {}
    for skip in skips:
        skip_summary[skip.reason] = skip_summary.get(skip.reason, 0) + 1
    logger.info("skip reasons: %s", skip_summary)

    sessions = sorted(session.isoformat() for session in complete_session_dates)
    put_sessions = sorted(session.isoformat() for session in put_complete_session_dates)
    observed_partitions = [session.isoformat() for session in observed_partition_dates]
    opportunity_signals = [
        signal
        for set_name in (SET_TRADE_READY, SET_GTH_LEVEL_CANDIDATE)
        for signal in signal_sets[set_name]
    ]
    opportunities = build_opportunity_artifacts(
        features_root,
        opportunity_signals,
        opportunity_results,
        cutoff_at=cutoff_at,
    )
    artifact = build_artifact(
        generated_at=generated_at,
        features_root=features_root,
        data_root=data_root,
        sessions=sessions,
        put_sessions=put_sessions,
        observed_partitions=observed_partitions,
        cutoff_at=cutoff_at,
        as_of=as_of,
        signal_counts=signal_counts,
        intent_coverage=intent_coverage,
        signal_sets=signal_sets,
        trades=trades,
        skips=skips,
        strategy_readiness=strategy_readiness,
        opportunities=opportunities,
    )
    return write_outputs(output_dir, artifact, trades)
