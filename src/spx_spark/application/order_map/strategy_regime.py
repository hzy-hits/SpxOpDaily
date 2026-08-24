"""Deterministic regime dimensions for strategy selection."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping


from spx_spark.analytics.options.strategy_payoff import (
    CLOSE_CONVERGENCE_BUTTERFLY_MANAGEMENT_POLICY,
    DEFAULT_MANAGEMENT_POLICY,
    PIN_BUTTERFLY_MANAGEMENT_POLICY,
    ManagementPolicy,
)

# Forward mark horizons for strategy_outcomes (v3). Frozen code constant.
MARK_HORIZONS_MINUTES: tuple[int, ...] = (1, 2, 3, 4, 5, 7, 10, 15, 20)

__all__ = (
    "CLOSE_CONVERGENCE_BUTTERFLY_MANAGEMENT_POLICY",
    "DEFAULT_MANAGEMENT_POLICY",
    "DEFAULT_STRATEGY_POLICY",
    "MARK_HORIZONS_MINUTES",
    "ManagementPolicy",
    "PIN_BUTTERFLY_MANAGEMENT_POLICY",
    "StrategyPolicy",
    "assess_regime",
    "butterfly_entry_clock_open",
    "butterfly_max_entry_minutes",
    "five_wide_look_mass_ready",
    "hmm_owns_trend_direction",
    "look_mass_ready",
    "pin_blocks_directional_spreads",
    "pin_look_trade_widths",
    "pin_look_window",
    "pin_stable_center",
    "pin_stable_next_step_text",
    "pin_stable_watch_phase",
    "pin_trade_center",
    "pin_watch_center",
)


@dataclass(frozen=True, slots=True)
class StrategyPolicy:
    policy_version: str = "strategy_policy.bootstrap.v48"
    # v47: the RTH iron-condor minimum conservative credit/wing fraction is
    # raised from 15% to 20%. The held-out 2026-08-03--21 replay retained 9
    # trades across 12 opportunities, with 8 wins and +$664.96 after the
    # existing fee/slippage stress. All other v46 geometry, management,
    # manual-only authority, and GTH map-only boundaries remain unchanged.
    # v46: the user-authorized RTH iron-condor lane is one 20-delta,
    # fixed-10-wide manual candidate per session between 10:00 and 11:30 ET.
    # It requires exact four-leg BBO age <=15s/skew <=2s, 15%-55% credit,
    # spot inside shorts, and <=$1,000 defined risk. Management is buy back at
    # 0.5x entry credit, stop at 3x liability (200% loss on credit), otherwise
    # close at 15:45 ET. GTH remains map-only and ordering remains disabled.
    # v45: candidate-specific entry-frozen ATM / put-call skew / put-call
    # curvature bumps replace the global d3/d4 direction prior. Surface may
    # subtract at most 0.05 from structure rank; it cannot add direction,
    # bypass a hard gate, authorize an iron condor, or enable ordering.
    # v44: the user-authorized 15:00 ET physical close-convergence lane may
    # enumerate one manual-only Butterfly from the causal online-pool modal
    # center. It compares 10/15/20-point C/P tents on 51 settlement quantiles,
    # exact BBO and the frozen risk objective, then holds to 15:55 ET. It does
    # not inherit STABLE_PIN, dealer, GEX, wall, q-mode, or direction rules.
    # v43: a STABLE_PIN body is observation-only until the same selected
    # center survives at least three decision snapshots and ten minutes. A
    # small challenger cannot replace the previous center unless its score
    # leads by 0.05; only the confirmed top center reaches candidate
    # enumeration. The 11:00–13:00 ladder starts at 10-wide and rank no
    # longer hard-prefers the tightest tent. An accepted RTH pin card keeps
    # its exact center/width/right for the existing 15-minute winner window.
    # v42: blind GTH width/delta scans remain in the rejection funnel but no
    # longer authorize Trade Ready. Recent live cards showed rapid direction
    # flips without forward edge evidence. GTH manual debit now requires
    # confirmed level/dip-reclaim evidence; existing RTH authorities remain.
    # GTH directional hysteresis is 30 minutes, same-setup cards cool down for
    # 15 minutes, and each direction is capped at two accepted cards/session.
    # GTH debit above 45% of width fails closed.
    # v41: user-authorized RTH wall competing-risk hazard may produce a
    # forward-unvalidated manual debit candidate. The four-feature frozen
    # model supplies direction only; exact BBO, structure quality, PIN,
    # geometry, debit, and conservative target-payoff EV remain hard gates.
    # v40: explicit user authorization promotes the RTH causal 15-second
    # pre-average pullback detector to a manual-only 60-delta/15-point debit
    # lane. It stays marked forward-unvalidated, uses Schwab exact BBO <=5s,
    # and does not inherit HMM, GEX direction, or legacy entry-quality gates.
    # v39: POST_EVENT_DISCOVERY no longer blocks ES_VOLUME_MOMENTUM after
    # the RTH open. Debit management drops the v1 20-minute time stop;
    # verticals keep the 50% premium stop, trail, and 15:45 ET hard close.
    # Opposite cash HMM TREND, add-needs-new-impulse, and flip-needs-HMM
    # TREND stay. User named the two gates to delete.
    # v38: RTH ES_VOLUME_MOMENTUM is a human debit again. Failed-break,
    # trend-pullback, and breakout stay funnel-only.
    # v37: GTH width/delta debit prints on TREND or TRANSITION when the
    # ES path direction matches. 2026-08-18 GTH dumped in TRANSITION DOWN
    # (efficiency 0.33, below trend_efficiency 0.45); the 7730/7725 put
    # outscored the iron condor but died on TREND-only plus 0.45 debit
    # cap (0.52). GTH debit cap is 0.55. UNCERTAIN / opposite side stay
    # closed.
    # v36: iron condor stays on the desk map and does not print a human
    # card. Geometry-ready 5–20Δ 10-wide condors were winning every GTH
    # cycle after unevidenced debit was gated, and the winner overlay
    # reused the 20-minute debit management policy. Human debit is still
    # GTH direction-aligned width/delta plus confirmed level / dip-reclaim.
    # v35: GTH can still print a human debit. TREND-aligned width/delta
    # scans and confirmed GTH level / dip-reclaim verticals remain
    # manual candidates. Desk Map copy stays 不做; winners still go
    # through trade_ready. RTH ES_VOLUME_MOMENTUM and leftover RTH
    # directionals stay blocked by unevidenced_debit_not_human_authorized.
    # v34: unevidenced debit verticals no longer authorize a human card.
    # GTH width/delta scans, RTH ES_VOLUME_MOMENTUM, failed-break, trend
    # pullback, breakout, and GTH level-path verticals stay enumerated for
    # the desk map and funnel. Human debit is EVENT_SETTLEMENT_THRESHOLD
    # only. RTH pin TRADE butterflies stay. Replay on persisted cards found
    # no edge in width-scan / volume-momentum debit under v1 management.
    # v33: same-direction RTH adds need a new impulse (cash HMM TREND the
    # same way and |5m|/ATR5m at least es_momentum_add_min_return_5m_atr).
    # v33 also blocked ES_VOLUME_MOMENTUM in POST_EVENT_DISCOVERY after
    # the RTH open grace; v39 removes that gate. entry_allowed stays
    # true in post_event so event-settlement is not collateral damage.
    # v32: ES_VOLUME_MOMENTUM stays the only RTH directional setup. The first
    # card does not wait for TREND or a pullback. Cash HMM TREND opposite
    # blocks a first print. A session that already printed the opposite RTH
    # human card may flip only when cash HMM owns TREND the new way.
    # Rank/delivery also stick the RTH winner for rth_winner_stick_seconds.
    # v31: RTH human directional cards come from ES_VOLUME_MOMENTUM only
    # (elevated ES pace + 1m/5m momentum). TREND_PULLBACK / FAILED_BREAK /
    # BREAKOUT_ACCEPTANCE stay as audit facts and GTH labels; they no longer
    # authorize an RTH card. Short-cycle Late Chase ignores VWAP+15m impulse
    # and uses 5m ATR exhaustion plus 50% progress. Pin LOOK/TRADE still
    # vetoes the new setup. Event-settlement and GTH scans stay.
    # v30: LOOK or TRADE pin vetoes RTH directional debit verticals
    # (failed-break, trend-pullback, breakout). Event-settlement and GTH
    # scans stay. PIN_MIGRATING / UNCERTAIN do not block spreads.
    # v29: 11:00–13:00 TRADE does not bind fly width. The look ladder is
    # 10/15/20/50; a width is enumerated when local mass is already piled
    # inside [K−W, K+W]. Rank prefers any pin fly over a vertical, then the
    # tightest passing tent. Late RTH still uses 5/10/15/20 on 12 min/point.
    # v28: 11:00–13:00 TRADE evaluates 10-wide flies by default. 5-wide in
    # that window only when local 5pt mass is concentrated within ±5 of the
    # body. The look clock opens 5 and 10; 15/20 stay on 12 min/point.
    # v27: PIN splits LOOK vs TRADE. LOOK (11:00–13:00, 1 excursion, not
    # migrating) is observation only and never authorizes a butterfly card.
    # TRADE remains PIN_STABLE with the existing hard stack (2 excursions).
    # v26: PIN alignment uses the local 5pt mass peak, not the global density
    # argmax. The previous peak sticks when it is still a top-2 local mass
    # center and within 5 points of the current local peak.
    # v25: PIN_STABLE hold does not drop on a 2→1 excursion flicker, and a
    # far-OTM Q-mode spike is replaced by the local 5pt mass peak.
    # v24: PIN_STABLE may be assessed from 11:00 ET (300 minutes to close),
    # matching the 5-wide look window. The old 12:30 / 210-minute floor is gone.
    # v23: 5-wide PIN_STABLE has two clocks. 11:00–13:00 ET is the look
    # window (12:38 must not be labeled too-early). 14:50 ET slack (≤70)
    # remains the late pin window. 14:30 leftover of 90 minutes stays closed.
    # 10-wide and wider keep 12 min/point. No dwell/hold gate: 2026-08-14
    # PIN_STABLE flickered as single-cycle hits.
    # v22: FAILED_BREAK_RECLAIM windows close at 50% trigger→target progress
    # (session-episode and entry quality). TREND_PULLBACK Late Chase stays 60%.
    # PIN_STABLE 5-wide flies get 10 minutes of clock slack so 14:50–15:00 ET
    # is not a false early veto; 10-wide and wider keep 12 min/point.
    # v21: Butterflies are RTH-only (STABLE_PIN). GTH ATM flies are not
    # enumerated or human-authoritative; night path is too hard to pin.
    # v20: GTH winner stick and delivery direction lock only count cards the
    # outbox accepted. Selected-but-never-pushed cycles must not lock the desk.
    # v19: GTH Call/Put debit verticals require TREND aligned with the
    # candidate direction. Cheapness ranking stays, but TRANSITION/UNCERTAIN
    # or the opposite side is a hard-gate zero. ATM butterflies may still pass.
    # v18: GTH keeps one human direction at a time and sticks the winner for
    # gth_winner_stick_seconds. Rank may not flip UP/DOWN/NEUTRAL, and delivery
    # may not print the opposite side, until that hysteresis expires.
    # v17: one perception contract, session-selected owners. Cash HMM may own
    # RTH path_direction (SPX). Globex HMM never owns GTH path_direction; it
    # only publishes cross_state (NQ/YM/RTY vs ES). GTH direction is ES path.
    # HMM still cannot skip hard gates or order.
    # v16: session-selected index HMM owns path_state when the cash (RTH) or
    # globex-futures (GTH) basket is ready. ES path remains the fallback and
    # a VWAP direction check. HMM still cannot skip hard gates or order.
    # v15: RTH pin butterflies no longer require OI-GEX as a capability gate.
    # STABLE_PIN management holds to 15:45 ET with trail; debit verticals
    # used the v1 20-minute time stop until v39.
    # v14: RTH pin butterflies must keep spot inside the tent, wait until
    # minutes_to_close <= 12 per width point (5-wide from 15:00 ET), and not
    # pin a body while a wall still sits inside 1.5x remaining EM outside the
    # wings. Card text prints the three legs. PIN_STABLE itself is unchanged
    # so iron-condor 12:30 timing does not move.
    # v13: RTH confirmation stays open for two extra 5m bars so a human card
    # can still print; session-episode reclaim expires at the same 60%
    # progress cap as debit chase; flood caps are per session_mode so GTH
    # scans cannot silence RTH.
    # v12: "20Δ 以下" means at-or-below 20, never the richer nearest strike.
    # GTH debit longs use the same 5–20Δ ladder, not 25Δ.
    # v11: short-leg band is 5–20Δ (naked short delta, not 25). GTH iron
    # condors are path-forwarded to the 12:00–13:00 ET clearing window.
    # v10: sell 5–25Δ short legs with a 10-point defined-risk wing; do not pair
    # 25Δ shorts with 5Δ longs. GTH debit longs must sit inside remaining EM.
    # v9: GTH desk map is a live structure scan, not an empty health heartbeat.
    # Always recompute the iron condor from 1-minute quotes. Widen the
    # Call/Put/butterfly scan around spot±5 and 5–20Δ anchors. Winners still
    # push only on trade_ready; unpassed debit spreads are not 可看.
    # v8: GTH enumerates 5-50pt Call/Put debit verticals and butterflies from
    # quotes no older than 60s, then pushes only rank winners on trade_ready.
    # v7: GTH human cards authorize only NEUTRAL session-advance; dip-reclaim
    # requires an aged bullish regime. Continuation m1 stays observe-only.
    trend_score: float = 6.0
    trend_efficiency: float = 0.45
    trend_max_vwap_crosses: float = 2.0
    trend_min_breadth: float = 0.55
    balanced_max_score: float = 3.0
    balanced_max_efficiency: float = 0.30
    balanced_min_vwap_crosses: float = 2.0
    quote_max_age_seconds: float = 15.0
    quote_max_skew_seconds: float = 2.0
    gth_quote_max_age_seconds: float = 60.0
    gth_quote_max_skew_seconds: float = 60.0
    gth_widths: tuple[float, ...] = (5.0, 10.0, 15.0, 20.0, 30.0, 40.0, 50.0)
    gth_long_offsets: tuple[float, ...] = (-5.0, 0.0, 5.0)
    gth_delta_targets: tuple[float, ...] = (0.20, 0.15, 0.10, 0.05)
    iron_condor_short_deltas: tuple[float, ...] = (0.20, 0.15, 0.10, 0.05)
    iron_condor_wing_width: float = 10.0
    surface_bump_vol_points: float = 1.0
    surface_risk_modifier_cap: float = 0.05
    opportunity_ttl_seconds: float = 300.0
    min_target_room_ratio: float = 1.5
    failed_break_min_target_room_ratio: float = 1.8
    max_debit_fraction: float = 0.45
    gth_max_debit_fraction: float = 0.45
    gth_max_risk_usd: float = 1000.0
    failed_break_max_debit_fraction: float = 0.40
    min_stop_atr: float = 0.25
    max_stop_atr: float = 1.0
    late_chase_distance_atr: float = 1.0
    late_chase_impulse_atr: float = 1.0
    es_momentum_min_return_1m: float = 0.35
    es_momentum_min_return_5m: float = 1.0
    es_momentum_max_return_5m_atr: float = 1.50
    es_momentum_max_progress: float = 0.50
    es_momentum_add_min_return_5m_atr: float = 0.50
    wall_hazard_min_side_probability: float = 0.17
    wall_hazard_min_execution_ev_points: float = 0.0
    pin_thresholds: tuple[float, ...] = (0.25, 2.5, 5.0, 5.0, 8.0, 0.35, 0.55)
    pin_stable_max_minutes_to_close: float = 300.0
    pin_stable_enter_min_excursions: int = 2
    pin_stable_hold_min_excursions: int = 1
    pin_look_min_excursions: int = 1
    butterfly_look_clock_widths: tuple[float, ...] = (10.0, 15.0, 20.0, 50.0)
    pin_look_min_mass_fraction: float = 0.50
    pin_q_mode_hold_max_distance_points: float = 5.0
    pin_center_hold_max_distance_points: float = 10.0
    pin_center_switch_min_score_margin: float = 0.05
    pin_center_min_confirmation_snapshots: int = 3
    pin_center_min_dwell_seconds: float = 600.0
    pin_body_max_center_distance_points: float = 5.0
    pin_body_max_spot_distance_points: float = 15.0
    hmm_trend_min_probability: float = 0.55
    hmm_balanced_min_probability: float = 0.50
    hmm_max_age_seconds: float = 90.0
    gth_trend_min_abs_return_points: float = 1.0
    butterfly_max_debit_fraction: float = 0.35
    butterfly_max_risk_usd: float = 1000.0
    close_convergence_widths: tuple[float, ...] = (10.0, 15.0, 20.0)
    close_convergence_max_debit_fraction: float = 0.45
    close_convergence_min_training_sessions: int = 15
    butterfly_minutes_per_width_point: float = 12.0
    butterfly_five_wide_early_slack_minutes: float = 10.0
    # 11:00–13:00 ET look window, expressed as minutes remaining to 16:00.
    butterfly_five_wide_look_max_minutes: float = 300.0
    butterfly_five_wide_look_min_minutes: float = 180.0
    butterfly_unresolved_wall_em_multiple: float = 1.5
    # v5: debit vertical short strike may not pass the target, and width may
    # not exceed remaining 0DTE expected move. Missing EM fails closed.
    # V3-3a flood control (activated with policy_version bump to bootstrap.v2).
    candidate_cooldown_seconds: float = 900.0
    max_cards_per_direction_per_session: int = 2
    # GTH hysteresis from the start of the current direction streak, not a
    # sliding window: reprinting the same winner must not refresh the lock.
    gth_winner_stick_seconds: float = 1800.0
    # RTH same-direction hold after an accepted human card. Flip after this
    # window still needs cash HMM TREND the new way (see v32).
    rth_winner_stick_seconds: float = 900.0
    # Confirmation bar plus this many subsequent 5m bars remain ENTRY_WINDOW_OPEN.
    rth_setup_hold_bars: int = 2
    max_trigger_target_progress: float = 0.60
    failed_break_max_trigger_target_progress: float = 0.50

    def entry_quality_kwargs(self) -> dict[str, float]:
        names = (
            "min_target_room_ratio", "failed_break_min_target_room_ratio",
            "max_debit_fraction", "failed_break_max_debit_fraction",
            "min_stop_atr", "max_stop_atr", "late_chase_distance_atr",
            "late_chase_impulse_atr",
            "failed_break_max_trigger_target_progress",
            "max_trigger_target_progress",
            "es_momentum_max_progress",
        )
        return {name: getattr(self, name) for name in names}


DEFAULT_STRATEGY_POLICY = StrategyPolicy()


def butterfly_max_entry_minutes(
    width: float | None, policy: StrategyPolicy = DEFAULT_STRATEGY_POLICY
) -> float | None:
    """Late-window remaining minutes that may still authorize a pin butterfly.

    5-wide adds ``butterfly_five_wide_early_slack_minutes`` (70 at the
    default 12 min/point). Wider tents stay on the raw 12 min/point clock.
    The 11:00–13:00 look window is a separate width-ladder opening, not this cap.
    """

    if width is None or width <= 0:
        return None
    minutes = float(width) * policy.butterfly_minutes_per_width_point
    if width == 5.0:
        minutes += policy.butterfly_five_wide_early_slack_minutes
    return minutes


def butterfly_entry_clock_open(
    width: float | None,
    minutes_to_close: float | None,
    policy: StrategyPolicy = DEFAULT_STRATEGY_POLICY,
) -> bool:
    """True when the pin-fly clock allows this width to be ranked.

    Look-ladder widths (10/15/20/50): 11:00–13:00 ET, or their late clocks.
    A leftover of 90 minutes (about 14:30 ET) stays closed for 5-wide.
    """

    if minutes_to_close is None or width is None or width <= 0:
        return False
    late = butterfly_max_entry_minutes(width, policy)
    if late is not None and minutes_to_close <= late:
        return True
    if width not in policy.butterfly_look_clock_widths:
        return False
    return pin_look_window(minutes_to_close, policy)


def pin_look_window(
    minutes_to_close: float | None,
    policy: StrategyPolicy = DEFAULT_STRATEGY_POLICY,
) -> bool:
    """True during the 11:00–13:00 ET look window."""

    return (
        minutes_to_close is not None
        and policy.butterfly_five_wide_look_min_minutes
        <= float(minutes_to_close)
        <= policy.butterfly_five_wide_look_max_minutes
    )


def pin_look_trade_widths(
    minutes_to_close: float | None,
    center: float | None,
    mass: Mapping[str, Any],
    policy: StrategyPolicy = DEFAULT_STRATEGY_POLICY,
) -> tuple[float, ...]:
    """Widths enumerated for a STABLE_PIN body.

    Look window follows the mass box on the 10/15/20/50 ladder. Late RTH
    keeps the 5/10/15/20 scan; 50 is look-window only.
    """

    if not pin_look_window(minutes_to_close, policy):
        return (5.0, 10.0, 15.0, 20.0)
    if center is None:
        return ()
    return tuple(
        width
        for width in policy.butterfly_look_clock_widths
        if look_mass_ready(mass, center, width, policy)
    )


def look_mass_ready(
    mass: Mapping[str, Any],
    center: float,
    width: float,
    policy: StrategyPolicy = DEFAULT_STRATEGY_POLICY,
) -> bool:
    """True when local mass is piled inside [K−W, K+W]."""

    if width <= 0:
        return False
    total = 0.0
    near = 0.0
    for key, value in mass.items():
        weight = _number(value)
        if weight is None:
            continue
        try:
            strike = float(key)
        except (TypeError, ValueError):
            continue
        total += weight
        if abs(strike - center) <= width:
            near += weight
    return total > 0 and near / total >= policy.pin_look_min_mass_fraction


def five_wide_look_mass_ready(
    mass: Mapping[str, Any],
    center: float,
    policy: StrategyPolicy = DEFAULT_STRATEGY_POLICY,
) -> bool:
    """True when local 5-point mass is piled inside [K−5, K+5]."""

    return look_mass_ready(mass, center, 5.0, policy)


def pin_stable_center(regime: Mapping[str, Any] | None) -> float | None:
    """Best PIN_STABLE body, or None when the terminal state is not stable."""

    payload = _map(regime)
    if payload.get("terminal_state") != "PIN_STABLE":
        return None
    return _pin_top_center(payload)


def pin_trade_center(regime: Mapping[str, Any] | None) -> float | None:
    """Confirmed PIN_STABLE body that may enter candidate enumeration."""

    payload = _map(regime)
    pin = _map(payload.get("pin"))
    if (
        payload.get("terminal_state") != "PIN_STABLE"
        or pin.get("center_confirmation_ready") is not True
    ):
        return None
    return _pin_top_center(payload)


def pin_blocks_directional_spreads(regime: Mapping[str, Any] | None) -> bool:
    """True when a forming or stable pin forbids RTH directional debit cards."""

    payload = _map(regime)
    if payload.get("terminal_state") == "PIN_STABLE":
        return True
    return str(_map(payload.get("pin")).get("grade") or "") == "look"


def pin_watch_center(regime: Mapping[str, Any] | None) -> float | None:
    """Observation body for LOOK or TRADE pin. Never a trade authorization."""

    payload = _map(regime)
    grade = str(_map(payload.get("pin")).get("grade") or "")
    if payload.get("terminal_state") != "PIN_STABLE" and grade != "look":
        return None
    return _pin_top_center(payload)


def _pin_top_center(regime: Mapping[str, Any]) -> float | None:
    ranked = _map(regime.get("pin")).get("top_centers") or ()
    if not isinstance(ranked, (list, tuple)) or not ranked:
        return None
    return _number(_map(ranked[0]).get("center"))


def pin_stable_watch_phase(
    minutes_to_close: float | None,
    policy: StrategyPolicy = DEFAULT_STRATEGY_POLICY,
) -> str:
    """Look-window latch, late clock-open, or the 13:00–14:50 wait gap."""

    late = butterfly_max_entry_minutes(5.0, policy)
    if late is not None and minutes_to_close is not None and minutes_to_close <= late:
        return "clock_open"
    if minutes_to_close is not None and (
        policy.butterfly_five_wide_look_min_minutes
        <= minutes_to_close
        <= policy.butterfly_five_wide_look_max_minutes
    ):
        return "look"
    return "wait"


def pin_stable_next_step_text(
    minutes_to_close: float | None,
    policy: StrategyPolicy = DEFAULT_STRATEGY_POLICY,
) -> str:
    phase = pin_stable_watch_phase(minutes_to_close, policy)
    if phase == "look":
        return "11–13 仅评已确认中轴的 10–50 点蝶；提交前刷新三腿报价"
    if phase == "clock_open":
        return "5 点蝶尾盘时钟已开，等待精确三腿报价与赔率"
    late = butterfly_max_entry_minutes(5.0, policy)
    if late is None:
        return "午盘看蝶窗已过，等待 5 点蝶尾盘时钟"
    return f"午盘看蝶窗已过；5 点限价等距收盘 ≤{late:g} 分钟（约 14:50 ET）"


HMM_STATE_DIRECTION = {
    "state_00": "DOWN",
    "state_01": None,
    "state_02": "UP",
}


def hmm_owns_trend_direction(regime: Mapping[str, Any]) -> str | None:
    """Return UP/DOWN when cash HMM owns a TREND path; else None."""

    hmm = _map(regime.get("hmm"))
    if hmm.get("owns_path") is not True:
        return None
    if str(regime.get("path_state") or "") != "TREND":
        return None
    direction = str(regime.get("path_direction") or "").upper()
    return direction if direction in {"UP", "DOWN"} else None


def assess_regime(
    facts: Mapping[str, Any], policy: StrategyPolicy = DEFAULT_STRATEGY_POLICY
) -> dict[str, Any]:
    path, event = _map(facts.get("path")), _map(facts.get("event"))
    score, efficiency = _number(path.get("direction_score")), _number(path.get("efficiency_ratio_30m"))
    crosses, breadth = _number(path.get("vwap_crosses_30m")), _number(path.get("breadth_above_vwap"))
    slope, price_side = _number(path.get("vwap_slope")), str(path.get("price_vs_vwap") or "").lower()
    if not price_side:
        distance = _number(path.get("distance_to_vwap_points"))
        price_side = "above" if distance is not None and distance > 0 else "below" if distance is not None and distance < 0 else ""
    direction = "UP" if score is not None and score > 0 else "DOWN" if score is not None and score < 0 else None
    inputs = (score, efficiency, crosses, breadth, slope)
    contradictions: list[str] = []
    cross = _map(facts.get("cross_index"))
    source = str(cross.get("source") or "")
    session_mode = str(_map(facts.get("session")).get("mode") or "")
    if source == "globex_index":
        distance = _number(path.get("distance_to_vwap_points"))
        if distance is not None:
            price_side = (
                "above" if distance > 0 else "below" if distance < 0 else ""
            )
    hmm_cross = _hmm_cross_map(facts, policy)
    if hmm_cross is None:
        cross_state, cross_direction = None, None
        hmm_payload = {**_hmm_unused_payload(facts), "owns_path": False}
    else:
        cross_state, cross_direction, _cross_reasons, hmm_payload = hmm_cross
        hmm_payload = {**hmm_payload, "owns_path": False}
    hmm_owns_path = hmm_cross is not None and source == "cash_index"
    if hmm_owns_path:
        state, direction, reasons, _ = hmm_cross
        hmm_payload = {**hmm_payload, "owns_path": True}
        if (
            state == "TREND"
            and direction in {"UP", "DOWN"}
            and price_side
            and ("above" if direction == "UP" else "below") not in price_side
        ):
            state = "TRANSITION"
            contradictions.append("price_vwap_direction_conflict")
            reasons = [*reasons, "hmm_price_vwap_contradiction"]
        confidence = round(float(hmm_payload["max_state_probability"]), 2)
    else:
        if hmm_cross is not None:
            hmm_payload = {**hmm_payload, "reason": "hmm_cross_state_only_not_path"}
        state, direction, reasons, contradictions, confidence = _coordinate_path(
            facts,
            policy,
            inputs=inputs,
            price_side=price_side,
            direction=direction,
            use_es_path=source == "globex_index" or session_mode == "gth",
        )
    event_state = {
        "pre_event": "SCHEDULED_EVENT_RISK", "post_event": "POST_EVENT_DISCOVERY",
        "normal": "NORMAL",
    }.get(str(event.get("state") or "unavailable"), "UNCERTAIN")
    pin = _pin_assessment(facts, policy)
    coordinate = {
        "cash_index": "index:SPX",
        "globex_index": "future:ES",
    }.get(source) or cross.get("anchor")
    return {
        "schema_version": "regime_assessment.v1", "policy_version": policy.policy_version,
        "path_state": state, "path_direction": direction, "terminal_state": pin["terminal_state"],
        "event_state": event_state, "entry_state": "INSUFFICIENT_DATA",
        "cross_state": cross_state, "cross_direction": cross_direction,
        "coordinate": coordinate,
        "confidence": confidence,
        "reasons": reasons, "contradictions": contradictions, "pin": pin,
        "hmm": hmm_payload,
    }


def _coordinate_path(
    facts: Mapping[str, Any],
    policy: StrategyPolicy,
    *,
    inputs: tuple[Any, ...],
    price_side: str,
    direction: str | None,
    use_es_path: bool,
) -> tuple[str, str | None, list[str], list[str], float]:
    if use_es_path:
        return _es_coordinate_path(_map(facts.get("path")), policy, price_side=price_side)
    score, efficiency, crosses, breadth, slope = inputs
    contradictions: list[str] = []
    capabilities = _map(facts.get("capabilities"))
    path_capability = _map(capabilities.get("path"))
    path_capability_ready = (
        path_capability.get("ready") is True
        if path_capability
        else _map(facts.get("quality")).get("status") == "ready"
    )
    confidence = round(sum(value is not None for value in inputs) / 5, 2)
    if not path_capability_ready:
        return "UNCERTAIN", direction, ["strategy_facts_degraded"], contradictions, confidence
    if None in inputs:
        return "UNCERTAIN", direction, ["path_inputs_unavailable"], contradictions, confidence
    trend = bool(
        abs(float(score)) >= policy.trend_score
        and float(efficiency) >= policy.trend_efficiency
        and float(crosses) <= policy.trend_max_vwap_crosses
        and ((float(score) > 0 and float(breadth) >= policy.trend_min_breadth and float(slope) > 0)
             or (float(score) < 0 and float(breadth) <= 1 - policy.trend_min_breadth and float(slope) < 0))
    )
    if trend and price_side and ("above" if float(score) > 0 else "below") not in price_side:
        trend = False
        contradictions.append("price_vwap_direction_conflict")
    balanced = bool(
        abs(score) <= policy.balanced_max_score
        and efficiency < policy.balanced_max_efficiency
        and crosses >= policy.balanced_min_vwap_crosses
    )
    if trend:
        return "TREND", direction, ["direction_score_confirmed", "path_efficiency_confirmed"], contradictions, 1.0
    if balanced:
        return "BALANCED", None, ["low_path_efficiency", "multiple_vwap_crosses"], contradictions, confidence
    return "TRANSITION", direction, ["path_inputs_not_aligned"], contradictions, confidence


def _es_coordinate_path(
    path: Mapping[str, Any],
    policy: StrategyPolicy,
    *,
    price_side: str,
) -> tuple[str, str | None, list[str], list[str], float]:
    ret5 = _number(path.get("return_5m_points"))
    ret15 = _first_number(path.get("impulse_15m_points"), path.get("return_15m_points"))
    ret1 = _number(path.get("return_1m_points"))
    returns = [value for value in (ret5, ret15, ret1) if value is not None]
    contradictions: list[str] = []
    if not returns:
        return "UNCERTAIN", None, ["es_path_returns_unavailable"], contradictions, 0.0
    threshold = policy.gth_trend_min_abs_return_points
    signed = next((value for value in returns if abs(value) >= threshold), returns[0])
    direction = "UP" if signed > 0 else "DOWN" if signed < 0 else None
    if direction is None:
        return "TRANSITION", None, ["es_path_flat"], contradictions, 0.4
    vwap_conflict = bool(
        price_side and ("above" if direction == "UP" else "below") not in price_side
    )
    if vwap_conflict:
        contradictions.append("price_vwap_direction_conflict")
    efficiency = _number(path.get("efficiency_ratio_30m"))
    trend = bool(
        efficiency is not None
        and float(efficiency) >= policy.trend_efficiency
        and abs(float(signed)) >= threshold
        and not vwap_conflict
    )
    if trend:
        return "TREND", direction, ["es_path_return_confirmed", "path_efficiency_confirmed"], contradictions, 0.7
    if vwap_conflict:
        return "TRANSITION", direction, ["es_price_vwap_contradiction"], contradictions, 0.5
    return "TRANSITION", direction, ["es_path_not_aligned"], contradictions, 0.5


def _hmm_cross_map(
    facts: Mapping[str, Any], policy: StrategyPolicy
) -> tuple[str, str | None, list[str], dict[str, Any]] | None:
    hmm = _map(facts.get("hmm"))
    cross = _map(facts.get("cross_index"))
    source = str(cross.get("source") or "")
    posterior = _map(hmm.get("posterior"))
    probabilities = {
        state: _number(posterior.get(state)) for state in HMM_STATE_DIRECTION
    }
    if (
        hmm.get("status") != "available"
        or cross.get("status") != "ready"
        or cross.get("session_open") is not True
        or source not in {"cash_index", "globex_index"}
        or any(value is None for value in probabilities.values())
    ):
        return None
    resolved = {state: float(value) for state, value in probabilities.items() if value is not None}
    dominant = max(resolved, key=resolved.__getitem__)
    max_probability = resolved[dominant]
    direction = HMM_STATE_DIRECTION[dominant]
    payload = {
        "used": True,
        "status": "available",
        "source": source,
        "anchor": cross.get("anchor"),
        "dominant_state": dominant,
        "max_state_probability": round(max_probability, 4),
        "posterior": {state: round(value, 4) for state, value in resolved.items()},
        "reason": None,
    }
    if dominant == "state_01" and max_probability >= policy.hmm_balanced_min_probability:
        return "BALANCED", None, ["hmm_index_balanced"], payload
    if direction in {"UP", "DOWN"} and max_probability >= policy.hmm_trend_min_probability:
        return (
            "TREND",
            direction,
            ["hmm_index_trend", f"hmm_dominant:{dominant}"],
            payload,
        )
    return "TRANSITION", direction, ["hmm_index_mixed_posterior"], payload


def _hmm_unused_payload(facts: Mapping[str, Any]) -> dict[str, Any]:
    hmm = _map(facts.get("hmm"))
    cross = _map(facts.get("cross_index"))
    reason = str(hmm.get("reason") or "")
    if hmm.get("status") != "available":
        reason = reason or "hmm_unavailable"
    elif cross.get("status") != "ready" or cross.get("session_open") is not True:
        reason = reason or "hmm_index_basket_not_ready"
    else:
        reason = reason or "hmm_index_not_used"
    return {
        "used": False,
        "status": hmm.get("status") or "unavailable",
        "source": cross.get("source"),
        "reason": reason,
    }


def _pin_assessment(facts: Mapping[str, Any], policy: StrategyPolicy) -> dict[str, Any]:
    path, vc, structure = _map(facts.get("path")), _map(facts.get("value_center")), _map(facts.get("structure"))
    vol, mass = _map(facts.get("volatility")), _map(structure.get("q_local_mass_5pt"))
    er, vc15, vc30, vc60 = (_number(path.get("efficiency_ratio_30m")), *(_number(vc.get(f"spx_{w}")) for w in ("15m", "30m", "60m")))
    decay = _number(vol.get("atm_straddle_decay_15m"))
    closes = [float(value) for value in path.get("pin_path_spx") or () if isinstance(value, int | float)]
    breadth = _number(path.get("breadth_above_vwap"))
    vix = _number(vol.get("vix_return_15m_pct"))
    q_mode, q_mode_source = _pin_q_mode(mass, _map(facts.get("pin_latch")), policy)
    required = (er, vc15, vc30, vc60, q_mode, decay, breadth, vix)
    if None in required or len(closes) < 4 or not mass:
        return {"terminal_state": "UNCERTAIN", "reason": "pin_inputs_unavailable", "top_centers": []}
    shock_state = str(_map(facts.get("shock")).get("state") or "NONE")
    if shock_state in {"ACTIVE", "POST_SHOCK_DISCOVERY"}:
        return {
            "terminal_state": "NONE",
            "reason": f"shock_{shock_state.lower()}",
            "depin_risk": 1.0,
            "top_centers": [],
        }
    centers = [float(key) for key in mass if str(key).replace(".", "", 1).isdigit()]
    returns = {center: _excursion_returns(closes, center) for center in centers}
    drift30, drift60 = float(vc15) - float(vc30), float(vc15) - float(vc60)
    extreme = abs(closes[-1] - closes[-4]) >= 5 and closes[-1] in {min(closes[-4:]), max(closes[-4:])}
    depin = min(1.0, 0.25 * max(abs(drift30) / 5, abs(drift60) / 8)
                + 0.20 * min(float(er) / 0.4, 1) + 0.20 * (abs(float(breadth) - 0.5) * 2 if breadth is not None else 0)
                + 0.15 * min(max(float(vix), 0) / 0.01, 1) + 0.10 * extreme
                + 0.10 * min(max(-float(decay), 0) / 0.05, 1))
    refs = [_number(structure.get(key)) for key in ("zero_gamma", "put_wall", "call_wall")]
    flip = structure.get("flip_zone")
    if isinstance(flip, (list, tuple)) and len(flip) >= 2:
        refs.append(sum(map(float, flip[:2])) / 2)
    gamma = min((value for value in refs if value is not None), key=lambda value: abs(value - float(q_mode)), default=None)
    max_mass = max(float(value) for value in mass.values()) or 1.0
    ranked = sorted(({
        "center": center,
        "score": round(0.25 * math.exp(-min((abs(center - value) for value in refs if value is not None), default=30) / 5)
                       + 0.25 * (0.5 * math.exp(-abs(center - float(vc30)) / 5) + 0.5 * math.exp(-abs(center - float(vc60)) / 7.5))
                       + 0.20 * float(mass[f"{center:g}"]) / max_mass
                       + 0.15 * (0, 0.4, 0.7, 1)[min(returns[center], 3)] + 0.10 * (float(decay) > 0)
                       - 0.25 * min(max(abs(drift30) / 5, abs(drift60) / 8), 1) - 0.20 * depin, 4),
        "excursion_returns": returns[center],
    } for center in centers), key=lambda row: row["score"], reverse=True)
    latch = _map(facts.get("pin_latch"))
    raw_leader = _number(_map(ranked[0]).get("center")) if ranked else None
    center_source = "current_score"
    previous_center = _number(latch.get("center"))
    if ranked and previous_center is not None:
        previous_row = next(
            (
                row
                for row in ranked
                if abs(float(row["center"]) - previous_center) <= 0.01
            ),
            None,
        )
        leader = ranked[0]
        if (
            previous_row is not None
            and previous_row is not leader
            and abs(float(leader["center"]) - previous_center)
            <= policy.pin_center_hold_max_distance_points
            and float(leader["score"]) - float(previous_row["score"])
            < policy.pin_center_switch_min_score_margin
        ):
            ranked = [previous_row, *(row for row in ranked if row is not previous_row)]
            center_source = "previous_center_score_margin_hold"
    er_max, drift30_max, drift60_max, migrate30, migrate60, stable_risk, block_risk = policy.pin_thresholds
    migrating = abs(drift30) > migrate30 or abs(drift60) > migrate60 or float(er) > 0.40 or extreme
    aligned = gamma is not None and max(float(q_mode), float(vc30), gamma) - min(float(q_mode), float(vc30), gamma) <= 5
    minutes_to_close = facts.get("minutes_to_close")
    excursions = max(returns.values(), default=0)
    held = _pin_stable_hold(facts, returns, policy)
    stable = (
        minutes_to_close is not None
        and int(minutes_to_close) <= policy.pin_stable_max_minutes_to_close
        and float(er) < er_max and abs(drift30) <= drift30_max and abs(drift60) <= drift60_max
        and (
            excursions >= policy.pin_stable_enter_min_excursions
            or (held and excursions >= policy.pin_stable_hold_min_excursions)
        )
        and float(vix) <= 0.01 and not extreme and aligned
        and float(decay) > 0 and depin < stable_risk
    )
    gth = str(_map(facts.get("session")).get("mode") or "").strip().lower() == "gth"
    in_look_window = pin_look_window(
        float(minutes_to_close) if isinstance(minutes_to_close, int | float) else None,
        policy,
    )
    look = (
        not gth
        and not migrating
        and depin < block_risk
        and excursions >= policy.pin_look_min_excursions
        and in_look_window
        and q_mode is not None
    )
    if migrating or depin >= block_risk:
        terminal, grade = "PIN_MIGRATING", "migrating"
    elif stable:
        terminal, grade = "PIN_STABLE", "stable"
    elif look:
        terminal, grade = "NONE", "look"
    else:
        terminal, grade = "NONE", "none"
    selected_center = _number(_map(ranked[0]).get("center")) if ranked else None
    decision_at = _time(facts.get("decision_at"))
    same_center = (
        selected_center is not None
        and previous_center is not None
        and abs(selected_center - previous_center) <= 0.01
    )
    if same_center:
        previous_count = int(_number(latch.get("center_confirmation_count")) or 1)
        confirmation_count = previous_count + 1
        first_seen_at = (
            _time(latch.get("center_first_seen_at"))
            or _time(latch.get("decision_at"))
            or decision_at
        )
    else:
        confirmation_count = 1 if selected_center is not None else 0
        first_seen_at = decision_at
    confirmation_age = (
        max((decision_at - first_seen_at).total_seconds(), 0.0)
        if decision_at is not None and first_seen_at is not None
        else 0.0
    )
    confirmation_ready = (
        terminal == "PIN_STABLE"
        and confirmation_count >= policy.pin_center_min_confirmation_snapshots
        and confirmation_age >= policy.pin_center_min_dwell_seconds
    )
    confirmation_reason = (
        "pin_not_stable"
        if terminal != "PIN_STABLE"
        else "center_snapshot_confirmation_pending"
        if confirmation_count < policy.pin_center_min_confirmation_snapshots
        else "center_dwell_confirmation_pending"
        if confirmation_age < policy.pin_center_min_dwell_seconds
        else "confirmed"
    )
    return {
        "terminal_state": terminal,
        "depin_risk": round(depin, 4),
        "drift_30m": round(drift30, 2),
        "drift_60m": round(drift60, 2),
        "recent_extreme_acceptance": extreme,
        "q_mode": q_mode,
        "q_mode_source": q_mode_source,
        "grade": grade,
        "center": selected_center,
        "raw_leader_center": raw_leader,
        "center_source": center_source,
        "center_confirmation_count": confirmation_count,
        "center_confirmation_required": policy.pin_center_min_confirmation_snapshots,
        "center_first_seen_at": first_seen_at.isoformat() if first_seen_at else None,
        "center_confirmation_age_seconds": round(confirmation_age, 3),
        "center_confirmation_min_seconds": policy.pin_center_min_dwell_seconds,
        "center_confirmation_ready": confirmation_ready,
        "center_confirmation_reason": confirmation_reason,
        "excursion_held": bool(held and stable and excursions < policy.pin_stable_enter_min_excursions),
        "top_centers": ranked[:3],
    }


def _pin_q_mode(
    mass: Mapping[str, Any],
    latch: Mapping[str, Any],
    policy: StrategyPolicy,
) -> tuple[float | None, str]:
    """PIN alignment uses the local 5-point mass peak, with a one-bin hold."""

    ranked = _mass_ranked_centers(mass)
    local = ranked[0] if ranked else None
    if local is None:
        return None, "missing"
    if latch.get("terminal_state") != "PIN_STABLE":
        return local, "local_mass"
    previous = _number(latch.get("q_mode"))
    if (
        previous is not None
        and abs(previous - local) <= policy.pin_q_mode_hold_max_distance_points
        and any(abs(center - previous) <= 0.01 for center in ranked[:2])
        and abs(previous - local) > 0.01
    ):
        return previous, "local_mass_held"
    return local, "local_mass"


def _mass_ranked_centers(mass: Mapping[str, Any]) -> list[float]:
    ranked: list[tuple[float, float]] = []
    for key, value in mass.items():
        try:
            center = float(key)
        except (TypeError, ValueError):
            continue
        weight = _number(value)
        if weight is None:
            continue
        ranked.append((center, weight))
    ranked.sort(key=lambda item: item[1], reverse=True)
    return [center for center, _weight in ranked]


def _pin_stable_hold(
    facts: Mapping[str, Any],
    returns: Mapping[float, int],
    policy: StrategyPolicy,
) -> bool:
    latch = _map(facts.get("pin_latch"))
    if latch.get("terminal_state") != "PIN_STABLE":
        return False
    if str(latch.get("session_date") or "") != str(facts.get("session_date") or ""):
        return False
    center = _number(latch.get("center"))
    if center is None:
        return False
    reach = policy.pin_body_max_center_distance_points
    return any(
        abs(candidate - center) <= reach and count >= policy.pin_stable_hold_min_excursions
        for candidate, count in returns.items()
    )


def _excursion_returns(values: list[float], center: float) -> int:
    away, count = False, 0
    for value in values:
        if abs(value - center) >= 5:
            away = True
        elif away and abs(value - center) <= 2.5:
            count += 1
            away = False
    return count


def _map(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) else None


def _time(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else None


def _first_number(*values: object) -> float | None:
    for value in values:
        number = _number(value)
        if number is not None:
            return number
    return None
