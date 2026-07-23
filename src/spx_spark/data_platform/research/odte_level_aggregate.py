"""Aggregate 0DTE backtest results and write portable report artifacts."""

from __future__ import annotations

import csv
import json
import statistics
from dataclasses import asdict, fields
from datetime import date, datetime
from pathlib import Path
from typing import Mapping, Sequence

from .odte_level_report import _render_report
from .odte_level_signals import (
    DEFAULT_BASIS_POINTS,
    DELTA_MAX,
    DELTA_MIN,
    DELTA_TARGET,
    FT_GATE_EM_FRACTION,
    FT_GATE_POINTS,
    FT_GATE_SECONDS,
    INVALIDATION_BUFFER_POINTS,
    MAX_ENTRY_LEG_SKEW,
    MAX_ENTRY_QUOTE_AGE,
    MAX_HOLD,
    MAX_MARK_LEG_SKEW,
    MAX_MARK_QUOTE_AGE,
    MAX_UNDERLIER_QUOTE_AGE,
    PROFILES,
    PROFIT_TARGET_MULTIPLE,
    SET_CONFIRMED,
    SET_GTH_DIP,
    SET_ORDER,
    SET_PREFILL,
    SET_TRADE_READY,
    SPREAD_WIDTHS,
    TIME_STOP_DELAY,
    TRAILING_ACTIVATION_FRACTION,
    TRAILING_GIVEBACK_FRACTION,
    VARIANT_NAKED,
    VARIANTS,
    Profile,
    Signal,
    Skip,
    Trade,
    hour_bucket,
)


def _stats(rows: Sequence[Trade]) -> dict:
    """Headline stats for one set x variant bucket (expectancy = mean pnl)."""
    if not rows:
        return {
            "n": 0,
            "winrate": None,
            "avg_pnl_usd": None,
            "median_pnl_usd": None,
            "profit_factor": None,
            "total_pnl_usd": 0.0,
            "expectancy_usd": None,
        }
    pnls = [row.pnl_usd for row in rows]
    wins = [pnl for pnl in pnls if pnl > 0]
    losses = [pnl for pnl in pnls if pnl < 0]
    gross_loss = abs(sum(losses))
    return {
        "n": len(rows),
        "winrate": len(wins) / len(rows),
        "avg_pnl_usd": statistics.fmean(pnls),
        "median_pnl_usd": statistics.median(pnls),
        "profit_factor": (sum(wins) / gross_loss) if gross_loss > 0 else None,
        "total_pnl_usd": sum(pnls),
        "expectancy_usd": statistics.fmean(pnls),
    }


def _slice_stats(rows: Sequence[Trade]) -> dict:
    pnls = [row.pnl_usd for row in rows]
    return {
        "n": len(rows),
        "winrate": sum(pnl > 0 for pnl in pnls) / len(rows),
        "avg_pnl_usd": statistics.fmean(pnls),
        "total_pnl_usd": sum(pnls),
    }


def _slices(rows: Sequence[Trade], key_fn) -> dict[str, dict]:
    grouped: dict[str, list[Trade]] = {}
    for row in rows:
        key = key_fn(row)
        if key is None:
            continue
        grouped.setdefault(str(key), []).append(row)
    return {key: _slice_stats(grouped[key]) for key in sorted(grouped)}


def _trade_session_date(row: Trade) -> date:
    """Resolve the economic session, preferring the SPXW contract expiry."""
    parts = row.contract_id.split(":")
    if len(parts) >= 4 and parts[:3] == ["option", "SPX", "SPXW"]:
        try:
            return datetime.strptime(parts[3], "%Y%m%d").date()
        except ValueError:
            pass
    return datetime.fromisoformat(row.entry_time).date()


def aggregate(
    trades: Sequence[Trade],
    skips: Sequence[Skip],
    signal_counts: dict[str, int],
    profiles: Sequence[Profile] = PROFILES,
) -> dict:
    """Per profile x set x variant aggregates with signal and session slices."""
    by_profile: dict[str, dict] = {}
    for profile in profiles:
        profile_rows = [row for row in trades if row.profile == profile.name]
        profile_skips = [skip for skip in skips if skip.profile == profile.name]
        sets: dict[str, dict] = {}
        for set_name in SET_ORDER:
            set_rows = [row for row in profile_rows if row.set_name == set_name]
            set_skips = [skip for skip in profile_skips if skip.set_name == set_name]
            variants: dict[str, dict] = {}
            for variant in VARIANTS:
                rows = [row for row in set_rows if row.variant == variant]
                skipped: dict[str, int] = {}
                for skip in set_skips:
                    if skip.variant == variant:
                        skipped[skip.reason] = skipped.get(skip.reason, 0) + 1
                bucket = _stats(rows)
                bucket["skipped"] = skipped
                bucket["exit_reasons"] = {
                    reason: sum(row.exit_reason == reason for row in rows)
                    for reason in sorted({row.exit_reason for row in rows})
                }
                bucket["slices"] = {
                    "by_thesis": _slices(rows, lambda row: row.play),
                    "by_direction": _slices(rows, lambda row: row.direction),
                    "by_level_kind": _slices(rows, lambda row: row.level_kind),
                    "by_trend_regime": _slices(rows, lambda row: row.trend_regime),
                    "by_hour_bucket": _slices(
                        rows, lambda row: hour_bucket(datetime.fromisoformat(row.entry_time))
                    ),
                    "by_session_date": _slices(rows, _trade_session_date),
                    "by_weekday": _slices(rows, lambda row: _trade_session_date(row).strftime("%A")),
                }
                if set_name == SET_PREFILL:
                    bucket["ft_gate"] = {
                        "gated": _slice_stats([row for row in rows if row.ft_pass_15s2p is True])
                        if any(row.ft_pass_15s2p is True for row in rows)
                        else None,
                        "ungated": _slice_stats([row for row in rows if row.ft_pass_15s2p is False])
                        if any(row.ft_pass_15s2p is False for row in rows)
                        else None,
                    }
                variants[variant] = bucket
            sets[set_name] = {"signals": signal_counts.get(set_name, 0), "variants": variants}
        by_profile[profile.name] = sets
    return by_profile


def build_artifact(
    *,
    generated_at: datetime,
    features_root: Path,
    data_root: Path,
    sessions: Sequence[str],
    observed_partitions: Sequence[str],
    cutoff_at: datetime,
    as_of: date | datetime | None,
    signal_counts: dict[str, int],
    intent_coverage: dict,
    signal_sets: Mapping[str, Sequence[Signal]],
    trades: Sequence[Trade],
    skips: Sequence[Skip],
    strategy_readiness: Mapping[str, object],
) -> dict:
    """Build the versioned backtest artifact from evaluated signals."""
    profile_configs = [
        {
            "name": profile.name,
            "invalidation_em_fraction": profile.invalidation_em_fraction,
            "profit_target_mode": profile.profit_target_mode,
            "gth_time_stop_minutes": (
                profile.gth_time_stop.total_seconds() / 60 if profile.gth_time_stop else None
            ),
            "gth_max_hold_minutes": (
                profile.gth_max_hold.total_seconds() / 60 if profile.gth_max_hold else None
            ),
            "gth_clock_exit": profile.gth_clock_exit,
            "gth_only": profile.gth_only,
            "spread_only": profile.spread_only,
            "set_names": list(profile.set_names) if profile.set_names else None,
        }
        for profile in PROFILES
    ]
    aggregated_profiles = aggregate(trades, skips, signal_counts)
    production_total = aggregated_profiles[PROFILES[0].name][SET_TRADE_READY]["variants"][
        VARIANT_NAKED
    ]
    production_trades = {
        row.key: row
        for row in trades
        if row.set_name == SET_TRADE_READY
        and row.profile == PROFILES[0].name
        and row.variant == VARIANT_NAKED
    }
    production_skips = {
        row.key: row
        for row in skips
        if row.set_name == SET_TRADE_READY
        and row.profile == PROFILES[0].name
        and row.variant == VARIANT_NAKED
    }
    trade_ready_decisions = []
    for signal in signal_sets[SET_TRADE_READY]:
        trade = production_trades.get(signal.key)
        skip = production_skips.get(signal.key)
        trade_ready_decisions.append(
            {
                "intent_id": signal.key,
                "evaluated_at": signal.at.isoformat(),
                "direction": signal.direction,
                "play": signal.thesis,
                "contract_id": signal.contract_id,
                "provider": signal.entry_provider,
                "entry_limit": signal.entry_limit,
                "expires_at": (
                    signal.entry_expires_at.isoformat() if signal.entry_expires_at else None
                ),
                "invalidation_spx": signal.invalidation_level,
                "target_spx": signal.target_level,
                "execution_result": "filled" if trade is not None else "skipped",
                "skip_reason": skip.reason if skip is not None else None,
                "entry_time": trade.entry_time if trade is not None else None,
                "entry_px": trade.entry_px if trade is not None else None,
                "pnl_usd": trade.pnl_usd if trade is not None else None,
            }
        )
    return {
        "schema_version": 6,
        "generated_at": generated_at.isoformat(),
        "features_root": str(features_root),
        "data_root": str(data_root),
        "window": {
            "first_session": sessions[0] if sessions else None,
            "last_session": sessions[-1] if sessions else None,
            "trading_days": len(sessions),
            "complete_sessions": sessions,
            "observed_partition_count": len(observed_partitions),
            "observed_partitions": list(observed_partitions),
            "cutoff_at": cutoff_at.isoformat(),
            "as_of": as_of.isoformat() if as_of is not None else None,
        },
        "signal_counts": signal_counts,
        "strategy_readiness": dict(strategy_readiness),
        "trade_intent_coverage": intent_coverage,
        "trade_ready_decisions": trade_ready_decisions,
        "production_strategy_total": {
            "set_name": SET_TRADE_READY,
            "profile": PROFILES[0].name,
            "variant": VARIANT_NAKED,
            "result": production_total,
            "excluded_sets": [SET_CONFIRMED, SET_PREFILL, SET_GTH_DIP],
        },
        "profile_configs": profile_configs,
        "method": {
            "variants": list(VARIANTS),
            "spread_widths": SPREAD_WIDTHS,
            "set_roles": {
                SET_CONFIRMED: "control_proxy_fsm_confirmed",
                SET_PREFILL: "follow_through_only_observational_proxy",
                SET_GTH_DIP: "gth_confirmation_proxy",
                SET_TRADE_READY: "production_entry_decisions",
            },
            "session_cohort": (
                "backtest complete sessions are readiness.sessions.details rows with "
                "complete=true; observed feature partitions remain a separate coverage concept"
            ),
            "production_strategy_set": SET_TRADE_READY,
            "entry": (
                "control/proxy sets use long ask; S2 reprices after follow-through; "
                "trade_ready requires recorded-provider ask <= recorded limit before expires_at"
            ),
            "s2_follow_through": {
                "hold_seconds": FT_GATE_SECONDS,
                "minimum_points": FT_GATE_POINTS,
                "expected_move_fraction": FT_GATE_EM_FRACTION,
                "distance_anchor": "spot_minus_trigger_level",
            },
            "s3_spread_wall": "exact persisted production long/short strikes; no delta rebuild",
            "trade_ready_entry": {
                "execution": "naked only",
                "window_end": "exclusive expires_at",
                "fill": "first recorded-provider lake ask at or below recorded entry_limit",
                "pre_entry": (
                    "skip if recorded target/invalidation is reached before fill; "
                    "missing/stale underlier fails closed"
                ),
                "direction": "reported as a slice; never used as an allow/deny rule",
                "outcome_horizons_used_as_gate": False,
            },
            "provider_selection": (
                "control/proxy: earliest executable entry quote only (long ask / short bid); "
                "trade_ready: exact persisted provider"
            ),
            "gth_session_clock": {
                "exit_clock": "expiry-date 09:45 America/New_York",
                "latest_hold": "expiry-date 16:00 America/New_York",
            },
            "exit_rules_order": [
                "invalidation",
                "target_wall",
                "profit_target|trailing_tp",
                "time_stop",
                "end_of_data",
            ],
            "max_entry_quote_age_seconds": MAX_ENTRY_QUOTE_AGE.total_seconds(),
            "max_entry_leg_skew_seconds": MAX_ENTRY_LEG_SKEW.total_seconds(),
            "max_mark_quote_age_seconds": MAX_MARK_QUOTE_AGE.total_seconds(),
            "max_mark_leg_skew_seconds": MAX_MARK_LEG_SKEW.total_seconds(),
            "max_underlier_quote_age_seconds": MAX_UNDERLIER_QUOTE_AGE.total_seconds(),
            "time_stop_minutes": TIME_STOP_DELAY.total_seconds() / 60,
            "max_hold_minutes": MAX_HOLD.total_seconds() / 60,
            "profit_target_multiple": PROFIT_TARGET_MULTIPLE,
            "trailing_activation_fraction": TRAILING_ACTIVATION_FRACTION,
            "trailing_giveback_fraction": TRAILING_GIVEBACK_FRACTION,
            "invalidation_buffer_points": INVALIDATION_BUFFER_POINTS,
            "default_basis_points": DEFAULT_BASIS_POINTS,
            "delta_band": [DELTA_MIN, DELTA_MAX, DELTA_TARGET],
        },
        "profiles": aggregated_profiles,
        "skipped_detail": [asdict(skip) for skip in skips],
        "limitations": [
            "small_sample",
            "fills_assume_full_size_at_top_of_book",
            "commissions_slippage_queue_partial_fills_and_market_impact_not_modeled",
            "gth_underlier_uses_es_minus_fixed_basis",
            "readiness_health_complete_sessions_only",
            "legacy_gth_without_recorded_spread_has_no_spread_wall_trade",
            "prefill_is_follow_through_only_observational_proxy",
            "trade_ready_sample_is_not_an_out_of_sample_edge_claim",
            "trade_ready_replay_uses_stored_window_and_does_not_model_operator_alert_latency",
        ],
    }


def write_outputs(output_dir: Path, artifact: dict, trades: Sequence[Trade]) -> Path:
    """Write artifact, report and trade ledger with the existing file contract."""
    target = Path(output_dir).expanduser().resolve()
    target.mkdir(parents=True, exist_ok=True)
    (target / "artifact.json").write_text(
        json.dumps(artifact, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (target / "readiness.json").write_text(
        json.dumps(artifact.get("strategy_readiness") or {}, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (target / "report.md").write_text(_render_report(artifact, trades), encoding="utf-8")
    fieldnames = [field.name for field in fields(Trade)]
    with (target / "trades.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if trades:
            writer.writeheader()
            writer.writerows(asdict(row) for row in trades)
    return target
