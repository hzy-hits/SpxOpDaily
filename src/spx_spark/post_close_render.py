"""Deterministic Markdown rendering for the SPX post-close review."""

from __future__ import annotations

from typing import Any

from spx_spark.steven_validation import FORWARD_METRICS_DISCLAIMER


def fmt(value: Any, digits: int = 2) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def fmt_bps(value: Any) -> str:
    return "-" if value is None else f"{float(value):+.1f} bps"


def change_cell(metric: dict[str, Any], digits: int = 4) -> str:
    return (
        f"{fmt(metric.get('first'), digits)} -> {fmt(metric.get('last'), digits)} "
        f"({fmt(metric.get('change'), digits)})"
    )


def completeness_value(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.4f}"
    if isinstance(value, (list, tuple)):
        return ", ".join(str(item) for item in value) or "-"
    return str(value)


def price_row(label: str, stats: dict[str, Any]) -> str:
    return (
        f"| {label} | {fmt(stats.get('first'))} | {fmt(stats.get('last'))} | "
        f"{fmt(stats.get('change_points'))} / {fmt_bps(stats.get('change_bps'))} | "
        f"{fmt(stats.get('high'))} | {fmt(stats.get('low'))} | {stats.get('count', 0)} |"
    )


def surface_row(expiry: dict[str, Any]) -> str:
    qualities = expiry.get("quality_counts") or {}
    quality_text = ", ".join(f"{key}:{value}" for key, value in qualities.items()) or "-"
    return (
        f"| {expiry['expiry']} | {change_cell(expiry['atm_iv'], 4)} | "
        f"{change_cell(expiry['expected_move_points'], 2)} | "
        f"{change_cell(expiry['put_skew_ratio'], 3)} | "
        f"{change_cell(expiry['call_skew_ratio'], 3)} | "
        f"{expiry.get('gamma_state_first')} -> {expiry.get('gamma_state_last')} | "
        f"{fmt(expiry.get('put_wall_first'), 0)} -> {fmt(expiry.get('put_wall_last'), 0)} | "
        f"{fmt(expiry.get('call_wall_first'), 0)} -> {fmt(expiry.get('call_wall_last'), 0)} | "
        f"{quality_text} |"
    )


def render_markdown(payload: dict[str, Any]) -> str:
    coverage = payload["coverage"]
    spx = payload["spx"]
    options = payload["spxw_options"]
    lines = [
        f"# SPX/SPXW Post-Close Review - {payload['trading_date']}",
        "",
        "Scope: SPX, SPXW option structure, and ES confirmation only. This is a post-session review, not an order recommendation.",
        "",
        "## Summary",
        "",
        f"- Status: `{payload['verdict']['status']}`",
        f"- Raw quote rows: {coverage['raw_quote_rows']}; IV surface snapshots: {coverage['iv_surface_snapshots']}",
        f"- SPX rows: {coverage['spx_rows']}; ES rows: {coverage['es_rows']}; SPXW contracts: {options['unique_contracts']}",
        f"- SPX change: {fmt(spx.get('change_points'))} pts / {fmt_bps(spx.get('change_bps'))}; range: {fmt(spx.get('range_points'))} pts",
        "",
    ]
    warnings = payload["verdict"].get("warnings") or []
    if warnings:
        lines.extend(["## Data Warnings", ""])
        lines.extend(f"- {warning}" for warning in warnings)
        lines.append("")

    checks = payload.get("completeness", {}).get("checks", [])
    lines.extend(
        [
            "## Data Completeness",
            "",
            "| Check | Measured | Threshold | Pass | Reason |",
            "|---|---:|---:|:---:|---|",
        ]
    )
    for check in checks:
        reason = str(check.get("reason") or "-").replace("|", "\\|")
        lines.append(
            f"| {check.get('name', '-')} | {completeness_value(check.get('measured'))} | "
            f"{completeness_value(check.get('threshold'))} | "
            f"{'yes' if check.get('passed') else 'no'} | {reason} |"
        )
    lines.extend(
        [
            "",
            "## Price Path",
            "",
            "| Instrument | First | Last | Change | High | Low | Rows |",
            "|---|---:|---:|---:|---:|---:|---:|",
            price_row("SPX", spx),
            price_row("ES", payload["es"]),
            price_row("MES", payload["mes"]),
            "",
            "## SPXW Quote Coverage",
            "",
            f"- Rows: {options['rows']}; unique contracts: {options['unique_contracts']}",
            f"- Expiries: {', '.join(options['expiries']) if options['expiries'] else '-'}",
            f"- Strike window: {fmt(options['min_strike'], 0)} - {fmt(options['max_strike'], 0)}",
            f"- With IV: {options['with_iv']}; with gamma: {options['with_gamma']}; with OI: {options['with_open_interest']}",
            f"- Average spread: {fmt(options['avg_spread_bps'], 1)} bps",
            "",
            "## IV Surface And Walls",
            "",
        ]
    )
    surface = payload["iv_surface"]
    if surface["snapshot_count"] == 0:
        lines.extend(["- No SPXW IV surface snapshots were available.", ""])
    else:
        lines.extend(
            [
                f"- Snapshots: {surface['snapshot_count']} ({surface['first_as_of']} -> {surface['last_as_of']})",
                f"- Underlier: {fmt(surface['underlier_first'])} -> {fmt(surface['underlier_last'])}",
                f"- 0DTE vs next ATM IV gap: {change_cell(surface['front_vs_next_atm_iv_gap'], 4)}",
                "",
                "| Expiry | ATM IV | Exp move | Put skew | Call skew | Gamma | Put wall | Call wall | Quality |",
                "|---|---:|---:|---:|---:|---|---:|---:|---|",
            ]
        )
        lines.extend(surface_row(expiry) for expiry in surface["expiries"][:4])
        lines.append("")

    greeks = payload.get("spxw_0dte_greeks_reference")
    lines.extend(["## 0DTE Greeks Reference", ""])
    if not isinstance(greeks, dict) or greeks.get("snapshot_count", 0) == 0:
        lines.extend(
            [
                "- No persisted same-day SPXW Greeks reference snapshots were available.",
                "- This optional shadow layer does not change the review completeness verdict.",
                "",
            ]
        )
    else:
        lines.extend(
            [
                "- Mode: `reference_only`; position sign/direction: `unknown` / `unknown`.",
                f"- Snapshots: {greeks['snapshot_count']} total; {greeks.get('comparison_snapshot_count', 0)} share the comparison universe ({greeks.get('first_as_of')} -> {greeks.get('last_as_of')}).",
                "- Gross values are OI-only absolute sensitivities, not signed dealer exposure or directional signals.",
                f"- Coverage usable ratio: {greeks.get('coverage', {}).get('usable_ratio')}; OI ratio: {greeks.get('coverage', {}).get('oi_ratio')}.",
                "",
                "| Metric | First | Last | Peak |",
                "|---|---:|---:|---:|",
            ]
        )
        for name, metric in (greeks.get("metrics") or {}).items():
            if isinstance(metric, dict):
                lines.append(
                    f"| {name} | {fmt(metric.get('first'), 6)} | "
                    f"{fmt(metric.get('last'), 6)} | {fmt(metric.get('peak'), 6)} |"
                )
        if not greeks.get("metrics"):
            lines.append("| No comparable same-universe pair | - | - | - |")
        lines.append("")

    steven = payload.get("steven_episode")
    if isinstance(steven, dict):
        metrics = (
            steven.get("forward_metrics") if isinstance(steven.get("forward_metrics"), dict) else {}
        )
        lines.extend(
            [
                "## Steven Episode (observe_only audit)",
                "",
                f"- Episode: `{steven.get('episode_id')}`; setups: {steven.get('setup_count')}; final_state: `{steven.get('final_state') or '-'}`.",
                f"- Forward quality: `{metrics.get('quality')}`; direction hypothesis: `{metrics.get('direction_hypothesis')}`; reference: {fmt(metrics.get('reference_price'))}.",
                f"- MFE/MAE (bps): {fmt_bps(metrics.get('mfe_bps'))} / {fmt_bps(metrics.get('mae_bps'))}.",
                f"- {FORWARD_METRICS_DISCLAIMER}",
                "",
            ]
        )

    lines.extend(
        [
            "## Hermes Attachment Note",
            "",
            "This section is designed to be appended to the local Hermes daily report after the post-close delay. Hidden cross-market inputs remain outside the human-facing SPX review.",
            "",
        ]
    )
    return "\n".join(lines)
