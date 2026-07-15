from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from spx_spark.analytics.options.quote_policy import gth_analytical_quote
from spx_spark.config import StorageSettings
from spx_spark.greek_reference import (
    SCHEMA_VERSION,
    build_zero_dte_greeks_reference,
    is_spxw_zero_dte,
    write_zero_dte_greeks_snapshot,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET
from spx_spark.marketdata import Quote, quote_use_decision
from spx_spark.options_map import (
    UNDERLIER_MISMATCH_SOURCES,
    OptionsMap,
    build_gex_by_strike,
    build_options_map,
    is_spxw_option,
    pair_by_strike,
)
from spx_spark.settings import load_app_settings
from spx_spark.storage import LatestState, LatestStateStore, configured_quote_use_decision


ALLOWED_TRIGGER_KINDS = frozenset(
    {"periodic", "shock", "reclaim", "gth_dip_reclaim_call"}
)
SIGNED_GEX_METHOD = "call_positive_put_negative_oi_proxy_not_dealer_position"
SHADOW_MODE = "research_shadow_only"


def _analytically_fresh_quotes(
    quotes: tuple[Quote, ...],
    *,
    as_of: datetime,
) -> tuple[Quote, ...]:
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        return ()
    if not DEFAULT_MARKET_CALENDAR.is_spx_gth_open(as_of):
        return tuple(
            quote
            for quote in quotes
            if configured_quote_use_decision(quote, as_of=as_of).pricing_allowed
        )
    max_age = load_app_settings().analytics.gth_max_chain_age_seconds
    return tuple(
        quote
        for quote in quotes
        if quote_use_decision(
            gth_analytical_quote(quote, as_of=as_of, max_age_seconds=max_age),
            as_of=as_of,
            stale_after_seconds=max_age,
        ).pricing_allowed
    )


@dataclass(frozen=True)
class GreekShadowResult:
    """Small operational result; the full reference stays in the snapshot file."""

    status: str
    reference_status: str
    reason: str | None
    as_of: str
    expiry: str
    trigger: dict[str, Any]
    paths: dict[str, str] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _trigger_payload(
    *,
    kind: str,
    event_id: str | None,
    event_at: datetime | None,
    metadata: Mapping[str, str | int | float | bool | None] | None,
) -> dict[str, Any]:
    normalized_kind = kind.strip().lower()
    if normalized_kind not in ALLOWED_TRIGGER_KINDS:
        raise ValueError(
            f"unsupported Greek shadow trigger {kind!r}; expected periodic, shock, reclaim, or gth_dip_reclaim_call"
        )
    if normalized_kind == "periodic" and (event_id is not None or event_at is not None):
        raise ValueError("periodic Greek shadow trigger cannot carry event identity")
    payload: dict[str, Any] = {"kind": normalized_kind}
    if event_id:
        payload["event_id"] = event_id
    if event_at is not None:
        if event_at.tzinfo is None:
            raise ValueError("Greek shadow event_at must be timezone-aware")
        payload["event_at"] = event_at.isoformat()
    if metadata:
        payload["metadata"] = dict(metadata)
    return payload


def _unavailable_reference(
    state: LatestState,
    *,
    expiry: str,
    reason: str,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "snapshot",
        "mode": "reference_only",
        "status": "unavailable",
        "as_of": state.as_of.isoformat(),
        "expiry": expiry,
        "direction": "unknown",
        "position_sign": "unknown",
        "reason": reason,
        "aggregate": None,
        "contracts": [],
    }


def _same_day_quotes(state: LatestState, expiry: str) -> tuple[Any, ...]:
    return tuple(
        quote
        for quote in state.best_quotes
        if is_spxw_option(quote) and (quote.instrument.expiry or "") == expiry
    )


def _signed_oi_gex_proxy(
    quotes: tuple[Any, ...],
    *,
    state: LatestState,
    spot: float | None,
) -> dict[str, Any]:
    """Compute a deliberately labeled OI proxy, never a dealer-position estimate."""

    actionable = tuple(
        quote
        for quote in quotes
        if is_spxw_zero_dte(quote, as_of=state.as_of)
        and configured_quote_use_decision(quote, as_of=state.as_of).pricing_allowed
    )
    rows = (
        build_gex_by_strike(
            pair_by_strike(actionable),
            underlier=spot,
            intraday=False,
        )
        if spot is not None and spot > 0
        else []
    )
    call_gex = sum(row.call_gex for row in rows) if rows else None
    put_gex = sum(row.put_gex for row in rows) if rows else None
    net_gex = call_gex + put_gex if call_gex is not None and put_gex is not None else None
    abs_gex = sum(row.abs_gex for row in rows) if rows else None
    ratio = (
        net_gex / abs_gex if net_gex is not None and abs_gex is not None and abs_gex > 0 else None
    )
    return {
        "method": SIGNED_GEX_METHOD,
        "sign_convention": "calls_positive_puts_negative",
        "weighting": "open_interest_only",
        "dealer_position_sign": "unknown",
        "direction": "unknown",
        "quality": "available" if rows else "unavailable",
        "call_gex": call_gex,
        "put_gex": put_gex,
        "net_gex": net_gex,
        "abs_gex": abs_gex,
        "net_gamma_ratio": ratio,
        "fresh_contract_count": len(actionable),
        "strike_count": len(rows),
    }


def _front_expiry(options_map: OptionsMap | None, expiry: str) -> Any | None:
    if options_map is None:
        return None
    return next(
        (row for row in options_map.expiries if str(getattr(row, "expiry", "")) == expiry),
        None,
    )


def _decorate_shadow_payload(
    payload: dict[str, Any],
    *,
    trigger: dict[str, Any],
    signed_gex_proxy: dict[str, Any],
    total_contract_count: int,
    fresh_contract_count: int,
    options_map: OptionsMap | None,
    expiry: str,
) -> None:
    front = _front_expiry(options_map, expiry)
    payload["shadow_sample"] = {
        "mode": SHADOW_MODE,
        "trigger": trigger,
        "notification_allowed": False,
        "order_placement_allowed": False,
        "strategy_action_allowed": False,
        "freshness": {
            "exact_expiry_contract_count": total_contract_count,
            "fresh_pricing_contract_count": fresh_contract_count,
            "stale_or_unusable_contract_count": max(
                total_contract_count - fresh_contract_count,
                0,
            ),
            "fresh_ratio": (
                fresh_contract_count / total_contract_count if total_contract_count else 0.0
            ),
        },
    }
    payload["signed_gex_proxy"] = signed_gex_proxy
    payload["intraday_map_gex_context"] = {
        "net_gex": getattr(front, "net_gex", None),
        "abs_gex": getattr(front, "abs_gex", None),
        "net_gamma_ratio": getattr(front, "net_gamma_ratio", None),
        "gamma_state": getattr(front, "gamma_state", "unknown"),
        "weighting": getattr(front, "gex_weighting", "unknown"),
        "dealer_position_sign": "unknown",
        "direction": "unknown",
    }


def sample_zero_dte_greeks_shadow(
    state: LatestState,
    *,
    data_root: str | Path,
    trigger_kind: str = "periodic",
    event_id: str | None = None,
    event_at: datetime | None = None,
    trigger_metadata: Mapping[str, str | int | float | bool | None] | None = None,
    options_map: OptionsMap | None = None,
) -> GreekShadowResult:
    """Build and persist one strictly 0DTE, research-only Greeks sample.

    The caller may invoke this on a timer or immediately after a shock/reclaim.
    It never notifies and never returns an actionable strategy decision. Data
    failures are persisted as unavailable snapshots and returned as ``blocked``.
    """

    expiry = (
        DEFAULT_MARKET_CALENDAR.research_expiry(state.as_of).strftime("%Y%m%d")
        if state.as_of.tzinfo is not None
        and DEFAULT_MARKET_CALENDAR.is_spx_gth_open(state.as_of)
        else state.as_of.astimezone(ET).strftime("%Y%m%d")
        if state.as_of.tzinfo is not None
        else state.as_of.strftime("%Y%m%d")
    )
    try:
        trigger = _trigger_payload(
            kind=trigger_kind,
            event_id=event_id,
            event_at=event_at,
            metadata=trigger_metadata,
        )
    except (TypeError, ValueError) as exc:
        return GreekShadowResult(
            status="error",
            reference_status="unavailable",
            reason=str(exc),
            as_of=state.as_of.isoformat(),
            expiry=expiry,
            trigger={"kind": str(trigger_kind)},
        )

    hard_block_reason: str | None = None
    if state.as_of.tzinfo is None:
        hard_block_reason = "latest_state_as_of_timezone_missing"
        payload = _unavailable_reference(
            state,
            expiry=expiry,
            reason=hard_block_reason,
        )
        options = options_map
    else:
        at_et = state.as_of.astimezone(ET)
        if not DEFAULT_MARKET_CALENDAR.is_rth_open(
            at_et
        ) and not DEFAULT_MARKET_CALENDAR.is_spx_gth_open(at_et):
            hard_block_reason = "outside_spx_trading_session"
            payload = _unavailable_reference(
                state,
                expiry=expiry,
                reason=hard_block_reason,
            )
            options = options_map
        else:
            try:
                options = options_map or build_options_map(state)
                payload = build_zero_dte_greeks_reference(state, options_map=options)
            except Exception as exc:  # Shadow collection must not break the service loop.
                return GreekShadowResult(
                    status="error",
                    reference_status="unavailable",
                    reason=f"greek_shadow_build_error:{type(exc).__name__}:{exc}",
                    as_of=state.as_of.isoformat(),
                    expiry=expiry,
                    trigger=trigger,
                )

    same_day_quotes = _same_day_quotes(state, expiry)
    zero_dte_quotes = tuple(
        quote for quote in same_day_quotes if is_spxw_zero_dte(quote, as_of=state.as_of)
    )
    fresh_quotes = _analytically_fresh_quotes(
        zero_dte_quotes,
        as_of=state.as_of,
    )

    underlier_source = getattr(getattr(options, "underlier", None), "source", None)
    if hard_block_reason is not None:
        pass
    elif underlier_source in UNDERLIER_MISMATCH_SOURCES:
        payload = _unavailable_reference(
            state,
            expiry=expiry,
            reason=f"underlier_mismatch:{underlier_source}",
        )
    elif not zero_dte_quotes:
        reason = str(payload.get("reason") or "exact_same_day_spxw_0dte_unavailable")
        payload = _unavailable_reference(state, expiry=expiry, reason=reason)
    elif not fresh_quotes:
        payload = _unavailable_reference(
            state,
            expiry=expiry,
            reason="exact_same_day_quotes_stale_or_unusable",
        )
    elif len(fresh_quotes) < len(zero_dte_quotes):
        warnings = list(payload.get("warnings") or ())
        if "partial_exact_expiry_stale_or_unusable" not in warnings:
            warnings.append("partial_exact_expiry_stale_or_unusable")
        payload["warnings"] = warnings

    model = payload.get("model")
    spot = model.get("spot") if isinstance(model, Mapping) else None
    signed_gex_proxy = _signed_oi_gex_proxy(
        same_day_quotes,
        state=state,
        spot=float(spot) if isinstance(spot, (int, float)) else None,
    )
    _decorate_shadow_payload(
        payload,
        trigger=trigger,
        signed_gex_proxy=signed_gex_proxy,
        total_contract_count=len(zero_dte_quotes),
        fresh_contract_count=len(fresh_quotes),
        options_map=options,
        expiry=expiry,
    )

    try:
        paths = write_zero_dte_greeks_snapshot(payload, data_root=data_root)
    except Exception as exc:  # Shadow persistence must not break the service loop.
        return GreekShadowResult(
            status="error",
            reference_status=str(payload.get("status") or "unavailable"),
            reason=f"greek_shadow_write_error:{type(exc).__name__}:{exc}",
            as_of=state.as_of.isoformat(),
            expiry=expiry,
            trigger=trigger,
        )
    if paths is None:
        return GreekShadowResult(
            status="error",
            reference_status=str(payload.get("status") or "unavailable"),
            reason="greek_shadow_snapshot_rejected",
            as_of=state.as_of.isoformat(),
            expiry=expiry,
            trigger=trigger,
        )

    reference_status = str(payload.get("status") or "unavailable")
    degraded_reason = (
        "partial_exact_expiry_stale_or_unusable"
        if reference_status == "degraded"
        and "partial_exact_expiry_stale_or_unusable" in set(payload.get("warnings") or ())
        else "reference_quality_degraded"
        if reference_status == "degraded"
        else None
    )
    return GreekShadowResult(
        status=(
            "blocked"
            if reference_status == "unavailable"
            else "written_degraded"
            if reference_status == "degraded"
            else "written"
        ),
        reference_status=reference_status,
        reason=(str(payload.get("reason")) if payload.get("reason") else degraded_reason),
        as_of=state.as_of.isoformat(),
        expiry=expiry,
        trigger=trigger,
        paths=paths,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Persist one read-only SPXW 0DTE Greeks sample.")
    parser.add_argument("--json", action="store_true", help="Print a compact JSON result.")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    storage = StorageSettings.from_env()
    latest = LatestStateStore(storage).load()
    if latest.as_of.tzinfo is not None:
        at_et = latest.as_of.astimezone(ET)
        session_open = DEFAULT_MARKET_CALENDAR.is_rth_open(
            at_et
        ) or DEFAULT_MARKET_CALENDAR.is_spx_gth_open(at_et)
    else:
        session_open = False
    if latest.as_of.tzinfo is not None and (
        not session_open
    ):
        result = GreekShadowResult(
            status="skipped",
            reference_status="unavailable",
            reason="outside_spx_trading_session",
            as_of=latest.as_of.isoformat(),
            expiry=DEFAULT_MARKET_CALENDAR.research_expiry(latest.as_of).strftime("%Y%m%d"),
            trigger={"kind": "periodic"},
        )
    else:
        result = sample_zero_dte_greeks_shadow(
            latest,
            data_root=storage.data_root,
            trigger_kind="periodic",
        )
    if args.json:
        print(json.dumps(result.to_dict(), ensure_ascii=False, sort_keys=True))
    else:
        print(f"0DTE Greeks shadow: {result.status} ({result.reason or result.reference_status})")
    return 0 if result.status != "error" else 1


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
