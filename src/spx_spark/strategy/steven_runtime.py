"""State projection, cycle orchestration, and CLI for Steven guidance."""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable, Mapping
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from spx_spark.config import StorageSettings
from spx_spark.features.bar_builder import SpxBar, SpxBarBuilder
from spx_spark.features.exposure_map import ExposureMap, build_exposure_map, persist_exposure_map
from spx_spark.market_calendar import ET
from spx_spark.settings import settings_value
from spx_spark.storage import LatestState, LatestStateStore, configured_quote_use_decision
from spx_spark.strategy.steven_models import (
    ANCHOR_SOURCES,
    CONTRACT_SCHEMA_VERSION,
    CONTRACT_SOURCE,
    EXPRESSION_FAMILIES,
    MACHINE_STATES,
    StevenInputs,
    StevenSettings,
    _as_utc,
)
from spx_spark.strategy.steven_repository import (
    _parse_optional_dt,
    load_steven_state,
    maybe_append_episode_revision,
    persist_steven_state,
)
from spx_spark.strategy.steven import build_steven_signal, trading_date_et


def _session_phase_for(as_of: datetime) -> str:
    local = _as_utc(as_of).astimezone(ET)
    minutes = local.hour * 60 + local.minute
    if minutes < 9 * 60 + 30:
        return "premarket"
    if minutes < 10 * 60 + 30:
        return "open"
    if minutes < 15 * 60:
        return "midday"
    if minutes < 16 * 60:
        return "late"
    return "closed"


def _quote_source_at(quote: Any) -> datetime:
    return _as_utc(quote.quote_time or quote.trade_time or quote.received_at)


def _underlier_from_state(state: LatestState) -> tuple[float | None, str | None]:
    """Hard gate 6: only index:SPX or chain_implied — never Hyperliquid SP500."""
    quote = state.best_quote("index:SPX")
    if quote is not None:
        decision = configured_quote_use_decision(quote, as_of=state.as_of)
        price = quote.effective_price
        if decision.pricing_allowed and price is not None and price > 0:
            return float(price), "index:SPX"
    # Prefer exposure/options chain_implied via build_exposure_map underlier when available.
    try:
        exposure = build_exposure_map(state)
    except Exception:  # noqa: BLE001
        exposure = None
    if exposure is not None and getattr(exposure.underlier, "source", None) == "chain_implied":
        price = getattr(exposure.underlier, "price", None)
        if price is not None and price > 0:
            return float(price), "chain_implied"
    if exposure is not None and getattr(exposure.underlier, "source", None) == "index:SPX":
        price = getattr(exposure.underlier, "price", None)
        if price is not None and price > 0:
            return float(price), "index:SPX"
    return None, None


def _load_json_mapping(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _load_bars_from_latest(
    data_root: Path,
    *,
    as_of: datetime,
    max_age_seconds: float,
) -> tuple[tuple[SpxBar, ...], tuple[SpxBar, ...]]:
    path_1m = data_root / "latest" / "spx_bars_1m.json"
    path_5m = data_root / "latest" / "spx_bars_5m.json"
    payload_1m = _load_json_mapping(path_1m)
    if payload_1m is None:
        return (), ()
    updated = _parse_optional_dt(payload_1m.get("updated_at"))
    if updated is None or (_as_utc(as_of) - updated).total_seconds() > max_age_seconds:
        return (), ()

    def _parse_bars(payload: Mapping[str, Any] | None) -> tuple[SpxBar, ...]:
        if payload is None:
            return ()
        rows = payload.get("bars")
        if not isinstance(rows, list):
            return ()
        bars: list[SpxBar] = []
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            start = _parse_optional_dt(row.get("bar_start"))
            if start is None:
                continue
            bars.append(
                SpxBar(
                    bar_start=start,
                    interval_seconds=int(row.get("interval_seconds") or 60),
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    sample_count=int(row.get("sample_count") or 0),
                    quality=str(row.get("quality") or "partial"),
                    gap_before=bool(row.get("gap_before")),
                    provider=str(row.get("provider") or "unknown"),
                )
            )
        return tuple(bars)

    return _parse_bars(payload_1m), _parse_bars(_load_json_mapping(path_5m))


def inputs_from_latest_state(
    state: LatestState,
    *,
    data_root: Path | str | None = None,
    exposure: ExposureMap | None = None,
    bars_1m: tuple[SpxBar, ...] | None = None,
    bars_5m: tuple[SpxBar, ...] | None = None,
    shock_state: dict[str, Any] | None = None,
    es_volume: dict[str, Any] | None = None,
    hl_volume: dict[str, Any] | None = None,
    event_tags: Iterable[str] = (),
    previous_payload: Mapping[str, Any] | None = None,
    settings: StevenSettings | None = None,
    reset_warning: str | None = None,
) -> StevenInputs:
    settings = settings or StevenSettings.from_env()
    root = Path(data_root) if data_root is not None else Path(StorageSettings.from_env().data_root)
    underlier_price, underlier_source = _underlier_from_state(state)
    if exposure is None:
        try:
            exposure = build_exposure_map(state)
        except Exception:  # noqa: BLE001
            exposure = None
    if exposure is not None and underlier_price is None:
        src = getattr(exposure.underlier, "source", None)
        price = getattr(exposure.underlier, "price", None)
        if src in ANCHOR_SOURCES and price is not None:
            underlier_price = float(price)
            underlier_source = str(src)
    if bars_1m is None or bars_5m is None:
        loaded_1m, loaded_5m = _load_bars_from_latest(
            root,
            as_of=state.as_of,
            max_age_seconds=settings.bars_source_max_age_seconds,
        )
        if bars_1m is None:
            bars_1m = loaded_1m
        if bars_5m is None:
            bars_5m = loaded_5m
    if shock_state is None:
        shock_state = _load_json_mapping(root / "latest" / "intraday_shock_state.json")
    if es_volume is None:
        order_map = _load_json_mapping(root / "latest" / "order_map.json")
        if isinstance(order_map, dict):
            payload = order_map.get("es_volume_signal")
            es_volume = payload if isinstance(payload, dict) else None
            hl_payload = order_map.get("hl_volume_signal")
            if hl_volume is None:
                hl_volume = hl_payload if isinstance(hl_payload, dict) else None
    tags = tuple(event_tags)
    if not tags:
        raw_tags = settings_value("human_focus.event_tags")
        if isinstance(raw_tags, list):
            tags = tuple(str(item) for item in raw_tags)

    previous_state = "OBSERVE_ONLY"
    previous_state_since = None
    trading_date = None
    daily_setup_count = 0
    lockout_until = None
    data_healthy_since = None
    watch_exit_since = None
    consumed_event_tags: tuple[str, ...] = ()
    if isinstance(previous_payload, Mapping):
        previous_state = str(previous_payload.get("machine_state") or "OBSERVE_ONLY")
        previous_state_since = _parse_optional_dt(previous_payload.get("state_since"))
        trading_date = (
            str(previous_payload["trading_date"]) if previous_payload.get("trading_date") else None
        )
        daily_setup_count = int(previous_payload.get("daily_setup_count") or 0)
        lockout_until = _parse_optional_dt(previous_payload.get("lockout_until"))
        data_healthy_since = _parse_optional_dt(previous_payload.get("data_healthy_since"))
        watch_exit_since = _parse_optional_dt(previous_payload.get("watch_exit_since"))
        raw_consumed = previous_payload.get("consumed_event_tags")
        if isinstance(raw_consumed, list):
            consumed_event_tags = tuple(str(item) for item in raw_consumed)

    return StevenInputs(
        created_at=datetime.now(tz=timezone.utc),
        as_of=state.as_of,
        underlier_price=underlier_price,
        underlier_source=underlier_source,
        exposure=exposure,
        bars_1m=tuple(bars_1m or ()),
        bars_5m=tuple(bars_5m or ()),
        shock_state=shock_state,
        es_volume=es_volume,
        hl_volume=hl_volume,
        session_phase=_session_phase_for(state.as_of),
        event_tags=tags,
        consumed_event_tags=consumed_event_tags,
        previous_state=previous_state,
        previous_state_since=previous_state_since,
        trading_date=trading_date,
        daily_setup_count=daily_setup_count,
        lockout_until=lockout_until,
        data_healthy_since=data_healthy_since,
        watch_exit_since=watch_exit_since,
        settings=settings,
    )


def _ingest_spx_bar_sample(
    builder: SpxBarBuilder,
    state: LatestState,
) -> None:
    quote = state.best_quote("index:SPX")
    if quote is None:
        return
    if not configured_quote_use_decision(quote, as_of=state.as_of).pricing_allowed:
        return
    price = quote.effective_price
    if price is None or price <= 0:
        return
    builder.ingest(_quote_source_at(quote), float(price), quote.provider.value)


def evaluate_steven_cycle(
    state: LatestState,
    *,
    data_root: Path | str | None = None,
    settings: StevenSettings | None = None,
    bar_builder: SpxBarBuilder | None = None,
    persist: bool = True,
) -> dict[str, Any]:
    settings = settings or StevenSettings.from_env()
    if not settings.enabled:
        return {"enabled": False, "skipped": True}
    root = Path(data_root) if data_root is not None else Path(StorageSettings.from_env().data_root)
    previous_payload, reset_reason = load_steven_state(root / "latest" / "steven_state.json")
    exposure = build_exposure_map(state)
    if persist:
        persist_exposure_map(exposure, root)

    builder = bar_builder or SpxBarBuilder()
    if bar_builder is None:
        bars_1m, bars_5m = _load_bars_from_latest(
            root,
            as_of=state.as_of,
            max_age_seconds=settings.bars_source_max_age_seconds,
        )
        for bar in bars_1m:
            builder._closed_1m.append(bar)
        for bar in bars_5m:
            builder._closed_5m.append(bar)
    _ingest_spx_bar_sample(builder, state)
    trading_date = trading_date_et(state.as_of)
    if persist:
        builder.persist(root, as_of=state.as_of, trading_date=trading_date)

    inputs = inputs_from_latest_state(
        state,
        data_root=root,
        exposure=exposure,
        bars_1m=builder.closed_bars_1m(),
        bars_5m=builder.closed_bars_5m(),
        previous_payload=previous_payload,
        settings=settings,
        reset_warning=reset_reason,
    )
    signal = build_steven_signal(inputs)
    warnings = list(signal.warnings)
    if reset_reason:
        warnings.append(f"steven_state_reset:{reset_reason}")
        signal = replace(signal, warnings=tuple(dict.fromkeys(warnings)))

    seq_last = -1
    if isinstance(previous_payload, Mapping) and isinstance(
        previous_payload.get("episode_seq_last"), int
    ):
        seq_last = int(previous_payload["episode_seq_last"])
    if persist:
        seq_last = maybe_append_episode_revision(
            data_root=root,
            trading_date=trading_date,
            signal=signal,
            previous_payload=previous_payload,
            settings=settings,
        )
        persist_steven_state(
            signal,
            data_root=root,
            trading_date=trading_date,
            episode_seq_last=seq_last,
            previous_payload=previous_payload,
            transition_rule=signal.transition_rule,
        )
    return {
        "enabled": True,
        "skipped": False,
        "trading_date": trading_date,
        "machine_state": signal.machine_state,
        "status": signal.status,
        "episode_seq_last": seq_last,
        "contract": signal.to_dict(),
        "warnings": list(signal.warnings),
    }


def load_steven_state_for_alerts(data_root: Path | str | None = None) -> dict[str, Any] | None:
    root = Path(data_root) if data_root is not None else Path(StorageSettings.from_env().data_root)
    payload, _reason = load_steven_state(root / "latest" / "steven_state.json")
    return payload


def validate_contract_dict(contract: Mapping[str, Any]) -> list[str]:
    """Lightweight schema checks without requiring jsonschema dependency."""
    errors: list[str] = []
    required = {
        "schema_version",
        "source",
        "created_at",
        "as_of",
        "status",
        "machine_state",
        "regime",
        "regime_breadth",
        "map",
        "trigger",
        "invalidation",
        "expression_family",
        "confidence",
        "flow_confirmation",
        "data_quality",
        "warnings",
    }
    missing = required - set(contract)
    if missing:
        errors.append(f"missing:{sorted(missing)}")
    if contract.get("schema_version") != CONTRACT_SCHEMA_VERSION:
        errors.append("schema_version")
    if contract.get("source") != CONTRACT_SOURCE:
        errors.append("source")
    if contract.get("status") not in {"observe_only", "watch", "confirmed_for_review", "invalid"}:
        errors.append("status")
    if contract.get("machine_state") not in MACHINE_STATES:
        errors.append("machine_state")
    if contract.get("regime") not in {"bullish", "bearish", "mixed", "unknown"}:
        errors.append("regime")
    if contract.get("expression_family") not in EXPRESSION_FAMILIES:
        errors.append("expression_family")
    if contract.get("confidence") not in {"low", "medium", "high"}:
        errors.append("confidence")
    if set(contract) - required:
        errors.append(f"additionalProperties:{sorted(set(contract) - required)}")
    return errors


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate Steven observe-only guidance.")
    parser.add_argument("--json", action="store_true", help="Print JSON summary.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run even when steven.enabled is false (still observe_only).",
    )
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = StevenSettings.from_env()
    if not settings.enabled and not args.force:
        payload = {"enabled": False, "skipped": True}
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        return 0
    if not settings.enabled and args.force:
        settings = replace(settings, enabled=True)
    storage = StorageSettings.from_env()
    state = LatestStateStore(storage).load()
    result = evaluate_steven_cycle(state, data_root=storage.data_root, settings=settings)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(
            f"Steven {result.get('status')} state={result.get('machine_state')} "
            f"date={result.get('trading_date')}"
        )
    return 0


def main() -> None:
    raise SystemExit(run())
