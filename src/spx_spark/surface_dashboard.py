"""Persistent read-only projection for the SPXW exposure-surface dashboard."""

from __future__ import annotations

import argparse
import json
import math
import os
import signal
import threading
import time
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import FrameType
from typing import Any, Protocol

from spx_spark.analytics.options.pricing import finite_float, option_iv
from spx_spark.config import StorageSettings
from spx_spark.features.exposure_surface import (
    SCHEMA_VERSION as SURFACE_SCHEMA_VERSION,
)
from spx_spark.features.exposure_surface import SurfaceContract, build_exposure_surface
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import OptionRight, Quote, as_utc
from spx_spark.options_map.orchestration import (
    actionable_chain_implied_spot,
    group_spxw_option_quotes,
)
from spx_spark.state_io import atomic_write_json_secure
from spx_spark.storage import LatestState, LatestStateStore, configured_quote_use_decision
from spx_spark.surface_artifact import canonical_sha256


DASHBOARD_SCHEMA_VERSION = 1
DASHBOARD_KIND = "spxw_surface_dashboard"
DEFAULT_INTERVAL_SECONDS = 5.0
DEFAULT_OUTPUT_NAME = "spxw_surface_dashboard.json"
MAX_CHAIN_LEG_SKEW_SECONDS = 5.0


class StopEvent(Protocol):
    def is_set(self) -> bool: ...

    def set(self) -> None: ...

    def wait(self, timeout: float | None = None) -> bool: ...


SurfaceBuilder = Callable[..., Any]


def _finite_number(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    return finite_float(value)


def default_output_path(settings: StorageSettings) -> Path:
    return Path(settings.data_root).expanduser() / "latest" / DEFAULT_OUTPUT_NAME


def resolve_output_path(
    output_path: str | os.PathLike[str] | None,
    settings: StorageSettings,
) -> Path:
    if output_path is None:
        return default_output_path(settings)
    path = Path(output_path).expanduser()
    return path if path.is_absolute() else (Path.cwd() / path).resolve()


def _session_payload(now: datetime) -> dict[str, object]:
    expiries = DEFAULT_MARKET_CALENDAR.research_expiries(now)
    rth_open = DEFAULT_MARKET_CALENDAR.is_rth_open(now)
    spx_gth_open = DEFAULT_MARKET_CALENDAR.is_spx_gth_open(now)
    return {
        "state": "rth" if rth_open else "gth" if spx_gth_open else "closed",
        "rth_open": rth_open,
        "spx_gth_open": spx_gth_open,
        "globex_open": DEFAULT_MARKET_CALENDAR.is_globex_open(now),
        "research_expiries": [expiry.strftime("%Y%m%d") for expiry in expiries],
    }


def _transport_time(quote: Quote) -> datetime:
    return as_utc(
        quote.last_update_at
        or quote.quote_time
        or quote.trade_time
        or quote.received_at
    )


def _fresh_underlier(
    state: LatestState,
    *,
    settings: StorageSettings,
    front_expiry: str,
    front_quotes: list[Quote],
) -> dict[str, object] | None:
    spx_quote = state.best_quote("index:SPX")
    if spx_quote is not None:
        decision = configured_quote_use_decision(
            spx_quote,
            as_of=state.as_of,
            settings=settings,
        )
        price = _finite_number(spx_quote.effective_price)
        if decision.pricing_allowed and price is not None and price > 0:
            source_at = _transport_time(spx_quote)
            return {
                "price": price,
                "source": "index:SPX",
                "provider": spx_quote.provider.value,
                "quality": decision.feed_mode.value,
                "source_at": source_at.isoformat(),
                "age_seconds": round((as_utc(state.as_of) - source_at).total_seconds(), 6),
            }

    selected_chain_state = LatestState(
        created_at=state.created_at,
        as_of=state.as_of,
        quotes=tuple(front_quotes),
        best_quotes=tuple(front_quotes),
        provider_states=state.provider_states,
    )
    implied = actionable_chain_implied_spot(
        selected_chain_state,
        expiry=front_expiry,
        as_of=state.as_of,
        max_leg_skew_seconds=MAX_CHAIN_LEG_SKEW_SECONDS,
    )
    implied_price = _finite_number(implied)
    if implied_price is None or implied_price <= 0:
        return None
    pricing_quotes = [
        quote
        for quote in front_quotes
        if configured_quote_use_decision(quote, as_of=state.as_of).pricing_allowed
    ]
    source_clock = max(
        (_transport_time(quote) for quote in pricing_quotes),
        default=None,
    )
    providers = sorted({quote.provider.value for quote in pricing_quotes})
    return {
        "price": implied_price,
        "source": "chain_implied",
        "provider": (
            providers[0]
            if len(providers) == 1
            else "mixed"
            if providers
            else None
        ),
        "quality": "derived_fresh_pairs",
        "source_at": source_clock.isoformat() if source_clock is not None else None,
        "age_seconds": (
            round((as_utc(state.as_of) - source_clock).total_seconds(), 6)
            if source_clock is not None
            else None
        ),
    }


def _nonnegative(value: object) -> float | None:
    parsed = _finite_number(value)
    return parsed if parsed is not None and parsed >= 0 else None


def _contracts_for_expiry(
    quotes: list[Quote],
    *,
    expiry: str,
    state: LatestState,
    settings: StorageSettings,
) -> tuple[list[SurfaceContract], dict[str, object]]:
    contracts: list[SurfaceContract] = []
    providers: set[str] = set()
    fresh_contracts = 0
    with_open_interest = 0
    with_positive_open_interest = 0
    with_volume = 0
    with_positive_volume = 0
    call_count = 0
    put_count = 0
    strikes: set[float] = set()
    transport_times: list[datetime] = []
    received_times: list[datetime] = []
    structure_times: list[datetime] = []

    for quote in quotes:
        decision = configured_quote_use_decision(
            quote,
            as_of=state.as_of,
            settings=settings,
        )
        if not decision.pricing_allowed:
            continue
        fresh_contracts += 1
        strike = _finite_number(quote.instrument.strike)
        if quote.greeks is not None and isinstance(quote.greeks.implied_vol, bool):
            continue
        iv = option_iv(quote)
        right = quote.instrument.right
        if strike is None or strike <= 0 or iv is None or right is None:
            continue

        open_interest = _nonnegative(quote.open_interest)
        volume = _nonnegative(quote.volume)
        contracts.append(
            SurfaceContract(
                expiry=expiry,
                strike=strike,
                right=right.value,
                iv=iv,
                open_interest=quote.open_interest,
                volume=quote.volume,
            )
        )
        transport_times.append(_transport_time(quote))
        received_times.append(as_utc(quote.received_at))
        structure_times.append(as_utc(quote.structure_time or quote.received_at))
        providers.add(quote.provider.value)
        strikes.add(strike)
        with_open_interest += int(open_interest is not None)
        with_positive_open_interest += int(
            open_interest is not None and open_interest > 0
        )
        with_volume += int(volume is not None)
        with_positive_volume += int(volume is not None and volume > 0)
        call_count += int(right is OptionRight.CALL)
        put_count += int(right is OptionRight.PUT)

    total = len(quotes)
    usable = len(contracts)
    metadata: dict[str, object] = {
        "contract_count": usable,
        "call_count": call_count,
        "put_count": put_count,
        "providers": sorted(providers),
        "coverage": {
            "total_contracts": total,
            "fresh_contracts": fresh_contracts,
            "usable_contracts": usable,
            "usable_ratio": usable / total if total else 0.0,
            "unique_strikes": len(strikes),
            "with_open_interest": with_open_interest,
            "with_positive_open_interest": with_positive_open_interest,
            "with_volume": with_volume,
            "with_positive_volume": with_positive_volume,
        },
        "input_clocks": _input_clock_payload(
            as_of=state.as_of,
            transport_times=transport_times,
            received_times=received_times,
            structure_times=structure_times,
        ),
    }
    return contracts, metadata


def _input_clock_payload(
    *,
    as_of: datetime,
    transport_times: list[datetime],
    received_times: list[datetime],
    structure_times: list[datetime],
) -> dict[str, object]:
    selection_clock = as_utc(as_of)
    known_times = [
        max(transport, received, structure)
        for transport, received, structure in zip(
            transport_times,
            received_times,
            structure_times,
            strict=True,
        )
    ]

    def encoded_max(values: list[datetime]) -> str | None:
        return max(values).isoformat() if values else None

    def maximum_age(values: list[datetime]) -> float | None:
        if not values:
            return None
        return max((selection_clock - value).total_seconds() for value in values)

    return {
        "selection_as_of": selection_clock.isoformat(),
        "contract_clock_count": len(known_times),
        "min_source_at": min(transport_times).isoformat() if transport_times else None,
        "max_source_at": encoded_max(transport_times),
        "max_received_at": encoded_max(received_times),
        "max_structure_at": encoded_max(structure_times),
        "max_known_at": encoded_max(known_times),
        "max_transport_age_seconds": maximum_age(transport_times),
        "max_structure_age_seconds": maximum_age(structure_times),
        "future_clock_count": sum(value > selection_clock for value in known_times),
    }


def _expiry_surface(
    *,
    expiry: str,
    role: str,
    quotes: list[Quote],
    state: LatestState,
    settings: StorageSettings,
    underlier_price: float,
    surface_builder: SurfaceBuilder,
) -> tuple[dict[str, object] | None, str | None]:
    expiry_day = datetime.strptime(expiry, "%Y%m%d").date()
    market_session = DEFAULT_MARKET_CALENDAR.session(expiry_day)
    if market_session is None:
        return None, f"{role}_expiry_session_unavailable"

    contracts, metadata = _contracts_for_expiry(
        quotes,
        expiry=expiry,
        state=state,
        settings=settings,
    )
    if not contracts:
        return None, f"{role}_fresh_iv_contracts_unavailable"

    try:
        surface = surface_builder(
            contracts,
            spot=underlier_price,
            as_of=state.as_of,
            expiry_close=market_session.close_at,
        )
        surface_payload = surface.to_dict()
    except (TypeError, ValueError, ArithmeticError) as exc:
        return None, f"{role}_surface_build_error:{type(exc).__name__}"

    if not isinstance(surface_payload, Mapping):
        return None, f"{role}_surface_invalid_payload"
    surface_payload = dict(surface_payload)
    kernel_quality = str(surface_payload.get("quality") or "unavailable")
    if kernel_quality == "unavailable":
        warnings = surface_payload.get("warnings")
        suffix = ""
        if isinstance(warnings, list | tuple) and warnings:
            suffix = f":{str(warnings[0])}"
        return None, f"{role}_surface_unavailable{suffix}"
    if kernel_quality not in {"ok", "degraded"}:
        return None, f"{role}_surface_invalid_quality:{kernel_quality}"
    strike_ladder = surface_payload.get("strike_ladder")
    if not isinstance(strike_ladder, list) or not all(
        isinstance(row, Mapping) for row in strike_ladder
    ):
        return None, f"{role}_surface_invalid_strike_ladder"

    quality = "ready" if kernel_quality == "ok" else "degraded"
    expiry_warnings = surface_payload.get("warnings")
    expiry_payload = {
        "expiry": expiry,
        "role": role,
        "expiry_close": market_session.close_at.isoformat(),
        **metadata,
        "strike_ladder": strike_ladder,
        "quality": quality,
        "warnings": list(expiry_warnings) if isinstance(expiry_warnings, list | tuple) else [],
        "surface": surface_payload,
    }
    degraded_reason = f"{role}_surface_degraded" if quality == "degraded" else None
    return expiry_payload, degraded_reason


def build_dashboard_snapshot(
    state: LatestState,
    *,
    storage_settings: StorageSettings,
    now: datetime | None = None,
    interval_seconds: float = DEFAULT_INTERVAL_SECONDS,
    surface_builder: SurfaceBuilder = build_exposure_surface,
) -> dict[str, object]:
    """Build one fail-closed front/next-expiry dashboard snapshot."""

    if interval_seconds <= 0 or not math.isfinite(interval_seconds):
        raise ValueError("interval_seconds must be positive and finite")
    created_at = as_utc(now or datetime.now(tz=timezone.utc))
    lease_seconds = interval_seconds * 2.0
    projection = build_surface_projection(
        state,
        storage_settings=storage_settings,
        surface_builder=surface_builder,
    )

    quality = dict(projection["quality"])
    quality.update(
        {
            "refresh_interval_seconds": interval_seconds,
            "lease_seconds": lease_seconds,
        }
    )
    payload: dict[str, object] = {
        "schema_version": DASHBOARD_SCHEMA_VERSION,
        "kind": DASHBOARD_KIND,
        "surface_version": SURFACE_SCHEMA_VERSION,
        "created_at": created_at.isoformat(),
        "as_of": projection["as_of"],
        "valid_until": (created_at + timedelta(seconds=lease_seconds)).isoformat(),
        "status": projection["status"],
        "automatic_ordering": False,
        "session": projection["session"],
        "underlier": projection["underlier"],
        "quality": quality,
        "expiries": projection["expiries"],
        "source_state": {
            "created_at": as_utc(state.created_at).isoformat(),
            "selection_as_of": projection["as_of"],
        },
    }
    payload["artifact_sha256"] = canonical_sha256(payload)
    return payload


def build_surface_projection(
    state: LatestState,
    *,
    storage_settings: StorageSettings,
    surface_builder: SurfaceBuilder = build_exposure_surface,
) -> dict[str, object]:
    """Build the clock-neutral surface body shared by live and replay envelopes."""

    as_of = as_utc(state.as_of)
    session = _session_payload(as_of)
    requested_expiries = list(session["research_expiries"])
    front_expiry = requested_expiries[0]

    # Reuse the options-map quote selection while applying stricter pricing
    # freshness below. ES/MES/SPY are never used as an SPX-strike coordinate.
    grouped = group_spxw_option_quotes(state, storage_settings=storage_settings)
    underlier = _fresh_underlier(
        state,
        settings=storage_settings,
        front_expiry=front_expiry,
        front_quotes=grouped.get(front_expiry, []),
    )

    reasons: list[str] = []
    published: list[dict[str, object]] = []
    if underlier is None:
        reasons.append("underlier_unavailable")
        underlier_payload: dict[str, object] = {
            "price": None,
            "source": None,
            "provider": None,
            "quality": "unavailable",
            "source_at": None,
            "age_seconds": None,
        }
    else:
        underlier_payload = underlier
        underlier_price = float(underlier["price"])
        for role, expiry in zip(("front", "next"), requested_expiries):
            expiry_payload, reason = _expiry_surface(
                expiry=expiry,
                role=role,
                quotes=grouped.get(expiry, []),
                state=state,
                settings=storage_settings,
                underlier_price=underlier_price,
                surface_builder=surface_builder,
            )
            if reason is not None:
                reasons.append(reason)
            if expiry_payload is not None:
                published.append(expiry_payload)

    if not published:
        status = "unavailable"
        published = []
    elif len(published) == len(requested_expiries) and all(
        item["quality"] == "ready" for item in published
    ):
        status = "ready"
    else:
        status = "degraded"

    return {
        "as_of": as_of.isoformat(),
        "status": status,
        "session": session,
        "underlier": underlier_payload,
        "quality": {
            "status": status,
            "reasons": list(dict.fromkeys(reasons)),
            "requested_expiry_count": len(requested_expiries),
            "published_expiry_count": len(published),
        },
        "expiries": published,
    }


def write_dashboard_snapshot(
    payload: Mapping[str, object],
    *,
    output_path: str | os.PathLike[str],
) -> Path:
    """Atomically publish the isolated dashboard projection owner-read-only."""

    path = Path(output_path)
    atomic_write_json_secure(path, payload)
    return path


def run_once(
    *,
    storage_settings: StorageSettings | None = None,
    now: datetime | None = None,
    interval_seconds: float = DEFAULT_INTERVAL_SECONDS,
    output_path: str | os.PathLike[str] | None = None,
) -> dict[str, object]:
    settings = storage_settings or StorageSettings.from_env()
    current = as_utc(now or datetime.now(tz=timezone.utc))
    state = LatestStateStore(settings).load(now=current)
    payload = build_dashboard_snapshot(
        state,
        storage_settings=settings,
        now=current,
        interval_seconds=interval_seconds,
    )
    write_dashboard_snapshot(
        payload,
        output_path=resolve_output_path(output_path, settings),
    )
    return payload


def run_loop(
    *,
    storage_settings: StorageSettings,
    interval_seconds: float,
    output_path: str | os.PathLike[str] | None,
    stop_event: StopEvent,
    max_cycles: int | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    utcnow: Callable[[], datetime] = lambda: datetime.now(tz=timezone.utc),
    emit: Callable[[dict[str, object]], None] | None = None,
) -> int:
    """Run non-overlapping projections on a start-anchored cadence."""

    if interval_seconds <= 0 or not math.isfinite(interval_seconds):
        raise ValueError("interval_seconds must be positive and finite")
    if max_cycles is not None and max_cycles <= 0:
        raise ValueError("max_cycles must be positive when provided")

    cycle = 0
    while not stop_event.is_set():
        cycle += 1
        started = monotonic()
        payload = run_once(
            storage_settings=storage_settings,
            now=utcnow(),
            interval_seconds=interval_seconds,
            output_path=output_path,
        )
        duration = max(monotonic() - started, 0.0)
        if emit is not None:
            emit(
                {
                    "event": "surface_dashboard_published",
                    "cycle": cycle,
                    "status": payload["status"],
                    "as_of": payload["as_of"],
                    "valid_until": payload["valid_until"],
                    "published_expiry_count": payload["quality"][
                        "published_expiry_count"
                    ],
                    "duration_ms": duration * 1000.0,
                }
            )
        if max_cycles is not None and cycle >= max_cycles:
            break
        if stop_event.wait(max(interval_seconds - duration, 0.0)):
            break
    return 0


def install_stop_handlers(stop_event: StopEvent) -> None:
    def request_stop(signum: int, frame: FrameType | None) -> None:  # noqa: ARG001
        stop_event.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish the read-only SPXW exposure-surface dashboard snapshot."
    )
    parser.add_argument("--once", action="store_true", help="Publish one snapshot and exit.")
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=DEFAULT_INTERVAL_SECONDS,
        help="Start-to-start projection cadence (default: 5 seconds).",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        help=(
            "Snapshot path; defaults to "
            "{MARKET_DATA_DATA_ROOT}/latest/spxw_surface_dashboard.json."
        ),
    )
    parser.add_argument("--json", action="store_true", help="Print JSON output.")
    return parser.parse_args(argv)


def _print_json(payload: Mapping[str, object]) -> None:
    print(json.dumps(payload, sort_keys=True, allow_nan=False), flush=True)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.interval_seconds <= 0 or not math.isfinite(args.interval_seconds):
        raise SystemExit("--interval-seconds must be positive and finite")
    settings = StorageSettings.from_env()
    output_path = resolve_output_path(args.output_path, settings)
    if args.once:
        payload = run_once(
            storage_settings=settings,
            interval_seconds=args.interval_seconds,
            output_path=output_path,
        )
        if args.json:
            _print_json(payload)
        else:
            print(f"{payload['status']} {output_path}", flush=True)
        return 0

    stop_event = threading.Event()
    install_stop_handlers(stop_event)
    return run_loop(
        storage_settings=settings,
        interval_seconds=args.interval_seconds,
        output_path=output_path,
        stop_event=stop_event,
        emit=_print_json if args.json else None,
    )


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
