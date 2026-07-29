"""LatestState orchestration for SPXW options maps (not pure analytics)."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from statistics import median

from spx_spark.analytics.options.chain import (
    chain_implied_spot,
    enrich_option_greeks,
    enrich_open_interest,
    is_spxw_option,
    pair_by_strike,
)
from spx_spark.analytics.options.constants import BAD_QUALITIES, UNDERLIER_CANDIDATES, UNDERLIER_MISMATCH_SOURCES
from spx_spark.analytics.options.levels import build_spy_confluence
from spx_spark.analytics.options.models import OptionsMap, UnderlierReference
from spx_spark.analytics.options.quote_policy import analytical_option_quote
from spx_spark.analytics.options.service import build_expiry_map
from spx_spark.config import StorageSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import (
    FUTURE_TIMESTAMP_TOLERANCE_SECONDS,
    OptionRight,
    Provider,
    ProviderStatus,
    Quote,
    as_utc,
)
from spx_spark.storage import (
    LatestState,
    configured_quote_use_decision,
    degrade_stale_quote,
    select_best_quotes,
)

DIRECT_UNDERLIER_SOURCE_MAX_AGE_SECONDS = 90.0


@dataclass(frozen=True)
class _LiveUnderlierObservation:
    price: float
    price_kind: str
    source_at: datetime


def _spx_session(as_of: datetime) -> str:
    if DEFAULT_MARKET_CALENDAR.is_rth_open(as_of):
        return "rth"
    if DEFAULT_MARKET_CALENDAR.is_spx_gth_open(as_of):
        return "gth"
    return "closed"


def select_underlier(state: LatestState) -> UnderlierReference:
    for instrument_id, multiplier in UNDERLIER_CANDIDATES:
        quote = state.best_quote(instrument_id)
        if quote is None or quote.quality in BAD_QUALITIES:
            continue
        decision = configured_quote_use_decision(quote, as_of=state.as_of)
        if not decision.pricing_allowed:
            continue
        observations = _live_underlier_observations(
            quote,
            allow_mark=instrument_id != "index:SPX",
        )
        for observation in observations:
            if not _direct_underlier_is_current(
                quote,
                instrument_id=instrument_id,
                as_of=state.as_of,
                source_at=observation.source_at,
            ):
                continue
            return UnderlierReference(
                price=observation.price * multiplier,
                source=instrument_id,
                source_at=observation.source_at,
                received_at=quote.received_at,
                session=_spx_session(state.as_of),
                freshness="fresh",
                price_kind=observation.price_kind,
                pricing_allowed=True,
            )
    return UnderlierReference(price=None, source=None)


def _direct_underlier_is_current(
    quote: Quote,
    *,
    instrument_id: str,
    as_of: datetime,
    source_at: datetime,
) -> bool:
    # Cash SPX has no authoritative print outside RTH.  GTH maps must use a
    # fresh parity-derived SPX reference; ES/SPY remain mismatch references
    # only and therefore cannot authorize wall/gamma alerts.
    if instrument_id == "index:SPX" and not DEFAULT_MARKET_CALENDAR.is_rth_open(as_of):
        return False
    source_age = (as_utc(as_of) - as_utc(source_at)).total_seconds()
    receipt_age = (as_utc(as_of) - as_utc(quote.received_at)).total_seconds()
    return (
        -FUTURE_TIMESTAMP_TOLERANCE_SECONDS
        <= source_age
        <= DIRECT_UNDERLIER_SOURCE_MAX_AGE_SECONDS
        and -FUTURE_TIMESTAMP_TOLERANCE_SECONDS
        <= receipt_age
        <= DIRECT_UNDERLIER_SOURCE_MAX_AGE_SECONDS
    )


def _live_underlier_observations(
    quote: Quote,
    *,
    allow_mark: bool = False,
) -> tuple[_LiveUnderlierObservation, ...]:
    # Never use a prior close as a live SPX-strike coordinate.  Direct
    # underliers require a genuine last or a valid two-sided midpoint.  A
    # provider mark is derived/ambiguous and cannot establish economic
    # freshness by itself.
    observations: list[_LiveUnderlierObservation] = []
    if quote.last is not None and quote.last > 0 and quote.trade_time is not None:
        observations.append(
            _LiveUnderlierObservation(
                price=float(quote.last),
                price_kind="last",
                source_at=quote.trade_time,
            )
        )
    mid = quote.mid
    if mid is not None and mid > 0 and quote.quote_time is not None:
        observations.append(
            _LiveUnderlierObservation(
                price=float(mid),
                price_kind="mid",
                source_at=quote.quote_time,
            )
        )
    if allow_mark and quote.mark is not None and quote.mark > 0 and quote.quote_time is not None:
        observations.append(
            _LiveUnderlierObservation(
                price=float(quote.mark),
                price_kind="mark_context_only",
                source_at=quote.quote_time,
            )
        )
    return tuple(observations)


def _chain_implied_reference(
    quotes: list[Quote],
    *,
    as_of: datetime,
) -> UnderlierReference | None:
    pairs = _quote_clocked_parity_pairs(quotes, as_of=as_of)
    implied = chain_implied_spot(pairs)
    if implied is None or implied <= 0:
        return None
    source_times: list[datetime] = []
    received_times: list[datetime] = []
    for sides in pairs.values():
        call = sides.get(OptionRight.CALL)
        put = sides.get(OptionRight.PUT)
        if call is None or put is None:
            continue
        for quote in (call, put):
            source_at = quote.quote_time
            if source_at is None:
                return None
            source_times.append(source_at)
            received_times.append(quote.received_at)
    if not source_times or not received_times:
        return None
    source_at = min(source_times)
    received_at = min(received_times)
    source_age = (as_utc(as_of) - as_utc(source_at)).total_seconds()
    receipt_age = (as_utc(as_of) - as_utc(received_at)).total_seconds()
    if not (
        -FUTURE_TIMESTAMP_TOLERANCE_SECONDS
        <= source_age
        <= DIRECT_UNDERLIER_SOURCE_MAX_AGE_SECONDS
        and -FUTURE_TIMESTAMP_TOLERANCE_SECONDS
        <= receipt_age
        <= DIRECT_UNDERLIER_SOURCE_MAX_AGE_SECONDS
    ):
        return None
    return UnderlierReference(
        price=implied,
        source="chain_implied",
        source_at=source_at,
        received_at=received_at,
        session=_spx_session(as_of),
        freshness="fresh",
        price_kind="chain_implied",
        pricing_allowed=True,
    )


def ibkr_provider_unavailable(state: LatestState) -> bool:
    for provider_state in state.provider_states:
        if provider_state.provider != Provider.IBKR:
            continue
        if provider_state.status == ProviderStatus.UNAVAILABLE:
            return True
        if provider_state.status == ProviderStatus.DEGRADED:
            return True
    return False


def actionable_chain_implied_spot(
    state: LatestState,
    *,
    expiry: str,
    as_of: datetime,
    max_leg_skew_seconds: float | None = None,
) -> float | None:
    """SPX spot from fresh, pricing-allowed SPXW call/put parity pairs."""

    quotes = [
        quote
        for quote in state.best_quotes
        if is_spxw_option(quote)
        and (quote.instrument.expiry or "") == expiry
        and configured_quote_use_decision(quote, as_of=as_of).pricing_allowed
    ]
    cofresh_pairs = _quote_clocked_parity_pairs(
        quotes,
        as_of=as_of,
        max_leg_skew_seconds=max_leg_skew_seconds,
    )
    return chain_implied_spot(cofresh_pairs)


def _quote_clocked_parity_pairs(
    quotes: list[Quote],
    *,
    as_of: datetime,
    max_leg_skew_seconds: float | None = None,
) -> dict[float, dict[OptionRight, Quote]]:
    """Keep only two-sided parity pairs clocked by their own NBBO updates."""

    now = as_utc(as_of)
    cofresh_pairs: dict[float, dict[OptionRight, Quote]] = {}
    for strike, sides in pair_by_strike(quotes).items():
        call = sides.get(OptionRight.CALL)
        put = sides.get(OptionRight.PUT)
        if call is None or put is None:
            continue
        if any(
            quote.quote_time is None
            or quote.bid is None
            or quote.mid is None
            or quote.ask is None
            or not 0 < quote.bid <= quote.mid <= quote.ask
            for quote in (call, put)
        ):
            continue
        call_time = as_utc(call.quote_time)
        put_time = as_utc(put.quote_time)
        ages = (
            (now - call_time).total_seconds(),
            (now - put_time).total_seconds(),
        )
        if (
            min(ages) < -FUTURE_TIMESTAMP_TOLERANCE_SECONDS
            or max(ages) > DIRECT_UNDERLIER_SOURCE_MAX_AGE_SECONDS
            or (
                max_leg_skew_seconds is not None
                and abs((call_time - put_time).total_seconds())
                > max_leg_skew_seconds
            )
        ):
            continue
        cofresh_pairs[strike] = sides
    return cofresh_pairs


def actionable_chain_implied_reference(
    state: LatestState,
    *,
    expiry: str,
    as_of: datetime,
    required_provider: Provider | None = None,
    max_age_seconds: float = 15.0,
    max_leg_skew_seconds: float = 5.0,
    min_pair_count: int = 3,
    max_dispersion_points: float = 5.0,
    max_pair_interval_points: float = 5.0,
) -> dict[str, object] | None:
    """Return a strict, provenance-rich parity reference for manual GTH decisions."""

    now = as_utc(as_of)
    candidates: list[dict[str, object]] = []
    for strike, sides in pair_by_strike(
        [
            quote
            for quote in state.best_quotes
            if is_spxw_option(quote)
            and (quote.instrument.expiry or "") == expiry
            and (required_provider is None or quote.provider is required_provider)
            and configured_quote_use_decision(quote, as_of=now).pricing_allowed
        ]
    ).items():
        call = sides.get(OptionRight.CALL)
        put = sides.get(OptionRight.PUT)
        if call is None or put is None or call.provider is not put.provider:
            continue
        call_mid = call.mid
        put_mid = put.mid
        if (
            call.bid is None
            or call_mid is None
            or call.ask is None
            or put.bid is None
            or put_mid is None
            or put.ask is None
            or not 0 < call.bid <= call_mid <= call.ask
            or not 0 < put.bid <= put_mid <= put.ask
        ):
            continue
        parity_lower = float(strike) + call.bid - put.ask
        parity_upper = float(strike) + call.ask - put.bid
        if parity_upper - parity_lower > max_pair_interval_points:
            continue
        # Bid/ask parity must be clocked by the quote observation itself.
        # A new last trade cannot freshen an older or unclocked NBBO.
        call_source = call.quote_time
        put_source = put.quote_time
        call_transport = call.last_update_at or call.received_at
        put_transport = put.last_update_at or put.received_at
        if call_source is None or put_source is None:
            continue
        source_ages = (
            (now - as_utc(call_source)).total_seconds(),
            (now - as_utc(put_source)).total_seconds(),
        )
        transport_ages = (
            (now - as_utc(call_transport)).total_seconds(),
            (now - as_utc(put_transport)).total_seconds(),
        )
        if (
            min(source_ages) < -FUTURE_TIMESTAMP_TOLERANCE_SECONDS
            or max(source_ages) > max_age_seconds
            or min(transport_ages) < -FUTURE_TIMESTAMP_TOLERANCE_SECONDS
            or max(transport_ages) > max_age_seconds
            or abs((as_utc(call_source) - as_utc(put_source)).total_seconds())
            > max_leg_skew_seconds
            or abs((as_utc(call_transport) - as_utc(put_transport)).total_seconds())
            > max_leg_skew_seconds
        ):
            continue
        candidates.append(
            {
                "distance": abs(call_mid - put_mid),
                "price": float(strike) + call_mid - put_mid,
                "lower_bound": parity_lower,
                "upper_bound": parity_upper,
                "strike": float(strike),
                "provider": call.provider.value,
                "source_at": min(as_utc(call_source), as_utc(put_source)),
                "transport_at": min(as_utc(call_transport), as_utc(put_transport)),
                "call": {
                    "bid": call.bid,
                    "mid": call_mid,
                    "ask": call.ask,
                    "source_at": as_utc(call_source).isoformat(),
                    "transport_at": as_utc(call_transport).isoformat(),
                },
                "put": {
                    "bid": put.bid,
                    "mid": put_mid,
                    "ask": put.ask,
                    "source_at": as_utc(put_source).isoformat(),
                    "transport_at": as_utc(put_transport).isoformat(),
                },
            }
        )
    if len(candidates) < min_pair_count:
        return None
    selected = sorted(candidates, key=lambda item: float(item["distance"]))[
        : min(5, len(candidates))
    ]
    selected_source_times = [item["source_at"] for item in selected]
    selected_transport_times = [item["transport_at"] for item in selected]
    if (
        max(selected_source_times) - min(selected_source_times)
    ).total_seconds() > max_leg_skew_seconds or (
        max(selected_transport_times) - min(selected_transport_times)
    ).total_seconds() > max_leg_skew_seconds:
        return None
    values = [float(item["price"]) for item in selected]
    dispersion = max(values) - min(values)
    if dispersion > max_dispersion_points:
        return None
    center = median(values)
    median_absolute_deviation = median(abs(value - center) for value in values)
    lower_bound = min(float(item["lower_bound"]) for item in selected)
    upper_bound = max(float(item["upper_bound"]) for item in selected)
    uncertainty = max(center - lower_bound, upper_bound - center)
    return {
        "kind": "chain_implied_spx",
        "instrument_id": "synthetic:SPXW_PARITY",
        "price": center,
        "expiry": expiry,
        "provider": (
            required_provider.value
            if required_provider is not None
            else str(selected[0]["provider"])
        ),
        "pair_count": len(candidates),
        "selected_pair_count": len(selected),
        "dispersion_points": dispersion,
        "median_absolute_deviation_points": median_absolute_deviation,
        "uncertainty_points": uncertainty,
        "lower_bound": lower_bound,
        "upper_bound": upper_bound,
        "selected_pairs": [
            {
                **{
                    key: item[key]
                    for key in (
                        "strike",
                        "price",
                        "lower_bound",
                        "upper_bound",
                        "provider",
                        "call",
                        "put",
                    )
                },
                "source_at": item["source_at"].isoformat(),
                "transport_at": item["transport_at"].isoformat(),
            }
            for item in selected
        ],
        "source_at": min(item["source_at"] for item in selected).isoformat(),
        "transport_at": min(item["transport_at"] for item in selected).isoformat(),
        "as_of": now.isoformat(),
        "method": "median_tightest_put_call_parity_pairs",
    }


def group_spxw_option_quotes(
    state: LatestState,
    *,
    storage_settings: StorageSettings | None = None,
) -> dict[str, list[Quote]]:
    ibkr_down = ibkr_provider_unavailable(state)
    settings = storage_settings or StorageSettings.from_env()
    core_analytical_max_age = (
        settings.rotation_stale_after_seconds
        if DEFAULT_MARKET_CALENDAR.is_spx_gth_open(state.as_of)
        else settings.latest_stale_after_seconds
    )
    structural_candidates = tuple(
        degrade_stale_quote(
            quote,
            as_of=state.as_of,
            stale_after_seconds=settings.latest_stale_after_seconds,
            delayed_stale_after_seconds=settings.delayed_stale_after_seconds,
            slow_stale_after_seconds=settings.slow_index_stale_after_seconds,
            slow_labels=settings.slow_index_labels,
            rotation_stale_after_seconds=settings.rotation_stale_after_seconds,
        )
        for quote in state.quotes
        if is_spxw_option(quote)
    )
    analytical_candidates = tuple(
        analytical_option_quote(
            quote,
            as_of=state.as_of,
            core_max_age_seconds=core_analytical_max_age,
            rotation_max_age_seconds=settings.rotation_stale_after_seconds,
        )
        for quote in structural_candidates
    )
    candidates = tuple(
        quote
        for quote in analytical_candidates
        if not (quote.provider == Provider.IBKR and ibkr_down)
    )
    selected = enrich_option_greeks(
        enrich_open_interest(
            select_best_quotes(
                candidates,
                as_of=state.as_of,
                provider_priority=settings.provider_priority,
                failover_mode=state.failover_mode,
            ),
            structural_candidates,
        ),
        tuple(
            quote
            for quote in structural_candidates
            if not (quote.provider == Provider.IBKR and ibkr_down)
        ),
        as_of=state.as_of,
        core_max_age_seconds=core_analytical_max_age,
        rotation_max_age_seconds=settings.rotation_stale_after_seconds,
    )
    grouped: dict[str, list[Quote]] = defaultdict(list)
    for quote in selected:
        expiry = quote.instrument.expiry or "unknown"
        grouped[expiry].append(quote)
    return grouped


def build_options_map(
    state: LatestState,
    *,
    storage_settings: StorageSettings | None = None,
) -> OptionsMap:
    underlier = select_underlier(state)
    all_grouped = group_spxw_option_quotes(state, storage_settings=storage_settings)
    active_expiries = {
        expiry.strftime("%Y%m%d")
        for expiry in DEFAULT_MARKET_CALENDAR.research_expiries(state.as_of)
    }
    grouped = {
        expiry: quotes for expiry, quotes in all_grouped.items() if expiry in active_expiries
    }

    warnings: list[str] = []
    if set(all_grouped) - set(grouped):
        warnings.append("expired SPXW option rows suppressed after research rollover")
    underlier_mismatch = (
        underlier.source is not None and underlier.source in UNDERLIER_MISMATCH_SOURCES
    )
    if (underlier.price is None or underlier_mismatch) and grouped:
        front_expiry = sorted(grouped)[0]
        implied_reference = _chain_implied_reference(
            grouped[front_expiry],
            as_of=state.as_of,
        )
        implied = implied_reference.price if implied_reference is not None else None
        reference = underlier.price
        implied_plausible = implied is not None and (
            reference is None or abs(implied / reference - 1.0) <= 0.02
        )
        if implied_plausible:
            underlier = implied_reference
            underlier_mismatch = False
    if underlier.price is None:
        warnings.append("missing SPX underlier reference")
    elif underlier_mismatch:
        warnings.append(
            "underlier_mismatch: using "
            f"{underlier.source} price for SPX strikes; wall/gamma alerts suppressed"
        )
    if not grouped:
        warnings.append("missing SPXW option quotes")
    if ibkr_provider_unavailable(state):
        warnings.append("IBKR feed unavailable; stale SPXW option quotes suppressed")

    expiries = tuple(
        build_expiry_map(
            expiry,
            quotes,
            underlier.price,
            as_of=state.as_of,
            underlier_mismatch=underlier_mismatch,
        )
        for expiry, quotes in sorted(grouped.items())
    )
    front_spxw = expiries[0] if expiries else None
    spy_quote = state.best_quote("equity:SPY")
    spx_quote = state.best_quote("index:SPX")
    spy_confluence = build_spy_confluence(
        state.best_quotes,
        front_spxw,
        as_of=state.as_of,
        spy_underlier=spy_quote.effective_price if spy_quote is not None else None,
        spx_underlier=spx_quote.effective_price if spx_quote is not None else None,
    )
    return OptionsMap(
        created_at=datetime.now(tz=state.as_of.tzinfo),
        as_of=state.as_of,
        underlier=underlier,
        expiries=expiries,
        warnings=tuple(dict.fromkeys(warnings)),
        spy_confluence=spy_confluence,
    )
