"""Production OptionsAnalyticsKernel: MarketSnapshot → formal AnalyticsResult."""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone

from spx_spark.analytics.options.chain import (
    chain_implied_spot,
    enrich_open_interest,
    is_spxw_option,
    pair_by_strike,
)
from spx_spark.analytics.options.constants import (
    BAD_QUALITIES,
    UNDERLIER_CANDIDATES,
    UNDERLIER_MISMATCH_SOURCES,
)
from spx_spark.analytics.options.models import (
    DensityQuality,
    OptionsMap,
    UnderlierReference,
)
from spx_spark.analytics.options.pricing import option_mid
from spx_spark.analytics.options.service import build_expiry_map
from spx_spark.domain.analytics import (
    AnalyticsDiagnostics,
    AnalyticsResult,
    AnalyticsStatus,
)
from spx_spark.domain.market import MarketSnapshot
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import OptionRight, Provider, ProviderStatus, Quote
from spx_spark.options_map.orchestration import select_underlier
from spx_spark.settings.analytics import AnalyticsSettings
from spx_spark.storage import LatestState, select_best_quotes


MODEL_VERSIONS = {
    "options_kernel": "1",
    "density": "breeden_litzenberger_v1",
    "greeks": "vendor_plus_bs_gamma_v1",
}


def _as_quotes(snapshot: MarketSnapshot) -> tuple[Quote, ...]:
    return tuple(item for item in snapshot.quotes if isinstance(item, Quote))


def snapshot_to_latest_state(
    snapshot: MarketSnapshot,
    *,
    policy: AnalyticsSettings | None = None,
) -> LatestState:
    """Project a domain MarketSnapshot into LatestState for options orchestration."""

    quotes = _as_quotes(snapshot)
    configured = (policy or AnalyticsSettings()).provider_priority
    provider_priority = (
        *configured,
        "hyperliquid",
        "polymarket",
        "internal",
        "mock",
    )
    best = select_best_quotes(quotes, as_of=snapshot.as_of, provider_priority=provider_priority)
    return LatestState(
        created_at=snapshot.received_at,
        as_of=snapshot.as_of,
        quotes=quotes,
        best_quotes=best,
        provider_states=tuple(snapshot.provider_states),
    )


def _ibkr_down(state: LatestState) -> bool:
    for provider_state in state.provider_states:
        if provider_state.provider != Provider.IBKR:
            continue
        if provider_state.status == ProviderStatus.UNAVAILABLE:
            return True
        if (
            provider_state.status == ProviderStatus.DEGRADED
            and provider_state.connected is not True
        ):
            return True
    return False


def build_options_map_from_snapshot(
    snapshot: MarketSnapshot,
    *,
    policy: AnalyticsSettings | None = None,
) -> OptionsMap:
    """Compose OptionsMap from snapshot quotes via analytics/options service."""

    policy = policy or AnalyticsSettings()
    state = snapshot_to_latest_state(snapshot, policy=policy)
    underlier = select_underlier(state)
    ibkr_down = _ibkr_down(state)
    structural_candidates = [
        quote for quote in state.quotes if is_spxw_option(quote)
    ]
    candidates = [
        quote
        for quote in structural_candidates
        if not (quote.provider == Provider.IBKR and ibkr_down)
    ]
    selected = enrich_open_interest(
        select_best_quotes(
            candidates,
            as_of=state.as_of,
            provider_priority=policy.provider_priority,
        ),
        structural_candidates,
    )
    all_grouped: dict[str, list[Quote]] = defaultdict(list)
    for quote in selected:
        expiry = quote.instrument.expiry or "unknown"
        all_grouped[expiry].append(quote)

    active = {
        expiry.strftime("%Y%m%d")
        for expiry in DEFAULT_MARKET_CALENDAR.research_expiries(state.as_of)
    }
    grouped = {
        expiry: quotes for expiry, quotes in all_grouped.items() if expiry in active
    }

    warnings: list[str] = []
    if set(all_grouped) - set(grouped):
        warnings.append("expired SPXW option rows suppressed after research rollover")
    underlier_mismatch = (
        underlier.source is not None and underlier.source in UNDERLIER_MISMATCH_SOURCES
    )
    if (underlier.price is None or underlier_mismatch) and grouped:
        front_expiry = sorted(grouped)[0]
        implied = chain_implied_spot(pair_by_strike(grouped[front_expiry]))
        reference = underlier.price
        implied_plausible = implied is not None and (
            reference is None
            or abs(implied / reference - 1.0)
            <= policy.underlier_reference_tolerance_fraction
        )
        if implied_plausible:
            underlier = UnderlierReference(price=implied, source="chain_implied")
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
    if ibkr_down:
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
    return OptionsMap(
        created_at=datetime.now(tz=state.as_of.tzinfo or timezone.utc),
        as_of=state.as_of,
        underlier=underlier,
        expiries=expiries,
        warnings=tuple(dict.fromkeys(warnings)),
        spy_confluence=None,
    )


def front_month_status(options_map: OptionsMap) -> AnalyticsStatus:
    """Explicit front-month success gate (not merely non-empty / non-throwing)."""

    if not options_map.expiries:
        return AnalyticsStatus.FAILED
    front = options_map.expiries[0]
    if front.option_count <= 0 or front.strike_count <= 0:
        return AnalyticsStatus.FAILED
    if options_map.underlier is None or options_map.underlier.price is None:
        return AnalyticsStatus.FAILED
    if front.coverage.with_mid <= 0:
        return AnalyticsStatus.FAILED

    density = front.rn_density
    if density is None:
        return AnalyticsStatus.DEGRADED
    if density.quality is DensityQuality.OK:
        return AnalyticsStatus.SUCCESS
    if density.quality in {
        DensityQuality.NOISY_QUOTES,
        DensityQuality.NARROW_RANGE,
    }:
        return AnalyticsStatus.DEGRADED
    return AnalyticsStatus.FAILED


def _usable_strike_metrics(
    quotes: list[Quote],
    *,
    underlier: float | None,
) -> tuple[int, float, int, int]:
    """Return usable_strikes, two_sided_ratio, lower_wing, upper_wing."""

    pairs = pair_by_strike(quotes)
    usable_strikes = 0
    two_sided = 0
    strikes: list[float] = []
    for strike, sides in pairs.items():
        call_mid = option_mid(sides.get(OptionRight.CALL))
        put_mid = option_mid(sides.get(OptionRight.PUT))
        if call_mid is None and put_mid is None:
            continue
        usable_strikes += 1
        strikes.append(strike)
        if call_mid is not None and put_mid is not None:
            two_sided += 1
    ratio = (two_sided / usable_strikes) if usable_strikes else 0.0
    if underlier is None or not strikes:
        return usable_strikes, ratio, 0, 0
    lower = sum(1 for strike in strikes if strike < underlier)
    upper = sum(1 for strike in strikes if strike > underlier)
    return usable_strikes, ratio, lower, upper


@dataclass(frozen=True)
class ChainFreshnessThresholds:
    max_age_seconds: float = 15.0
    min_usable_strikes: int = 21
    min_two_sided_ratio: float = 0.80
    min_wing_strikes_each_side: int = 8

    @classmethod
    def from_settings(cls, settings: AnalyticsSettings | None) -> "ChainFreshnessThresholds":
        if settings is None:
            return cls()
        return cls(
            max_age_seconds=settings.max_chain_age_seconds,
            min_usable_strikes=settings.min_usable_strikes,
            min_two_sided_ratio=settings.min_two_sided_ratio,
            min_wing_strikes_each_side=settings.min_wing_strikes_each_side,
        )


def evaluate_front_chain_fresh(
    snapshot: MarketSnapshot,
    *,
    now: datetime | None = None,
    thresholds: ChainFreshnessThresholds | None = None,
) -> bool:
    """Front-month SPXW freshness: age, usable strikes, two-sided rate, wing width."""

    thresholds = thresholds or ChainFreshnessThresholds()
    now = now or datetime.now(tz=timezone.utc)
    options = [quote for quote in _as_quotes(snapshot) if is_spxw_option(quote)]
    if not options:
        return False

    by_expiry: dict[str, list[Quote]] = defaultdict(list)
    for quote in options:
        expiry = quote.instrument.expiry or ""
        if expiry:
            by_expiry[expiry].append(quote)
    if not by_expiry:
        return False
    front_expiry = sorted(by_expiry)[0]
    fresh_quotes: list[Quote] = []
    for quote in by_expiry[front_expiry]:
        if quote.quality in BAD_QUALITIES:
            continue
        age_ms = quote.quote_age_ms(now)
        if age_ms is None or age_ms / 1000.0 > thresholds.max_age_seconds:
            continue
        fresh_quotes.append(quote)
    if not fresh_quotes:
        return False
    liveish = list(select_best_quotes(fresh_quotes, as_of=now))
    underlier_price = None
    for instrument_id, multiplier in UNDERLIER_CANDIDATES:
        for quote in _as_quotes(snapshot):
            if quote.instrument.canonical_id != instrument_id:
                continue
            if quote.quality in BAD_QUALITIES:
                continue
            price = quote.effective_price
            if price is not None and price > 0:
                underlier_price = price * multiplier
                break
        if underlier_price is not None:
            break

    usable, two_sided_ratio, lower, upper = _usable_strike_metrics(
        liveish, underlier=underlier_price
    )
    if usable < thresholds.min_usable_strikes:
        return False
    if two_sided_ratio < thresholds.min_two_sided_ratio:
        return False
    if (
        lower < thresholds.min_wing_strikes_each_side
        or upper < thresholds.min_wing_strikes_each_side
    ):
        return False
    return True


@dataclass
class OptionsAnalyticsKernel:
    """Wire analytics/options service + density into the RealtimeEngine port."""

    policy: AnalyticsSettings | None = None

    def compute(self, snapshot: MarketSnapshot, *, now: datetime) -> AnalyticsResult:
        started = time.perf_counter()
        warnings: list[str] = []
        try:
            options_map = build_options_map_from_snapshot(snapshot, policy=self.policy)
            status = front_month_status(options_map)
            warnings.extend(options_map.warnings)
            if status is not AnalyticsStatus.SUCCESS:
                warnings.append(f"front_month_status={status.value}")
            input_legs = sum(expiry.option_count for expiry in options_map.expiries)
            usable_legs = sum(
                expiry.coverage.with_mid for expiry in options_map.expiries
            )
            return AnalyticsResult(
                schema_version=1,
                result_id=f"an:{snapshot.snapshot_id}",
                input_snapshot_id=snapshot.snapshot_id,
                computed_at=now,
                underlier=options_map.underlier,
                expiries=options_map.expiries,
                diagnostics=AnalyticsDiagnostics(
                    input_legs=input_legs,
                    usable_legs=usable_legs,
                    duration_ms=(time.perf_counter() - started) * 1000.0,
                    warnings=tuple(dict.fromkeys(warnings)),
                    model_versions=MODEL_VERSIONS,
                ),
                status=status,
            )
        except Exception as exc:  # noqa: BLE001 — failed analytics, not engine crash
            return AnalyticsResult(
                schema_version=1,
                result_id=f"an:{snapshot.snapshot_id}:failed",
                input_snapshot_id=snapshot.snapshot_id,
                computed_at=now,
                underlier=None,
                expiries=(),
                diagnostics=AnalyticsDiagnostics(
                    input_legs=len(snapshot.quotes),
                    usable_legs=0,
                    duration_ms=(time.perf_counter() - started) * 1000.0,
                    warnings=(f"options_kernel_error:{type(exc).__name__}:{exc}",),
                    model_versions=MODEL_VERSIONS,
                ),
                status=AnalyticsStatus.FAILED,
            )
