"""Session-aware quote normalization for non-execution option analytics."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from collections.abc import Mapping
from typing import Any

from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import (
    FUTURE_TIMESTAMP_TOLERANCE_SECONDS,
    InstrumentType,
    MarketDataQuality,
    Provider,
    Quote,
    as_utc,
    parse_timestamp,
    quality_from_market_data_type,
)


ANALYTICAL_CORE_LANE = "core"
ANALYTICAL_ROTATION_LANE = "rotation"


def _explicit_delayed_flag(raw: Mapping[str, Any] | None) -> bool | None:
    """Read a provider delay entitlement without treating absence as live."""

    if not isinstance(raw, Mapping):
        return None
    saw_false = False
    for key, value in raw.items():
        key_text = str(key).lower()
        # Provider payloads contain dozens of unrelated provenance keys.  Keep
        # accepting punctuation/case variants, but do not normalize every key
        # on every analytical pass.
        if "delay" not in key_text and not all(
            character in key_text for character in "delay"
        ):
            continue
        normalized = "".join(
            character for character in key_text if character.isalnum()
        )
        if normalized not in {"isdelayed", "delayed"}:
            continue
        parsed: bool | None = None
        if isinstance(value, bool):
            parsed = value
        elif isinstance(value, (int, float)) and value in {0, 1}:
            parsed = bool(value)
        elif isinstance(value, str):
            text = value.strip().lower()
            if text in {"true", "1", "yes"}:
                parsed = True
            elif text in {"false", "0", "no"}:
                parsed = False
        if parsed is True:
            return True
        if parsed is False:
            saw_false = True
    return False if saw_false else None


def _bool_or_none(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in {0, 1}:
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    return None


def option_field_provider(quote: Quote, *, field: str) -> Provider:
    """Return a field's real provider after independent-field enrichment."""

    raw = quote.raw if isinstance(quote.raw, Mapping) else {}
    value = raw.get(f"{field}_provider")
    if value is not None:
        try:
            return Provider(str(value))
        except ValueError:
            return Provider.UNKNOWN
    return quote.provider


def option_field_sampling_mode(quote: Quote, *, field: str) -> str | None:
    """Return field-specific sampling without borrowing another field's lane."""

    raw = quote.raw if isinstance(quote.raw, Mapping) else {}
    value = raw.get(f"{field}_sampling_mode")
    if value is not None:
        return str(value)
    if option_field_provider(quote, field=field) is quote.provider:
        return quote.sampling_mode
    return None


def option_field_market_data_type(
    quote: Quote,
    *,
    field: str,
) -> str | int | None:
    """Return field-specific feed mode without top-level cross-provider leakage."""

    raw = quote.raw if isinstance(quote.raw, Mapping) else {}
    value = raw.get(f"{field}_market_data_type")
    if isinstance(value, (str, int)):
        return value
    if option_field_provider(quote, field=field) is quote.provider:
        return quote.market_data_type
    return None


def option_field_explicit_delayed(quote: Quote, *, field: str) -> bool | None:
    """Return the provider's explicit delayed flag for exactly one field."""

    raw = quote.raw if isinstance(quote.raw, Mapping) else {}
    for suffix in ("explicit_delayed", "is_delayed", "delayed"):
        value = _bool_or_none(raw.get(f"{field}_{suffix}"))
        if value is not None:
            return value
    if option_field_provider(quote, field=field) is quote.provider:
        return _explicit_delayed_flag(raw)
    return None


def _option_field_live_entitlement(
    quote: Quote,
    *,
    field: str,
) -> tuple[bool, str | None]:
    raw = quote.raw if isinstance(quote.raw, Mapping) else {}
    provider = option_field_provider(quote, field=field)
    market_data_type = option_field_market_data_type(quote, field=field)
    feed_mode = quality_from_market_data_type(market_data_type)
    explicit_delayed = option_field_explicit_delayed(quote, field=field)
    persisted_entitlement = _bool_or_none(raw.get(f"{field}_live_entitlement"))
    persisted_source = raw.get(f"{field}_live_entitlement_source")

    # Negative feed evidence always wins. In particular a stale top-level
    # quality may still carry an explicitly live field, but delayed/frozen may
    # never be upgraded merely because another layer stamped quality=LIVE.
    if explicit_delayed is True:
        return False, "explicit_delayed"
    if feed_mode in {
        MarketDataQuality.FROZEN,
        MarketDataQuality.DELAYED,
        MarketDataQuality.DELAYED_FROZEN,
    }:
        return False, f"market_data_type_{feed_mode.value}"
    if persisted_entitlement is False:
        return False, str(persisted_source or "persisted_not_live")
    if provider is quote.provider and quote.quality in {
        MarketDataQuality.FROZEN,
        MarketDataQuality.DELAYED,
        MarketDataQuality.DELAYED_FROZEN,
    }:
        return False, f"quality_{quote.quality.value}"

    if explicit_delayed is False:
        return (
            True,
            "schwab_explicit_not_delayed"
            if provider is Provider.SCHWAB
            else "explicit_not_delayed",
        )
    if feed_mode is MarketDataQuality.LIVE:
        return True, "market_data_type_live"
    if persisted_entitlement is True:
        return True, str(persisted_source or "persisted_live")
    if provider is quote.provider and quote.quality is MarketDataQuality.LIVE:
        return True, "quality_live"
    return False, None


def option_field_live_entitlement(quote: Quote, *, field: str) -> bool:
    """Whether one field has affirmative, non-delayed live entitlement."""

    return _option_field_live_entitlement(quote, field=field)[0]


def option_field_live_entitlement_source(
    quote: Quote,
    *,
    field: str,
) -> str | None:
    return _option_field_live_entitlement(quote, field=field)[1]


def option_analytical_lane(quote: Quote, *, field: str = "pricing") -> str:
    """Return the bounded freshness lane for one option field.

    Pricing and Greeks may come from different providers after analytical
    enrichment.  The field-specific sampling mode in ``raw`` therefore wins
    over the quote's top-level pricing sampling mode.
    """

    sampling_mode = str(option_field_sampling_mode(quote, field=field) or "")
    return (
        ANALYTICAL_ROTATION_LANE
        if "rotation" in sampling_mode.lower()
        else ANALYTICAL_CORE_LANE
    )


def option_field_observed_at(quote: Quote, *, field: str) -> datetime:
    """Return the receipt clock for pricing, Greeks, or open interest."""

    raw = quote.raw if isinstance(quote.raw, Mapping) else {}
    key = {
        "pricing": "pricing_observed_at",
        "greeks": "greeks_observed_at",
        "open_interest": "open_interest_observed_at",
    }.get(field)
    explicit = parse_timestamp(raw.get(key)) if key else None
    if explicit is not None:
        return as_utc(explicit)
    if (
        field in {"greeks", "open_interest"}
        and option_field_provider(quote, field=field) is not quote.provider
    ):
        # The top-level clocks belong to pricing (or another merged field).
        # Missing cross-provider field provenance must age out, never inherit
        # the current process flush time.
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    if (
        quote.provider is Provider.IBKR
        and quote.sampling_mode is None
        and not raw.get(f"{field}_sampling_mode")
    ):
        # Legacy IBKR rows predate explicit core/rotation labels. Preserve
        # their historical source-clock contract instead of treating receipt
        # time as a subscription re-confirmation.
        return as_utc(quote.quote_time or quote.trade_time or quote.received_at)
    if field in {"greeks", "open_interest"} and quote.structure_time is not None:
        return as_utc(quote.structure_time)
    if (
        field in {"greeks", "open_interest"}
        and quote.provider is Provider.IBKR
        and quote.sampling_mode is not None
    ):
        # Every current IBKR stream row must carry an independent field clock
        # (raw provenance or a structural timestamp). Missing provenance is a
        # contract failure, not evidence of observation at flush time.
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    return as_utc(quote.last_update_at or quote.received_at)


def option_field_age_seconds(
    quote: Quote,
    *,
    as_of: datetime,
    field: str,
) -> float:
    return (as_utc(as_of) - option_field_observed_at(quote, field=field)).total_seconds()


def option_analytical_max_age_seconds(
    quote: Quote,
    *,
    core_max_age_seconds: float,
    rotation_max_age_seconds: float,
    field: str = "pricing",
) -> float:
    return (
        rotation_max_age_seconds
        if option_analytical_lane(quote, field=field) == ANALYTICAL_ROTATION_LANE
        else core_max_age_seconds
    )


def option_analytical_pricing_allowed(quote: Quote) -> bool:
    """Accept a normalized option NBBO for read-only analytics only.

    This is deliberately separate from ``configured_quote_use_decision``:
    analytical clones must remain non-executable even when their bounded
    observation clock is current.  Consumers using this helper must therefore
    be read-only projections and may not turn the result into an order price.
    """

    raw = quote.raw if isinstance(quote.raw, Mapping) else {}
    return bool(
        quote.instrument.instrument_type is InstrumentType.OPTION
        and raw.get("analytical_only") is True
        and "analytical_rejection_reason" in raw
        and raw.get("analytical_rejection_reason") is None
        and raw.get("nbbo_interpolated") is False
        and option_field_live_entitlement(quote, field="pricing")
        and quote.mid is not None
    )


def option_analytical_iv_allowed(quote: Quote) -> bool:
    """Accept contract-valid IV/Greeks for read-only analytical surfaces."""

    raw = quote.raw if isinstance(quote.raw, Mapping) else {}
    return bool(
        option_analytical_pricing_allowed(quote)
        and raw.get("greeks_analytical_allowed") is True
        and option_field_live_entitlement(quote, field="greeks")
        and quote.greeks is not None
    )


def analytical_option_quote(
    quote: Quote,
    *,
    as_of: datetime,
    core_max_age_seconds: float,
    rotation_max_age_seconds: float,
) -> Quote:
    """Normalize one bounded analytical quote without granting execution use.

    ``observed_at`` means the provider most recently confirmed the field.
    ``source_at`` remains the exchange's last-change clock.  A quiet REST
    snapshot can therefore remain analytically current even when its price did
    not change, while a sparse stream delta ages from its actual receipt.  The
    returned clone is only for option analytics and is marked accordingly in
    ``raw``; the canonical latest-state quote is never mutated.
    """

    if quote.instrument.instrument_type is not InstrumentType.OPTION:
        return quote
    lane = option_analytical_lane(quote)
    maximum = option_analytical_max_age_seconds(
        quote,
        core_max_age_seconds=core_max_age_seconds,
        rotation_max_age_seconds=rotation_max_age_seconds,
    )
    observed_at = option_field_observed_at(quote, field="pricing")
    source_at = as_utc(quote.quote_time or quote.trade_time or quote.received_at)
    observed_age = (as_utc(as_of) - observed_at).total_seconds()
    source_age = (as_utc(as_of) - source_at).total_seconds()
    pricing_provider = option_field_provider(quote, field="pricing")
    pricing_sampling_mode = option_field_sampling_mode(quote, field="pricing")
    pricing_market_data_type = option_field_market_data_type(quote, field="pricing")
    pricing_explicit_delayed = option_field_explicit_delayed(
        quote,
        field="pricing",
    )
    live_feed, live_entitlement_source = _option_field_live_entitlement(
        quote,
        field="pricing",
    )
    raw: dict[str, Any] = dict(quote.raw or {})
    raw.update(
        {
            "analytical_only": True,
            "analytical_lane": lane,
            "analytical_max_age_seconds": maximum,
            "analytical_original_quality": quote.quality.value,
            "pricing_provider": pricing_provider.value,
            "pricing_sampling_mode": pricing_sampling_mode,
            "pricing_market_data_type": pricing_market_data_type,
            "pricing_explicit_delayed": pricing_explicit_delayed,
            "pricing_live_entitlement": live_feed,
            "pricing_live_entitlement_source": live_entitlement_source,
            "pricing_observed_at": observed_at.isoformat(),
            "pricing_source_at": source_at.isoformat(),
            "pricing_observed_age_seconds": observed_age,
            "pricing_source_age_seconds": source_age,
            "nbbo_interpolated": False,
        }
    )
    valid = (
        quote.quality not in {MarketDataQuality.MISSING, MarketDataQuality.ERROR}
        and live_feed
        and quote.has_price
        and -FUTURE_TIMESTAMP_TOLERANCE_SECONDS <= observed_age <= maximum
        and source_age >= -FUTURE_TIMESTAMP_TOLERANCE_SECONDS
    )
    if not valid:
        if observed_age < -FUTURE_TIMESTAMP_TOLERANCE_SECONDS:
            reason = "pricing_observed_at_in_future"
        elif observed_age > maximum:
            reason = f"{lane}_pricing_observation_stale"
        elif source_age < -FUTURE_TIMESTAMP_TOLERANCE_SECONDS:
            reason = "pricing_source_at_in_future"
        elif not live_feed:
            reason = "pricing_feed_not_live"
        elif not quote.has_price:
            reason = "pricing_missing"
        else:
            reason = f"pricing_quality_{quote.quality.value}"
        raw["analytical_rejection_reason"] = reason
        return replace(quote, raw=raw)
    raw["analytical_rejection_reason"] = None
    return replace(quote, quality=MarketDataQuality.LIVE, raw=raw)


def gth_analytical_quote(
    quote: Quote,
    *,
    as_of: datetime,
    max_age_seconds: float,
) -> Quote:
    """Treat a recent IBKR rotation row as live for analytics, never execution."""

    if (
        not DEFAULT_MARKET_CALENDAR.is_spx_gth_open(as_of)
        or quote.provider is not Provider.IBKR
    ):
        return quote
    return analytical_option_quote(
        quote,
        as_of=as_of,
        core_max_age_seconds=max_age_seconds,
        rotation_max_age_seconds=max_age_seconds,
    )
