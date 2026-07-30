"""Session-authoritative quote selection for manual signal cards."""

from __future__ import annotations

from datetime import datetime

from spx_spark.marketdata import (
    Provider,
    choose_best_quote,
    instrument_matches_id,
)
from spx_spark.storage import LatestState


def provider_quote(
    latest: LatestState,
    contract_id: str,
    *,
    provider: Provider,
    now: datetime,
):
    return choose_best_quote(
        (
            quote
            for quote in latest.quotes
            if quote.provider is provider and instrument_matches_id(quote.instrument, contract_id)
        ),
        provider_priority=(provider,),
        as_of=now,
    )


def rth_execution_quote(
    latest: LatestState,
    contract_id: str,
    *,
    now: datetime,
):
    """Prefer Schwab in RTH and use IBKR only as an exact-quote fallback."""

    return provider_quote(
        latest,
        contract_id,
        provider=Provider.SCHWAB,
        now=now,
    ) or provider_quote(
        latest,
        contract_id,
        provider=Provider.IBKR,
        now=now,
    )
