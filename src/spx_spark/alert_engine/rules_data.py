from __future__ import annotations

from spx_spark.alert_engine.constants import BAD_QUALITIES
from spx_spark.alert_model import Alert, severity_for_priority
from spx_spark.alert_profile import AlertWindow
from spx_spark.marketdata import Quote
from spx_spark.storage import LatestState


def find_best(state: LatestState, instrument_id: str) -> Quote | None:
    return state.best_quote(instrument_id)


def quote_health_alert(
    *,
    instrument_id: str,
    quote: Quote | None,
    window: AlertWindow,
    required: bool,
) -> Alert | None:
    if quote is None:
        severity = severity_for_priority(window.priority) if required else "low"
        return Alert(
            severity=severity,
            kind="required_data_missing" if required else "optional_data_missing",
            instrument_id=instrument_id,
            title=f"{instrument_id} missing",
            detail=f"{instrument_id} has no usable best quote in latest state.",
        )

    if quote.quality in BAD_QUALITIES:
        severity = severity_for_priority(window.priority) if required else "low"
        return Alert(
            severity=severity,
            kind="required_data_degraded" if required else "optional_data_degraded",
            instrument_id=instrument_id,
            title=f"{instrument_id} {quote.quality.value}",
            detail=f"{instrument_id} best quote is {quote.quality.value}.",
            provider=quote.provider.value,
            quality=quote.quality.value,
        )

    return None


