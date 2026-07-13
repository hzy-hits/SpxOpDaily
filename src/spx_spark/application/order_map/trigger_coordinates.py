"""Coordinate contract that keeps wall levels and trigger prices comparable."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from enum import StrEnum
from typing import Mapping

from spx_spark.analytics.options.models import OptionsMap
from spx_spark.analytics.options.pricing import finite_float
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.options_map import actionable_chain_implied_spot
from spx_spark.storage import LatestState, configured_quote_use_decision


class TriggerCoordinateKind(StrEnum):
    OFFICIAL_SPX = "official_spx"
    CHAIN_IMPLIED_SPX = "chain_implied_spx"
    ES_EQUIVALENT = "es_equivalent"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class TriggerCoordinate:
    kind: TriggerCoordinateKind
    instrument_id: str | None
    observed_value: float | None
    spx_observed_value: float | None
    basis_points: float | None
    source: str
    as_of: datetime
    reason: str

    @property
    def usable(self) -> bool:
        return self.observed_value is not None and self.instrument_id is not None

    def trigger_level(self, spx_level: float) -> float | None:
        if not self.usable:
            return None
        if self.kind is TriggerCoordinateKind.ES_EQUIVALENT:
            return spx_level + float(self.basis_points or 0.0)
        return spx_level

    def transform_levels(self, spx_levels: Mapping[str, float]) -> dict[str, float]:
        return {
            key: transformed
            for key, value in spx_levels.items()
            if (transformed := self.trigger_level(float(value))) is not None
        }

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["kind"] = self.kind.value
        payload["as_of"] = self.as_of.isoformat()
        return payload


def resolve_trigger_coordinate(
    state: LatestState,
    options_map: OptionsMap | None,
    *,
    now: datetime,
    qualified_es_basis: float | None,
) -> TriggerCoordinate:
    """Resolve one coordinate; callers must transform both price and levels."""

    if DEFAULT_MARKET_CALENDAR.is_rth_open(now):
        spx = _actionable_price(state, "index:SPX", now=now)
        if spx is not None:
            return TriggerCoordinate(
                kind=TriggerCoordinateKind.OFFICIAL_SPX,
                instrument_id="index:SPX",
                observed_value=spx,
                spx_observed_value=spx,
                basis_points=None,
                source="index:SPX",
                as_of=now,
                reason="rth_official_spx",
            )
    elif options_map is not None and options_map.expiries:
        implied = actionable_chain_implied_spot(
            state,
            expiry=options_map.expiries[0].expiry,
            as_of=now,
        )
        if implied is not None:
            return TriggerCoordinate(
                kind=TriggerCoordinateKind.CHAIN_IMPLIED_SPX,
                instrument_id="synthetic:SPXW_PARITY",
                observed_value=implied,
                spx_observed_value=implied,
                basis_points=None,
                source="chain_implied",
                as_of=now,
                reason="gth_actionable_put_call_parity",
            )

    es = _actionable_price(state, "future:ES", now=now)
    if es is not None and qualified_es_basis is not None:
        return TriggerCoordinate(
            kind=TriggerCoordinateKind.ES_EQUIVALENT,
            instrument_id="future:ES",
            observed_value=es,
            spx_observed_value=es - qualified_es_basis,
            basis_points=qualified_es_basis,
            source=f"future:ES+basis:{qualified_es_basis:.4f}",
            as_of=now,
            reason="spx_coordinate_unavailable_using_es_equivalent",
        )

    return TriggerCoordinate(
        kind=TriggerCoordinateKind.UNAVAILABLE,
        instrument_id=None,
        observed_value=None,
        spx_observed_value=None,
        basis_points=qualified_es_basis,
        source="unavailable",
        as_of=now,
        reason="no_actionable_trigger_coordinate",
    )


def _actionable_price(state: LatestState, instrument_id: str, *, now: datetime) -> float | None:
    quote = state.best_quote(instrument_id)
    if quote is None:
        return None
    decision = configured_quote_use_decision(quote, as_of=now)
    price = finite_float(quote.effective_price)
    return price if decision.pricing_allowed and price is not None and price > 0 else None
