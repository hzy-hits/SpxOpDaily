"""BBO-only readiness tracking for dynamically pinned exact option legs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from spx_spark.ibkr.quote_demand import ExactLegQuoteDemand
from spx_spark.ibkr.verifier import VerifyRow, clean_float
from spx_spark.market_calendar import MarketCalendar
from spx_spark.marketdata import parse_timestamp


PIN_READY_MAX_AGE_SECONDS = 5.0
PIN_READY_MAX_SKEW_SECONDS = 5.0
PIN_READY_FUTURE_TOLERANCE_SECONDS = 1.0
PIN_BID_PRICE_TICK_TYPE = 1
PIN_ASK_PRICE_TICK_TYPE = 2

Subscription = tuple[Any, VerifyRow]
Subscriptions = dict[str, Subscription]


@dataclass
class _QuoteEvidence:
    ticker: Any
    last_tick_batch: object | None = None
    last_tick_count: int = 0
    bid: float | None = None
    ask: float | None = None
    bid_receipt_at: datetime | None = None
    ask_receipt_at: datetime | None = None
    bid_transport_at: datetime | None = None
    ask_transport_at: datetime | None = None


class ExactLegQuoteReadiness:
    """Own callbacks and fail-closed BBO evidence for one pin lifecycle."""

    def __init__(self) -> None:
        self._watchers: dict[str, tuple[Any, Any]] = {}
        self._evidence: dict[str, _QuoteEvidence] = {}

    def clear_evidence(self) -> None:
        self._evidence = {}

    def reset(self) -> None:
        self.detach()
        self.clear_evidence()

    def arm(
        self,
        subscriptions: Subscriptions,
        *,
        cold_labels: frozenset[str],
        now: datetime,
    ) -> None:
        """Capture BBO-only evidence; Greeks/OI updates must not refresh it."""

        self.detach()
        self.clear_evidence()
        for label, (ticker, _row) in subscriptions.items():
            if ticker is None:
                continue
            ticks = getattr(ticker, "ticks", None)
            cold = label in cold_labels
            evidence = _QuoteEvidence(
                ticker=ticker,
                last_tick_batch=None if cold else ticks,
                last_tick_count=0 if cold else _tick_count(ticks),
            )
            self._evidence[label] = evidence

            event = getattr(ticker, "updateEvent", None)
            connect = getattr(event, "connect", None)
            if callable(connect):

                def on_update(
                    *_args: object,
                    pin_label: str = label,
                    pin_ticker: Any = ticker,
                ) -> None:
                    self.observe(
                        label=pin_label,
                        ticker=pin_ticker,
                        transport_at=datetime.now(tz=timezone.utc),
                    )

                connect(on_update)
                self._watchers[label] = (event, on_update)
            if cold:
                self.observe(label=label, ticker=ticker, transport_at=now)

    def detach(self) -> None:
        for event, callback in self._watchers.values():
            disconnect = getattr(event, "disconnect", None)
            if callable(disconnect):
                try:
                    disconnect(callback)
                except (TypeError, ValueError):
                    pass
        self._watchers = {}

    def observe(
        self,
        *,
        label: str,
        ticker: Any,
        transport_at: datetime,
    ) -> None:
        evidence = self._evidence.get(label)
        if evidence is None or evidence.ticker is not ticker:
            return
        ticks = getattr(ticker, "ticks", None)
        if ticks is evidence.last_tick_batch:
            start = min(evidence.last_tick_count, _tick_count(ticks))
        else:
            start = 0
        try:
            new_ticks = list(ticks[start:])  # type: ignore[index]
        except (TypeError, AttributeError):
            new_ticks = []
        evidence.last_tick_batch = ticks
        evidence.last_tick_count = _tick_count(ticks)
        bid_ticks = [
            tick
            for tick in new_ticks
            if getattr(tick, "tickType", None) == PIN_BID_PRICE_TICK_TYPE
        ]
        ask_ticks = [
            tick
            for tick in new_ticks
            if getattr(tick, "tickType", None) == PIN_ASK_PRICE_TICK_TYPE
        ]
        bid, ask = _ticker_nbbo(ticker)
        if bid_ticks:
            bid_receipts = _tick_receipts(bid_ticks)
            if bid is None or bid <= 0 or not bid_receipts:
                evidence.bid = None
                evidence.bid_receipt_at = None
                evidence.bid_transport_at = None
            else:
                evidence.bid = bid
                evidence.bid_receipt_at = max(bid_receipts)
                evidence.bid_transport_at = transport_at
        if ask_ticks:
            ask_receipts = _tick_receipts(ask_ticks)
            if ask is None or ask <= 0 or not ask_receipts:
                evidence.ask = None
                evidence.ask_receipt_at = None
                evidence.ask_transport_at = None
            else:
                evidence.ask = ask
                evidence.ask_receipt_at = max(ask_receipts)
                evidence.ask_transport_at = transport_at

    def sample(
        self,
        *,
        demand: ExactLegQuoteDemand,
        pinned: Subscriptions,
        now: datetime,
        market_calendar: MarketCalendar,
        connection_generation: int,
    ) -> dict[str, object] | None:
        """Apply the action-time five-second contract to both sides of both legs."""

        desired_labels = tuple(leg.label for leg in demand.legs)
        if len(pinned) != 2 or set(pinned) != set(desired_labels):
            return None
        if demand.session_date != market_calendar.research_expiry(now).isoformat():
            return None

        prices: dict[str, tuple[float, float, float]] = {}
        receipt_times: dict[str, tuple[datetime, datetime]] = {}
        transport_times: dict[str, tuple[datetime, datetime]] = {}
        receipt_ages: dict[str, float] = {}
        transport_ages: dict[str, float] = {}
        side_metrics: dict[str, float] = {}
        for leg in demand.legs:
            subscription = pinned.get(leg.label)
            if subscription is None:
                return None
            ticker, row = subscription
            self.observe(label=leg.label, ticker=ticker, transport_at=now)
            evidence = self._evidence.get(leg.label)
            market_data_type = getattr(ticker, "marketDataType", row.market_data_type)
            if (
                ticker is None
                or evidence is None
                or row.kind != "option"
                or row.symbol != "SPX"
                or not row.subscribed
                or row.error
                or market_data_type != 1
            ):
                return None
            bid, ask = _ticker_nbbo(ticker)
            if bid is None or ask is None or bid <= 0 or ask <= 0 or ask < bid:
                return None
            if evidence.bid != bid or evidence.ask != ask:
                return None
            midpoint = (bid + ask) / 2.0
            bid_receipt_at = evidence.bid_receipt_at
            ask_receipt_at = evidence.ask_receipt_at
            bid_transport_at = evidence.bid_transport_at
            ask_transport_at = evidence.ask_transport_at
            if (
                bid_receipt_at is None
                or ask_receipt_at is None
                or bid_transport_at is None
                or ask_transport_at is None
            ):
                return None
            bid_receipt_age = (now - bid_receipt_at).total_seconds()
            ask_receipt_age = (now - ask_receipt_at).total_seconds()
            bid_transport_age = (now - bid_transport_at).total_seconds()
            ask_transport_age = (now - ask_transport_at).total_seconds()
            if not _ages_valid(
                bid_receipt_age,
                ask_receipt_age,
                bid_transport_age,
                ask_transport_age,
            ):
                return None
            side_receipt_skew = abs(
                (bid_receipt_at - ask_receipt_at).total_seconds()
            )
            side_transport_skew = abs(
                (bid_transport_at - ask_transport_at).total_seconds()
            )
            if (
                side_receipt_skew > PIN_READY_MAX_SKEW_SECONDS
                or side_transport_skew > PIN_READY_MAX_SKEW_SECONDS
            ):
                return None
            prices[leg.role] = (bid, midpoint, ask)
            receipt_times[leg.role] = (bid_receipt_at, ask_receipt_at)
            transport_times[leg.role] = (bid_transport_at, ask_transport_at)
            receipt_ages[leg.role] = max(bid_receipt_age, ask_receipt_age)
            transport_ages[leg.role] = max(bid_transport_age, ask_transport_age)
            side_metrics.update(
                _side_metrics(
                    role=leg.role,
                    bid_receipt_age=bid_receipt_age,
                    ask_receipt_age=ask_receipt_age,
                    bid_transport_age=bid_transport_age,
                    ask_transport_age=ask_transport_age,
                    receipt_skew=side_receipt_skew,
                    transport_skew=side_transport_skew,
                )
            )

        if set(prices) != {"long", "short"}:
            return None
        receipt_skew = _cross_leg_skew(receipt_times)
        transport_skew = _cross_leg_skew(transport_times)
        if (
            receipt_skew > PIN_READY_MAX_SKEW_SECONDS
            or transport_skew > PIN_READY_MAX_SKEW_SECONDS
        ):
            return None
        long_bid, long_mid, long_ask = prices["long"]
        short_bid, short_mid, short_ask = prices["short"]
        net_bid = long_bid - short_ask
        net_mid = long_mid - short_mid
        net_ask = long_ask - short_bid
        if net_mid <= 0 or net_ask <= 0 or not net_bid <= net_mid <= net_ask:
            return None
        return {
            "long_nbbo_receipt_age_seconds": receipt_ages["long"],
            "short_nbbo_receipt_age_seconds": receipt_ages["short"],
            "long_nbbo_transport_age_seconds": transport_ages["long"],
            "short_nbbo_transport_age_seconds": transport_ages["short"],
            "nbbo_cross_leg_receipt_skew_seconds": receipt_skew,
            "nbbo_cross_leg_transport_skew_seconds": transport_skew,
            "nbbo_receipt_time_basis": "ib_async_owner_packet_received_at",
            "nbbo_transport_time_basis": "exact_leg_watcher_observed_at",
            "net_bid": net_bid,
            "net_mid": net_mid,
            "net_ask": net_ask,
            "source_session": f"ibkr-stream:{connection_generation}",
            **side_metrics,
        }


def _tick_count(ticks: object | None) -> int:
    try:
        return len(ticks)  # type: ignore[arg-type]
    except TypeError:
        return 0


def _ticker_nbbo(ticker: Any) -> tuple[float | None, float | None]:
    return clean_float(getattr(ticker, "bid", None)), clean_float(
        getattr(ticker, "ask", None)
    )


def _tick_receipts(ticks: list[object]) -> list[datetime]:
    return [
        parsed
        for tick in ticks
        if (parsed := parse_timestamp(getattr(tick, "time", None))) is not None
    ]


def _ages_valid(*ages: float) -> bool:
    return all(
        -PIN_READY_FUTURE_TOLERANCE_SECONDS <= age <= PIN_READY_MAX_AGE_SECONDS
        for age in ages
    )


def _cross_leg_skew(times: dict[str, tuple[datetime, datetime]]) -> float:
    return max(
        abs((long_at - short_at).total_seconds())
        for long_at in times["long"]
        for short_at in times["short"]
    )


def _side_metrics(
    *,
    role: str,
    bid_receipt_age: float,
    ask_receipt_age: float,
    bid_transport_age: float,
    ask_transport_age: float,
    receipt_skew: float,
    transport_skew: float,
) -> dict[str, float]:
    return {
        f"{role}_bid_receipt_age_seconds": bid_receipt_age,
        f"{role}_ask_receipt_age_seconds": ask_receipt_age,
        f"{role}_bid_transport_age_seconds": bid_transport_age,
        f"{role}_ask_transport_age_seconds": ask_transport_age,
        f"{role}_nbbo_side_receipt_skew_seconds": receipt_skew,
        f"{role}_nbbo_side_transport_skew_seconds": transport_skew,
    }
