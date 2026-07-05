from __future__ import annotations

import json
from datetime import datetime, timezone

from spx_spark.config import PolymarketSettings
from spx_spark.marketdata import InstrumentType, MarketDataQuality, Provider
from spx_spark.polymarket.collector import (
    build_features,
    collect_records,
    parse_json_array,
    quote_from_record,
    record_from_market,
)


def make_settings(**overrides) -> PolymarketSettings:
    values = {
        "gamma_api_base_url": "https://gamma-api.polymarket.com",
        "request_timeout_seconds": 12.0,
        "search_terms": ["SPY"],
        "event_slugs": [],
        "market_slugs": [],
        "max_results_per_query": 5,
        "max_markets_per_run": 80,
        "min_liquidity": 0.0,
        "min_volume_24h": 0.0,
        "min_relevance_score": 0.35,
        "include_closed": False,
        "user_agent": "test-agent",
    }
    values.update(overrides)
    return PolymarketSettings(**values)


def make_market(
    market_id: str = "101",
    *,
    slug: str = "spy-up-today",
    liquidity: float = 100.0,
    volume_24h: float = 25.0,
) -> dict[str, object]:
    return {
        "id": market_id,
        "slug": slug,
        "question": "Will SPY close green today?",
        "conditionId": f"condition-{market_id}",
        "endDate": "2099-07-06T20:00:00Z",
        "active": True,
        "closed": False,
        "enableOrderBook": True,
        "outcomes": json.dumps(["Yes", "No"]),
        "outcomePrices": json.dumps(["0.62", "0.38"]),
        "liquidityNum": str(liquidity),
        "volume24hr": str(volume_24h),
        "openInterest": "44",
        "clobTokenIds": json.dumps(["yes-token", "no-token"]),
    }


def test_parse_json_array_accepts_gamma_strings() -> None:
    assert parse_json_array('["Yes", "No"]') == ["Yes", "No"]
    assert parse_json_array(["A", "B"]) == ["A", "B"]
    assert parse_json_array("not json") == []


def test_record_from_market_normalizes_yes_no_probability() -> None:
    now = datetime(2026, 7, 5, 12, 0, tzinfo=timezone.utc)
    event = {"id": "event-1", "slug": "sp-500", "title": "S&P 500 daily markets"}

    record = record_from_market(
        search_term="SPY",
        event=event,
        market=make_market(),
        received_at=now,
    )

    assert record is not None
    assert record.category == "spx_spy"
    assert record.yes_price == 0.62
    assert record.no_price == 0.38
    assert record.liquidity == 100.0
    assert record.clob_token_ids == ("yes-token", "no-token")


def test_quote_from_record_is_research_only_prediction_market() -> None:
    now = datetime(2026, 7, 5, 12, 0, tzinfo=timezone.utc)
    record = record_from_market(
        search_term="SPY",
        event=None,
        market=make_market(),
        received_at=now,
    )
    assert record is not None

    quote = quote_from_record(record)

    assert quote.provider == Provider.POLYMARKET
    assert quote.instrument.instrument_type == InstrumentType.PREDICTION_MARKET
    assert quote.quality == MarketDataQuality.LIVE
    assert quote.mark == 0.62
    assert quote.close is None
    assert quote.market_data_type == "research_only"


def test_collect_records_dedupes_and_filters_low_liquidity() -> None:
    class FakeClient:
        def get(self, path: str, params=None):  # noqa: ANN001
            assert path == "public-search"
            assert params == {"q": "SPY", "limit": 5}
            return {
                "events": [
                    {
                        "id": "event-1",
                        "slug": "sp-500",
                        "title": "S&P 500",
                        "markets": [
                            make_market("101", liquidity=75.0),
                            make_market("102", liquidity=10.0),
                            make_market("101", liquidity=75.0),
                        ],
                    }
                ]
            }

    records = collect_records(
        FakeClient(),
        make_settings(min_liquidity=50.0),
    )

    assert len(records) == 1
    assert records[0].market_id == "101"


def test_build_features_is_context_only_and_not_kelly_input() -> None:
    now = datetime(2026, 7, 5, 12, 0, tzinfo=timezone.utc)
    record = record_from_market(
        search_term="SPY",
        event=None,
        market=make_market(),
        received_at=now,
    )
    assert record is not None

    features = build_features((record,), as_of=now)

    assert features["research_only"] is True
    assert features["human_visible"] is False
    assert features["usage_gate"] == "context_only_no_kelly_no_direct_alert"
    assert features["category_counts"] == {"spx_spy": 1}
