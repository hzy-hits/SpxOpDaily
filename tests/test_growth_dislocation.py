from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from spx_spark.analytics.growth_dislocation import candidate_state
from spx_spark.ibkr.adapter import IvPercentileSnapshot
from spx_spark.infrastructure.growth_dislocation import (
    Universe,
    UniverseMember,
    load_universe,
    render_notification,
    scan_once,
    scheduled_mode,
)
from spx_spark.market_calendar import ET
from spx_spark.notifier.human_policy import quiet_window_suppresses
from spx_spark.notifier.model import NotificationEnvelope
from spx_spark.settings import load_app_settings


NOW = datetime(2026, 8, 20, 15, 30, tzinfo=ET)


def _ivp_fetcher(
    symbols: list[str],
    *,
    ivp_13w: float = 0.047619,
    ivp_26w: float = 0.06,
    ivp_52w: float = 0.08,
):
    return {
        symbol.replace(".", " ").replace("/", " "): IvPercentileSnapshot(
            provider_symbol=symbol,
            conid=index + 1,
            observed_at=NOW,
            ivp_13w=ivp_13w,
            ivp_26w=ivp_26w,
            ivp_52w=ivp_52w,
            iv_rank_13w=0.10,
            iv_rank_26w=0.12,
            iv_rank_52w=0.15,
        )
        for index, symbol in enumerate(symbols)
    }


class FakeSchwabClient:
    def __init__(self, now: datetime, *, chain_iv_percent: float = 20.0) -> None:
        self.now = now
        self.chain_iv_percent = chain_iv_percent
        self.requests: list[tuple[str, dict[str, object]]] = []

    def get_json(self, path: str, params: dict[str, object]):
        self.requests.append((path, params))
        if path.endswith("/quotes"):
            symbols = str(params["symbols"]).split(",")
            return 200, {symbol: self._quote(symbol) for symbol in symbols}
        if path.endswith("/pricehistory"):
            return 200, {"empty": False, "candles": self._candles(str(params["symbol"]))}
        if path.endswith("/chains"):
            expiry = self.now.date() + timedelta(days=600)
            contract = {
                "symbol": f"{params['symbol']} LEAPS",
                "strikePrice": 90.0,
                "daysToExpiration": 600,
                "delta": 0.70,
                "bid": 20.0,
                "ask": 21.0,
                "volatility": self.chain_iv_percent,
                "openInterest": 1200,
                "totalVolume": 25,
                "quoteTimeInLong": int(self.now.timestamp() * 1000),
            }
            return 200, {
                "status": "SUCCESS",
                "isDelayed": False,
                "callExpDateMap": {f"{expiry.isoformat()}:600": {"90.0": [contract]}},
            }
        raise AssertionError(path)

    def _quote(self, symbol: str) -> dict[str, object]:
        last = 110.0
        low, high = (100.0, 150.0)
        if symbol == "HIGH":
            last = 145.0
        if symbol in {"SPY", "XLK"}:
            low, high = (80.0, 120.0)
        return {
            "realtime": True,
            "quote": {
                "mark": last,
                "lastPrice": last,
                "52WeekLow": low,
                "52WeekHigh": high,
                "quoteTime": int(self.now.timestamp() * 1000),
            },
            "reference": {"optionable": symbol not in {"SPY", "XLK"}},
            "fundamental": {
                "sharesOutstanding": 100_000_000,
                "divYield": 0.0,
            },
        }

    def _candles(self, symbol: str) -> list[dict[str, object]]:
        base = 80.0 if symbol in {"SPY", "XLK"} else 90.0
        closes: list[float] = []
        price = base
        for index in range(50):
            price *= 1.03 if index % 2 == 0 else 0.97
            closes.append(price)
        return [
            {
                "datetime": int(
                    datetime.combine(
                        self.now.date() - timedelta(days=50 - index),
                        datetime.min.time(),
                        tzinfo=ET,
                    ).timestamp()
                    * 1000
                ),
                "close": close,
            }
            for index, close in enumerate(closes)
        ]


def _policy():
    return load_app_settings().growth_dislocation


def _universe() -> Universe:
    return Universe(
        members=(
            UniverseMember(
                symbol="TEST",
                provider_symbol="TEST",
                company="Test Growth",
                sector="Technology",
                subindustry="Technology",
                classification_level="sector",
                sector_benchmark="XLK",
                memberships=("IWM",),
            ),
            UniverseMember(
                symbol="HIGH",
                provider_symbol="HIGH",
                company="High Location",
                sector="Technology",
                subindustry="Technology",
                classification_level="sector",
                sector_benchmark="XLK",
                memberships=("IWM",),
            ),
        ),
        metadata=("test universe",),
    )


def test_candidate_state_requires_recovery_and_three_confirmations_for_trigger() -> None:
    policy = _policy()

    assert candidate_state(
        rsi_now=42.0,
        rsi_min_20d=25.0,
        ma5=101.0,
        ma10=100.0,
        rs5_market=0.01,
        rs5_sector=-0.01,
        distance_from_20d_low=0.08,
        policy=policy,
    ) == "TRIGGER"
    assert candidate_state(
        rsi_now=28.0,
        rsi_min_20d=25.0,
        ma5=99.0,
        ma10=100.0,
        rs5_market=-0.01,
        rs5_sector=-0.01,
        distance_from_20d_low=0.01,
        policy=policy,
    ) == "WATCH"


def test_scanner_builds_strict_table_and_only_pushes_rth_candidate_additions(
    tmp_path: Path,
) -> None:
    client = FakeSchwabClient(NOW)
    deliveries: list[NotificationEnvelope] = []

    def enqueue(_settings, envelope, **_kwargs):
        deliveries.append(envelope)
        return SimpleNamespace(accepted=True, outcome="pending")

    first = scan_once(
        now=NOW,
        mode="rth",
        client=client,  # type: ignore[arg-type]
        policy=_policy(),
        universe=_universe(),
        data_root=tmp_path,
        iv_percentile_fetcher=_ivp_fetcher,
        notification_settings=SimpleNamespace(),  # type: ignore[arg-type]
        enqueue=enqueue,  # type: ignore[arg-type]
    )
    # Contract IV changes the material fingerprint, but the existing symbol is
    # not an addition and therefore must not produce another RTH push.
    client.chain_iv_percent = 25.0
    second = scan_once(
        now=NOW,
        mode="rth",
        client=client,  # type: ignore[arg-type]
        policy=_policy(),
        universe=_universe(),
        data_root=tmp_path,
        iv_percentile_fetcher=_ivp_fetcher,
        notification_settings=SimpleNamespace(),  # type: ignore[arg-type]
        enqueue=enqueue,  # type: ignore[arg-type]
    )

    assert first.document["counts"]["universe"] == 2
    assert first.document["counts"]["hard_survivors"] == 1
    assert first.document["counts"]["strict_candidates"] == 1
    assert first.document["top10"][0]["symbol"] == "TEST"
    assert first.document["top10"][0]["ivp_13w"] == 0.047619
    assert first.document["top10"][0]["ivp_26w"] == 0.06
    assert first.document["top10"][0]["iv_filter_source"] == (
        "ibkr_tws_option_implied_volatility_history"
    )
    assert first.document["top10"][0]["leaps_symbol"] == "TEST LEAPS"
    assert first.document["top10"][0]["option_quote_status"] == "live"
    assert first.document["automatic_ordering"] is False
    assert first.document["added_symbols"] == ["TEST"]
    assert first.document["material_fingerprint"] != second.document[
        "material_fingerprint"
    ]
    assert second.document["added_symbols"] == []
    assert second.notification is None
    assert len(deliveries) == 1
    assert deliveries[0].lane == "growth_dislocation"
    assert (tmp_path / "latest" / "growth_dislocation_leaps.json").is_file()
    _title, text = render_notification(first.document)
    candidate = first.document["top10"][0]
    assert f"{candidate['final_score']:.2f}" in text
    assert f"{candidate['price_location_52w'] * 100.0:.2f}%" in text
    assert "4.76% / 6.00%" in text
    assert f"{candidate['rsi14']:.2f}" in text
    assert f"{candidate['leaps_spread_mid'] * 100.0:.2f}%" in text


def test_first_scan_uses_ibkr_iv_percentiles_without_multiweek_warmup(
    tmp_path: Path,
) -> None:
    outcome = scan_once(
        now=NOW,
        mode="rth",
        client=FakeSchwabClient(NOW),  # type: ignore[arg-type]
        policy=_policy(),
        universe=_universe(),
        data_root=tmp_path,
        iv_percentile_fetcher=_ivp_fetcher,
    )

    candidate = outcome.document["top10"][0]
    assert outcome.document["counts"]["strict_candidates"] == 1
    assert candidate["ivp_13w"] == 0.047619
    assert candidate["ivp_26w"] == 0.06
    assert candidate["ivp_52w"] == 0.08
    assert candidate["iv_rank_13w"] == 0.10
    assert candidate["iv_filter_source"] == "ibkr_tws_option_implied_volatility_history"
    assert candidate["current_iv"] == 0.20
    assert candidate["iv_score"] > 0.0
    assert candidate["iv_data_notes"] == []


def test_direct_ibkr_ivp_hard_gate_rejects_expensive_regime(tmp_path: Path) -> None:
    outcome = scan_once(
        now=NOW,
        mode="rth",
        client=FakeSchwabClient(NOW),  # type: ignore[arg-type]
        policy=_policy(),
        universe=_universe(),
        data_root=tmp_path,
        iv_percentile_fetcher=lambda symbols: _ivp_fetcher(symbols, ivp_13w=0.11),
    )

    assert outcome.document["counts"]["strict_candidates"] == 0
    assert outcome.document["rejection_counts"]["ivp_13w_above_limit"] == 1


def test_missing_ibkr_ivp_fails_closed_to_watchlist(tmp_path: Path) -> None:
    outcome = scan_once(
        now=NOW,
        mode="rth",
        client=FakeSchwabClient(NOW),  # type: ignore[arg-type]
        policy=_policy(),
        universe=_universe(),
        data_root=tmp_path,
        iv_percentile_fetcher=lambda _symbols: {},
    )

    assert outcome.document["counts"]["strict_candidates"] == 0
    assert outcome.document["counts"]["warming_rows"] == 1
    assert outcome.document["scan_complete"] is False
    assert "ibkr_iv_percentile_missing" in outcome.document["watchlist"][0][
        "data_quality_reasons"
    ]
    _title, text = render_notification(outcome.document)
    assert "未完成数据行（正文隐藏） **1**" in text
    assert "## Warming" not in text
    assert "**TEST**" not in text


def test_daily_summary_pushes_even_when_material_table_is_unchanged(tmp_path: Path) -> None:
    client = FakeSchwabClient(NOW)
    deliveries: list[NotificationEnvelope] = []

    def enqueue(_settings, envelope, **_kwargs):
        deliveries.append(envelope)
        return SimpleNamespace(accepted=True, outcome="pending")

    scan_once(
        now=NOW,
        mode="rth",
        client=client,  # type: ignore[arg-type]
        policy=_policy(),
        universe=_universe(),
        data_root=tmp_path,
        iv_percentile_fetcher=_ivp_fetcher,
        notification_settings=SimpleNamespace(),  # type: ignore[arg-type]
        enqueue=enqueue,  # type: ignore[arg-type]
    )
    scan_once(
        now=NOW.replace(hour=20, minute=0),
        mode="daily",
        client=client,  # type: ignore[arg-type]
        policy=_policy(),
        universe=_universe(),
        data_root=tmp_path,
        iv_percentile_fetcher=_ivp_fetcher,
        notification_settings=SimpleNamespace(),  # type: ignore[arg-type]
        enqueue=enqueue,  # type: ignore[arg-type]
    )

    assert len(deliveries) == 2
    assert all(envelope.kind == "growth_dislocation_scan" for envelope in deliveries)


def test_request_budget_fails_closed_to_partial_warming_table(tmp_path: Path) -> None:
    outcome = scan_once(
        now=NOW,
        mode="rth",
        client=FakeSchwabClient(NOW),  # type: ignore[arg-type]
        policy=replace(_policy(), rth_request_budget=1),
        universe=_universe(),
        data_root=tmp_path,
        iv_percentile_fetcher=_ivp_fetcher,
    )

    assert outcome.document["scan_complete"] is False
    assert outcome.document["counts"]["strict_candidates"] == 0
    assert outcome.document["data_quality"]["request_limited"] is True
    assert outcome.document["requests_used"] == 1


def test_partial_scan_preserves_membership_and_does_not_create_false_reentry(
    tmp_path: Path,
) -> None:
    client = FakeSchwabClient(NOW)
    deliveries: list[NotificationEnvelope] = []

    def enqueue(_settings, envelope, **_kwargs):
        deliveries.append(envelope)
        return SimpleNamespace(accepted=True, outcome="pending")

    common = {
        "now": NOW,
        "mode": "rth",
        "client": client,
        "universe": _universe(),
        "data_root": tmp_path,
        "iv_percentile_fetcher": _ivp_fetcher,
        "notification_settings": SimpleNamespace(),
        "enqueue": enqueue,
    }
    first = scan_once(policy=_policy(), **common)  # type: ignore[arg-type]
    partial = scan_once(
        policy=replace(_policy(), rth_request_budget=1),
        **common,  # type: ignore[arg-type]
    )
    recovered = scan_once(policy=_policy(), **common)  # type: ignore[arg-type]

    assert first.document["added_symbols"] == ["TEST"]
    assert partial.document["scan_complete"] is False
    assert partial.document["added_symbols"] == []
    assert recovered.document["added_symbols"] == []
    assert len(deliveries) == 1


def test_schedule_is_exchange_local_and_daily_lane_bypasses_quiet_window() -> None:
    assert scheduled_mode(datetime(2026, 8, 20, 9, 30, tzinfo=ET)) == "rth"
    assert scheduled_mode(datetime(2026, 8, 20, 10, 0, tzinfo=ET)) is None
    daily_at = datetime(2026, 8, 20, 20, 0, tzinfo=ET)
    assert scheduled_mode(daily_at) == "daily"
    envelope = NotificationEnvelope(
        event_id="growth-dislocation:test",
        source="growth_dislocation",
        kind="growth_dislocation_scan",
        lane="growth_dislocation",
        occurred_at=daily_at,
    )
    assert quiet_window_suppresses(envelope, now=daily_at) is False


def test_universe_loader_preserves_source_metadata_and_classification(tmp_path: Path) -> None:
    path = tmp_path / "universe.csv"
    path.write_text(
        "# source=official\n"
        "symbol,provider_symbol,company,sector,subindustry,classification_level,sector_benchmark,memberships\n"
        "BRK.B,BRK/B,Berkshire,Financials,Financials,sector,XLF,SPY|IWM\n",
        encoding="utf-8",
    )

    universe = load_universe(path)

    assert universe.metadata == ("source=official",)
    assert universe.members[0].provider_symbol == "BRK/B"
    assert universe.members[0].memberships == ("SPY", "IWM")
