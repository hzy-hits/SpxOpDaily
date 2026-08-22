from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from spx_spark.analytics.growth_dislocation import (
    apply_crowding,
    candidate_state,
    extrinsic_value_ratio,
    priority_sort_key,
    rsi_recovery_score,
    score_candidate,
    select_target_leaps,
)
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
        self.test_last = 110.0
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
        last = self.test_last if symbol == "TEST" else 110.0
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


class MissingLeapsClient(FakeSchwabClient):
    def get_json(self, path: str, params: dict[str, object]):
        if path.endswith("/chains"):
            self.requests.append((path, params))
            return 200, {
                "status": "SUCCESS",
                "isDelayed": False,
                "callExpDateMap": {},
            }
        return super().get_json(path, params)


def _policy():
    return load_app_settings().growth_dislocation


def _eligible_score_row() -> dict[str, float | int]:
    return {
        "rsi14": 42.0,
        "rsi14_min_20d": 25.0,
        "return_5d": 0.04,
        "return_10d": 0.03,
        "sector_return_5d": 0.01,
        "sector_return_10d": 0.01,
        "ma10": 100.0,
        "last": 101.0,
        "spread_mid": 0.08,
        "leaps_dte": 600,
        "leaps_delta": 0.72,
        "leaps_bid": 20.0,
        "leaps_ask": 21.0,
        "leaps_strike": 90.0,
        "target_leaps_oi": 1000,
        "current_iv": 0.20,
        "realized_vol_20d": 0.30,
        "ivp_13w": 0.05,
        "ivp_26w": 0.10,
        "ivp_52w": 0.08,
        "price_location_52w": 0.05,
    }


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


def test_v10_defaults_encode_the_three_hard_gates() -> None:
    policy = _policy()

    assert policy.min_market_cap == 10_000_000_000.0
    assert (policy.max_ivp_13w, policy.max_ivp_26w, policy.max_ivp_52w) == (
        0.20,
        0.20,
        0.20,
    )
    assert (policy.min_leaps_dte, policy.max_leaps_dte) == (450, 730)
    assert (policy.target_delta_min, policy.target_delta_max) == (0.68, 0.80)
    assert policy.max_leaps_spread_mid == 0.08
    assert policy.max_current_leaps_iv == 0.60
    assert policy.max_iv_rv_ratio == 1.00
    assert policy.min_target_leaps_open_interest == 100
    assert policy.max_extrinsic_value_ratio == 0.20


def test_candidate_state_follows_watch_armed_trigger_contract() -> None:
    assert (
        candidate_state(
            close=99.0,
            ma10=100.0,
            rs5_sector=0.01,
        )
        == "ARMED"
    )
    assert (
        candidate_state(
            close=101.0,
            ma10=100.0,
            rs5_sector=0.01,
        )
        == "TRIGGER"
    )
    assert (
        candidate_state(
            close=99.0,
            ma10=100.0,
            rs5_sector=-0.01,
        )
        == "WATCH"
    )


def test_rsi_recovery_score_is_continuous_at_state_thresholds() -> None:
    policy = _policy()

    assert [
        rsi_recovery_score(value, 25.0, policy)
        for value in (20.0, 30.0, 35.0, 40.0, 55.0, 75.0)
    ] == [20.0, 60.0, 80.0, 100.0, 100.0, 20.0]
    for boundary in (30.0, 35.0, 40.0, 55.0):
        below = rsi_recovery_score(boundary - 0.001, 25.0, policy)
        above = rsi_recovery_score(boundary + 0.001, 25.0, policy)
        assert abs(above - below) < 0.02
    assert rsi_recovery_score(45.0, 30.0, policy) == 30.0


def test_v1_score_uses_only_iv_rsi_recovery_and_sector_strength() -> None:
    row = _eligible_score_row()

    scored = score_candidate(row, _policy())

    assert scored is not None
    assert round(scored["iv_score"], 2) == 30.00
    assert scored["rsi_score"] == 100.0
    assert round(scored["rs_score"], 2) == 60.40
    assert round(scored["final_score"], 2) == 57.08
    assert round(scored["price_dislocation_score"], 2) == 75.00
    assert round(scored["ivp_52w_score"], 2) == 60.00
    assert round(scored["priority_score"], 2) == 67.50
    assert round(scored["extrinsic_value_ratio"], 4) == 0.0941
    assert scored["state"] == "TRIGGER"


def test_v1_hard_option_spread_gate_is_eight_percent() -> None:
    row = _eligible_score_row()
    row["spread_mid"] = 0.09

    assert score_candidate(row, _policy()) is None


def test_target_leaps_rejects_wide_contract_before_selection() -> None:
    wide = SimpleNamespace(
        symbol="WIDE",
        dte=700,
        delta=0.70,
        bid=20.0,
        ask=23.0,
        strike=90.0,
        volatility=0.20,
        open_interest=2000,
    )
    tight = SimpleNamespace(
        symbol="TIGHT",
        dte=600,
        delta=0.76,
        bid=20.0,
        ask=21.0,
        strike=90.0,
        volatility=0.20,
        open_interest=100,
    )

    assert select_target_leaps([wide, tight], _policy()) is tight


def test_target_leaps_prefers_target_delta_before_tighter_spread() -> None:
    tight_edge = SimpleNamespace(
        symbol="TIGHT_EDGE",
        dte=600,
        delta=0.68,
        bid=20.0,
        ask=20.4,
        strike=90.0,
        volatility=0.20,
        open_interest=1000,
    )
    target_delta = SimpleNamespace(
        symbol="TARGET_DELTA",
        dte=600,
        delta=0.74,
        bid=20.0,
        ask=21.0,
        strike=90.0,
        volatility=0.20,
        open_interest=1000,
    )

    assert select_target_leaps([tight_edge, target_delta], _policy()) is target_delta


def test_extrinsic_value_rejects_mid_below_call_intrinsic_value() -> None:
    assert (
        extrinsic_value_ratio(
            spot=101.0,
            strike=90.0,
            bid=10.50,
            ask=11.00,
        )
        is None
    )


def test_three_gate_contract_rejects_high_ivp_iv_rv_and_time_value() -> None:
    row = _eligible_score_row()
    for field, value in (
        ("ivp_52w", 0.21),
        ("current_iv", 0.61),
        ("realized_vol_20d", 0.19),
        ("target_leaps_oi", 99),
        ("leaps_ask", 50.0),
    ):
        rejected = dict(row)
        rejected[field] = value
        assert score_candidate(rejected, _policy()) is None


def test_eligible_candidates_rank_by_market_cap_then_score() -> None:
    candidates = [
        {
            "symbol": "SMALL_HIGH_SCORE",
            "market_cap": 5_000_000_000.0,
            "final_score": 99.0,
            "crowding_group": "sector:XLY",
        },
        {
            "symbol": "LARGE_LOW_SCORE",
            "market_cap": 100_000_000_000.0,
            "final_score": 20.0,
            "crowding_group": "sector:XLK",
        },
        {
            "symbol": "LARGE_HIGH_SCORE",
            "market_cap": 100_000_000_000.0,
            "final_score": 80.0,
            "crowding_group": "sector:XLC",
        },
    ]

    top, reserve = apply_crowding(candidates, _policy())

    assert [row["symbol"] for row in top] == [
        "LARGE_HIGH_SCORE",
        "LARGE_LOW_SCORE",
        "SMALL_HIGH_SCORE",
    ]
    assert reserve == []


def test_notification_candidates_rank_by_52w_priority_then_timing_score() -> None:
    candidates = [
        {
            "symbol": "HIGH_52W_PRIORITY",
            "market_cap": 5_000_000_000.0,
            "final_score": 20.0,
            "priority_score": 90.0,
            "crowding_group": "sector:XLY",
        },
        {
            "symbol": "LOW_52W_PRIORITY",
            "market_cap": 100_000_000_000.0,
            "final_score": 99.0,
            "priority_score": 30.0,
            "crowding_group": "sector:XLK",
        },
        {
            "symbol": "MISSING_52W_IVP",
            "market_cap": 100_000_000_000.0,
            "final_score": 100.0,
            "priority_score": None,
            "crowding_group": "sector:XLC",
        },
    ]

    top, reserve = apply_crowding(candidates, _policy(), sort_key=priority_sort_key)

    assert [row["symbol"] for row in top] == [
        "HIGH_52W_PRIORITY",
        "LOW_52W_PRIORITY",
        "MISSING_52W_IVP",
    ]
    assert reserve == []


def test_rth_scanner_updates_today_state_without_changing_core_membership(
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
    assert first.document["notification_top10"][0]["symbol"] == "TEST"
    assert first.document["top10"][0]["ivp_13w"] == 0.047619
    assert first.document["top10"][0]["ivp_26w"] == 0.06
    assert first.document["top10"][0]["iv_filter_source"] == (
        "ibkr_tws_option_implied_volatility_history"
    )
    assert first.document["top10"][0]["leaps_symbol"] == "TEST LEAPS"
    assert first.document["top10"][0]["option_quote_status"] == "live"
    assert first.document["automatic_ordering"] is False
    assert first.document["added_symbols"] == []
    assert first.document["core_pool_status"] == "BOOTSTRAP_PENDING"
    assert first.document["core_top_opportunities"] == []
    assert first.document["material_fingerprint"] != second.document["material_fingerprint"]
    assert second.document["added_symbols"] == []
    assert second.notification is None
    assert deliveries == []
    assert (tmp_path / "latest" / "growth_dislocation_leaps.json").is_file()


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
        iv_percentile_fetcher=lambda symbols: _ivp_fetcher(symbols, ivp_13w=0.21),
    )

    assert outcome.document["counts"]["strict_candidates"] == 0
    assert outcome.document["rejection_counts"]["ivp_13w_above_limit"] == 1


def test_direct_ibkr_52w_ivp_is_also_a_hard_gate(tmp_path: Path) -> None:
    outcome = scan_once(
        now=NOW,
        mode="rth",
        client=FakeSchwabClient(NOW),  # type: ignore[arg-type]
        policy=_policy(),
        universe=_universe(),
        data_root=tmp_path,
        iv_percentile_fetcher=lambda symbols: _ivp_fetcher(symbols, ivp_52w=0.21),
    )

    assert outcome.document["counts"]["strict_candidates"] == 0
    assert outcome.document["rejection_counts"]["ivp_52w_above_limit"] == 1


def test_missing_target_leaps_is_hard_rejection_not_warming(tmp_path: Path) -> None:
    outcome = scan_once(
        now=NOW,
        mode="rth",
        client=MissingLeapsClient(NOW),  # type: ignore[arg-type]
        policy=_policy(),
        universe=_universe(),
        data_root=tmp_path,
        iv_percentile_fetcher=_ivp_fetcher,
    )

    assert outcome.document["scan_complete"] is True
    assert outcome.document["counts"]["detailed_this_run"] == 1
    assert outcome.document["counts"]["strict_candidates"] == 0
    assert outcome.document["counts"]["warming_rows"] == 0
    assert outcome.document["rejection_counts"]["target_leaps_missing"] == 1


def test_target_leaps_contract_gate_rejects_high_absolute_iv(tmp_path: Path) -> None:
    outcome = scan_once(
        now=NOW,
        mode="rth",
        client=FakeSchwabClient(NOW, chain_iv_percent=61.0),  # type: ignore[arg-type]
        policy=_policy(),
        universe=_universe(),
        data_root=tmp_path,
        iv_percentile_fetcher=_ivp_fetcher,
    )

    assert outcome.document["counts"]["strict_candidates"] == 0
    assert outcome.document["rejection_counts"]["target_leaps_hard_filter"] == 1


def test_missing_ibkr_ivp_fails_closed_to_symbol_rejection(tmp_path: Path) -> None:
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
    assert outcome.document["counts"]["warming_rows"] == 0
    assert outcome.document["scan_complete"] is True
    assert outcome.document["data_quality"]["status"] == "complete"
    assert outcome.document["rejection_counts"]["ibkr_iv_percentile_missing"] == 1
    assert outcome.document["watchlist"] == []
    _title, text = render_notification(outcome.document)
    assert "未完成数据行（正文隐藏） **0**" in text
    assert "## Warming" not in text
    assert "**TEST**" not in text


def test_missing_symbol_ivp_does_not_freeze_complete_core_pool_update(tmp_path: Path) -> None:
    base = _universe().members[0]
    missing = replace(base, symbol="MISS", provider_symbol="MISS", company="Missing IV")
    universe = Universe(members=(base, missing), metadata=("test universe",))

    outcome = scan_once(
        now=NOW.replace(hour=20, minute=0),
        mode="daily",
        client=FakeSchwabClient(NOW),  # type: ignore[arg-type]
        policy=_policy(),
        universe=universe,
        data_root=tmp_path,
        iv_percentile_fetcher=lambda symbols: _ivp_fetcher(
            [symbol for symbol in symbols if symbol != "MISS"]
        ),
    )

    assert outcome.document["data_quality"]["status"] == "complete"
    assert outcome.document["rejection_counts"]["ibkr_iv_percentile_missing"] == 1
    assert outcome.document["added_symbols"] == ["TEST"]
    assert outcome.document["counts"]["core_pool"] == 1


def test_incomplete_ibkr_ivp_is_not_cached_and_retries_next_scan(tmp_path: Path) -> None:
    calls: list[list[str]] = []

    def fetch(symbols: list[str]):
        calls.append(symbols)
        if len(calls) == 1:
            return _ivp_fetcher(  # type: ignore[arg-type]
                symbols,
                ivp_13w=None,
                ivp_26w=None,
                ivp_52w=None,
            )
        return _ivp_fetcher(symbols)

    first = scan_once(
        now=NOW,
        mode="rth",
        client=FakeSchwabClient(NOW),  # type: ignore[arg-type]
        policy=_policy(),
        universe=_universe(),
        data_root=tmp_path,
        iv_percentile_fetcher=fetch,
    )
    second = scan_once(
        now=NOW,
        mode="rth",
        client=FakeSchwabClient(NOW),  # type: ignore[arg-type]
        policy=_policy(),
        universe=_universe(),
        data_root=tmp_path,
        iv_percentile_fetcher=fetch,
    )

    assert first.document["counts"]["strict_candidates"] == 0
    assert first.document["counts"]["ivp_refreshed"] == 0
    assert second.document["counts"]["strict_candidates"] == 1
    assert second.document["counts"]["ivp_refreshed"] == 1
    assert calls == [["TEST"], ["TEST"]]


def test_incomplete_daily_refresh_keeps_complete_cached_ivp(tmp_path: Path) -> None:
    client = FakeSchwabClient(NOW)
    first = scan_once(
        now=NOW,
        mode="rth",
        client=client,  # type: ignore[arg-type]
        policy=_policy(),
        universe=_universe(),
        data_root=tmp_path,
        iv_percentile_fetcher=_ivp_fetcher,
    )
    second = scan_once(
        now=NOW,
        mode="daily",
        client=client,  # type: ignore[arg-type]
        policy=_policy(),
        universe=_universe(),
        data_root=tmp_path,
        iv_percentile_fetcher=lambda symbols: _ivp_fetcher(  # type: ignore[arg-type]
            symbols,
            ivp_13w=None,
            ivp_26w=None,
        ),
    )

    assert first.document["top10"][0]["ivp_13w"] == 0.047619
    assert second.document["top10"][0]["ivp_13w"] == 0.047619
    assert second.document["counts"]["ivp_refreshed"] == 0
    assert "ibkr_ivp:partial" not in second.document["data_quality"]["errors"]
    assert second.document["scan_complete"] is True


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

    assert len(deliveries) == 1
    assert all(envelope.kind == "growth_dislocation_scan" for envelope in deliveries)
    document = scan_once(
        now=NOW.replace(hour=20, minute=0),
        mode="daily",
        client=client,  # type: ignore[arg-type]
        policy=_policy(),
        universe=_universe(),
        data_root=tmp_path,
        iv_percentile_fetcher=_ivp_fetcher,
    ).document
    assert document["core_pool_status"] == "ACTIVE"
    assert document["added_symbols"] == []
    assert document["core_top_opportunities"][0]["symbol"] == "TEST"
    _title, text = render_notification(document)
    candidate = document["core_top_opportunities"][0]
    assert f"{candidate['final_score']:.2f}" in text
    assert f"{candidate['priority_score']:.2f}" in text
    assert "$11.00B" in text
    assert f"{candidate['price_location_52w'] * 100.0:.2f}%" in text
    assert "4.76% / 6.00%" in text
    assert "8.00%" in text
    assert f"{candidate['rsi14']:.2f}" in text
    assert f"{candidate['current_iv'] * 100.0:.2f}%" in text
    assert f"{candidate['iv_rv_ratio']:.2f}" in text
    assert f"{candidate['target_leaps_oi']:.2f}" in text
    assert f"{candidate['extrinsic_value_ratio'] * 100.0:.2f}%" in text
    assert f"{candidate['leaps_spread_mid'] * 100.0:.2f}%" in text


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

    daily = NOW.replace(hour=20, minute=0)
    common = {
        "now": daily,
        "mode": "daily",
        "client": client,
        "universe": _universe(),
        "data_root": tmp_path,
        "iv_percentile_fetcher": _ivp_fetcher,
        "notification_settings": SimpleNamespace(),
        "enqueue": enqueue,
    }
    first = scan_once(policy=_policy(), **common)  # type: ignore[arg-type]
    partial = scan_once(
        policy=replace(_policy(), rth_request_budget=1, daily_request_budget=1),
        **common,  # type: ignore[arg-type]
    )
    recovered = scan_once(policy=_policy(), **common)  # type: ignore[arg-type]

    assert first.document["added_symbols"] == ["TEST"]
    assert first.document["core_pool_status"] == "ACTIVE"
    assert partial.document["scan_complete"] is False
    assert partial.document["core_pool_status"] == "FROZEN"
    assert partial.document["core_changes"]["message"] == (
        "Membership frozen due to partial scan"
    )
    assert partial.document["core_top_opportunities"][0]["symbol"] == "TEST"
    assert partial.document["core_top_opportunities"][0]["today_state"] == "STALE"
    assert partial.document["added_symbols"] == []
    assert recovered.document["added_symbols"] == []
    assert len(deliveries) == 3


def test_core_pool_requires_two_complete_daily_passes_after_bootstrap(tmp_path: Path) -> None:
    client = FakeSchwabClient(NOW)
    daily = NOW.replace(hour=20, minute=0)
    common = {
        "now": daily,
        "mode": "daily",
        "client": client,
        "policy": _policy(),
        "universe": _universe(),
        "data_root": tmp_path,
    }

    bootstrap = scan_once(
        iv_percentile_fetcher=lambda symbols: _ivp_fetcher(symbols, ivp_13w=0.21),
        **common,  # type: ignore[arg-type]
    )
    first_pass = scan_once(
        iv_percentile_fetcher=_ivp_fetcher,
        **common,  # type: ignore[arg-type]
    )
    second_pass = scan_once(
        iv_percentile_fetcher=_ivp_fetcher,
        **common,  # type: ignore[arg-type]
    )

    assert bootstrap.document["counts"]["core_pool"] == 0
    assert first_pass.document["counts"]["core_pool"] == 0
    assert first_pass.document["core_entry_pending"] == [
        {"symbol": "TEST", "complete_daily_passes": 1}
    ]
    assert second_pass.document["added_symbols"] == ["TEST"]
    assert second_pass.document["counts"]["core_pool"] == 1


def test_core_pool_exits_only_after_three_complete_price_recovery_scans(
    tmp_path: Path,
) -> None:
    client = FakeSchwabClient(NOW)
    daily = NOW.replace(hour=20, minute=0)
    common = {
        "now": daily,
        "mode": "daily",
        "client": client,
        "policy": _policy(),
        "universe": _universe(),
        "data_root": tmp_path,
        "iv_percentile_fetcher": _ivp_fetcher,
    }

    bootstrap = scan_once(**common)  # type: ignore[arg-type]
    client.test_last = 120.0
    first = scan_once(**common)  # type: ignore[arg-type]
    second = scan_once(**common)  # type: ignore[arg-type]
    third = scan_once(**common)  # type: ignore[arg-type]

    assert bootstrap.document["added_symbols"] == ["TEST"]
    assert first.document["core_pool"][0]["exit_streak"] == 1
    assert first.document["core_pool"][0]["exit_reason"] == "PRICE_RECOVERED"
    assert second.document["counts"]["core_pool"] == 1
    assert third.document["exited_symbols"] == ["TEST"]
    assert third.document["counts"]["core_pool"] == 0


def test_schedule_is_exchange_local_and_daily_lane_bypasses_quiet_window() -> None:
    assert scheduled_mode(datetime(2026, 8, 20, 9, 30, tzinfo=ET)) == "rth"
    assert scheduled_mode(datetime(2026, 8, 20, 9, 31, tzinfo=ET)) == "rth"
    assert scheduled_mode(datetime(2026, 8, 20, 9, 34, tzinfo=ET)) == "rth"
    assert scheduled_mode(datetime(2026, 8, 20, 9, 35, tzinfo=ET)) is None
    assert scheduled_mode(datetime(2026, 8, 20, 10, 0, tzinfo=ET)) is None
    daily_at = datetime(2026, 8, 20, 20, 0, tzinfo=ET)
    assert scheduled_mode(daily_at) == "daily"
    assert scheduled_mode(daily_at.replace(minute=1)) == "daily"
    assert scheduled_mode(daily_at.replace(minute=4)) == "daily"
    assert scheduled_mode(daily_at.replace(minute=5)) is None
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


def test_production_universe_covers_russell_1000_fallen_angels() -> None:
    universe = load_universe(Path("config/growth_dislocation_universe.csv"))
    by_symbol = {member.symbol: member for member in universe.members}

    assert "IWB holdings=Aug 18, 2026" in "\n".join(universe.metadata)
    assert by_symbol["APP"].sector_benchmark == "XLC"
    assert by_symbol["RBLX"].sector_benchmark == "XLC"
    assert "IWB" in by_symbol["RBLX"].memberships
    assert by_symbol["APP"].crowding_group == by_symbol["RBLX"].crowding_group
