from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from spx_spark.config import IvSurfaceSettings, StorageSettings
from spx_spark.greek_reference import SCHEMA_VERSION
from spx_spark.iv_surface import (
    IvSurfaceExpiry,
    IvSurfaceSnapshot,
    build_iv_surface_snapshot,
    write_snapshot,
)
from spx_spark.market_calendar import ET
from spx_spark.marketdata import InstrumentId, MarketDataQuality, OptionGreeks, Provider, Quote
from spx_spark.post_close_review import (
    ReviewCompletenessPolicy,
    ReviewLlmSettings,
    build_review_payload,
    build_review_payload_from_data,
    build_push_summary,
    maybe_write_llm_review,
    render_markdown,
    ready_auto_review_date,
    resolve_trading_date,
    review_paths,
    session_window,
    write_outputs,
)
from spx_spark.storage import JsonlQuoteWriter, LatestState


def storage_settings(tmp_path: Path) -> StorageSettings:
    return StorageSettings(
        data_root=str(tmp_path / "data"),
        latest_state_path=str(tmp_path / "data" / "latest" / "state.json"),
        raw_file_name="quotes.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=15.0,
        slow_index_stale_after_seconds=300.0,
        slow_index_labels=frozenset({"index:SKEW", "index:VVIX"}),
    )


def iv_settings(tmp_path: Path) -> IvSurfaceSettings:
    return IvSurfaceSettings(
        data_root=str(tmp_path / "data"),
        latest_surface_path=str(tmp_path / "data" / "latest" / "iv_surface.json"),
        raw_file_name="snapshots.jsonl",
        wide_quote_spread_bps=250.0,
        diff_max_gap_seconds=600.0,
    )


def index_quote(symbol: str, mark: float, now: datetime) -> Quote:
    return Quote(
        instrument=InstrumentId.index(symbol),
        provider=Provider.IBKR,
        provider_symbol=f"index:{symbol}",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=mark,
        quote_time=now,
    )


def future_quote(symbol: str, mark: float, now: datetime) -> Quote:
    return Quote(
        instrument=InstrumentId.future(symbol, expiry="202609"),
        provider=Provider.IBKR,
        provider_symbol=f"future:{symbol}",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=mark,
        quote_time=now,
    )


def option_quote(
    expiry: str, strike: float, right: str, mark: float, iv: float, now: datetime
) -> Quote:
    return Quote(
        instrument=InstrumentId.option(
            "SPX", expiry=expiry, strike=strike, right=right, trading_class="SPXW"
        ),
        provider=Provider.IBKR,
        provider_symbol=f"SPXW:{expiry}:{strike}:{right}",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        bid=mark - 0.1,
        ask=mark + 0.1,
        mark=mark,
        open_interest=1000,
        quote_time=now,
        greeks=OptionGreeks(implied_vol=iv, gamma=0.003, delta=0.5 if right == "C" else -0.5),
    )


def state_from_quotes(*quotes: Quote, now: datetime) -> LatestState:
    return LatestState(created_at=now, as_of=now, quotes=quotes, best_quotes=quotes)


def surface_snapshot(
    expiry: str,
    now: datetime,
    *,
    iv_coverage_ratio: float = 1.0,
    gamma_coverage_ratio: float = 1.0,
) -> IvSurfaceSnapshot:
    expiry_row = IvSurfaceExpiry(
        expiry=expiry,
        atm_iv=0.20,
        atm_straddle_mid=20.0,
        expected_move_points=25.0,
        expected_move_pct=0.0033,
        put_skew_ratio=1.1,
        call_skew_ratio=0.95,
        smile_slope=-0.02,
        smile_curvature=0.01,
        iv_surface_level=0.21,
        iv_surface_shift_5m=0.0,
        atm_iv_jump_5m=0.0,
        put_skew_steepening_5m=0.0,
        call_wing_bid=False,
        smile_curvature_change_5m=0.0,
        surface_fit_quality="raw_grid",
        wide_quote_surface_degraded=False,
        gamma_state="positive",
        zero_gamma=7500.0,
        put_wall=7450.0,
        call_wall=7550.0,
        option_count=20,
        iv_coverage_ratio=iv_coverage_ratio,
        gamma_coverage_ratio=gamma_coverage_ratio,
        avg_spread_bps=100.0,
        warnings=(),
    )
    return IvSurfaceSnapshot(
        created_at=now,
        as_of=now,
        underlier_price=7500.0,
        underlier_source="SPX",
        front_expiry=expiry,
        next_expiry=None,
        front_vs_next_atm_iv_gap=None,
        expiries=(expiry_row,),
        warnings=(),
    )


def test_resolve_trading_date_uses_completed_ny_session() -> None:
    now = datetime(2026, 7, 7, 11, 15, tzinfo=timezone.utc)

    assert resolve_trading_date("auto", now=now).isoformat() == "2026-07-06"


def test_resolve_trading_date_skips_observed_us_market_holiday() -> None:
    now = datetime(2026, 7, 5, 6, 0, tzinfo=timezone.utc)

    assert resolve_trading_date("auto", now=now).isoformat() == "2026-07-02"


def test_review_readiness_and_early_close_delegate_to_market_calendar() -> None:
    before_ready = datetime(2026, 7, 6, 16, 59, tzinfo=ET)
    at_ready = datetime(2026, 7, 6, 17, 0, tzinfo=ET)

    assert resolve_trading_date("auto", now=before_ready) == date(2026, 7, 2)
    assert resolve_trading_date("auto", now=at_ready) == date(2026, 7, 6)

    start, end, ready = session_window(date(2026, 11, 27))
    assert start.hour == 9 and start.minute == 30
    assert end.hour == 13
    assert ready.hour == 17


def test_scheduled_auto_review_does_not_replay_prior_report() -> None:
    assert ready_auto_review_date(now=datetime(2026, 7, 6, 16, 59, tzinfo=ET)) is None
    assert ready_auto_review_date(now=datetime(2026, 7, 3, 17, 15, tzinfo=ET)) is None
    assert ready_auto_review_date(now=datetime(2026, 7, 6, 17, 15, tzinfo=ET)) == date(
        2026,
        7,
        6,
    )


def test_post_close_review_summarizes_spx_options_and_writes_hermes_export(tmp_path) -> None:
    settings = storage_settings(tmp_path)
    writer = JsonlQuoteWriter(settings)
    first = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    last = datetime(2026, 7, 6, 19, 45, tzinfo=timezone.utc)
    writer.write_quotes(
        [
            index_quote("SPX", 7500.0, first),
            index_quote("SPX", 7530.0, last),
            future_quote("ES", 7508.0, first),
            future_quote("ES", 7538.0, last),
            option_quote("20260706", 7500, "C", 10.0, 0.20, first),
            option_quote("20260706", 7500, "P", 11.0, 0.22, first),
            option_quote("20260706", 7500, "C", 12.0, 0.25, last),
            option_quote("20260706", 7500, "P", 13.0, 0.27, last),
            index_quote("VIX", 18.0, last),
        ]
    )

    surface_settings = iv_settings(tmp_path)
    first_surface = build_iv_surface_snapshot(
        state_from_quotes(
            index_quote("SPX", 7500.0, first),
            option_quote("20260706", 7500, "C", 10.0, 0.20, first),
            option_quote("20260706", 7500, "P", 11.0, 0.22, first),
            now=first,
        ),
        settings=surface_settings,
    )
    write_snapshot(surface_settings, first_surface)
    last_surface = build_iv_surface_snapshot(
        state_from_quotes(
            index_quote("SPX", 7530.0, last),
            option_quote("20260706", 7500, "C", 12.0, 0.25, last),
            option_quote("20260706", 7500, "P", 13.0, 0.27, last),
            now=last,
        ),
        settings=surface_settings,
        previous=first_surface,
    )
    write_snapshot(surface_settings, last_surface)

    payload = build_review_payload(
        trading_date=datetime(2026, 7, 6, tzinfo=timezone.utc).date(),
        settings=settings,
        now=last,
    )
    markdown = render_markdown(payload)
    paths = review_paths(
        trading_date=datetime(2026, 7, 6, tzinfo=timezone.utc).date(),
        settings=settings,
        hermes_export_dir=tmp_path / "hermes",
    )
    written = write_outputs(payload, markdown, paths)

    assert payload["verdict"]["status"] == "degraded"
    assert payload["verdict"]["passed_checks"] < payload["verdict"]["required_checks"]
    assert payload["spx"]["change_points"] == 30.0
    assert payload["spxw_options"]["unique_contracts"] == 2
    assert payload["iv_surface"]["snapshot_count"] == 2
    expiry = payload["iv_surface"]["expiries"][0]
    assert round(expiry["atm_iv"]["change"], 2) == 0.05
    assert "SPX/SPXW Post-Close Review" in markdown
    assert "Data Completeness" in markdown
    assert "VIX" not in markdown
    assert Path(written["hermes_latest_markdown_path"]).exists()


def test_one_row_payload_is_degraded_with_measured_checks() -> None:
    trading_date = date(2026, 7, 6)
    observed_at = datetime(2026, 7, 6, 9, 30, tzinfo=ET)
    payload = build_review_payload_from_data(
        trading_date=trading_date,
        quotes=(
            index_quote("SPX", 7500.0, observed_at),
            future_quote("ES", 7508.0, observed_at),
            option_quote("20260706", 7500, "C", 10.0, 0.20, observed_at),
        ),
        snapshots=(),
        now=observed_at,
        policy=ReviewCompletenessPolicy(),
    )

    assert payload["verdict"]["status"] == "degraded"
    checks = payload["completeness"]["checks"]
    assert checks
    assert all({"measured", "threshold", "passed", "reason"} <= check.keys() for check in checks)
    spx_coverage = next(
        check for check in checks if check["name"] == "spx_five_minute_bucket_coverage"
    )
    assert spx_coverage["measured"] == round(1 / 78, 6)
    assert spx_coverage["passed"] is False


def test_post_close_summarizes_optional_zero_dte_greeks_without_changing_verdict() -> None:
    trading_date = date(2026, 7, 6)
    first_at = datetime(2026, 7, 6, 10, 0, tzinfo=ET)
    last_at = datetime(2026, 7, 6, 15, 30, tzinfo=ET)
    base = {
        "schema_version": SCHEMA_VERSION,
        "kind": "snapshot",
        "mode": "reference_only",
        "status": "ok",
        "expiry": "20260706",
        "direction": "unknown",
        "position_sign": "unknown",
        "warnings": [],
        "aggregate_universe": {"fingerprint": "same-hot-cohort", "contract_count": 8},
        "coverage": {"usable_ratio": 0.8, "oi_ratio": 1.0},
    }
    first = {
        **base,
        "as_of": first_at.isoformat(),
        "aggregate": {
            "gross_gamma_abs": 100.0,
            "gross_charm_5m_abs": 20.0,
        },
    }
    last = {
        **base,
        "as_of": last_at.isoformat(),
        "aggregate": {
            "gross_gamma_abs": 150.0,
            "gross_charm_5m_abs": 10.0,
        },
    }
    payload = build_review_payload_from_data(
        trading_date=trading_date,
        quotes=(),
        snapshots=(),
        greek_snapshots=(first, last),
        now=datetime(2026, 7, 6, 17, 15, tzinfo=ET),
        policy=ReviewCompletenessPolicy(),
    )

    summary = payload["spxw_0dte_greeks_reference"]
    markdown = render_markdown(payload)
    assert payload["verdict"]["status"] == "degraded"
    assert summary["snapshot_count"] == 2
    assert summary["position_sign"] == "unknown"
    assert summary["metrics"]["gross_gamma_abs"] == {
        "first": 100.0,
        "last": 150.0,
        "peak": 150.0,
    }
    assert "## 0DTE Greeks Reference" in markdown
    assert "not signed dealer exposure" in markdown


def test_full_high_quality_pure_payload_is_complete() -> None:
    trading_date = date(2026, 7, 6)
    session_open = datetime(2026, 7, 6, 9, 30, tzinfo=ET)
    quote_times = [session_open + timedelta(minutes=5 * index) for index in range(78)]
    quotes: list[Quote] = []
    for index, observed_at in enumerate(quote_times):
        quotes.extend(
            (
                index_quote("SPX", 7500.0 + index * 0.25, observed_at),
                future_quote("ES", 7508.0 + index * 0.25, observed_at),
            )
        )

    option_at = quote_times[-1]
    for strike in range(7450, 7550, 10):
        quotes.extend(
            (
                option_quote("20260706", strike, "C", 10.0, 0.20, option_at),
                option_quote("20260706", strike, "P", 11.0, 0.22, option_at),
            )
        )

    surface_times = [session_open + timedelta(minutes=5 * index) for index in range(46)]
    surface_times.append(quote_times[-1])
    snapshots = tuple(surface_snapshot("20260706", observed_at) for observed_at in surface_times)
    payload = build_review_payload_from_data(
        trading_date=trading_date,
        quotes=tuple(quotes),
        snapshots=snapshots,
        now=datetime(2026, 7, 6, 17, 15, tzinfo=ET),
        policy=ReviewCompletenessPolicy(),
    )

    assert payload["verdict"]["status"] == "complete"
    assert payload["verdict"]["passed_checks"] == payload["verdict"]["required_checks"]
    assert all(check["passed"] for check in payload["completeness"]["checks"])
    assert payload["session"]["expected_five_minute_buckets"] == 78


def test_latest_low_iv_and_gamma_coverage_forces_degraded_verdict() -> None:
    trading_date = date(2026, 7, 8)
    session_open = datetime(2026, 7, 8, 9, 30, tzinfo=ET)
    quote_times = [session_open + timedelta(minutes=5 * index) for index in range(78)]
    quotes: list[Quote] = []
    for index, observed_at in enumerate(quote_times):
        quotes.extend(
            (
                index_quote("SPX", 7500.0 + index * 0.25, observed_at),
                future_quote("ES", 7508.0 + index * 0.25, observed_at),
            )
        )
    for strike in range(7450, 7550, 10):
        quotes.extend(
            (
                option_quote("20260708", strike, "C", 10.0, 0.20, quote_times[-1]),
                option_quote("20260708", strike, "P", 11.0, 0.22, quote_times[-1]),
            )
        )

    surface_times = [session_open + timedelta(minutes=5 * index) for index in range(46)]
    snapshots = [surface_snapshot("20260708", observed_at) for observed_at in surface_times]
    snapshots.append(
        surface_snapshot(
            "20260708",
            quote_times[-1],
            iv_coverage_ratio=0.28,
            gamma_coverage_ratio=0.28,
        )
    )
    payload = build_review_payload_from_data(
        trading_date=trading_date,
        quotes=tuple(quotes),
        snapshots=tuple(snapshots),
        now=datetime(2026, 7, 8, 17, 15, tzinfo=ET),
        policy=ReviewCompletenessPolicy(),
    )

    failed = {check["name"] for check in payload["completeness"]["checks"] if not check["passed"]}
    assert payload["verdict"]["status"] == "degraded"
    assert failed == {
        "latest_front_iv_coverage_ratio",
        "latest_front_gamma_coverage_ratio",
    }


def test_completeness_policy_rejects_invalid_threshold() -> None:
    with pytest.raises(ValueError, match="between 0 and 1"):
        ReviewCompletenessPolicy(min_option_iv_ratio=1.01)


def test_post_close_timer_uses_new_york_wall_time() -> None:
    timer_path = Path(__file__).parents[1] / "systemd" / "spx-spark-post-close-review.timer"
    timer = timer_path.read_text(encoding="utf-8")

    assert "OnCalendar=Mon..Fri *-*-* 17:15:00 America/New_York" in timer


def test_llm_writer_disabled_keeps_template() -> None:
    payload = {"trading_date": "2026-07-06"}
    markdown = "# SPX/SPXW Post-Close Review - 2026-07-06\n\nTemplate"
    settings = ReviewLlmSettings(
        enabled=False,
        provider="deepseek",
        model="deepseek-v4-pro",
        url="https://api.deepseek.com/v1/chat/completions",
        env_file="/no/such/file",
        timeout_seconds=1,
        max_tokens=100,
    )

    output = maybe_write_llm_review(payload, markdown, settings)

    assert output == markdown
    assert payload["llm_writer"]["status"] == "disabled"


def test_llm_writer_falls_back_without_key(monkeypatch) -> None:
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    payload = {"trading_date": "2026-07-06"}
    markdown = "# SPX/SPXW Post-Close Review - 2026-07-06\n\nTemplate"
    settings = ReviewLlmSettings(
        enabled=True,
        provider="deepseek",
        model="deepseek-v4-pro",
        url="https://api.deepseek.com/v1/chat/completions",
        env_file="/no/such/file",
        timeout_seconds=1,
        max_tokens=100,
    )

    output = maybe_write_llm_review(payload, markdown, settings)

    assert output == markdown
    assert payload["llm_writer"]["status"] == "fallback_template"
    assert "missing DEEPSEEK_API_KEY" in payload["llm_writer"]["error"]


def test_successful_llm_writer_preserves_deterministic_completeness(
    monkeypatch,
) -> None:
    payload = {"trading_date": "2026-07-06"}
    deterministic = (
        "# SPX/SPXW Post-Close Review - 2026-07-06\n\n"
        "## Data Warnings\n\n- coverage low\n\n"
        "## Data Completeness\n\n| Check | Pass |\n|---|---|\n"
    )
    settings = ReviewLlmSettings(
        enabled=True,
        provider="deepseek",
        model="test",
        url="https://example.invalid",
        env_file="/no/such/file",
        timeout_seconds=1,
        max_tokens=100,
    )
    monkeypatch.setattr(
        "spx_spark.post_close_review.call_deepseek_writer",
        lambda *args: (
            "# SPX/SPXW Post-Close Review - 2026-07-06\n\nNarrative only",
            None,
        ),
    )

    output = maybe_write_llm_review(payload, deterministic, settings)

    assert payload["llm_writer"]["status"] == "ok"
    assert "## Data Warnings" in output
    assert "## Data Completeness" in output
    assert "## LLM Commentary" in output
    assert "Narrative only" in output


def test_push_summary_does_not_label_next_expiry_as_0dte() -> None:
    payload = {
        "trading_date": "2026-07-06",
        "spx": {},
        "iv_surface": {
            "expiries": [
                {
                    "expiry": "20260707",
                    "put_wall_last": 7450.0,
                    "call_wall_last": 7550.0,
                }
            ]
        },
        "verdict": {"status": "degraded", "warnings": []},
    }

    summary = build_push_summary(payload, latest_markdown_path="/tmp/review.md")

    assert "0DTE 收盘墙位: put - call -" in summary
    assert "7450" not in summary
    assert "7550" not in summary
