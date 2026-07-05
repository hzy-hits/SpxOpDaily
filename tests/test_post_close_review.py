from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from spx_spark.config import IvSurfaceSettings, StorageSettings
from spx_spark.iv_surface import build_iv_surface_snapshot, write_snapshot
from spx_spark.marketdata import InstrumentId, MarketDataQuality, OptionGreeks, Provider, Quote
from spx_spark.post_close_review import (
    ReviewLlmSettings,
    build_review_payload,
    maybe_write_llm_review,
    render_markdown,
    resolve_trading_date,
    review_paths,
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
    )


def iv_settings(tmp_path: Path) -> IvSurfaceSettings:
    return IvSurfaceSettings(
        data_root=str(tmp_path / "data"),
        latest_surface_path=str(tmp_path / "data" / "latest" / "iv_surface.json"),
        raw_file_name="snapshots.jsonl",
        wide_quote_spread_bps=250.0,
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


def option_quote(expiry: str, strike: float, right: str, mark: float, iv: float, now: datetime) -> Quote:
    return Quote(
        instrument=InstrumentId.option("SPX", expiry=expiry, strike=strike, right=right, trading_class="SPXW"),
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


def test_resolve_trading_date_uses_completed_ny_session() -> None:
    now = datetime(2026, 7, 7, 11, 15, tzinfo=timezone.utc)

    assert resolve_trading_date("auto", now=now).isoformat() == "2026-07-06"


def test_resolve_trading_date_skips_observed_us_market_holiday() -> None:
    now = datetime(2026, 7, 5, 6, 0, tzinfo=timezone.utc)

    assert resolve_trading_date("auto", now=now).isoformat() == "2026-07-02"


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

    assert payload["verdict"]["status"] == "complete"
    assert payload["spx"]["change_points"] == 30.0
    assert payload["spxw_options"]["unique_contracts"] == 2
    assert payload["iv_surface"]["snapshot_count"] == 2
    expiry = payload["iv_surface"]["expiries"][0]
    assert round(expiry["atm_iv"]["change"], 2) == 0.05
    assert "SPX/SPXW Post-Close Review" in markdown
    assert "VIX" not in markdown
    assert Path(written["hermes_latest_markdown_path"]).exists()


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
