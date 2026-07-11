from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from spx_spark.provider_adapter import ProviderSnapshot
from spx_spark.schwab import collector as schwab_collector
from spx_spark.schwab.symbols import (
    chain_interval_seconds_for,
    find_schwab_instrument,
    option_chain_strike_count_for,
)


def _isolate_collector_storage(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    data_root = tmp_path / "data"
    data_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("MARKET_DATA_DATA_ROOT", str(data_root))
    monkeypatch.setenv("MAINTENANCE_DATA_ROOT", str(data_root))
    return data_root


def _chain_payload(symbol: str, *, strike: float = 750.0) -> dict[str, Any]:
    padded = f"{symbol:<6}"
    return {
        "callExpDateMap": {
            "2026-07-06:0": {
                f"{strike:.1f}": [
                    {
                        "symbol": f"{padded}260706C{int(strike * 1000):08d}",
                        "putCall": "CALL",
                        "expirationDate": "2026-07-06T20:00:00+00:00",
                        "strikePrice": strike,
                        "bid": 1.0,
                        "ask": 1.2,
                        "mark": 1.1,
                    }
                ]
            }
        },
        "putExpDateMap": {},
    }


def test_fetch_chain_uses_calendar_research_expiries() -> None:
    captured: dict[str, Any] = {}

    class FakeClient:
        def get_json(self, path: str, params: dict[str, Any]):
            captured.update(params)
            return 200, {}

    schwab_collector.fetch_chain(
        FakeClient(),
        "SPY",
        SimpleNamespace(option_chain_strike_count=20),
        now=datetime(2026, 7, 2, 21, 0, tzinfo=timezone.utc),
    )

    assert captured["fromDate"] == "2026-07-06"
    assert captured["toDate"] == "2026-07-07"
    assert captured["strikeCount"] == 20


def test_fetch_chain_uses_per_instrument_strike_count_for_spx() -> None:
    captured: dict[str, Any] = {}

    class FakeClient:
        def get_json(self, path: str, params: dict[str, Any]):
            captured.update(params)
            return 200, {}

    schwab_collector.fetch_chain(
        FakeClient(),
        "SPX",
        SimpleNamespace(option_chain_strike_count=10),
        now=datetime(2026, 7, 2, 21, 0, tzinfo=timezone.utc),
    )

    assert captured["symbol"] == "$SPX"
    assert captured["strikeCount"] == 40


def test_runtime_chain_cadence_and_strike_overrides() -> None:
    assert chain_interval_seconds_for("SPX") == 5
    assert chain_interval_seconds_for("SPY") == 15
    assert chain_interval_seconds_for("XSP") == 15
    assert chain_interval_seconds_for("QQQ") == 15
    assert chain_interval_seconds_for("IWM") == 15
    assert option_chain_strike_count_for("SPX", 10) == 40
    assert option_chain_strike_count_for("SPY", 10) == 10
    spx = find_schwab_instrument("SPX")
    assert spx is not None
    assert spx.option_chain_strike_count == 40
    assert spx.chain_interval_seconds == 5


def test_chain_is_due_respects_interval() -> None:
    now = datetime(2026, 7, 11, 15, 0, 0, tzinfo=timezone.utc)
    assert schwab_collector.chain_is_due(last_fetched_at=None, now=now, interval_seconds=15)
    assert not schwab_collector.chain_is_due(
        last_fetched_at=now - timedelta(seconds=14),
        now=now,
        interval_seconds=15,
    )
    assert schwab_collector.chain_is_due(
        last_fetched_at=now - timedelta(seconds=15),
        now=now,
        interval_seconds=15,
    )


def test_collector_skips_without_token(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(schwab_collector, "build_schwab_client", lambda _settings: None)
    assert schwab_collector.run() == 0
    output = capsys.readouterr().out
    assert "missing_schwab_auth" in output


def test_collector_persists_chain_quotes(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _isolate_collector_storage(monkeypatch, tmp_path)
    monkeypatch.setenv("SCHWAB_COLLECT_QUOTES", "")
    monkeypatch.setenv("SCHWAB_COLLECT_CHAINS", "SPY")

    class FakeClient:
        def get_json(self, path: str, params: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            assert path == "/marketdata/v1/chains"
            assert params["symbol"] == "SPY"
            return 200, _chain_payload("SPY")

    persisted: list[ProviderSnapshot] = []

    monkeypatch.setattr(schwab_collector, "build_schwab_client", lambda _settings: FakeClient())
    monkeypatch.setattr(
        schwab_collector,
        "persist_provider_snapshot",
        lambda snapshot, _storage: (
            persisted.append(snapshot)
            or type(
                "WriteResult",
                (),
                {
                    "raw_paths": {},
                    "latest_state": "test",
                    "provider_quote_count": snapshot.quote_count,
                    "best_quote_count": snapshot.quote_count,
                    "updated_quote_count": snapshot.quote_count,
                },
            )()
        ),
    )

    assert schwab_collector.run() == 0
    output = json.loads(capsys.readouterr().out)
    assert output["ok"] is True
    assert output["quote_counts"]["SPY"] == 1
    assert output["request_count"] == 1
    assert len(persisted) == 1
    assert persisted[0].quote_count == 1


def test_collector_persists_batched_underlying_quotes(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _isolate_collector_storage(monkeypatch, tmp_path)
    monkeypatch.setenv("SCHWAB_COLLECT_QUOTES", "$SPX,SPY")
    monkeypatch.setenv("SCHWAB_COLLECT_CHAINS", "")

    class FakeClient:
        def get_json(self, path: str, params: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            assert path == "/marketdata/v1/quotes"
            assert params["symbols"] == "$SPX,SPY"
            return 200, {
                "$SPX": {
                    "assetMainType": "INDEX",
                    "quote": {"lastPrice": 7500.0},
                },
                "SPY": {
                    "assetMainType": "EQUITY",
                    "quote": {"lastPrice": 750.0},
                },
            }

    persisted: list[ProviderSnapshot] = []
    monkeypatch.setattr(schwab_collector, "build_schwab_client", lambda _settings: FakeClient())
    monkeypatch.setattr(
        schwab_collector,
        "persist_provider_snapshot",
        lambda snapshot, _storage: persisted.append(snapshot),
    )

    assert schwab_collector.run() == 0
    output = json.loads(capsys.readouterr().out)
    assert output["quote_counts"]["quotes:$SPX,SPY"] == 2
    assert output["request_count"] == 1
    assert len(persisted) == 1
    assert persisted[0].quote_count == 2


def test_collector_resolves_logical_es_root_and_persists_stable_identity(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _isolate_collector_storage(monkeypatch, tmp_path)
    monkeypatch.setenv("SCHWAB_COLLECT_QUOTES", "/ES")
    monkeypatch.setenv("SCHWAB_COLLECT_CHAINS", "")
    now = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)

    class FakeClient:
        def get_json(self, path: str, params: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            assert path == "/marketdata/v1/quotes"
            assert params["symbols"] == "/ESU26"
            return 200, {
                "/ESU26": {
                    "assetMainType": "FUTURE",
                    "quote": {
                        "lastPrice": 7525.0,
                        "quoteTime": int(now.timestamp() * 1000),
                    },
                }
            }

    persisted: list[ProviderSnapshot] = []
    monkeypatch.setattr(schwab_collector, "build_schwab_client", lambda _settings: FakeClient())
    monkeypatch.setattr(
        schwab_collector,
        "persist_provider_snapshot",
        lambda snapshot, _storage: persisted.append(snapshot),
    )

    assert schwab_collector.run(now=now) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["quote_counts"]["quotes:/ESU26"] == 1
    assert persisted[0].quotes[0].instrument.canonical_id == "future:ES"
    assert persisted[0].quotes[0].provider_symbol == "/ESU26"


def test_live_stream_symbols_are_not_polled_by_rest_collector(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _isolate_collector_storage(monkeypatch, tmp_path)
    monkeypatch.setenv("SCHWAB_STREAM_MODE", "live")
    monkeypatch.setenv("SCHWAB_STREAM_SYMBOLS", "SPX,SPY,RSP,ES,MES")
    monkeypatch.setenv(
        "SCHWAB_COLLECT_QUOTES",
        "$SPX,SPY,RSP,/ES,/MES,$XSP,$VIX",
    )
    monkeypatch.setenv("SCHWAB_COLLECT_CHAINS", "")
    now = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)
    requested: list[str] = []

    class FakeClient:
        def get_json(self, path: str, params: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            assert path == "/marketdata/v1/quotes"
            requested.append(params["symbols"])
            return 200, {
                "$XSP": {
                    "assetMainType": "INDEX",
                    "quote": {"lastPrice": 750.0},
                },
                "$VIX": {
                    "assetMainType": "INDEX",
                    "quote": {"lastPrice": 18.0},
                },
            }

    monkeypatch.setattr(schwab_collector, "build_schwab_client", lambda _settings: FakeClient())
    monkeypatch.setattr(
        schwab_collector,
        "persist_provider_snapshot",
        lambda _snapshot, _storage: None,
    )

    assert schwab_collector.run(now=now) == 0
    capsys.readouterr()
    assert requested == ["$XSP,$VIX"]


def test_collector_tiered_chain_cadence_and_request_counts(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _isolate_collector_storage(monkeypatch, tmp_path)
    monkeypatch.setenv("SCHWAB_COLLECT_QUOTES", "$SPX")
    monkeypatch.setenv("SCHWAB_COLLECT_CHAINS", "SPX,XSP,SPY,QQQ,IWM")

    chain_calls: list[tuple[str, int, datetime]] = []
    persisted: list[ProviderSnapshot] = []

    class FakeClient:
        def get_json(self, path: str, params: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            if path == "/marketdata/v1/quotes":
                return 200, {
                    "$SPX": {
                        "assetMainType": "INDEX",
                        "quote": {"lastPrice": 6500.0},
                    }
                }
            symbol = str(params["symbol"])
            strike_count = int(params["strikeCount"])
            chain_calls.append((symbol, strike_count, datetime.now(tz=timezone.utc)))
            underlier = symbol.lstrip("$")
            return 200, _chain_payload(underlier if underlier != "SPX" else "SPXW", strike=6500.0)

    monkeypatch.setattr(schwab_collector, "build_schwab_client", lambda _settings: FakeClient())
    monkeypatch.setattr(
        schwab_collector,
        "persist_provider_snapshot",
        lambda snapshot, _storage: persisted.append(snapshot),
    )

    t0 = datetime(2026, 7, 11, 14, 30, 0, tzinfo=timezone.utc)
    assert schwab_collector.run(now=t0) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["request_count"] == 6  # 1 quotes + 5 chains
    assert sorted(first["chains_fetched"]) == ["IWM", "QQQ", "SPX", "SPY", "XSP"]
    assert first["chains_skipped"] == []
    assert {symbol for symbol, _, _ in chain_calls} == {"$SPX", "$XSP", "SPY", "QQQ", "IWM"}
    assert ("$SPX", 40) in {(symbol, strike) for symbol, strike, _ in chain_calls}
    assert all(strike == 10 for symbol, strike, _ in chain_calls if symbol != "$SPX")
    first_chain_as_of = dict(first["chain_as_of"])
    assert all(first_chain_as_of[symbol] == t0.isoformat() for symbol in first_chain_as_of)
    first_persisted_count = len(persisted)
    first_spx_received_at = next(
        snapshot.received_at
        for snapshot in persisted
        if snapshot.quotes and snapshot.quotes[0].instrument.underlier == "SPX"
    )

    chain_calls.clear()
    t1 = t0 + timedelta(seconds=5)
    assert schwab_collector.run(now=t1) == 0
    second = json.loads(capsys.readouterr().out)
    assert second["request_count"] == 2  # quotes + $SPX only
    assert second["chains_fetched"] == ["SPX"]
    assert sorted(second["chains_skipped"]) == ["IWM", "QQQ", "SPY", "XSP"]
    assert [symbol for symbol, _, _ in chain_calls] == ["$SPX"]
    assert second["chain_as_of"]["SPX"] == t1.isoformat()
    assert second["chain_as_of"]["SPY"] == t0.isoformat()
    assert second["requests_last_minute"] == 8
    # Skipped B-tier chains must not re-persist / forge freshness.
    assert len(persisted) == first_persisted_count + 2  # quotes + SPX chain
    latest_spx = next(
        snapshot
        for snapshot in reversed(persisted)
        if snapshot.quotes and snapshot.quotes[0].instrument.underlier == "SPX"
    )
    assert latest_spx.received_at == t1
    assert first_spx_received_at == t0

    chain_calls.clear()
    t2 = t0 + timedelta(seconds=15)
    assert schwab_collector.run(now=t2) == 0
    third = json.loads(capsys.readouterr().out)
    assert third["request_count"] == 6
    assert sorted(third["chains_fetched"]) == ["IWM", "QQQ", "SPX", "SPY", "XSP"]
    assert third["chains_skipped"] == []
    assert third["requests_last_minute"] == 14


def test_skipped_chain_does_not_forge_received_at(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _isolate_collector_storage(monkeypatch, tmp_path)
    monkeypatch.setenv("SCHWAB_COLLECT_QUOTES", "")
    monkeypatch.setenv("SCHWAB_COLLECT_CHAINS", "SPY")
    persisted_received_at: list[datetime] = []

    class FakeClient:
        def get_json(self, path: str, params: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            return 200, _chain_payload("SPY")

    monkeypatch.setattr(schwab_collector, "build_schwab_client", lambda _settings: FakeClient())
    monkeypatch.setattr(
        schwab_collector,
        "persist_provider_snapshot",
        lambda snapshot, _storage: persisted_received_at.append(snapshot.received_at),
    )

    t0 = datetime(2026, 7, 11, 15, 0, 0, tzinfo=timezone.utc)
    assert schwab_collector.run(now=t0) == 0
    capsys.readouterr()
    assert persisted_received_at == [t0]

    t1 = t0 + timedelta(seconds=5)
    assert schwab_collector.run(now=t1) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["chains_skipped"] == ["SPY"]
    assert output["chains_fetched"] == []
    assert output["chain_as_of"]["SPY"] == t0.isoformat()
    assert output["request_count"] == 0
    # No second persist: prior chain timestamp must remain the only write.
    assert persisted_received_at == [t0]


def test_collector_warns_when_request_budget_exceeded(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    data_root = _isolate_collector_storage(monkeypatch, tmp_path)
    monkeypatch.setenv("SCHWAB_COLLECT_QUOTES", "$SPX")
    monkeypatch.setenv("SCHWAB_COLLECT_CHAINS", "")
    now = datetime(2026, 7, 11, 16, 0, 0, tzinfo=timezone.utc)
    state = schwab_collector.CollectorBudgetState(
        request_timestamps=[now.timestamp() - 1.0] * 100,
    )
    schwab_collector.save_collector_budget_state(
        data_root / "latest" / schwab_collector.COLLECTOR_STATE_FILE_NAME,
        state,
    )

    class FakeClient:
        def get_json(self, path: str, params: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            return 200, {
                "$SPX": {
                    "assetMainType": "INDEX",
                    "quote": {"lastPrice": 6500.0},
                }
            }

    monkeypatch.setattr(schwab_collector, "build_schwab_client", lambda _settings: FakeClient())
    monkeypatch.setattr(
        schwab_collector,
        "persist_provider_snapshot",
        lambda _snapshot, _storage: None,
    )

    with caplog.at_level(logging.WARNING, logger=schwab_collector.LOGGER.name):
        assert schwab_collector.run(now=now) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["requests_last_minute"] == 101
    assert any("request budget soft guardrail exceeded" in message for message in caplog.messages)
