from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from spx_spark.provider_adapter import ProviderSnapshot
from spx_spark.schwab import collector as schwab_collector


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


def test_collector_skips_without_token(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(schwab_collector, "build_schwab_client", lambda _settings: None)
    assert schwab_collector.run() == 0
    output = capsys.readouterr().out
    assert "missing_schwab_auth" in output


def test_collector_persists_chain_quotes(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("SCHWAB_COLLECT_QUOTES", "")
    monkeypatch.setenv("SCHWAB_COLLECT_CHAINS", "SPY")

    class FakeClient:
        def get_json(self, path: str, params: dict[str, Any]) -> tuple[int, dict[str, Any]]:
            assert path == "/marketdata/v1/chains"
            assert params["symbol"] == "SPY"
            return 200, {
                "callExpDateMap": {
                    "2026-07-06:0": {
                        "750.0": [
                            {
                                "symbol": "SPY   260706C00750000",
                                "putCall": "CALL",
                                "expirationDate": "2026-07-06T20:00:00+00:00",
                                "strikePrice": 750.0,
                                "bid": 1.0,
                                "ask": 1.2,
                                "mark": 1.1,
                            }
                        ]
                    }
                },
                "putExpDateMap": {},
            }

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
    assert len(persisted) == 1
    assert persisted[0].quote_count == 1


def test_collector_persists_batched_underlying_quotes(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
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
    assert len(persisted) == 1
    assert persisted[0].quote_count == 2


def test_collector_resolves_logical_es_root_and_persists_stable_identity(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
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
) -> None:
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
