from __future__ import annotations

import json
from typing import Any

import pytest

from spx_spark.provider_adapter import ProviderSnapshot
from spx_spark.schwab import collector as schwab_collector


def test_collector_skips_without_token(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setattr(schwab_collector, "load_access_token", lambda _settings: "")
    assert schwab_collector.run() == 0
    output = capsys.readouterr().out
    assert "missing_schwab_token" in output


def test_collector_persists_chain_quotes(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
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

    monkeypatch.setattr(schwab_collector, "load_access_token", lambda _settings: "token")
    monkeypatch.setattr(schwab_collector, "SchwabClient", lambda _settings, _token: FakeClient())
    monkeypatch.setattr(
        schwab_collector,
        "persist_provider_snapshot",
        lambda snapshot, _storage: persisted.append(snapshot) or type(
            "WriteResult",
            (),
            {
                "raw_paths": {},
                "latest_state": "test",
                "provider_quote_count": snapshot.quote_count,
                "best_quote_count": snapshot.quote_count,
                "updated_quote_count": snapshot.quote_count,
            },
        )(),
    )

    assert schwab_collector.run() == 0
    output = json.loads(capsys.readouterr().out)
    assert output["ok"] is True
    assert output["quote_counts"]["SPY"] == 1
    assert len(persisted) == 1
    assert persisted[0].quote_count == 1
