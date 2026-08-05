from __future__ import annotations

import inspect

from spx_spark.application.market_features import service


def test_distribution_forecast_runs_after_trade_critical_delivery_and_action_reload() -> None:
    source = inspect.getsource(service.run)

    delivery = source.index("_record_and_process_trade_intent(")
    action_reload = source.index("action_latest = LatestStateStore(storage).load")
    distribution = source.index("process_strategy_distribution_forecast(")
    manual_candidate = source.index("process_gth_level_manual_candidate(")

    assert delivery < action_reload < distribution < manual_candidate
    assert "strategy_distribution_forecast_error" in source
    assert "except Exception as exc" in source[distribution:manual_candidate]


def test_distribution_forecast_is_observational_and_does_not_gate_manual_signal() -> None:
    source = inspect.getsource(service.run)
    distribution = source.index("process_strategy_distribution_forecast(")
    manual_candidate = source.index("process_gth_level_manual_candidate(")
    between = source[distribution:manual_candidate]

    assert "action_provider_entry_control" not in between
    assert "new_entries_allowed" not in between
    assert "trade_intent" not in between
