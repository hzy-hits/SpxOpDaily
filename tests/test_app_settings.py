import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from spx_spark.app_settings import AppSettings
from spx_spark.cli import app
from spx_spark.config import NotificationSettings


@pytest.fixture
def settings_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    config = tmp_path / "config"
    config.mkdir()
    (config / "defaults.toml").write_text(
        'data_root = "/defaults"\nlog_level = "INFO"\n', encoding="utf-8"
    )
    (config / "production.toml").write_text(
        'data_root = "/production"\nlog_level = "WARNING"\n', encoding="utf-8"
    )
    monkeypatch.chdir(tmp_path)
    return config


def test_settings_priority_is_toml_then_env_then_init(
    settings_cwd: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert AppSettings().data_root == Path("/production")

    monkeypatch.setenv("SPX_DATA_ROOT", "/environment")
    assert AppSettings().data_root == Path("/environment")
    assert AppSettings(data_root=Path("/init")).data_root == Path("/init")


def test_unknown_toml_key_is_rejected(settings_cwd: Path) -> None:
    (settings_cwd / "production.toml").write_text('unexpected = "value"\n', encoding="utf-8")

    with pytest.raises(ValidationError, match="unexpected"):
        AppSettings()


def test_core_runtime_paths_are_typed_and_cli_is_registered(
    settings_cwd: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPX_CORE_SOCKET_PATH", "/run/user/1001/spx-core.sock")
    monkeypatch.setenv("SPX_CORE_LOCK_ROOT", "/run/user/1001")
    settings = AppSettings()

    assert settings.core_socket_path == Path("/run/user/1001/spx-core.sock")
    assert settings.core_lock_root == Path("/run/user/1001")
    result = CliRunner().invoke(app, ["core", "--help"])
    assert result.exit_code == 0
    assert "run" in result.stdout


def test_status_forwards_provider_and_instrument_filters(
    settings_cwd: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: list[list[str]] = []
    monkeypatch.setattr(
        "spx_spark.latest_state.run",
        lambda argv: captured.append(list(argv)) or 0,
    )

    result = CliRunner().invoke(
        app,
        ["status", "--all-providers", "--instrument", "index:SPX"],
    )

    assert result.exit_code == 0
    assert captured == [["--all-providers", "--instrument", "index:SPX"]]


def test_notify_test_queues_one_real_verification_event(
    settings_cwd: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: list[object] = []
    monkeypatch.setattr(
        NotificationSettings,
        "from_env",
        classmethod(lambda _cls: object()),
    )

    def enqueue(settings, envelope, **kwargs):
        captured.append((settings, envelope, kwargs))
        return SimpleNamespace(
            accepted=True,
            outcome="pending",
            targets=("bark", "feishu"),
        )

    monkeypatch.setattr(
        "spx_spark.notifier.dispatcher.enqueue_notification",
        enqueue,
    )

    result = CliRunner().invoke(app, ["notify", "test"])

    assert result.exit_code == 0
    output = json.loads(result.stdout.strip())
    assert output["accepted"] is True
    assert output["outcome"] == "pending"
    assert output["targets"] == ["bark", "feishu"]
    _settings, envelope, kwargs = captured[0]
    assert envelope.source == "spx_cli"
    assert envelope.kind == "notification_test"
    assert envelope.lane == "execution_safety"
    assert kwargs["title"] == "SPX notification test"


def test_data_command_forwards_to_existing_owners(
    settings_cwd: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[str, list[str]]] = []
    monkeypatch.setattr(
        "spx_spark.data_platform.cli.run",
        lambda argv: calls.append(("data", argv)) or 0,
    )
    monkeypatch.setattr(
        "spx_spark.data_platform.lake.compact.main",
        lambda argv: calls.append(("compact", list(argv))) or 0,
    )

    status = CliRunner().invoke(app, ["data", "status"])
    compact = CliRunner().invoke(app, ["data", "compact", "--json", "--limit", "8"])

    assert status.exit_code == compact.exit_code == 0
    assert calls == [
        ("data", ["status"]),
        ("compact", ["--json", "--limit", "8"]),
    ]


@pytest.mark.parametrize(
    ("target", "command"),
    (
        ("spx_spark.alert_profile.run", ["ops", "alert-profile"]),
        ("spx_spark.maintenance.run", ["ops", "maintenance"]),
        ("spx_spark.mock_collector.run", ["ops", "mock-collector"]),
        ("spx_spark.runtime_mode.main", ["ops", "runtime-mode"]),
        ("spx_spark.sampling.run", ["ops", "sampling-plan"]),
        ("spx_spark.ibkr.verifier.run", ["verify", "ibkr"]),
        ("spx_spark.schwab.verifier.run", ["verify", "schwab"]),
        ("spx_spark.ibkr.trading_hours_report.run", ["report", "ibkr-hours"]),
        ("spx_spark.strategy.micopedia.run", ["report", "micopedia"]),
        ("spx_spark.options_map.run", ["report", "options-map"]),
        ("spx_spark.strategy.steven_replay.run", ["replay", "steven"]),
        ("spx_spark.surface_dashboard_replay.run", ["replay", "surface"]),
        ("spx_spark.ibkr.collector.run", ["ibkr", "collect"]),
        ("spx_spark.ibkr.stream.cli.run", ["ibkr", "stream"]),
        ("spx_spark.ibkr.farm_health.run_probe_cli", ["ibkr", "farm-probe"]),
        ("spx_spark.ibkr.position_watcher.run", ["ibkr", "positions"]),
        ("spx_spark.schwab.collector.run", ["schwab", "collect"]),
        ("spx_spark.schwab.oauth_service.run", ["schwab", "oauth"]),
        ("spx_spark.application.morning_map.service.run", ["job", "morning-map"]),
        ("spx_spark.application.order_map.service.run", ["job", "order-map"]),
        ("spx_spark.post_close_review.run", ["job", "post-close-review"]),
        (
            "spx_spark.application.order_map.rth_daily_acceptance.main",
            ["job", "rth-daily-acceptance"],
        ),
    ),
)
def test_operator_commands_forward_arguments(
    settings_cwd: Path,
    monkeypatch: pytest.MonkeyPatch,
    target: str,
    command: list[str],
) -> None:
    captured: list[list[str]] = []
    monkeypatch.setattr(target, lambda argv: captured.append(list(argv)) or 0)

    result = CliRunner().invoke(app, [*command, "--json", "value"])

    assert result.exit_code == 0
    assert captured == [["--json", "value"]]


def test_schwab_marketdata_command_calls_existing_loop(
    settings_cwd: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: list[bool] = []
    monkeypatch.setattr(
        "spx_spark.schwab.collector.run_loop",
        lambda: captured.append(True) or 0,
    )

    result = CliRunner().invoke(app, ["schwab", "marketdata"])

    assert result.exit_code == 0
    assert captured == [True]
