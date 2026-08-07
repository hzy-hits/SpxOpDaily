from __future__ import annotations

import pytest

from spx_spark.data_platform.settings import DataPlatformSettings


def test_data_platform_settings_are_safe_by_default(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MARKET_DATA_DATA_ROOT", str(tmp_path / "market"))
    monkeypatch.delenv("DATA_PLATFORM_ENABLED", raising=False)
    monkeypatch.delenv("DATA_PLATFORM_RAW_DELETE_ENABLED", raising=False)

    settings = DataPlatformSettings.from_env()

    assert settings.enabled is False
    assert settings.raw_delete_enabled is False
    assert settings.ledger_path.endswith("/runtime/research-ledger.sqlite3")
    assert settings.lake_root.endswith("/lake")
    assert settings.replay_raw_delete_grace_hours == 24
    assert settings.replay_finalize_backlog_days == 7
    assert settings.storage_pressure_action_free_bytes == 30_064_771_072
    assert settings.storage_pressure_warning_free_bytes == 25_769_803_776
    assert settings.storage_pressure_critical_free_bytes == 21_474_836_480


def test_blank_optional_path_overrides_use_safe_defaults(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MARKET_DATA_DATA_ROOT", str(tmp_path))
    for name in (
        "DATA_PLATFORM_LEDGER_PATH",
        "DATA_PLATFORM_FALLBACK_SPOOL_PATH",
        "DATA_PLATFORM_LAKE_ROOT",
        "DATA_PLATFORM_MANIFEST_ROOT",
        "DATA_PLATFORM_RESEARCH_CATALOG_PATH",
        "DATA_PLATFORM_WRITER_VERSION",
    ):
        monkeypatch.setenv(name, "")

    settings = DataPlatformSettings.from_env()

    assert settings.ledger_path == str(tmp_path / "runtime/research-ledger.sqlite3")
    assert settings.fallback_spool_path == str(
        tmp_path / "runtime/research-ledger-fallback.jsonl"
    )
    assert settings.lake_root == str(tmp_path / "lake")
    assert settings.manifest_root == str(tmp_path / "manifests")
    assert settings.research_catalog_path == str(tmp_path / "analytics/research.duckdb")
    assert settings.writer_version == "spx-spark-v1"


def test_data_platform_rejects_short_raw_delete_grace() -> None:
    with pytest.raises(ValueError, match="at least 24 hours"):
        DataPlatformSettings(
            enabled=True,
            data_root="data",
            ledger_path="data/runtime/ledger.sqlite3",
            fallback_spool_path="data/runtime/fallback.jsonl",
            fallback_spool_max_bytes=67_108_864,
            lake_root="data/lake",
            manifest_root="data/manifests",
            research_catalog_path="data/analytics/research.duckdb",
            sqlite_busy_timeout_ms=250,
            compaction_min_age_seconds=300,
            raw_delete_enabled=False,
            raw_delete_grace_hours=1,
            writer_version="test",
        )


def test_replay_pressure_settings_accept_typed_environment_overrides(
    monkeypatch, tmp_path
) -> None:
    monkeypatch.setenv("MARKET_DATA_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("DATA_PLATFORM_REPLAY_RAW_DELETE_GRACE_HOURS", "30")
    monkeypatch.setenv("DATA_PLATFORM_REPLAY_FINALIZE_BACKLOG_DAYS", "5")
    monkeypatch.setenv("DATA_PLATFORM_STORAGE_PRESSURE_ACTION_FREE_BYTES", "34359738368")
    monkeypatch.setenv("DATA_PLATFORM_STORAGE_PRESSURE_WARNING_FREE_BYTES", "30064771072")
    monkeypatch.setenv("DATA_PLATFORM_STORAGE_PRESSURE_CRITICAL_FREE_BYTES", "25769803776")

    settings = DataPlatformSettings.from_env()

    assert settings.replay_raw_delete_grace_hours == 30
    assert settings.replay_finalize_backlog_days == 5
    assert settings.storage_pressure_action_free_bytes == 34_359_738_368
    assert settings.storage_pressure_warning_free_bytes == 30_064_771_072
    assert settings.storage_pressure_critical_free_bytes == 25_769_803_776


def test_data_platform_rejects_unsafe_replay_pressure_thresholds() -> None:
    common = {
        "enabled": True,
        "data_root": "data",
        "ledger_path": "data/runtime/ledger.sqlite3",
        "fallback_spool_path": "data/runtime/fallback.jsonl",
        "fallback_spool_max_bytes": 67_108_864,
        "lake_root": "data/lake",
        "manifest_root": "data/manifests",
        "research_catalog_path": "data/analytics/research.duckdb",
        "sqlite_busy_timeout_ms": 250,
        "compaction_min_age_seconds": 300,
        "raw_delete_enabled": False,
        "raw_delete_grace_hours": 48,
        "writer_version": "test",
    }
    with pytest.raises(ValueError, match="at least 24 hours"):
        DataPlatformSettings(**common, replay_raw_delete_grace_hours=23)
    with pytest.raises(ValueError, match="must be positive"):
        DataPlatformSettings(**common, replay_finalize_backlog_days=0)
    with pytest.raises(ValueError, match="at least 20 GiB"):
        DataPlatformSettings(
            **common,
            storage_pressure_critical_free_bytes=19 * 1024**3,
        )
    with pytest.raises(ValueError, match="action > warning >= critical"):
        DataPlatformSettings(
            **common,
            storage_pressure_action_free_bytes=24 * 1024**3,
            storage_pressure_warning_free_bytes=25 * 1024**3,
            storage_pressure_critical_free_bytes=20 * 1024**3,
        )
