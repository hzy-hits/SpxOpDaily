"""Configuration for the optional research-data platform.

The realtime system remains authoritative while this subsystem is rolled out.
All paths default below the existing market-data root, and destructive raw
cleanup is deliberately disabled unless an operator enables it explicitly.
"""

from __future__ import annotations

from dataclasses import dataclass

from spx_spark.config import env_bool, env_int, env_str, load_dotenv
from spx_spark.runtime_config import runtime_value


@dataclass(frozen=True)
class DataPlatformSettings:
    enabled: bool
    data_root: str
    ledger_path: str
    fallback_spool_path: str
    fallback_spool_max_bytes: int
    lake_root: str
    manifest_root: str
    research_catalog_path: str
    sqlite_busy_timeout_ms: int
    compaction_min_age_seconds: int
    raw_delete_enabled: bool
    raw_delete_grace_hours: int
    writer_version: str

    @classmethod
    def from_env(cls) -> "DataPlatformSettings":
        load_dotenv()
        data_root = (
            env_str("MARKET_DATA_DATA_ROOT")
            or env_str("MAINTENANCE_DATA_ROOT")
            or str(runtime_value("maintenance.data_root"))
        ).rstrip("/")
        return cls(
            enabled=env_bool(
                "DATA_PLATFORM_ENABLED", bool(runtime_value("data_platform.enabled"))
            ),
            data_root=data_root,
            ledger_path=(
                env_str("DATA_PLATFORM_LEDGER_PATH")
                or f"{data_root}/runtime/research-ledger.sqlite3"
            ),
            fallback_spool_path=(
                env_str("DATA_PLATFORM_FALLBACK_SPOOL_PATH")
                or f"{data_root}/runtime/research-ledger-fallback.jsonl"
            ),
            fallback_spool_max_bytes=env_int(
                "DATA_PLATFORM_FALLBACK_SPOOL_MAX_BYTES",
                int(runtime_value("data_platform.fallback_spool_max_bytes")),
            ),
            lake_root=f"{data_root}/lake",
            manifest_root=f"{data_root}/manifests",
            research_catalog_path=f"{data_root}/analytics/research.duckdb",
            sqlite_busy_timeout_ms=env_int(
                "DATA_PLATFORM_SQLITE_BUSY_TIMEOUT_MS",
                int(runtime_value("data_platform.sqlite_busy_timeout_ms")),
            ),
            compaction_min_age_seconds=env_int(
                "DATA_PLATFORM_COMPACTION_MIN_AGE_SECONDS",
                int(runtime_value("data_platform.compaction_min_age_seconds")),
            ),
            raw_delete_enabled=env_bool(
                "DATA_PLATFORM_RAW_DELETE_ENABLED",
                bool(runtime_value("data_platform.raw_delete_enabled")),
            ),
            raw_delete_grace_hours=env_int(
                "DATA_PLATFORM_RAW_DELETE_GRACE_HOURS",
                int(runtime_value("data_platform.raw_delete_grace_hours")),
            ),
            writer_version=env_str(
                "DATA_PLATFORM_WRITER_VERSION",
                str(runtime_value("data_platform.writer_version")),
            )
            or str(runtime_value("data_platform.writer_version")),
        )

    def __post_init__(self) -> None:
        if self.sqlite_busy_timeout_ms < 0:
            raise ValueError("SQLite busy timeout cannot be negative")
        if self.fallback_spool_max_bytes <= 0:
            raise ValueError("fallback spool maximum must be positive")
        if self.compaction_min_age_seconds < 0:
            raise ValueError("compaction minimum age cannot be negative")
        if self.raw_delete_grace_hours < 24:
            raise ValueError("raw delete grace must be at least 24 hours")
        for name in (
            "data_root",
            "ledger_path",
            "fallback_spool_path",
            "lake_root",
            "manifest_root",
            "research_catalog_path",
            "writer_version",
        ):
            if not getattr(self, name):
                raise ValueError(f"{name} cannot be empty")
