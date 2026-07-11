"""Composition root for latency-sensitive storage ports.

DuckDB is intentionally absent: realtime processes should never import or
initialize the analytical engine. Research callers construct a catalog from
``spx_spark.data_platform.research`` explicitly, while batch callers construct
the historical-lake adapter from ``spx_spark.data_platform.adapters.parquet_lake``.
"""

from __future__ import annotations

from dataclasses import dataclass

from spx_spark.config import StorageSettings
from spx_spark.data_platform.adapters.jsonl_landing import JsonlQuoteLandingWriter
from spx_spark.data_platform.adapters.sqlite_ledger import SQLiteDecisionLedger
from spx_spark.data_platform.ports import DecisionLedger, QuoteLandingWriter
from spx_spark.data_platform.settings import DataPlatformSettings
from spx_spark.marketdata import Quote


@dataclass(frozen=True)
class OperationalDataPlatform:
    ledger: DecisionLedger
    landing: QuoteLandingWriter[Quote]


def build_operational_data_platform(
    settings: DataPlatformSettings,
    *,
    storage_settings: StorageSettings | None = None,
) -> OperationalDataPlatform:
    storage = storage_settings or StorageSettings.from_env()
    return OperationalDataPlatform(
        ledger=SQLiteDecisionLedger(
            settings.ledger_path,
            busy_timeout_ms=settings.sqlite_busy_timeout_ms,
        ),
        landing=JsonlQuoteLandingWriter(storage),
    )
