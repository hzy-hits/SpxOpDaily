"""Top-level typed settings aggregates.

Domain-specific slices live in sibling modules; AppSettings is the composition
root object injected into entrypoints. Business/analytics code must receive
typed settings rather than calling runtime_value().
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from spx_spark.settings.alerts import AlertSettings
from spx_spark.settings.analytics import AnalyticsSettings
from spx_spark.settings.globex_trend import GlobexTrendSettings
from spx_spark.settings.ibkr import IbkrSettingsSlice
from spx_spark.settings.level_decision import LevelDecisionPolicy
from spx_spark.settings.market_data import MarketDataSettings
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.settings.order_map import OrderMapPolicy
from spx_spark.settings.runtime import RuntimeSettingsSlice
from spx_spark.settings.schwab import SchwabSettingsSlice
from spx_spark.settings.shock import ShockSettings
from spx_spark.settings.spring_gamma_v3 import SpringGammaV3Settings
from spx_spark.settings.storage import StorageSettingsSlice


@dataclass(frozen=True)
class SettingSource:
    """Provenance for a resolved setting; never log secret values."""

    path: str
    origin: str  # defaults | deployment | environment


@dataclass(frozen=True)
class AppSettings:
    market_data: MarketDataSettings
    ibkr: IbkrSettingsSlice
    schwab: SchwabSettingsSlice
    analytics: AnalyticsSettings
    globex_trend: GlobexTrendSettings
    market_features: MarketFeatureSettings
    spring_gamma_v3: SpringGammaV3Settings
    alerts: AlertSettings
    runtime: RuntimeSettingsSlice
    shock: ShockSettings
    level_decision: LevelDecisionPolicy
    order_map: OrderMapPolicy
    storage: StorageSettingsSlice
    defaults_path: Path
    deployment_path: Path | None
    sources: Mapping[str, SettingSource]
    raw: Mapping[str, Any]
