"""Strategy plug-in registry and shadow runner.

Plugins are imported from ``SPX_STRATEGY_PLUGINS`` (comma-separated module
names) and must expose ``register(registry)``. Registering a strategy never
changes authorization: the runner writes shadow candidates with
``authority: none`` and the Core decision stays untouched.
"""

from __future__ import annotations

import importlib
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from spx_spark.domain.strategy import Strategy, StrategyCandidate
from spx_spark.state_io import atomic_write_json_secure

PLUGIN_ENV = "SPX_STRATEGY_PLUGINS"
SHADOW_PATH = "latest/strategy_shadow.json"


@dataclass(frozen=True)
class _Registration:
    strategy: Strategy
    shadow: bool


class StrategyRegistry:
    def __init__(self) -> None:
        self._entries: dict[str, _Registration] = {}
        self._plugins_loaded = False

    def register(self, strategy: Strategy, *, shadow: bool = True) -> None:
        strategy_id = str(getattr(strategy, "strategy_id", "") or "").strip()
        version = str(getattr(strategy, "version", "") or "").strip()
        if not strategy_id or not version:
            raise ValueError("strategy_id and version are required")
        for method in ("evaluate", "gates", "manage", "explain"):
            if not callable(getattr(strategy, method, None)):
                raise TypeError(f"strategy {strategy_id} is missing {method}()")
        if strategy_id in self._entries:
            raise ValueError(f"duplicate strategy: {strategy_id}")
        self._entries[strategy_id] = _Registration(strategy, shadow)

    def clear(self) -> None:
        self._entries.clear()
        self._plugins_loaded = False

    def active(self, *, shadow: bool | None = None) -> tuple[Strategy, ...]:
        return tuple(
            entry.strategy
            for entry in self._entries.values()
            if shadow is None or entry.shadow is shadow
        )

    def load_plugins(self, env_var: str = PLUGIN_ENV) -> None:
        if self._plugins_loaded:
            return
        self._plugins_loaded = True
        for module_name in os.getenv(env_var, "").split(","):
            name = module_name.strip()
            if not name:
                continue
            module = importlib.import_module(name)
            register = getattr(module, "register", None)
            if not callable(register):
                raise TypeError(f"strategy plugin {name} has no register(registry)")
            register(self)


REGISTRY = StrategyRegistry()


def run_shadow_strategies(
    data_root: str | Path,
    facts: Mapping[str, Any],
    *,
    now: datetime,
    registry: StrategyRegistry | None = None,
    persist: bool = True,
) -> tuple[dict[str, Any], ...]:
    selected = registry or REGISTRY
    try:
        selected.load_plugins()
    except Exception:
        # A broken shadow plug-in must never block the authorization cycle.
        return ()
    strategies = selected.active(shadow=True)
    if not strategies:
        return ()
    candidates: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for strategy in strategies:
        try:
            evaluated = strategy.evaluate(facts)
        except Exception as exc:
            errors.append({"strategy_id": strategy.strategy_id, "error": type(exc).__name__})
            continue
        for candidate in evaluated:
            if not isinstance(candidate, StrategyCandidate):
                errors.append(
                    {"strategy_id": strategy.strategy_id, "error": "invalid_candidate"}
                )
                continue
            gates = [
                gate.to_dict()
                for gate in strategy.gates(candidate, facts)
            ]
            row = candidate.to_dict()
            row["gates"] = gates
            candidates.append(row)
    payload = {
        "schema_version": 1,
        "as_of": now.isoformat(),
        "authority": "none",
        "candidates": candidates,
        "errors": errors,
    }
    if persist:
        try:
            atomic_write_json_secure(Path(data_root) / SHADOW_PATH, payload)
        except (OSError, TypeError, ValueError):
            # Shadow telemetry never blocks the decision cycle.
            pass
    return tuple(candidates)


def plugin_registry() -> StrategyRegistry:
    return REGISTRY
