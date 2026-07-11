"""Load documented runtime defaults from the repository YAML file."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


CONFIG_ENV_VAR = "SPX_SPARK_RUNTIME_CONFIG"
DEFAULT_CONFIG_RELATIVE_PATH = Path("config/runtime.yaml")


def runtime_config_path() -> Path:
    override = os.getenv(CONFIG_ENV_VAR, "").strip()
    if override:
        return Path(override).expanduser().resolve()
    cwd_candidate = (Path.cwd() / DEFAULT_CONFIG_RELATIVE_PATH).resolve()
    if cwd_candidate.is_file():
        return cwd_candidate
    repository_candidate = Path(__file__).resolve().parents[2] / DEFAULT_CONFIG_RELATIVE_PATH
    return repository_candidate.resolve()


@lru_cache(maxsize=4)
def _load_runtime_config(path_text: str) -> dict[str, Any]:
    path = Path(path_text)
    if not path.is_file():
        raise FileNotFoundError(
            f"Runtime configuration not found at {path}; set {CONFIG_ENV_VAR} explicitly"
        )
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Runtime configuration root must be a mapping: {path}")
    return payload


def runtime_config() -> dict[str, Any]:
    return _load_runtime_config(str(runtime_config_path()))


def runtime_value(dotted_path: str) -> Any:
    node: Any = runtime_config()
    for part in dotted_path.split("."):
        if not isinstance(node, dict) or part not in node:
            raise KeyError(f"Missing runtime configuration value: {dotted_path}")
        node = node[part]
    if not isinstance(node, dict) or "value" not in node:
        raise ValueError(f"Runtime setting must contain value and description: {dotted_path}")
    description = node.get("description")
    if not isinstance(description, str) or not description.strip():
        raise ValueError(f"Runtime setting has no description: {dotted_path}")
    return node["value"]


def runtime_csv(dotted_path: str) -> str:
    value = runtime_value(dotted_path)
    if not isinstance(value, list):
        raise TypeError(f"Runtime setting must be a list: {dotted_path}")
    return ",".join(str(item) for item in value)


def runtime_instrument_rows() -> list[dict[str, Any]]:
    rows = runtime_config().get("schwab", {}).get("instruments")
    if not isinstance(rows, list) or not rows:
        raise ValueError("schwab.instruments must be a non-empty list")
    validated: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise TypeError("Each schwab.instruments row must be a mapping")
        canonical = str(row.get("canonical_symbol", "")).strip().upper()
        description = str(row.get("description", "")).strip()
        if not canonical or not description:
            raise ValueError("Each Schwab instrument needs canonical_symbol and description")
        if canonical in seen:
            raise ValueError(f"Duplicate Schwab canonical symbol: {canonical}")
        seen.add(canonical)
        validated.append(dict(row, canonical_symbol=canonical))
    return validated


def runtime_schwab_symbols_by_type(instrument_type: str) -> list[str]:
    normalized = instrument_type.strip().lower()
    return [
        str(row["quote_symbol"]).strip().upper()
        for row in runtime_instrument_rows()
        if bool(row.get("collect_quote", False))
        and str(row.get("instrument_type", "")).strip().lower() == normalized
    ]


def runtime_schwab_option_chain_underliers() -> list[str]:
    return [
        str(row["canonical_symbol"]).strip().upper()
        for row in runtime_instrument_rows()
        if bool(row.get("collect_option_chain", False))
    ]
