"""Load documented runtime defaults from the repository YAML file."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


CONFIG_ENV_VAR = "SPX_SPARK_RUNTIME_CONFIG"
OVERRIDES_ENV_VAR = "SPX_SPARK_RUNTIME_OVERRIDES"
DEFAULT_CONFIG_RELATIVE_PATH = Path("config/runtime.yaml")
DEFAULT_OVERRIDES_RELATIVE_PATH = Path("config/runtime.local.yaml")


def runtime_config_path() -> Path:
    override = os.getenv(CONFIG_ENV_VAR, "").strip()
    if override:
        return Path(override).expanduser().resolve()
    cwd_candidate = (Path.cwd() / DEFAULT_CONFIG_RELATIVE_PATH).resolve()
    if cwd_candidate.is_file():
        return cwd_candidate
    repository_candidate = Path(__file__).resolve().parents[2] / DEFAULT_CONFIG_RELATIVE_PATH
    return repository_candidate.resolve()


def runtime_overrides_path() -> Path | None:
    explicit = os.getenv(OVERRIDES_ENV_VAR, "").strip()
    if explicit:
        path = Path(explicit).expanduser().resolve()
        if not path.is_file():
            raise FileNotFoundError(f"Runtime overrides not found at {path}")
        return path
    disabled = os.getenv("SPX_SPARK_DISABLE_RUNTIME_OVERRIDES", "").strip().lower()
    if disabled in {"1", "true", "yes", "y", "on"}:
        return None
    cwd_candidate = (Path.cwd() / DEFAULT_OVERRIDES_RELATIVE_PATH).resolve()
    if cwd_candidate.is_file():
        return cwd_candidate
    repository_candidate = Path(__file__).resolve().parents[2] / DEFAULT_OVERRIDES_RELATIVE_PATH
    if repository_candidate.is_file():
        return repository_candidate.resolve()
    return None


def _read_mapping(path: Path, *, label: str) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"{label} not found at {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{label} root must be a mapping: {path}")
    return payload


def _merge_runtime_overrides(
    base: Any,
    overrides: Any,
    *,
    dotted_path: str = "",
) -> Any:
    if isinstance(base, dict) and "value" in base:
        if not isinstance(overrides, dict) or set(overrides) != {"value"}:
            raise ValueError(
                f"Runtime override for {dotted_path} must contain only a value field"
            )
        base_value = base["value"]
        override_value = overrides["value"]
        _validate_override_value(base_value, override_value, dotted_path=dotted_path)
        return {**base, "value": override_value}
    if isinstance(base, dict):
        if not isinstance(overrides, dict):
            raise TypeError(f"Runtime override for {dotted_path or '<root>'} must be a mapping")
        unknown = sorted(set(overrides) - set(base))
        if unknown:
            location = dotted_path or "<root>"
            raise KeyError(f"Unknown runtime override keys at {location}: {unknown}")
        merged = dict(base)
        for key, value in overrides.items():
            child_path = f"{dotted_path}.{key}" if dotted_path else str(key)
            merged[key] = _merge_runtime_overrides(base[key], value, dotted_path=child_path)
        return merged
    if isinstance(base, list):
        if not isinstance(overrides, list):
            raise TypeError(f"Runtime override for {dotted_path} must be a list")
        return list(overrides)
    raise ValueError(
        f"Runtime override for undocumented scalar {dotted_path} is not supported"
    )


def _validate_override_value(base: Any, override: Any, *, dotted_path: str) -> None:
    if isinstance(base, bool):
        valid = isinstance(override, bool)
    elif isinstance(base, int):
        valid = isinstance(override, int) and not isinstance(override, bool)
    elif isinstance(base, float):
        valid = isinstance(override, int | float) and not isinstance(override, bool)
    else:
        valid = isinstance(override, type(base))
    if not valid:
        raise TypeError(
            f"Runtime override for {dotted_path} must match {type(base).__name__}, "
            f"got {type(override).__name__}"
        )


@lru_cache(maxsize=8)
def _load_runtime_config(path_text: str, overrides_path_text: str = "") -> dict[str, Any]:
    path = Path(path_text)
    payload = _read_mapping(path, label="Runtime configuration")
    if not overrides_path_text:
        return payload
    overrides_path = Path(overrides_path_text)
    overrides = _read_mapping(overrides_path, label="Runtime overrides")
    return _merge_runtime_overrides(payload, overrides)


def runtime_config() -> dict[str, Any]:
    overrides_path = runtime_overrides_path()
    return _load_runtime_config(
        str(runtime_config_path()),
        str(overrides_path) if overrides_path is not None else "",
    )


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
