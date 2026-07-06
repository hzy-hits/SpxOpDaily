from __future__ import annotations

import ast
from pathlib import Path

import pytest

LAYERS = {
    "config": 0,
    "marketdata": 0,
    "alert_model": 0,
    "storage": 1,
    "sampling": 1,
    "runtime_mode": 1,
    "provider_adapter": 1,
    "ibkr": 2,
    "schwab": 2,
    "hyperliquid": 2,
    "polymarket": 2,
    "mock_collector": 2,
    "options_map": 3,
    "iv_surface": 3,
    "market_context": 3,
    "human_focus": 3,
    "strategy": 3,
    "alert_profile": 4,
    "alert_engine": 4,
    "notifier": 4,
    "position_alerts": 4,
    "service_loop": 5,
    "maintenance": 5,
    "post_close_review": 5,
    "latest_state": 5,
    "morning_map": 5,
}

L0_MODULES = {"config", "marketdata", "alert_model"}
L2_PROVIDERS = {"ibkr", "schwab", "hyperliquid", "polymarket", "mock_collector"}
L5_MODULES = {"service_loop", "maintenance", "post_close_review", "latest_state", "morning_map"}

POSITION_ALERTS_ALLOWED_L2_IMPORT = "ibkr.position_watcher"

SRC_ROOT = Path(__file__).resolve().parent.parent / "src" / "spx_spark"


def _module_name_from_path(path: Path) -> str:
    rel = path.relative_to(SRC_ROOT)
    parts = rel.with_suffix("").parts
    return parts[0] if len(parts) == 1 else parts[0]


def _target_module_from_import(module: str) -> str | None:
    if not module.startswith("spx_spark."):
        return None
    remainder = module.removeprefix("spx_spark.")
    if not remainder:
        return None
    return remainder.split(".", 1)[0]


def _collect_spx_spark_imports(path: Path) -> list[tuple[str, str]]:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    imports: list[tuple[str, str]] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                target = _target_module_from_import(alias.name)
                if target is not None:
                    imports.append((alias.name, target))
        elif isinstance(node, ast.ImportFrom):
            if node.module and node.level == 0:
                target = _target_module_from_import(node.module)
                if target is not None:
                    imports.append((node.module, target))

    return imports


def _format_violation(source_module: str, imported_module: str, source_layer: int, target_layer: int) -> str:
    return f"{source_module} -> {imported_module} (层{source_layer} -> 层{target_layer})"


def test_layer_import_rules() -> None:
    violations: list[str] = []
    l0_violations: list[str] = []
    l2_cross_violations: list[str] = []
    l2_whitelist_violations: list[str] = []

    for path in sorted(SRC_ROOT.rglob("*.py")):
        source_module = _module_name_from_path(path)
        source_layer = LAYERS.get(source_module)
        if source_layer is None:
            continue

        for imported_full, imported_top in _collect_spx_spark_imports(path):
            target_layer = LAYERS.get(imported_top)
            if target_layer is None:
                continue

            rel_path = str(path.relative_to(SRC_ROOT.parent.parent))

            if source_module in L0_MODULES:
                l0_violations.append(
                    _format_violation(rel_path, imported_full, source_layer, target_layer)
                )
                continue

            if source_layer != 5 and target_layer > source_layer:
                violations.append(
                    _format_violation(rel_path, imported_full, source_layer, target_layer)
                )

            if source_module in L2_PROVIDERS and imported_top in L2_PROVIDERS and imported_top != source_module:
                l2_cross_violations.append(
                    _format_violation(rel_path, imported_full, source_layer, target_layer)
                )

            if (
                source_module not in L5_MODULES
                and source_module not in L2_PROVIDERS
                and imported_top in L2_PROVIDERS
            ):
                allowed = (
                    source_module == "position_alerts"
                    and imported_full == f"spx_spark.{POSITION_ALERTS_ALLOWED_L2_IMPORT}"
                )
                if not allowed:
                    l2_whitelist_violations.append(
                        _format_violation(rel_path, imported_full, source_layer, target_layer)
                    )

    messages: list[str] = []
    if violations:
        messages.append("分层违例:\n" + "\n".join(sorted(violations)))
    if l0_violations:
        messages.append("L0 内部依赖违例:\n" + "\n".join(sorted(l0_violations)))
    if l2_cross_violations:
        messages.append("L2 provider 互引违例:\n" + "\n".join(sorted(l2_cross_violations)))
    if l2_whitelist_violations:
        messages.append("L2 白名单违例:\n" + "\n".join(sorted(l2_whitelist_violations)))

    if messages:
        pytest.fail("\n\n".join(messages))
