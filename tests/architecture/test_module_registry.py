from __future__ import annotations

import ast
from pathlib import Path

import pytest

# Layer numbers align with module-architecture.md. New packages from the
# refactor acceptance plan use the target dependency direction:
# domain(0) / settings(0) -> analytics/providers -> application -> infrastructure/entrypoints
LAYERS = {
    "__init__": 0,
    "domain": 0,
    "settings": 0,
    "runtime_config": 0,
    "marketdata": 0,
    "market_calendar": 0,
    "macro_event_clock": 0,
    "alert_model": 0,
    "strategy_contract": 0,
    "config": 1,
    "config_ibkr": 1,
    "config_providers": 1,
    "storage": 1,
    "state_io": 1,
    "surface_artifact": 1,
    "sampling": 1,
    "runtime_mode": 1,
    "provider_adapter": 1,
    "market_data_policy": 1,
    "provider_failover": 1,
    "position_events": 1,
    "ibkr": 2,
    "schwab": 2,
    "hyperliquid": 2,
    "polymarket": 2,
    "mock_collector": 2,
    # Temporary: IBKR stream consumes the control document. Moves to application/.
    "provider_failover_controller": 2,
    "analytics": 3,
    "application": 5,
    "options_map": 3,
    "features": 3,
    "greek_reference": 3,
    "greek_reference_io": 3,
    "greek_reference_payload": 3,
    "iv_surface": 3,
    "market_context": 3,
    "human_focus": 3,
    "strategy": 3,
    "intraday_strategy": 3,
    "steven_validation": 3,
    "alert_profile": 4,
    "alert_engine": 4,
    "notifier": 4,
    "position_alerts": 4,
    "data_platform": 4,
    "greek_shadow": 4,
    "intraday_event_outcomes": 4,
    "intraday_shock": 5,
    "service_loop": 5,
    "maintenance": 5,
    "post_close_review": 5,
    "post_close_completeness": 5,
    "post_close_quality": 5,
    "post_close_render": 5,
    "post_close_runtime": 5,
    "latest_state": 5,
    "morning_map": 5,
    "order_map": 5,
    "surface_dashboard": 5,
    "surface_dashboard_replay": 5,
    "surface_replay_http": 5,
    "surface_replay_catalog_payload": 5,
    "surface_replay_service": 5,
    "surface_replay_session": 5,
    "surface_replay_session_data": 5,
    "surface_replay_session_frames": 5,
    "surface_replay_session_models": 5,
    "surface_replay_session_reference": 5,
    "surface_replay_trend": 5,
    "surface_live_session_http": 5,
    "surface_live_session_models": 5,
    "surface_live_session_projection": 5,
    "surface_live_session_store": 5,
    "surface_live_session_worker": 5,
    "infrastructure": 4,
}

L0_MODULES = {
    "domain",
    "settings",
    "runtime_config",
    "marketdata",
    "market_calendar",
    "macro_event_clock",
    "alert_model",
    "strategy_contract",
}
L2_PROVIDERS = {"ibkr", "schwab", "hyperliquid", "polymarket", "mock_collector"}
L5_MODULES = {module for module, layer in LAYERS.items() if layer == 5}

POSITION_ALERTS_ALLOWED_L2_IMPORT = "ibkr.position_watcher"

SRC_ROOT = Path(__file__).resolve().parents[2] / "src" / "spx_spark"
ENV_HELPERS = {
    "env_bool",
    "env_int",
    "env_float",
    "env_str",
    "env_csv",
    "env_csv_preserve",
}


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


def test_all_production_modules_are_classified() -> None:
    production_modules = {
        _module_name_from_path(path)
        for path in SRC_ROOT.rglob("*.py")
        if "__pycache__" not in path.parts
    }
    classified_modules = set(LAYERS)

    missing = sorted(production_modules - classified_modules)
    stale = sorted(classified_modules - production_modules)
    assert not missing, f"Production modules missing from architecture registry: {missing}"
    assert not stale, f"Stale architecture registry entries: {stale}"


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
            # settings may import sibling settings modules only.
            if source_module == "settings" and imported_top == "settings":
                continue
            if source_module == "domain" and imported_top == "domain":
                continue

            target_layer = LAYERS.get(imported_top)
            if target_layer is None:
                continue

            rel_path = str(path.relative_to(SRC_ROOT.parent.parent))

            if source_module in L0_MODULES and imported_top != source_module:
                # L0 packages may only import within themselves (or nothing).
                if source_module in {"domain", "settings"} and imported_top == source_module:
                    continue
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


def _call_name(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def _literal_default(node: ast.AST) -> bool:
    if isinstance(node, ast.Constant | ast.List | ast.Tuple | ast.Dict | ast.Set):
        return True
    if (
        isinstance(node, ast.UnaryOp)
        and isinstance(node.op, ast.USub)
        and isinstance(node.operand, ast.Constant)
    ):
        return True
    return False


def test_env_helper_defaults_are_not_literals() -> None:
    violations: list[str] = []

    for path in sorted(SRC_ROOT.rglob("*.py")):
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if _call_name(node) not in ENV_HELPERS or len(node.args) < 2:
                continue
            default_arg = node.args[1]
            if _literal_default(default_arg):
                rel_path = path.relative_to(SRC_ROOT.parent.parent)
                violations.append(f"{rel_path}:{node.lineno}")

    if violations:
        pytest.fail(
            "env_* defaults must come from runtime_value/runtime_csv or another non-literal source:\n"
            + "\n".join(violations)
        )


def test_domain_has_stdlib_only() -> None:
    domain_root = SRC_ROOT / "domain"
    if not domain_root.is_dir():
        pytest.skip("domain package not present")
    violations: list[str] = []
    for path in sorted(domain_root.rglob("*.py")):
        for imported_full, imported_top in _collect_spx_spark_imports(path):
            if imported_top != "domain":
                rel = path.relative_to(SRC_ROOT.parent.parent)
                violations.append(f"{rel} imports {imported_full}")
    assert not violations, "domain must be stdlib-only:\n" + "\n".join(violations)
