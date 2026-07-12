"""Analytics package must stay free of I/O and environment access."""

from __future__ import annotations

import ast
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[2] / "src" / "spx_spark"
ANALYTICS_ROOT = SRC_ROOT / "analytics"

FORBIDDEN_NAMES = {
    "open",
    "environ",
    "getenv",
    "system",
    "Popen",
    "run",
    "urlopen",
    "socket",
    "read_text",
    "write_text",
    "read_bytes",
    "write_bytes",
}

FORBIDDEN_MODULES = {
    "os",
    "subprocess",
    "socket",
    "urllib",
    "http",
    "pathlib",
}


def test_analytics_has_no_io_or_environment_access() -> None:
    violations: list[str] = []
    for path in sorted(ANALYTICS_ROOT.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        rel = path.relative_to(SRC_ROOT.parent.parent)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    top = alias.name.split(".", 1)[0]
                    if top in FORBIDDEN_MODULES:
                        violations.append(f"{rel}:{node.lineno} imports {alias.name}")
            elif isinstance(node, ast.ImportFrom) and node.module:
                top = node.module.split(".", 1)[0]
                if top in FORBIDDEN_MODULES:
                    violations.append(f"{rel}:{node.lineno} imports {node.module}")
            elif isinstance(node, ast.Call):
                name = None
                if isinstance(node.func, ast.Name):
                    name = node.func.id
                elif isinstance(node.func, ast.Attribute):
                    name = node.func.attr
                if name in FORBIDDEN_NAMES:
                    violations.append(f"{rel}:{node.lineno} calls {name}()")
            elif isinstance(node, ast.Attribute):
                if (
                    isinstance(node.value, ast.Name)
                    and node.value.id == "os"
                    and node.attr == "environ"
                ):
                    violations.append(f"{rel}:{node.lineno} accesses os.environ")
    assert not violations, "analytics I/O/env violations:\n" + "\n".join(violations)


def test_analytics_does_not_import_storage_config_or_notifier() -> None:
    """Pure kernels must not pull orchestration/config packages."""
    violations: list[str] = []
    blocked = {"config", "runtime_config", "notifier", "alert_engine", "service_loop", "storage"}
    for path in sorted(ANALYTICS_ROOT.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        rel = str(path.relative_to(SRC_ROOT)).replace("\\", "/")
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        for node in ast.walk(tree):
            modules: list[str] = []
            if isinstance(node, ast.Import):
                modules.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules.append(node.module)
            for module in modules:
                if not module.startswith("spx_spark."):
                    continue
                top = module.removeprefix("spx_spark.").split(".", 1)[0]
                if top in blocked:
                    violations.append(f"{rel} imports {module}")
    assert not violations, "analytics dependency violations:\n" + "\n".join(violations)
