from __future__ import annotations

import ast
from pathlib import Path


def test_domain_and_analytics_do_not_read_the_wall_clock() -> None:
    root = Path(__file__).resolve().parents[2] / "src" / "spx_spark"
    offenders: list[str] = []
    for package in ("domain", "analytics"):
        for path in (root / package).rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Attribute) or node.attr not in {"now", "utcnow"}:
                    continue
                if isinstance(node.value, ast.Name) and node.value.id == "datetime":
                    offenders.append(f"{path.relative_to(root)}:{node.lineno}")
    assert offenders == []
