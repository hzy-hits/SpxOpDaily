"""Architecture gate: compatibility facades stay within documented size budgets."""

from __future__ import annotations

from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[2] / "src" / "spx_spark"

FACADE_LINE_BUDGETS = {
    "options_map/__init__.py": 150,
    "order_map.py": 100,
    "ibkr/stream_collector.py": 100,
    "intraday_shock.py": 50,
    "morning_map.py": 50,
}


def _nonempty_lines(path: Path) -> int:
    return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())


def test_compatibility_facades_within_line_budget() -> None:
    offenders: list[str] = []
    for rel, budget in FACADE_LINE_BUDGETS.items():
        path = SRC_ROOT / rel
        if not path.is_file():
            offenders.append(f"{rel}: missing")
            continue
        count = _nonempty_lines(path)
        if count > budget:
            offenders.append(f"{rel}: {count} nonempty lines > {budget}")
    assert not offenders, "Facade size budget exceeded:\n" + "\n".join(offenders)
