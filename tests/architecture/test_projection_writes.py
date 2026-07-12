"""Architecture gate: canonical latest projection writes go through the port."""

from __future__ import annotations

import ast
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[2] / "src" / "spx_spark"

# Modules allowed to perform low-level latest JSON IO. New call sites must use
# LatestMarketProjectionStore (canonical projection boundary in storage.py).
LATEST_WRITE_ALLOWLIST = {
    "storage.py",
    "infrastructure/market_data/latest_projection.py",
    "state_io.py",
}


def _touches_latest_write(path: Path) -> list[str]:
    """Return heuristic hits for direct latest-projection file writes."""

    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    hits: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr in {"write_text", "replace", "rename"}:
                # Only flag when the surrounding source mentions latest state.
                if "latest_state" in source or "LatestState" in source:
                    # Exclude pure reads / comments by requiring Call near write.
                    hits.append(f"{path.name}:{node.lineno}:{node.func.attr}")
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id in {"atomic_write_json", "atomic_write_json_secure"}:
                if "latest_state" in source or "latest/" in source:
                    hits.append(f"{path.name}:{node.lineno}:{node.func.id}")
    return hits


def test_no_direct_canonical_projection_writes() -> None:
    offenders: list[str] = []
    for path in SRC_ROOT.rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        rel = str(path.relative_to(SRC_ROOT)).replace("\\", "/")
        if rel in LATEST_WRITE_ALLOWLIST:
            continue
        # Ignore tests under src (none) and generated.
        hits = _touches_latest_write(path)
        # Narrow: only storage-shaped latest paths, not every write_text in repo.
        # Flag modules that both mention latest_state_path and call write_text.
        source = path.read_text(encoding="utf-8")
        if "latest_state_path" in source and hits:
            # provider_adapter / collectors use LatestStateStore.update — OK if
            # they don't call write_text themselves.
            if "write_text" in {h.split(":")[-1] for h in hits}:
                if "LatestStateStore" in source or "LatestMarketProjectionStore" in source:
                    # Delegating to the store is fine.
                    continue
                offenders.extend(f"{rel}:{hit}" for hit in hits)
    assert not offenders, (
        "Direct canonical latest projection writes must go through "
        "LatestMarketProjectionStore / LatestStateStore:\n"
        + "\n".join(offenders)
    )


def test_latest_market_projection_store_is_exported() -> None:
    from spx_spark.infrastructure.market_data.latest_projection import (
        LatestMarketProjectionStore as InfraProjection,
    )
    from spx_spark.storage import LatestMarketProjectionStore, LatestStateStore

    assert LatestMarketProjectionStore is not None
    assert LatestStateStore is not None
    assert InfraProjection is LatestMarketProjectionStore
    assert issubclass(LatestMarketProjectionStore, LatestStateStore)
