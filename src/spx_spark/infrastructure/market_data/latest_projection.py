"""Latest market projection store — rebuildable projection, not an event queue.

The concrete store lives in ``spx_spark.storage`` (L1) so collectors and
``provider_adapter`` can write through the named boundary without importing
the infrastructure package. This module re-exports for application-layer
composition roots.
"""

from __future__ import annotations

from spx_spark.storage import (
    LatestMarketProjectionStore,
    LatestState,
    LatestStateStore,
    LatestUpdateResult,
)

__all__ = [
    "LatestMarketProjectionStore",
    "LatestState",
    "LatestStateStore",
    "LatestUpdateResult",
]
