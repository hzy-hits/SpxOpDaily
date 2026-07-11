"""Adapter from the existing JSONL quote writer to the landing-zone port."""

from __future__ import annotations

from collections.abc import Iterable

from spx_spark.config import StorageSettings
from spx_spark.data_platform.contracts import LandingWriteReceipt
from spx_spark.marketdata import Quote
from spx_spark.storage import JsonlQuoteWriter


class JsonlQuoteLandingWriter:
    """Preserve the current hot write path behind a capability interface."""

    def __init__(self, settings: StorageSettings) -> None:
        self._writer = JsonlQuoteWriter(settings)

    def append_quotes(self, quotes: Iterable[Quote]) -> LandingWriteReceipt:
        result = self._writer.write_quotes(quotes)
        return LandingWriteReceipt(
            row_count=result.row_count,
            path_counts=dict(result.path_counts),
        )
