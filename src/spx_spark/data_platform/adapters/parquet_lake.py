"""Parquet implementation of the immutable historical-lake port."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from spx_spark.data_platform.contracts import LakePartition, LakePublishReceipt
from spx_spark.data_platform.lake.compact import QuoteLakeCompactor
from spx_spark.data_platform.lake.layout import parse_raw_quote_partition


class ParquetHistoricalLake:
    """Publish verified closed-hour quote JSONL as ZSTD Parquet."""

    def __init__(self, compactor: QuoteLakeCompactor) -> None:
        self._compactor = compactor

    def publish_partition(
        self,
        partition: LakePartition,
        source_path: str | Path,
        *,
        as_of: datetime,
        dry_run: bool = False,
    ) -> LakePublishReceipt:
        if partition.dataset != "quotes":
            raise ValueError(f"unsupported Parquet dataset: {partition.dataset}")
        parsed = parse_raw_quote_partition(self._compactor.data_root, source_path)
        if parsed is None:
            raise ValueError("source path is not a canonical raw quote partition")
        if (
            parsed.session_date != partition.session_date.isoformat()
            or parsed.provider != partition.provider
            or parsed.hour != partition.hour
        ):
            raise ValueError("logical lake partition does not match the source path")
        result = self._compactor.compact_one(parsed, now=as_of, dry_run=dry_run)
        return LakePublishReceipt(
            partition=partition,
            source_path=result.source_path,
            output_path=result.output_path,
            status=result.status,
            row_count=result.row_count,
            source_sha256=result.source_sha256,
        )
