"""File integrity and row-count helpers for lake compaction."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import duckdb


@dataclass(frozen=True)
class SourceSnapshot:
    size: int
    mtime_ns: int
    sha256: str


def snapshot_source(path: str | Path) -> SourceSnapshot:
    source_path = Path(path)
    before = source_path.stat()
    digest = hashlib.sha256()
    with source_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    after = source_path.stat()
    if before.st_size != after.st_size or before.st_mtime_ns != after.st_mtime_ns:
        raise RuntimeError(f"source changed while hashing: {source_path}")
    return SourceSnapshot(size=after.st_size, mtime_ns=after.st_mtime_ns, sha256=digest.hexdigest())


def count_jsonl_rows(path: str | Path) -> int:
    count = 0
    with Path(path).open("rb") as handle:
        for line in handle:
            if line.strip():
                count += 1
    return count


def count_parquet_rows(path: str | Path) -> int:
    connection = duckdb.connect()
    try:
        row = connection.execute(
            "SELECT count(*)::BIGINT FROM read_parquet(?)",
            [str(path)],
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        raise RuntimeError(f"unable to count parquet rows: {path}")
    return int(row[0])
