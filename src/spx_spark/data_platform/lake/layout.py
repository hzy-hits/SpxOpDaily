from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


QUOTE_SCHEMA_VERSION = "v1"
QUOTE_WRITER_VERSION = "spx-spark-quote-compactor-v1"


@dataclass(frozen=True)
class RawQuotePartition:
    """A single collector JSONL file and the hour encoded by its path."""

    data_root: Path
    source_path: Path
    provider: str
    session_date: str
    hour: int

    @property
    def start_at(self) -> datetime:
        return datetime.strptime(
            f"{self.session_date}T{self.hour:02d}:00:00+00:00",
            "%Y-%m-%dT%H:%M:%S%z",
        ).astimezone(timezone.utc)

    @property
    def end_at(self) -> datetime:
        return self.start_at + timedelta(hours=1)

    @property
    def source_relative_path(self) -> str:
        return self.source_path.relative_to(self.data_root).as_posix()

    @property
    def parquet_path(self) -> Path:
        return (
            self.data_root
            / "lake"
            / "quotes"
            / f"schema={QUOTE_SCHEMA_VERSION}"
            / f"date={self.session_date}"
            / f"provider={self.provider}"
            / f"hour={self.hour:02d}"
            / "quotes.parquet"
        )

    @property
    def manifest_path(self) -> Path:
        return (
            self.data_root
            / "manifests"
            / "compaction"
            / f"schema={QUOTE_SCHEMA_VERSION}"
            / f"date={self.session_date}"
            / f"provider={self.provider}"
            / f"hour={self.hour:02d}"
            / "quotes.json"
        )


def discover_raw_quote_partitions(
    data_root: str | Path,
    *,
    raw_file_name: str = "quotes.jsonl",
) -> tuple[RawQuotePartition, ...]:
    root = Path(data_root)
    raw_root = root / "raw"
    if not raw_root.exists():
        return ()

    partitions: list[RawQuotePartition] = []
    pattern = f"provider=*/date=*/hour=*/{raw_file_name}"
    for path in raw_root.glob(pattern):
        if not path.is_file():
            continue
        parsed = parse_raw_quote_partition(root, path)
        if parsed is not None:
            partitions.append(parsed)
    return tuple(
        sorted(
            partitions,
            key=lambda item: (item.session_date, item.hour, item.provider, str(item.source_path)),
        )
    )


def parse_raw_quote_partition(
    data_root: str | Path,
    source_path: str | Path,
) -> RawQuotePartition | None:
    root = Path(data_root)
    path = Path(source_path)
    try:
        relative = path.relative_to(root / "raw")
    except ValueError:
        return None
    if len(relative.parts) != 4:
        return None

    provider_part, date_part, hour_part, _file_name = relative.parts
    if not provider_part.startswith("provider="):
        return None
    if not date_part.startswith("date=") or not hour_part.startswith("hour="):
        return None
    provider = provider_part.removeprefix("provider=").strip()
    session_date = date_part.removeprefix("date=").strip()
    hour_raw = hour_part.removeprefix("hour=").strip()
    if not provider or "/" in provider or "\\" in provider:
        return None
    try:
        datetime.strptime(session_date, "%Y-%m-%d")
        hour = int(hour_raw)
    except ValueError:
        return None
    if not 0 <= hour <= 23 or hour_raw != f"{hour:02d}":
        return None
    return RawQuotePartition(
        data_root=root,
        source_path=path,
        provider=provider,
        session_date=session_date,
        hour=hour,
    )
