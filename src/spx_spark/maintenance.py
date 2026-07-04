from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from spx_spark.config import MaintenanceSettings


@dataclass(frozen=True)
class FileEntry:
    path: str
    size_bytes: int
    modified_at: str
    category: str
    prune_candidate: bool
    reason: str | None


@dataclass(frozen=True)
class DirectorySummary:
    path: str
    exists: bool
    file_count: int
    total_bytes: int
    prune_candidate_count: int
    prune_candidate_bytes: int


@dataclass(frozen=True)
class MaintenanceReport:
    created_at: str
    disk_total_bytes: int
    disk_used_bytes: int
    disk_free_bytes: int
    disk_used_pct: float
    data_budget_bytes: int
    data_bytes: int
    data_budget_used_pct: float
    action_level: str
    settings: dict[str, object]
    summaries: list[DirectorySummary]
    prune_candidates: list[FileEntry]


def human_bytes(value: int) -> str:
    units = ("B", "KB", "MB", "GB", "TB")
    amount = float(value)
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            return f"{amount:.1f}{unit}"
        amount /= 1024
    return f"{amount:.1f}TB"


def classify_path(path: Path) -> str:
    parts = set(path.parts)
    if "raw" in parts:
        return "raw"
    if "features" in parts:
        if "interval=1s" in parts:
            return "feature_1s"
        if "interval=5s" in parts:
            return "feature_5s"
        return "features"
    if "alerts" in parts:
        return "alerts"
    if "reports" in parts:
        return "reports"
    if "trash" in parts:
        return "trash"
    return "logs" if "logs" in parts else "other"


def prune_reason(
    category: str,
    modified_at: datetime,
    now: datetime,
    settings: MaintenanceSettings,
) -> str | None:
    age = now - modified_at
    if category == "raw" and age > timedelta(days=settings.raw_retention_days):
        return f"raw older than {settings.raw_retention_days} days"
    if category == "feature_1s" and age > timedelta(days=settings.feature_1s_retention_days):
        return f"1s features older than {settings.feature_1s_retention_days} days"
    if category == "feature_5s" and age > timedelta(days=settings.feature_5s_retention_days):
        return f"5s features older than {settings.feature_5s_retention_days} days"
    if category == "logs" and age > timedelta(days=settings.log_retention_days):
        return f"logs older than {settings.log_retention_days} days"
    if category == "trash" and age > timedelta(days=settings.trash_retention_days):
        return f"trash older than {settings.trash_retention_days} days"
    return None


def scan_directory(
    root: Path,
    *,
    now: datetime,
    settings: MaintenanceSettings,
) -> tuple[DirectorySummary, list[FileEntry]]:
    if not root.exists():
        return DirectorySummary(str(root), False, 0, 0, 0, 0), []

    entries: list[FileEntry] = []
    total_bytes = 0
    candidate_bytes = 0
    candidate_count = 0

    for path in root.rglob("*"):
        if not path.is_file():
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        modified_at = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        category = classify_path(path)
        reason = prune_reason(category, modified_at, now, settings)
        total_bytes += stat.st_size
        if reason:
            candidate_count += 1
            candidate_bytes += stat.st_size
        entries.append(
            FileEntry(
                path=str(path),
                size_bytes=stat.st_size,
                modified_at=modified_at.isoformat(),
                category=category,
                prune_candidate=reason is not None,
                reason=reason,
            )
        )

    summary = DirectorySummary(
        path=str(root),
        exists=True,
        file_count=len(entries),
        total_bytes=total_bytes,
        prune_candidate_count=candidate_count,
        prune_candidate_bytes=candidate_bytes,
    )
    return summary, entries


def action_level(used_pct: float, settings: MaintenanceSettings) -> str:
    if used_pct >= settings.critical_pct:
        return "critical_stop_raw"
    if used_pct >= settings.prune_pct:
        return "prune"
    if used_pct >= settings.degraded_pct:
        return "degraded"
    if used_pct >= settings.compact_pct:
        return "compact"
    if used_pct >= settings.warn_pct:
        return "warn"
    return "ok"


def build_report(settings: MaintenanceSettings, now: datetime | None = None) -> MaintenanceReport:
    if now is None:
        now = datetime.now(tz=timezone.utc)

    data_root = Path(settings.data_root)
    logs_root = Path(settings.logs_root)
    roots = [data_root]
    if logs_root != data_root:
        roots.append(logs_root)

    summaries: list[DirectorySummary] = []
    entries: list[FileEntry] = []
    for root in roots:
        summary, root_entries = scan_directory(root, now=now, settings=settings)
        summaries.append(summary)
        entries.extend(root_entries)

    disk_usage = shutil.disk_usage(data_root if data_root.exists() else Path("."))
    data_bytes = sum(summary.total_bytes for summary in summaries if summary.path == str(data_root))
    data_budget_bytes = int(settings.data_budget_gb * 1024**3)
    disk_used_pct = (disk_usage.used / disk_usage.total) * 100 if disk_usage.total else 0.0
    data_budget_used_pct = (data_bytes / data_budget_bytes) * 100 if data_budget_bytes else 0.0
    candidates = [entry for entry in entries if entry.prune_candidate]
    return MaintenanceReport(
        created_at=now.isoformat(),
        disk_total_bytes=disk_usage.total,
        disk_used_bytes=disk_usage.used,
        disk_free_bytes=disk_usage.free,
        disk_used_pct=round(disk_used_pct, 2),
        data_budget_bytes=data_budget_bytes,
        data_bytes=data_bytes,
        data_budget_used_pct=round(data_budget_used_pct, 2),
        action_level=action_level(disk_used_pct, settings),
        settings=asdict(settings),
        summaries=summaries,
        prune_candidates=candidates,
    )


def write_report(report: MaintenanceReport, output_root: str) -> Path:
    output_dir = Path(output_root)
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d-%H%M%S")
    output_path = output_dir / f"maintenance-dry-run-{timestamp}.json"
    output_path.write_text(json.dumps(asdict(report), indent=2, sort_keys=True), encoding="utf-8")
    return output_path


def print_report(report: MaintenanceReport) -> None:
    print(f"Action level: {report.action_level}")
    print(
        "Disk: "
        f"{human_bytes(report.disk_used_bytes)} used / {human_bytes(report.disk_total_bytes)} "
        f"({report.disk_used_pct:.2f}%), free {human_bytes(report.disk_free_bytes)}"
    )
    print(
        "Project data budget: "
        f"{human_bytes(report.data_bytes)} / {human_bytes(report.data_budget_bytes)} "
        f"({report.data_budget_used_pct:.2f}%)"
    )
    print("\nDirectories:")
    for summary in report.summaries:
        print(
            f"- {summary.path}: exists={summary.exists} files={summary.file_count} "
            f"size={human_bytes(summary.total_bytes)} "
            f"prune_candidates={summary.prune_candidate_count} "
            f"prune_bytes={human_bytes(summary.prune_candidate_bytes)}"
        )
    total_prune = sum(entry.size_bytes for entry in report.prune_candidates)
    print(
        f"\nDry-run prune candidates: {len(report.prune_candidates)} files, "
        f"{human_bytes(total_prune)}"
    )
    for entry in report.prune_candidates[:20]:
        print(f"- {entry.path} {human_bytes(entry.size_bytes)}: {entry.reason}")
    if len(report.prune_candidates) > 20:
        print(f"... {len(report.prune_candidates) - 20} more candidates in JSON report")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SPX Spark maintenance utilities.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    dry_run = subparsers.add_parser("dry-run", help="Scan disk and report cleanup candidates.")
    dry_run.add_argument("--json", action="store_true", help="Print full JSON report to stdout.")
    dry_run.add_argument("--no-write", action="store_true", help="Do not write report to disk.")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = MaintenanceSettings.from_env()
    if args.command == "dry-run":
        report = build_report(settings)
        if args.json:
            print(json.dumps(asdict(report), indent=2, sort_keys=True))
        else:
            print_report(report)
        if not args.no_write:
            output_path = write_report(report, settings.output_root)
            print(f"\nWrote JSON report: {output_path}")
        return 0
    return 2


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
