#!/usr/bin/env python3
"""Build a reproducible audit of the Gamma, Steven, and 15-minute decision models."""

from __future__ import annotations

import glob
import json
import os
import re
import subprocess
import sys
from collections import Counter
from datetime import datetime, time, timezone
from pathlib import Path
from statistics import mean, median
from zoneinfo import ZoneInfo

import nbformat
from nbclient import NotebookClient


REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = Path(os.environ.get("SPX_SPARK_DATA_ROOT", "/srv/data/spx-spark/data"))
DOCS_ROOT = REPO_ROOT / "docs"
REPORT_DATE = "2026-07-24"
WINDOW_START = "2026-07-03"
WINDOW_END = "2026-07-23"
ARTIFACT_PATH = DOCS_ROOT / f"spx-gamma-decision-model-audit-{REPORT_DATE}.artifact.json"
NOTEBOOK_PATH = DOCS_ROOT / f"spx-gamma-decision-model-audit-{REPORT_DATE}.ipynb"
HTML_PATH = DOCS_ROOT / f"spx-gamma-decision-model-audit-{REPORT_DATE}.html"
PLUGIN_ROOT = Path(
    "/home/ubuntu/.codex/plugins/cache/openai-curated-remote/"
    "data-analytics/0.2.8-13ceeea1f599"
)
PORTABLE_BUILDER = PLUGIN_ROOT / "skills/build-report/scripts/build_portable_artifact.mjs"
PORTABLE_VERIFIER = PLUGIN_ROOT / "skills/build-report/scripts/verify_portable_artifact.mjs"
ET = ZoneInfo("America/New_York")


def read_jsonl(path: Path):
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            if isinstance(row, dict):
                yield row


def _visible_decision(text: str) -> tuple[int | None, str | None, str | None]:
    match = re.search(
        r"(?:判断|观察)\s+"
        r"(趋势偏多|趋势偏空|过渡偏多|过渡偏空|均值回归|方向过渡|证据不足)",
        text,
    )
    if match is None:
        return None, None, None
    label = match.group(1)
    side = 1 if "多" in label else -1 if "空" in label else 0
    mode = (
        "trending"
        if label.startswith("趋势")
        else "transition"
        if label.startswith("过渡")
        else "mean_reverting"
        if label == "均值回归"
        else "unavailable"
    )
    return side, mode, label


def _scores(text: str) -> tuple[float | None, float | None]:
    match = re.search(r"趋势\s+([0-9.]+)\s*/\s*回归\s+([0-9.]+)", text)
    if match is None:
        return None, None
    return float(match.group(1)), float(match.group(2))


def _gamma_state(text: str) -> str | None:
    for token, state in (
        ("ZeroGamma过渡", "zero_gamma_transition"),
        ("正Gamma", "positive_gamma_pin"),
        ("负Gamma", "negative_gamma_acceleration"),
        ("negative_gamma_acceleration", "negative_gamma_acceleration"),
        ("positive_gamma_pin", "positive_gamma_pin"),
        ("zero_gamma_transition", "zero_gamma_transition"),
        ("mixed_gamma", "mixed_gamma"),
    ):
        if token in text:
            return state
    return None


def _es_price(text: str) -> float | None:
    patterns = (
        r"SPX (?:代理|proxy)[:：]?[^\n]*?[；;]\s*ES\s+(-?\d+(?:\.\d+)?)",
        r"价格\s+SPX\s+[-\d.]+(?:\([^)]*\))?\s*[｜|　]+\s*ES\s+(-?\d+(?:\.\d+)?)",
        r"参考价[:：]\s*[-\d.]+\([^\n)]*\)\s*[；;,]\s*ES\s+(-?\d+(?:\.\d+)?)",
        r"时段[:：][^\n]*?SPX\s+[-\d.]+\([^)]*\)\s*,\s*ES\s+(-?\d+(?:\.\d+)?)",
    )
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return float(match.group(1))
    return None


def _es_source(text: str) -> str | None:
    match = re.search(r"(?:ES源|源)\s+(schwab|ibkr)", text, re.I)
    return match.group(1).lower() if match else None


def _zero_gamma(text: str) -> float | None:
    match = re.search(r"结构\s+ZeroGamma过渡[^\n]*?ZG\s+(-?\d+(?:\.\d+)?)", text)
    return float(match.group(1)) if match else None


def load_reports() -> list[dict]:
    rows: list[dict] = []
    for filename in glob.glob(str(DATA_ROOT / "audit/order_map_pricing/date=*/reports.jsonl")):
        for row in read_jsonl(Path(filename)):
            trading_date = str(row.get("trading_date") or "")
            if (
                row.get("report_kind") != "status"
                or not WINDOW_START <= trading_date <= WINDOW_END
            ):
                continue
            generated_at = datetime.fromisoformat(str(row["generated_at"]))
            local = generated_at.astimezone(ET)
            in_rth = (
                local.weekday() < 5
                and time(9, 30) <= local.time().replace(tzinfo=None) < time(16)
            )
            template = str(row.get("template") or "")
            delivered = str(row.get("delivered_text") or "")
            side, mode, label = _visible_decision(template or delivered)
            trend_score, reversion_score = _scores(template)
            underlier = row.get("underlier")
            spot = (
                underlier.get("price")
                if isinstance(underlier, dict)
                and isinstance(underlier.get("price"), int | float)
                else None
            )
            rows.append(
                {
                    "trading_date": trading_date,
                    "generated_at": generated_at,
                    "session": "RTH" if in_rth else "GTH",
                    "side": side,
                    "mode": mode,
                    "label": label,
                    "trend_score": trend_score,
                    "reversion_score": reversion_score,
                    "gamma_state": _gamma_state(template),
                    "es": _es_price(template),
                    "es_source": _es_source(template),
                    "spot": float(spot) if spot is not None else None,
                    "zero_gamma": _zero_gamma(template),
                }
            )
    rows.sort(key=lambda row: row["generated_at"])
    return rows


def _forward_value(rows: list[dict], row: dict, horizon_minutes: int) -> float | None:
    target = row["generated_at"].timestamp() + horizon_minutes * 60
    matches = [
        candidate
        for candidate in rows
        if candidate["trading_date"] == row["trading_date"]
        and candidate["es"] is not None
        and abs(candidate["generated_at"].timestamp() - target) <= 180
        and (
            row["es_source"] is None
            or candidate["es_source"] is None
            or row["es_source"] == candidate["es_source"]
        )
    ]
    if not matches or row["es"] is None:
        return None
    future = min(
        matches,
        key=lambda candidate: abs(candidate["generated_at"].timestamp() - target),
    )
    return float(future["es"]) - float(row["es"])


def _score_bucket(value: float) -> str:
    if value < 45:
        return "0–44"
    if value < 65:
        return "45–64"
    return "65+"


def score_performance(rows: list[dict]) -> list[dict]:
    result: list[dict] = []
    for session in ("RTH", "GTH"):
        for bucket in ("0–44", "45–64", "65+"):
            for horizon in (15, 30, 60):
                values: list[float] = []
                for row in rows:
                    score = row["trend_score"]
                    if (
                        row["session"] != session
                        or row["side"] not in (-1, 1)
                        or score is None
                        or _score_bucket(float(score)) != bucket
                    ):
                        continue
                    move = _forward_value(rows, row, horizon)
                    if move is not None:
                        values.append(float(row["side"]) * move)
                if values:
                    result.append(
                        {
                            "session": session,
                            "score_bucket": bucket,
                            "horizon": f"{horizon}m",
                            "n": len(values),
                            "hit_rate": sum(value > 0 for value in values) / len(values),
                            "mean_signed_es_points": mean(values),
                            "median_signed_es_points": median(values),
                        }
                    )
    return result


def mode_performance(rows: list[dict]) -> list[dict]:
    result: list[dict] = []
    for session in ("RTH", "GTH"):
        for mode in ("trending", "transition"):
            for horizon in (15, 30, 60):
                values: list[float] = []
                for row in rows:
                    if row["session"] != session or row["mode"] != mode or row["side"] not in (-1, 1):
                        continue
                    move = _forward_value(rows, row, horizon)
                    if move is not None:
                        values.append(float(row["side"]) * move)
                if values:
                    result.append(
                        {
                            "session": session,
                            "mode": mode,
                            "horizon": f"{horizon}m",
                            "n": len(values),
                            "hit_rate": sum(value > 0 for value in values) / len(values),
                            "mean_signed_es_points": mean(values),
                        }
                    )
    return result


def gamma_performance(rows: list[dict]) -> list[dict]:
    result: list[dict] = []
    states = (
        "negative_gamma_acceleration",
        "zero_gamma_transition",
        "positive_gamma_pin",
        "mixed_gamma",
    )
    for session in ("RTH", "GTH"):
        for state in states:
            for horizon in (15, 30, 60):
                values: list[float] = []
                for row in rows:
                    if (
                        row["session"] != session
                        or row["gamma_state"] != state
                        or row["side"] not in (-1, 1)
                    ):
                        continue
                    move = _forward_value(rows, row, horizon)
                    if move is not None:
                        values.append(float(row["side"]) * move)
                if values:
                    result.append(
                        {
                            "session": session,
                            "gamma_state": state,
                            "horizon": f"{horizon}m",
                            "n": len(values),
                            "hit_rate": sum(value > 0 for value in values) / len(values),
                            "mean_signed_es_points": mean(values),
                        }
                    )
    return result


def gamma_distribution(rows: list[dict]) -> list[dict]:
    result: list[dict] = []
    labels = {
        "zero_gamma_transition": "ZG",
        "negative_gamma_acceleration": "Neg",
        "positive_gamma_pin": "Pos",
        "mixed_gamma": "Mix",
        None: "Missing",
    }
    for session in ("RTH", "GTH"):
        counts = Counter(row["gamma_state"] for row in rows if row["session"] == session)
        total = sum(counts.values())
        for state in (
            "zero_gamma_transition",
            "negative_gamma_acceleration",
            "positive_gamma_pin",
            "mixed_gamma",
            None,
        ):
            result.append(
                {
                    "session": session,
                    "gamma_state": labels[state],
                    "count": counts[state],
                    "share": counts[state] / total if total else None,
                }
            )
    return result


def zero_gamma_distance(rows: list[dict]) -> list[dict]:
    result: list[dict] = []
    for session in ("ALL", "RTH", "GTH"):
        selected = [
            row
            for row in rows
            if (session == "ALL" or row["session"] == session)
            and row["gamma_state"] == "zero_gamma_transition"
            and row["spot"] not in (None, 0)
            and row["zero_gamma"] is not None
        ]
        distances = [abs(float(row["zero_gamma"]) - float(row["spot"])) for row in selected]
        result.append(
            {
                "session": session,
                "n": len(selected),
                "median_distance_points": median(distances) if distances else None,
                "within_0_10_pct": (
                    sum(
                        abs(float(row["zero_gamma"]) - float(row["spot"]))
                        / float(row["spot"])
                        <= 0.001
                        for row in selected
                    )
                    / len(selected)
                    if selected
                    else None
                ),
                "within_0_25_pct": (
                    sum(
                        abs(float(row["zero_gamma"]) - float(row["spot"]))
                        / float(row["spot"])
                        <= 0.0025
                        for row in selected
                    )
                    / len(selected)
                    if selected
                    else None
                ),
                "within_0_50_pct": (
                    sum(
                        abs(float(row["zero_gamma"]) - float(row["spot"]))
                        / float(row["spot"])
                        <= 0.005
                        for row in selected
                    )
                    / len(selected)
                    if selected
                    else None
                ),
            }
        )
    return result


def current_option_quality() -> tuple[list[dict], dict]:
    path = DATA_ROOT / "latest/market_feature_state.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    history = payload.get("option_history") or []
    rows: list[dict] = []
    for item in history:
        structure = item.get("structure") if isinstance(item.get("structure"), dict) else {}
        exposure = item.get("exposure") if isinstance(item.get("exposure"), dict) else {}
        rows.append(
            {
                "as_of": item.get("as_of"),
                "quality": item.get("quality"),
                "gamma_state": structure.get("gamma_state"),
                "net_gamma_ratio": structure.get("net_gamma_ratio"),
                "put_wall": structure.get("put_wall"),
                "call_wall": structure.get("call_wall"),
                "iv_coverage_ratio": exposure.get("iv_coverage_ratio"),
                "delta_coverage_ratio": exposure.get("delta_coverage_ratio"),
            }
        )
    ready = [row for row in rows if row["quality"] == "ready"]
    low = [
        row
        for row in ready
        if isinstance(row["iv_coverage_ratio"], int | float)
        and float(row["iv_coverage_ratio"]) < 0.5
    ]
    one_sided = [
        row
        for row in ready
        if (row["put_wall"] is None) != (row["call_wall"] is None)
    ]
    extreme = [
        row
        for row in low
        if isinstance(row["net_gamma_ratio"], int | float)
        and abs(float(row["net_gamma_ratio"])) > 0.95
    ]
    chart = [
        {"measure": "All cached frames", "count": len(rows)},
        {"measure": "Marked READY", "count": len(ready)},
        {"measure": "READY, IV coverage <50%", "count": len(low)},
        {"measure": "READY, one wall side missing", "count": len(one_sided)},
        {"measure": "Low coverage, |ratio|>0.95", "count": len(extreme)},
    ]
    latest = rows[-1] if rows else {}
    summary = {
        "snapshot_updated_at": payload.get("updated_at"),
        "history_frames": len(rows),
        "ready_frames": len(ready),
        "low_coverage_ready_frames": len(low),
        "one_sided_ready_frames": len(one_sided),
        "extreme_low_coverage_frames": len(extreme),
        "latest": latest,
    }
    return chart, summary


def steven_audit() -> tuple[list[dict], list[dict], dict]:
    events: list[dict] = []
    pattern = str(DATA_ROOT / "lake/steven/episodes/date=*/episode.jsonl")
    for filename in glob.glob(pattern):
        day = filename.split("date=", 1)[1].split("/", 1)[0]
        if not WINDOW_START <= day <= WINDOW_END:
            continue
        events.extend(read_jsonl(Path(filename)))
    event_counts = Counter(str(row.get("event_kind") or "unknown") for row in events)
    state_counts = Counter(str(row.get("to_state") or "unknown") for row in events)
    warnings = Counter(
        warning
        for row in events
        for warning in (
            (row.get("contract") or {}).get("warnings") or []
            if isinstance(row.get("contract"), dict)
            else []
        )
    )
    event_rows = [
        {"event_kind": key, "count": value}
        for key, value in sorted(event_counts.items(), key=lambda item: (-item[1], item[0]))
    ]
    state_rows = [
        {"machine_state": key, "count": value}
        for key, value in sorted(state_counts.items(), key=lambda item: (-item[1], item[0]))
    ]
    summary = {
        "events": len(events),
        "map_revisions": event_counts["map_revision"],
        "state_transitions": event_counts["state_transition"],
        "setup_confirmed": state_counts["SETUP_CONFIRMED"],
        "low_confidence": sum(
            (row.get("contract") or {}).get("confidence") == "low"
            for row in events
            if isinstance(row.get("contract"), dict)
        ),
        "missing_bars_1m": warnings["missing_bars_1m"],
        "missing_es_volume": warnings["missing_es_volume"],
        "missing_hl_volume": warnings["missing_hl_volume"],
        "empty_bar_files": sum(
            path.stat().st_size == 0
            for path in (DATA_ROOT / "lake/steven/bars").glob("date=*/spx_bars_1m.jsonl")
            if WINDOW_START <= path.parent.name.removeprefix("date=") <= WINDOW_END
        ),
    }
    return event_rows, state_rows, summary


def trade_intent_audit() -> tuple[list[dict], dict]:
    """Summarize persisted RTH intent signatures without treating them as trades."""
    statuses: Counter[str] = Counter()
    unique_events: dict[str, set[str]] = {}
    ready_rows: list[dict] = []
    for filename in glob.glob(str(DATA_ROOT / "features/trade_intents/date=*/events.jsonl")):
        day = filename.split("date=", 1)[1].split("/", 1)[0]
        if not "2026-07-14" <= day <= WINDOW_END:
            continue
        for row in read_jsonl(Path(filename)):
            evaluated_raw = row.get("evaluated_at")
            if not evaluated_raw:
                continue
            evaluated_at = datetime.fromisoformat(str(evaluated_raw))
            local = evaluated_at.astimezone(ET)
            if (
                local.weekday() >= 5
                or not time(9, 30) <= local.time().replace(tzinfo=None) < time(16)
            ):
                continue
            status = str(row.get("status") or "unknown")
            event_id = str(row.get("event_id") or "unknown")
            statuses[status] += 1
            unique_events.setdefault(status, set()).add(event_id)
            if status == "trade_ready":
                ready_rows.append(
                    {
                        "event_id": event_id,
                        "evaluated_at": evaluated_at.isoformat(),
                        "evaluated_at_et": local.isoformat(),
                        "valid_until": row.get("valid_until"),
                    }
                )
    rows = [
        {
            "status": status,
            "signature_records": statuses[status],
            "unique_events": len(unique_events.get(status, set())),
        }
        for status in ("observing", "blocked", "trade_ready")
    ]
    summary = {
        "records": sum(statuses.values()),
        "observing": statuses["observing"],
        "blocked": statuses["blocked"],
        "trade_ready": statuses["trade_ready"],
        "nonobserving_unique_events": len(
            unique_events.get("blocked", set()) | unique_events.get("trade_ready", set())
        ),
        "trade_ready_unique_events": len(unique_events.get("trade_ready", set())),
        "ready_rows": ready_rows,
    }
    return rows, summary


def collect_analysis() -> dict:
    reports = load_reports()
    current_quality_rows, current_quality = current_option_quality()
    steven_events, steven_states, steven = steven_audit()
    trade_intent_rows, trade_intent = trade_intent_audit()
    gamma_rows = gamma_distribution(reports)
    score_rows = score_performance(reports)
    mode_rows = mode_performance(reports)
    gamma_perf = gamma_performance(reports)
    zg_rows = zero_gamma_distance(reports)
    report_counts = Counter(row["session"] for row in reports)
    gamma_counts = Counter(row["gamma_state"] for row in reports)
    rth_60 = [
        row
        for row in score_rows
        if row["session"] == "RTH" and row["horizon"] == "60m"
    ]
    rth_mode_60 = [
        row
        for row in mode_rows
        if row["session"] == "RTH" and row["horizon"] == "60m"
    ]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "report_counts": {
            "total": len(reports),
            "RTH": report_counts["RTH"],
            "GTH": report_counts["GTH"],
            "zero_gamma_transition": gamma_counts["zero_gamma_transition"],
            "negative_gamma_acceleration": gamma_counts["negative_gamma_acceleration"],
            "positive_gamma_pin": gamma_counts["positive_gamma_pin"],
            "mixed_gamma": gamma_counts["mixed_gamma"],
            "gamma_missing": gamma_counts[None],
            "score_observations": sum(row["trend_score"] is not None for row in reports),
        },
        "gamma_distribution": gamma_rows,
        "zero_gamma_distance": zg_rows,
        "score_performance": score_rows,
        "rth_score_60m": rth_60,
        "mode_performance": mode_rows,
        "rth_mode_60m": rth_mode_60,
        "gamma_performance": gamma_perf,
        "current_quality_rows": current_quality_rows,
        "current_quality": current_quality,
        "steven_events": steven_events,
        "steven_states": steven_states,
        "steven": steven,
        "trade_intent_rows": trade_intent_rows,
        "trade_intent": trade_intent,
    }


def _source(
    source_id: str,
    label: str,
    path: str,
    description: str,
    definitions: list[str],
) -> dict:
    return {
        "id": source_id,
        "label": label,
        "path": path,
        "query": {
            "engine": "Python + source-code contract audit",
            "id": f"gamma-decision-audit-{source_id}-20260724",
            "sql": (
                "SELECT "
                f"'{path.replace(chr(39), chr(39) * 2)}' AS audited_source_path, "
                "'parsed by the reproducible Python companion notebook' AS method"
            ),
            "description": description,
            "executed_at": datetime.now(timezone.utc).isoformat(),
            "language": "sql",
            "filters": [
                f"{WINDOW_START} <= trading_date <= {WINDOW_END}",
                "status reports only where applicable",
                "future report linkage within ±180 seconds and same trading date",
            ],
            "metric_definitions": definitions,
            "tables_used": [path],
        },
    }


def _chart(
    chart_id: str,
    title: str,
    subtitle: str,
    dataset: str,
    source_id: str,
    x: str,
    y: str,
    *,
    color: str | None = None,
    y_format: str = "number",
    tooltips: list[dict] | None = None,
) -> dict:
    encodings: dict = {
        "x": {"field": x, "type": "nominal", "label": x},
        "y": {"field": y, "type": "quantitative", "format": y_format, "label": y},
        "tooltip": tooltips or [],
    }
    if color:
        encodings["color"] = {"field": color, "type": "nominal", "label": color}
    return {
        "id": chart_id,
        "title": title,
        "subtitle": subtitle,
        "intent": "comparison",
        "question": title,
        "rationale": subtitle,
        "type": "bar",
        "dataset": dataset,
        "sourceId": source_id,
        "encodings": encodings,
        "valueFormat": y_format,
        "layout": "full",
        "labels": {"values": "none"},
        "maxRows": 200,
        "settings": {
            "categoryLabelPolicy": "wrap",
            "groupMode": "grouped" if color else "single",
            "showValues": False,
            "sort": "none",
        },
        "surface": {
            "surface": "export",
            "interactiveLegend": False,
            "showControls": False,
            "viewMode": "visualization",
        },
    }


def _table(
    table_id: str,
    title: str,
    subtitle: str,
    dataset: str,
    source_id: str,
    columns: list[dict],
) -> dict:
    return {
        "id": table_id,
        "title": title,
        "subtitle": subtitle,
        "dataset": dataset,
        "sourceId": source_id,
        "density": "compact",
        "layout": "full",
        "columns": columns,
    }


def build_notebook() -> None:
    notebook = nbformat.v4.new_notebook(
        cells=[
            nbformat.v4.new_markdown_cell(
                "# SPX Gamma / Steven / 15 分钟判断模型审计\n\n"
                "技术审计窗口为 2026-07-03–2026-07-23。报告方向结果是重叠的观察样本，"
                "不能当作独立交易或净收益。"
            ),
            nbformat.v4.new_code_cell(
                """
from pathlib import Path
import sys

REPO_ROOT = next(
    path for path in (Path.cwd(), *Path.cwd().parents)
    if (path / "src/spx_spark").is_dir()
)
sys.path.insert(0, str(REPO_ROOT / "scripts"))
from build_gamma_decision_model_audit import collect_analysis  # noqa: E402

analysis = collect_analysis()
analysis["report_counts"]
""".strip()
            ),
            nbformat.v4.new_markdown_cell(
                "## Gamma classification quality\n\n"
                "Gamma state is a call-positive / put-negative OI-volume structure proxy. "
                "It is not a dealer-position estimate."
            ),
            nbformat.v4.new_code_cell(
                """
analysis["gamma_distribution"], analysis["zero_gamma_distance"], analysis["current_quality"]
""".strip()
            ),
            nbformat.v4.new_markdown_cell(
                "## Report-score calibration\n\n"
                "The score outcome joins each report to a same-session report at the requested horizon ±3 minutes."
            ),
            nbformat.v4.new_code_cell(
                """
analysis["rth_score_60m"], analysis["rth_mode_60m"]
""".strip()
            ),
            nbformat.v4.new_markdown_cell(
                "## Steven separation and dead-end audit\n\n"
                "Steven is observe-only and is not an input to the 15-minute report guidance."
            ),
            nbformat.v4.new_code_cell(
                """
analysis["steven"], analysis["steven_states"][:10]
""".strip()
            ),
            nbformat.v4.new_markdown_cell(
                "## RTH action funnel and report-cadence mismatch\n\n"
                "Rows are persisted intent-signature changes, so both record counts and unique event counts are retained."
            ),
            nbformat.v4.new_code_cell(
                """
analysis["trade_intent"], analysis["trade_intent_rows"]
""".strip()
            ),
            nbformat.v4.new_code_cell(
                """
assert analysis["report_counts"]["total"] == 521
assert analysis["report_counts"]["RTH"] == 129
assert analysis["report_counts"]["zero_gamma_transition"] == 397
assert analysis["steven"]["setup_confirmed"] == 0
assert analysis["steven"]["events"] == 5324
assert analysis["trade_intent"]["records"] == 18822
assert analysis["trade_intent"]["trade_ready"] == 2
assert analysis["trade_intent"]["trade_ready_unique_events"] == 2
assert analysis["current_quality"]["low_coverage_ready_frames"] > 0
print("VALIDATED: report/Gamma population, low-coverage classification, Steven dead-end, and RTH action funnel.")
""".strip()
            ),
        ],
        metadata={
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python", "version": f"{sys.version_info.major}.{sys.version_info.minor}"},
        },
    )
    NotebookClient(
        notebook,
        timeout=600,
        kernel_name="python3",
        resources={"metadata": {"path": str(REPO_ROOT)}},
    ).execute()
    nbformat.write(notebook, NOTEBOOK_PATH)


def build_artifact(analysis: dict) -> None:
    report_counts = analysis["report_counts"]
    current = analysis["current_quality"]
    steven = analysis["steven"]
    trade_intent = analysis["trade_intent"]
    rth_zero = next(
        row
        for row in analysis["gamma_distribution"]
        if row["session"] == "RTH" and row["gamma_state"] == "ZG"
    )
    score_65 = next(
        row for row in analysis["rth_score_60m"] if row["score_bucket"] == "65+"
    )
    score_mid = next(
        row for row in analysis["rth_score_60m"] if row["score_bucket"] == "45–64"
    )
    trend_mode = next(row for row in analysis["rth_mode_60m"] if row["mode"] == "trending")
    transition_mode = next(
        row for row in analysis["rth_mode_60m"] if row["mode"] == "transition"
    )
    ready_times = ", ".join(
        datetime.fromisoformat(row["evaluated_at_et"]).strftime("%m-%d %H:%M:%S ET")
        for row in sorted(trade_intent["ready_rows"], key=lambda item: item["evaluated_at_et"])
    )

    model_layers = [
        {
            "layer": "Options map / Gamma state",
            "actual_authority": "Structure context",
            "inputs": "Call+/Put− BS gamma × OI(+unsigned volume intraday), zero-gamma scan",
            "output": "walls, zero_gamma, net_gamma_ratio, gamma_state",
            "used_by_15m_direction": "No",
            "critical_issue": "Dealer sign unknown; weak completeness gate; 0.5% transition override",
        },
        {
            "layer": "15m regime_decision",
            "actual_authority": "Visible bias",
            "inputs": "ES 15/60m, efficiency, VWAP, price-volume, ES/SPY, VIX, swing",
            "output": "trending / transition / mean_reverting + up/down",
            "used_by_15m_direction": "Yes",
            "critical_issue": "Hand weights, correlated evidence, missing values score zero, no calibration",
        },
        {
            "layer": "Breakout filter",
            "actual_authority": "Breakout veto/support",
            "inputs": "Level path + local GEX share + DEX proxies + momentum/flow",
            "output": "blocked / pending / supported",
            "used_by_15m_direction": "Only after breakout path",
            "critical_issue": "Proxy quality warning does not invalidate all structure metrics",
        },
        {
            "layer": "Greek decision",
            "actual_authority": "Focused contract confidence",
            "inputs": "Delta/gamma/speed/color/theta/vanna for candidate contract",
            "output": "±25 confidence adjustment",
            "used_by_15m_direction": "Explicitly none",
            "critical_issue": "Usually explanation_only when coverage is weak",
        },
        {
            "layer": "Steven",
            "actual_authority": "Observe-only separate state machine",
            "inputs": "Cross-expiry Net DEX proxy, walls, bars, weak flow proxies",
            "output": "watch/setup/exit states",
            "used_by_15m_direction": "No",
            "critical_issue": "Deployment enables it, but bar/flow wiring is empty and report has no consumer",
        },
        {
            "layer": "Trade intent + guidance",
            "actual_authority": "Action / NO TRADE",
            "inputs": "Formal level CONFIRMED + RTH + follow-through + quotes + RR + regime gates",
            "output": "trade_ready / blocked / observing",
            "used_by_15m_direction": "Action only",
            "critical_issue": "Current Ready envelope ≤20s versus 900s report cadence; states collapse into NO TRADE",
        },
    ]

    score_weights = [
        {"model": "Trend", "feature": "ES 15m+60m aligned", "points": 25, "options_feature": "No"},
        {"model": "Trend", "feature": "60m trend efficiency", "points": 20, "options_feature": "No"},
        {"model": "Trend", "feature": "Price vs VWAP", "points": 10, "options_feature": "No"},
        {"model": "Trend", "feature": "VWAP slope", "points": 10, "options_feature": "No"},
        {"model": "Trend", "feature": "Price-volume alignment", "points": 15, "options_feature": "No"},
        {"model": "Trend", "feature": "ES/SPY confirmation", "points": 10, "options_feature": "No"},
        {"model": "Trend", "feature": "VIX/VVIX confirmation", "points": 10, "options_feature": "No"},
        {"model": "Trend", "feature": "Higher-low/lower-high", "points": 10, "options_feature": "No"},
        {"model": "Mean reversion", "feature": "Low efficiency", "points": 25, "options_feature": "No"},
        {"model": "Mean reversion", "feature": "Flat VWAP", "points": 15, "options_feature": "No"},
        {"model": "Mean reversion", "feature": "15m/60m conflict", "points": 15, "options_feature": "No"},
        {"model": "Mean reversion", "feature": "Volume/cross-asset/vol divergence", "points": 35, "options_feature": "No"},
        {"model": "Mean reversion", "feature": "Level rejection path", "points": 15, "options_feature": "Level FSM"},
        {"model": "Mean reversion", "feature": "Price between walls", "points": 5, "options_feature": "Only direct option input"},
    ]

    findings = [
        {
            "severity": "Critical",
            "finding": "Low-completeness Gamma can still be READY",
            "evidence": (
                f"{current['low_coverage_ready_frames']} cached READY frames had IV coverage <50%; "
                f"{current['one_sided_ready_frames']} READY frames were missing one wall side. "
                f"Latest ratio={current['latest'].get('net_gamma_ratio')} at "
                f"IV coverage={current['latest'].get('iv_coverage_ratio')}."
            ),
            "impact": "Missing Put or Call Greeks can mechanically force net_gamma_ratio toward ±1.",
            "required_fix": "Pair-balanced structural coverage gate before GEX aggregation/classification.",
        },
        {
            "severity": "Critical",
            "finding": "Gamma is not dealer Gamma",
            "evidence": "Code declares dealer_position_sign=unknown and applies call+ / put− house convention.",
            "impact": "It locates concentrations but cannot identify dealer hedging direction.",
            "required_fix": "Keep proxy label; add signed-flow/dealer inventory source before directional use.",
        },
        {
            "severity": "High",
            "finding": "Zero-gamma band collapses the state space",
            "evidence": (
                f"{rth_zero['count']}/{report_counts['RTH']} RTH reports were zero_gamma_transition; "
                "classifier checks ±0.5% (about 37 SPX points) before net ratio."
            ),
            "impact": "RTH has almost no positive/negative regime sample for validation or decisions.",
            "required_fix": "Calibrate a volatility-normalized band with hysteresis; shadow-test 0.10–0.25% candidates.",
        },
        {
            "severity": "High",
            "finding": "Visible report bias is not a Gamma model",
            "evidence": "Gamma/skew/charm/vanna are absent from trend_score; walls contribute only 5 mean-reversion points.",
            "impact": "The user sees ES momentum heuristics presented beside Gamma structure and reasonably assumes one model.",
            "required_fix": "Publish separate Structure regime, Direction probability, and Action state.",
        },
        {
            "severity": "High",
            "finding": "Trend score is not monotonically calibrated in RTH",
            "evidence": (
                f"At 60m, score 45–64: n={score_mid['n']}, hit={score_mid['hit_rate']:.1%}, "
                f"mean={score_mid['mean_signed_es_points']:+.2f}; score 65+: n={score_65['n']}, "
                f"hit={score_65['hit_rate']:.1%}, mean={score_65['mean_signed_es_points']:+.2f}."
            ),
            "impact": "A higher score does not mean higher directional edge.",
            "required_fix": "Replace point totals with walk-forward calibrated probabilities and abstention.",
        },
        {
            "severity": "High",
            "finding": "Steven is a disconnected dead end",
            "evidence": (
                f"{steven['events']:,} episode rows, {steven['map_revisions']:,} map revisions, "
                f"{steven['setup_confirmed']} SETUP_CONFIRMED; {steven['missing_bars_1m']:,} rows missing 1m bars."
            ),
            "impact": "It adds state churn and storage but gives the 15m report no guidance.",
            "required_fix": "Disable the deployment override or rebuild usable bars/flow and feed one audited projection.",
        },
        {
            "severity": "High",
            "finding": "The 15m report misses the action signal by design",
            "evidence": (
                f"RTH had {trade_intent['records']:,} persisted intent-signature records: "
                f"{trade_intent['observing']:,} observing, {trade_intent['blocked']:,} blocked, "
                f"{trade_intent['trade_ready']} Ready at {ready_times}. "
                "Those two old-schema rows have no lease; under the current policy each emitted Ready envelope "
                "is valid for at most 20s, while the status report cadence is 900s and reads only current intent."
            ),
            "impact": "A genuine Ready event can occur and expire between two reports.",
            "required_fix": "Add a recent-event window/immediate lane and render NO SETUP / SIGNAL BLOCKED / TRADE READY separately.",
        },
        {
            "severity": "High",
            "finding": "One mean-reversion feature is unreachable",
            "evidence": "The volume frame emits a nonnegative cumulative-volume delta, while the score waits for price_volume_divergent.",
            "impact": "The advertised divergence branch cannot earn its 15 mean-reversion points on live data.",
            "required_fix": "Define a signed price/volume divergence feature and prove both branches with replay tests.",
        },
        {
            "severity": "High",
            "finding": "The final 0DTE RTH window is structurally stale",
            "evidence": "Collector rolls away from same-day expiry at 15:30 ET, while analytics requests it until 17:00 ET.",
            "impact": "The most gamma-sensitive closing 30 minutes lose fresh same-day structure.",
            "required_fix": "Align collection and analytics expiry contracts; never analyze an expired chain with a floored 15m tenor.",
        },
        {
            "severity": "Medium",
            "finding": "Negative-Gamma display contract mismatch",
            "evidence": "Classifier emits negative_gamma_acceleration; prompt maps negative_gamma_expansion.",
            "impact": "User-facing reports leak raw internal enum instead of 负Gamma.",
            "required_fix": "One enum contract with exhaustive rendering test.",
        },
        {
            "severity": "Medium",
            "finding": "Micopedia semantics are distorted",
            "evidence": "positive_gamma_pin maps to pin/OPEX regime; zero_gamma_transition maps to negative_gamma_trend.",
            "impact": "Secondary narrative overstates OPEX pin and treats transition as negative Gamma.",
            "required_fix": "Separate positive, pin, transition, and event-tag semantics.",
        },
        {
            "severity": "Medium",
            "finding": "LLM report validation checks numbers, not meaning",
            "evidence": "The semantic validator requires numeric template lines but accepts reversed bias or NO TRADE changed to TRADE READY.",
            "impact": "The gate remains deterministic, but the natural-language report can invert its meaning.",
            "required_fix": "Validate direction/action enums and reason codes independently of prose.",
        },
    ]

    proposed_model = [
        {
            "stage": 1,
            "name": "Data validity",
            "output": "valid / degraded / invalid",
            "minimum_contract": "≥60% complete C/P Greeks in core + both wings + quote age + OI age",
            "action": "Invalid never emits Gamma regime or direction.",
        },
        {
            "stage": 2,
            "name": "Structure",
            "output": "pin / acceleration / transition probabilities",
            "minimum_contract": "OI-only baseline; volume shown separately unless signed/open-close flow exists",
            "action": "Smooth IV/GEX functions, never interpolate NBBO.",
        },
        {
            "stage": 3,
            "name": "Direction",
            "output": "P(up), P(down), P(abstain) at 15/30/60m",
            "minimum_contract": "Walk-forward day-blocked calibration; missingness indicators; no duplicated evidence points",
            "action": "Direction is separate from structure.",
        },
        {
            "stage": 4,
            "name": "Setup",
            "output": "fade / breakout / spread candidate",
            "minimum_contract": "Level FSM + structure interaction + signed movement outcome",
            "action": "Only candidates above pre-registered expected-value floor survive.",
        },
        {
            "stage": 5,
            "name": "Execution",
            "output": "NO SETUP / SIGNAL BLOCKED / TRADE READY",
            "minimum_contract": "Live NBBO, quote age, fill model, RR, exposure cap",
            "action": "Keep immediate events plus a recent-event window; expose one primary reason code.",
        },
    ]

    recommendations = [
        {
            "priority": 0,
            "change": "Freeze production parameter promotion",
            "why": "Current scores and Gamma states are not calibrated; Steven has zero setups.",
            "acceptance": "No model threshold promoted from overlapping report observations.",
        },
        {
            "priority": 1,
            "change": "Gamma completeness fail-closed",
            "why": "READY is possible at 1.85% IV coverage and with one wall side missing.",
            "acceptance": "Synthetic one-sided/low-coverage chains always return unknown_insufficient_structure.",
        },
        {
            "priority": 2,
            "change": "Align 0DTE collection and analytics expiry",
            "why": "Collector drops same-day SPXW at 15:30 ET while analytics requests it through 17:00.",
            "acceptance": "Fresh same-day coverage through the declared RTH close; expired chains always invalid.",
        },
        {
            "priority": 3,
            "change": "Remove unsigned volume from positioning Gamma",
            "why": "Cumulative volume lacks open/close and aggressor sign.",
            "acceptance": "OI baseline and volume-activity overlay are separate fields and charts.",
        },
        {
            "priority": 4,
            "change": "Repair unreachable and duplicated score features",
            "why": "Volume divergence cannot occur and correlated ES path evidence is counted repeatedly.",
            "acceptance": "Replay exercises every feature branch; ablation shows stable incremental value.",
        },
        {
            "priority": 5,
            "change": "Shadow calibrated zero-gamma bands",
            "why": "0.5% labels 96.90% of RTH reports as transition.",
            "acceptance": "Day-blocked out-of-sample comparison for 0.10%, 0.25%, EM-scaled bands and hysteresis.",
        },
        {
            "priority": 6,
            "change": "Replace point score with calibrated horizon models",
            "why": "RTH score 65+ underperforms the 45–64 bucket at 60m.",
            "acceptance": "Brier/log loss, calibration slope, hit/return by decile and explicit abstain rate.",
        },
        {
            "priority": 7,
            "change": "Unify and harden the report contract",
            "why": "Structure/direction/setup/action are conflated; 20s Ready events fall between 15m reports.",
            "acceptance": "Four fields, recent-event window, three action states, and enum-level semantic validation.",
        },
        {
            "priority": 8,
            "change": "Retire or rebuild Steven",
            "why": "5,324 events, 0 setups, empty 1m bar files, and no report consumer.",
            "acceptance": "One owner, one consumer, non-empty bars, flow availability, and replayed state transition tests.",
        },
    ]

    sources = [
        _source(
            "report_audit",
            "Persisted 15-minute status reports",
            "audit/order_map_pricing/date=*/reports.jsonl",
            "Extracts visible bias, score, Gamma label, underlier and later ES movement.",
            [
                "Directional result = visible side × later ES change.",
                "Observations overlap and are descriptive, not independent trades.",
            ],
        ),
        _source(
            "gamma_code",
            "Gamma proxy and classification source",
            "src/spx_spark/analytics/options/{exposure.py,service.py,levels.py}",
            "Audits formula, weighting, quality contract, zero-gamma scan and regime thresholds.",
            [
                "Call sign is +1 and Put sign is −1 by house convention.",
                "Dealer position sign is unknown.",
            ],
        ),
        _source(
            "decision_code",
            "15-minute decision and guidance source",
            "src/spx_spark/{market_calendar.py,application/market_features/{market.py,decision_filters.py},application/order_map/{guidance.py,writer.py}}",
            "Traces visible direction, score branches, expiry timing, intent projection and semantic rendering.",
            [
                "regime_decision owns visible bias.",
                "trade_intent and guidance own action authorization.",
            ],
        ),
        _source(
            "option_history",
            "Current cached option-structure history",
            "latest/market_feature_state.json",
            "Quantifies READY frames with low Greek coverage, one-sided walls and extreme ratios.",
            ["Low coverage is IV coverage <50%.", "Extreme ratio is |net_gamma_ratio| >0.95."],
        ),
        _source(
            "steven_audit",
            "Steven episode and bar lake",
            "config/runtime.yaml + deployed .env + lake/steven/{episodes,bars}/date=*",
            "Checks effective enablement and counts revisions, transitions, setups and missing inputs.",
            [
                "YAML default is off, but the deployed environment overrides Steven on.",
                "SETUP_CONFIRMED is the only counted completed setup state.",
            ],
        ),
        _source(
            "trade_intent_audit",
            "RTH trade-intent event lake",
            "features/trade_intents/date=*/events.jsonl",
            "Counts RTH observing, blocked and Ready signature records plus unique event IDs.",
            [
                "Window is 2026-07-14 through 2026-07-23, 09:30–16:00 ET.",
                "Evaluation counts are not independent trades.",
            ],
        ),
        _source(
            "notebook",
            "Executed companion notebook",
            NOTEBOOK_PATH.name,
            "Recomputes report, Gamma-quality, score-calibration and Steven assertions top-to-bottom.",
            ["Assertions fail if the frozen audit population changes."],
        ),
    ]

    headline = [
        {
            "reports": report_counts["total"],
            "rth_zero_gamma_share": rth_zero["share"],
            "low_coverage_ready": current["low_coverage_ready_frames"],
            "steven_events": steven["events"],
            "steven_setups": steven["setup_confirmed"],
            "rth_trade_ready": trade_intent["trade_ready"],
            "rth_high_score_60m_mean": score_65["mean_signed_es_points"],
        }
    ]
    cards = [
        {
            "id": "reports",
            "description": "Persisted status-report population through 2026-07-23.",
            "dataset": "headline",
            "sourceId": "report_audit",
            "metrics": [{"label": "15m 报告", "field": "reports", "format": "number"}],
        },
        {
            "id": "zero_share",
            "description": "RTH reports classified as zero-gamma transition.",
            "dataset": "headline",
            "sourceId": "report_audit",
            "metrics": [{"label": "RTH ZeroGamma占比", "field": "rth_zero_gamma_share", "format": "percent"}],
        },
        {
            "id": "low_coverage",
            "description": "Cached READY frames with IV coverage below 50%.",
            "dataset": "headline",
            "sourceId": "option_history",
            "metrics": [{"label": "低覆盖仍READY", "field": "low_coverage_ready", "format": "number"}],
        },
        {
            "id": "steven",
            "description": "Steven event volume versus completed setups.",
            "dataset": "headline",
            "sourceId": "steven_audit",
            "metrics": [
                {"label": "Steven events", "field": "steven_events", "format": "number"},
                {"label": "SETUP_CONFIRMED", "field": "steven_setups", "format": "number"},
            ],
        },
        {
            "id": "high_score",
            "description": "Mean signed ES points after 60m for RTH trend score ≥65.",
            "dataset": "headline",
            "sourceId": "report_audit",
            "metrics": [{"label": "RTH 65+ 60m均值", "field": "rth_high_score_60m_mean", "format": "number"}],
        },
        {
            "id": "trade_ready",
            "description": "RTH Ready signature records that occurred between 15-minute report slots.",
            "dataset": "headline",
            "sourceId": "trade_intent_audit",
            "metrics": [{"label": "RTH Trade Ready", "field": "rth_trade_ready", "format": "number"}],
        },
    ]

    charts = [
        _chart(
            "gamma_distribution_chart",
            "Gamma state distribution by session",
            "The 0.5% zero-gamma override collapses nearly all RTH observations into transition.",
            "gamma_distribution",
            "report_audit",
            "session",
            "count",
            color="gamma_state",
            tooltips=[
                {"field": "share", "type": "quantitative", "format": "percent", "label": "share"}
            ],
        ),
        _chart(
            "score_chart",
            "RTH 60m result by trend-score bucket",
            "The highest production score bucket is not the best-performing bucket.",
            "rth_score_60m",
            "report_audit",
            "score_bucket",
            "mean_signed_es_points",
            tooltips=[
                {"field": "n", "type": "quantitative", "format": "number", "label": "n"},
                {"field": "hit_rate", "type": "quantitative", "format": "percent", "label": "hit rate"},
            ],
        ),
    ]

    tables = [
        _table(
            "model_layers_table",
            "Actual model architecture",
            "The report is not driven by one unified Gamma model.",
            "model_layers",
            "decision_code",
            [
                {"field": "layer", "label": "Layer", "type": "text"},
                {"field": "actual_authority", "label": "Authority", "type": "text"},
                {"field": "inputs", "label": "Inputs", "type": "text"},
                {"field": "output", "label": "Output", "type": "text"},
                {"field": "used_by_15m_direction", "label": "15m方向?", "type": "text"},
                {"field": "critical_issue", "label": "Critical issue", "type": "text"},
            ],
        ),
        _table(
            "weights_table",
            "Hard-coded score specification",
            "Most points are correlated transformations of the same ES path.",
            "score_weights",
            "decision_code",
            [
                {"field": "model", "label": "Model", "type": "text"},
                {"field": "feature", "label": "Feature", "type": "text"},
                {"field": "points", "label": "Points", "format": "number"},
                {"field": "options_feature", "label": "Options input", "type": "text"},
            ],
        ),
        _table(
            "findings_table",
            "Validated model findings",
            "Severity reflects potential to misstate structure, direction or model readiness.",
            "findings",
            "decision_code",
            [
                {"field": "severity", "label": "Severity", "type": "text"},
                {"field": "finding", "label": "Finding", "type": "text"},
                {"field": "evidence", "label": "Evidence", "type": "text"},
                {"field": "impact", "label": "Impact", "type": "text"},
                {"field": "required_fix", "label": "Required fix", "type": "text"},
            ],
        ),
        _table(
            "gamma_perf_table",
            "Visible bias performance by Gamma label",
            "Sparse non-transition RTH cells are not estimable.",
            "gamma_performance",
            "report_audit",
            [
                {"field": "session", "label": "Session", "type": "text"},
                {"field": "gamma_state", "label": "Gamma state", "type": "text"},
                {"field": "horizon", "label": "Horizon", "type": "text"},
                {"field": "n", "label": "n", "format": "number"},
                {"field": "hit_rate", "label": "Hit rate", "format": "percent"},
                {"field": "mean_signed_es_points", "label": "Mean signed ES", "format": "number"},
            ],
        ),
        _table(
            "proposed_model_table",
            "Proposed v2 decision contract",
            "Structure, direction, setup and execution should be separately calibrated.",
            "proposed_model",
            "decision_code",
            [
                {"field": "stage", "label": "#", "format": "number"},
                {"field": "name", "label": "Stage", "type": "text"},
                {"field": "output", "label": "Output", "type": "text"},
                {"field": "minimum_contract", "label": "Minimum contract", "type": "text"},
                {"field": "action", "label": "Rule", "type": "text"},
            ],
        ),
        _table(
            "recommendations_table",
            "Implementation order and acceptance tests",
            "No live threshold change should precede validity and calibration.",
            "recommendations",
            "decision_code",
            [
                {"field": "priority", "label": "Priority", "format": "number"},
                {"field": "change", "label": "Change", "type": "text"},
                {"field": "why", "label": "Why", "type": "text"},
                {"field": "acceptance", "label": "Acceptance", "type": "text"},
            ],
        ),
    ]

    blocks = [
        {
            "id": "title",
            "type": "markdown",
            "layout": "full",
            "body": (
                "# SPX Gamma / Steven / 15 分钟判断模型审计\n\n"
                f"技术审计窗口：{WINDOW_START}–{WINDOW_END}；快照生成：{analysis['generated_at']}。"
            ),
        },
        {
            "id": "technical_summary",
            "type": "markdown",
            "layout": "full",
            "sourceId": "decision_code",
            "body": (
                "## Technical Summary\n\n"
                "**你的判断是对的：当前不是一个经过校准的 Gamma 决策模型。** "
                "报告可见方向来自手写 ES 动量/回归积分；Gamma 主要是结构地图，Steven 是另一套未接入报告的 observe-only 状态机，"
                "执行门控又是第四套逻辑。最严重的问题是 Gamma 完整性未 fail-closed：少数单边 Greeks 也能产生 ±1 的 net ratio 和 READY 状态。"
            ),
        },
        {
            "id": "metric_strip",
            "type": "metric-strip",
            "layout": "full",
            "cardIds": [card["id"] for card in cards],
        },
        {
            "id": "key_findings",
            "type": "markdown",
            "layout": "full",
            "body": (
                "## Key Findings\n\n"
                f"- RTH {rth_zero['count']}/{report_counts['RTH']}（{rth_zero['share']:.2%}）被归为 ZeroGamma 过渡，几乎没有可验证的状态对照组。\n"
                f"- 当前缓存里 {current['low_coverage_ready_frames']} 帧在 IV 覆盖 <50% 时仍为 READY；最新极端样本覆盖 "
                f"{float(current['latest'].get('iv_coverage_ratio') or 0):.2%}、ratio "
                f"{float(current['latest'].get('net_gamma_ratio') or 0):.2f}。\n"
                f"- RTH 60m：趋势分 65+ 的方向均值 {score_65['mean_signed_es_points']:+.2f} 点，低于 45–64 档 "
                f"{score_mid['mean_signed_es_points']:+.2f} 点；分数没有单调校准。\n"
                f"- 显示为 trending 的 RTH 60m 均值 {trend_mode['mean_signed_es_points']:+.2f} 点，transition 为 "
                f"{transition_mode['mean_signed_es_points']:+.2f} 点。\n"
                f"- RTH 只有 {trade_intent['trade_ready']} 条 Ready 记录，均落在 15 分钟报告时点之间；"
                "报告无法回看已过期的 Ready。\n"
                f"- Steven 产生 {steven['events']:,} 条事件但 0 setup；它没有进入 15 分钟报告。"
            ),
        },
        {"id": "gamma_chart_block", "type": "chart", "layout": "full", "chartId": "gamma_distribution_chart"},
        {"id": "score_chart_block", "type": "chart", "layout": "full", "chartId": "score_chart"},
        {
            "id": "scope",
            "type": "markdown",
            "layout": "full",
            "body": (
                "## Scope, Data, and Metric Definitions\n\n"
                "报告审计使用实际持久化 status payload。后续 ES 结果联结同一 trading_date、目标时间 ±3 分钟、来源兼容的报告。"
                "这些 15 分钟点位互相重叠且自相关，只用于诊断排序/校准，不能当独立交易 PnL。"
                "当前 option history 是 7/24 的滚动 181 帧快照；它证明合同缺陷，不代表三周质量占比。"
            ),
        },
        {"id": "architecture_table_block", "type": "table", "layout": "full", "tableId": "model_layers_table"},
        {
            "id": "model_spec",
            "type": "markdown",
            "layout": "full",
            "sourceId": "gamma_code",
            "body": (
                "## Model Specification and Validation\n\n"
                "现行 GEX 代理为 `sign × gamma × weight × 100 × spot² × 0.01`，Call sign=+1、Put sign=−1；"
                "0DTE `weight=OI+累计volume`。`net_gamma_ratio=sum(net_gex)/sum(abs_gex)`。分类先检查 "
                "`abs(zero_gamma-spot)/spot <= 0.005`，满足即 transition；否则 ±0.15 判正/负 Gamma。\n\n"
                "这里有两个不可忽略的语义限制：累计 volume 没有开/平仓与 aggressor sign；Call+/Put− 也不是 dealer inventory。"
                "所以它最多是结构集中度代理，不能直接推导 dealer 对冲方向。"
            ),
        },
        {"id": "weights_table_block", "type": "table", "layout": "full", "tableId": "weights_table"},
        {"id": "findings_table_block", "type": "table", "layout": "full", "tableId": "findings_table"},
        {"id": "gamma_perf_table_block", "type": "table", "layout": "full", "tableId": "gamma_perf_table"},
        {
            "id": "steven",
            "type": "markdown",
            "layout": "full",
            "sourceId": "steven_audit",
            "body": (
                "### Steven model status\n\n"
                "`config/runtime.yaml` 默认关闭 Steven，但部署环境已覆盖为开启，所以服务确实在运行。"
                "问题不是“没启动”，而是每轮只恢复 closed bars、不恢复 open partial bar，并读取错误的 flow 输入路径；"
                "因此 1m/5m bar 与 ES/HL volume 长期缺失。"
                "15 分钟 order-map payload 只装载 market/option/decision projections，不装载 `steven_state`。"
                f"审计期 {steven['events']:,} 行中 {steven['map_revisions']:,} 次 map revision、"
                f"{steven['state_transitions']:,} 次 state transition、0 次 SETUP_CONFIRMED；"
                f"{steven['missing_bars_1m']:,} 行标记 missing_bars_1m，相关 1m bar 文件为空。"
            ),
        },
        {
            "id": "methods",
            "type": "markdown",
            "layout": "full",
            "body": (
                "## Methodology\n\n"
                "1. 静态追踪公式、分类、质量、报告投影与执行门控的代码所有权。\n"
                "2. 对 521 份 status 报告解析 Gamma 标签、可见 regime、分数与 ES 后续路径。\n"
                "3. 对滚动 option history 重算低覆盖 READY、单边墙位和极端 ratio。\n"
                "4. 对 Steven episode/bar lake 统计状态产出与输入缺失。\n"
                "5. 对 RTH trade-intent lake 区分 signature record 与 unique event，并核对 Ready 时间。\n"
                "6. 所有结论由 companion notebook 的固定断言复算。"
            ),
        },
        {
            "id": "limitations",
            "type": "markdown",
            "layout": "full",
            "body": (
                "## Limitations, Uncertainty, and Robustness\n\n"
                "- 非 transition 的 RTH Gamma 样本只有 4 份，不能估计正/负 Gamma 的真实条件收益。\n"
                "- 报告结果是重叠时间窗，没有计期权 spread、slippage、theta、IV 与手续费。\n"
                "- 当前滚动 option history 只保存约 3 小时；三周完整 Greeks coverage 尚未持久化，属于数据合同缺口。\n"
                "- Gamma/dealer sign 缺失是识别限制，不是靠调阈值就能解决。\n"
                "- 近期真实账户亏损未与 broker fills/fees 完整对账，本报告不把模型方向结果冒充净 PnL 解释。"
            ),
        },
        {
            "id": "proposed_model",
            "type": "markdown",
            "layout": "full",
            "body": (
                "## Recommended Next Steps\n\n"
                "先修数据有效性，再建概率模型，最后才讨论阈值。生产参数现在不应放松；应该把候选模型放进 shadow，"
                "用按交易日分块的 expanding walk-forward 校准。"
            ),
        },
        {"id": "proposed_model_table_block", "type": "table", "layout": "full", "tableId": "proposed_model_table"},
        {"id": "recommendations_table_block", "type": "table", "layout": "full", "tableId": "recommendations_table"},
        {
            "id": "further_questions",
            "type": "markdown",
            "layout": "full",
            "body": (
                "## Further Questions\n\n"
                "- 15 分钟报告最终只服务执行，还是同时保留独立结构研究页？两个产品应分开。\n"
                "- 是否有可购入的 signed option flow / aggressor / dealer positioning 数据源？没有时必须保留 proxy 语义。\n"
                "- 下一轮实现是否接受先关闭低覆盖 Gamma 文案，即使短期报告会显示更多 `unknown`？"
            ),
        },
        {
            "id": "reproducibility",
            "type": "markdown",
            "layout": "full",
            "sourceId": "notebook",
            "body": (
                "## Reproducibility\n\n"
                f"伴随 notebook `{NOTEBOOK_PATH.name}` 已从仓库根目录 top-to-bottom 执行。"
                "它锁定 521 份报告、397 份 transition 标签、5,324 个 Steven events、0 setup，"
                "以及 18,822 条 RTH intent signature / 2 条 Ready，并验证低覆盖 Gamma READY 确实存在。"
            ),
        },
    ]

    artifact = {
        "surface": "report",
        "manifest": {
            "version": 1,
            "surface": "report",
            "title": "SPX Gamma / Steven / 15 分钟判断模型审计",
            "description": "审计 Gamma 结构代理、Steven 状态机、15 分钟可见方向与执行门控为何没有形成统一、可校准的信号模型。",
            "generatedAt": analysis["generated_at"],
            "sources": sources,
            "cards": cards,
            "charts": charts,
            "tables": tables,
            "blocks": blocks,
        },
        "snapshot": {
            "version": 1,
            "generatedAt": analysis["generated_at"],
            "status": "ready",
            "accessIssues": [],
            "datasets": {
                "headline": headline,
                "gamma_distribution": analysis["gamma_distribution"],
                "zero_gamma_distance": analysis["zero_gamma_distance"],
                "score_performance": analysis["score_performance"],
                "rth_score_60m": analysis["rth_score_60m"],
                "mode_performance": analysis["mode_performance"],
                "rth_mode_60m": analysis["rth_mode_60m"],
                "gamma_performance": analysis["gamma_performance"],
                "current_quality_rows": analysis["current_quality_rows"],
                "steven_events": analysis["steven_events"],
                "steven_states": analysis["steven_states"],
                "trade_intent_rows": analysis["trade_intent_rows"],
                "model_layers": model_layers,
                "score_weights": score_weights,
                "findings": findings,
                "proposed_model": proposed_model,
                "recommendations": recommendations,
            },
        },
    }
    ARTIFACT_PATH.write_text(
        json.dumps(artifact, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def build_portable_html() -> None:
    subprocess.run(
        [
            "node",
            str(PORTABLE_BUILDER),
            "--input",
            str(ARTIFACT_PATH),
            "--output",
            str(HTML_PATH),
        ],
        cwd=REPO_ROOT,
        check=True,
    )
    subprocess.run(
        [
            "node",
            str(PORTABLE_VERIFIER),
            "--html",
            str(HTML_PATH),
            "--artifact",
            str(ARTIFACT_PATH),
            "--timeout-ms",
            "30000",
            "--screenshot",
            str(HTML_PATH.with_suffix(".verification-failure.png")),
        ],
        cwd=REPO_ROOT,
        check=True,
    )


def main() -> int:
    analysis = collect_analysis()
    build_notebook()
    build_artifact(analysis)
    build_portable_html()
    print(NOTEBOOK_PATH)
    print(ARTIFACT_PATH)
    print(HTML_PATH)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
