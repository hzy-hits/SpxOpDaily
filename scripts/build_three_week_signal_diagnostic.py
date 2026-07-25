#!/usr/bin/env python3
"""Build the 2026-07-24 three-week signal/report diagnostic artifacts.

The requested calendar window is 2026-07-03 through 2026-07-23.  The builder
keeps the analytically different evidence layers separate:

* raw RTH quote and option-pair coverage, available from 2026-07-06;
* persisted production FSM/report history, available from 2026-07-13;
* intent/outcome-equivalent complete sessions, available from 2026-07-14;
* contract-consistent forward-v3 evidence, beginning with 2026-07-23.

It writes a canonical Data Analytics report artifact and an executed companion
notebook.  The portable HTML is produced separately by the packaged report
builder so the HTML and artifact use the same validated payload.
"""

from __future__ import annotations

import glob
import json
import math
import os
import re
from collections import Counter, defaultdict
from datetime import datetime, time, timezone
from pathlib import Path
from statistics import NormalDist, mean, median
from zoneinfo import ZoneInfo

import nbformat
from nbclient import NotebookClient


REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = Path(os.environ.get("SPX_SPARK_DATA_ROOT", "/srv/data/spx-spark/data"))
DOCS_ROOT = REPO_ROOT / "docs"
REPORT_DATE = "2026-07-24"
WINDOW_START = "2026-07-03"
WINDOW_END = "2026-07-23"
BACKTEST_ROOT = (
    DATA_ROOT
    / "reports/odte_level_backtest"
    / "diagnostic=2026-07-24-three-week"
    / "cutoff=2026-07-23"
)
BACKTEST_PATH = BACKTEST_ROOT / "artifact.json"
ARTIFACT_PATH = DOCS_ROOT / f"spx-three-week-signal-diagnostic-{REPORT_DATE}.artifact.json"
NOTEBOOK_PATH = DOCS_ROOT / f"spx-three-week-signal-diagnostic-{REPORT_DATE}.ipynb"
ET = ZoneInfo("America/New_York")

NO_TRADE_RE = re.compile(
    r"未通过执行门控|\bNO TRADE\b|当前不进场|暂停新开仓|当前不是下单计划|"
    r"当前不可预挂|不可执行定价|不生成交易判断|不生成[^\n]{0,30}(?:限价|下单建议)|"
    r"只观察|map_only|不执行",
    re.I,
)
TRADE_READY_RE = re.compile(
    r"(?:^|\n)\s*(?:结论\s+)?TRADE[ _-]?READY\b|决策门控已通过",
    re.I,
)


def read_jsonl(path: Path):
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            payload = json.loads(line)
            if isinstance(payload, dict):
                yield payload


def wilson_95(successes: int, sample_count: int) -> tuple[float | None, float | None]:
    if sample_count <= 0:
        return None, None
    z = NormalDist().inv_cdf(0.975)
    p = successes / sample_count
    denominator = 1.0 + z * z / sample_count
    center = (p + z * z / (2.0 * sample_count)) / denominator
    margin = (
        z
        * math.sqrt(
            p * (1.0 - p) / sample_count + z * z / (4.0 * sample_count**2)
        )
        / denominator
    )
    return max(0.0, center - margin), min(1.0, center + margin)


def visible_bias(text: str) -> tuple[int | None, str | None]:
    lines = [line.strip(" -*") for line in text.splitlines() if line.strip()]
    for prefix in ("判断", "观察"):
        for line in lines[:10]:
            if not line.startswith(prefix):
                continue
            for label, side in (
                ("趋势偏多", 1),
                ("过渡偏多", 1),
                ("趋势偏空", -1),
                ("过渡偏空", -1),
                ("偏多", 1),
                ("偏空", -1),
                ("均值回归", 0),
                ("方向过渡", 0),
                ("证据不足", None),
            ):
                if label in line:
                    return side, label
    top = "\n".join(lines[:8])
    for label, side in (
        ("趋势偏多", 1),
        ("过渡偏多", 1),
        ("趋势偏空", -1),
        ("过渡偏空", -1),
        ("偏多", 1),
        ("偏空", -1),
        ("均值回归", 0),
        ("中性", 0),
    ):
        if re.search(r"(?:主情景|主剧本|结论)[^\n]{0,80}" + re.escape(label), top):
            return side, label
    return None, None


def template_es_price(template: str) -> float | None:
    patterns = (
        r"SPX (?:代理|proxy)[:：]?[^\n]*?[；;]\s*ES\s+(-?\d+(?:\.\d+)?)",
        r"价格\s+SPX\s+[-\d.]+(?:\([^)]*\))?\s*[｜|　]+\s*ES\s+(-?\d+(?:\.\d+)?)",
        r"参考价[:：]\s*[-\d.]+\([^\n)]*\)\s*[；;,]\s*ES\s+(-?\d+(?:\.\d+)?)",
        r"时段[:：][^\n]*?SPX\s+[-\d.]+\([^)]*\)\s*,\s*ES\s+(-?\d+(?:\.\d+)?)",
    )
    for pattern in patterns:
        match = re.search(pattern, template)
        if match:
            return float(match.group(1))
    return None


def template_es_source(template: str) -> str | None:
    match = re.search(r"(?:ES源|源)\s+(schwab|ibkr)", template, re.I)
    return match.group(1).lower() if match else None


def load_reports() -> list[dict]:
    rows: list[dict] = []
    pattern = str(DATA_ROOT / "audit/order_map_pricing/date=*/reports.jsonl")
    for filename in glob.glob(pattern):
        for row in read_jsonl(Path(filename)):
            if row.get("report_kind") != "status":
                continue
            trading_date = str(row.get("trading_date") or "")
            if not WINDOW_START <= trading_date <= WINDOW_END:
                continue
            generated = datetime.fromisoformat(row["generated_at"])
            local = generated.astimezone(ET)
            in_rth = (
                local.weekday() < 5
                and time(9, 30) <= local.time().replace(tzinfo=None) < time(16, 0)
            )
            bias, bias_label = visible_bias(row.get("delivered_text") or "")
            candidates = row.get("candidates") or []
            row["_generated_at"] = generated
            row["_session"] = "RTH" if in_rth else "GTH"
            row["_bias"] = bias
            row["_bias_label"] = bias_label
            row["_es"] = template_es_price(row.get("template") or "")
            row["_es_source"] = template_es_source(row.get("template") or "")
            row["_no_trade"] = bool(NO_TRADE_RE.search(row.get("delivered_text") or ""))
            row["_trade_ready"] = bool(
                TRADE_READY_RE.search(row.get("delivered_text") or "")
            )
            row["_candidate_rights"] = {
                str(candidate.get("right"))
                for candidate in candidates
                if candidate.get("right") in {"C", "P"}
            }
            row["_executable_rights"] = {
                str(candidate.get("right"))
                for candidate in candidates
                if candidate.get("right") in {"C", "P"}
                and candidate.get("execution_quote_status") == "executable"
            }
            rows.append(row)
    rows.sort(key=lambda row: row["_generated_at"])
    return rows


def report_direction_summary(rows: list[dict]) -> list[dict]:
    links: list[tuple[dict, int, float]] = []
    for row in rows:
        if row["_bias"] not in (-1, 1) or row["_es"] is None:
            continue
        for horizon in (15, 30, 60):
            target = row["_generated_at"].timestamp() + horizon * 60
            candidates = [
                candidate
                for candidate in rows
                if candidate.get("trading_date") == row.get("trading_date")
                and candidate["_es"] is not None
                and abs(candidate["_generated_at"].timestamp() - target) <= 180
                and (
                    row["_es_source"] is None
                    or candidate["_es_source"] is None
                    or candidate["_es_source"] == row["_es_source"]
                )
            ]
            if not candidates:
                continue
            future = min(
                candidates,
                key=lambda candidate: abs(
                    candidate["_generated_at"].timestamp() - target
                ),
            )
            signed_points = row["_bias"] * (future["_es"] - row["_es"])
            links.append((row, horizon, signed_points))

    summary: list[dict] = []
    for session in ("ALL", "RTH", "GTH"):
        for horizon in (15, 30, 60):
            values = [
                value
                for row, linked_horizon, value in links
                if linked_horizon == horizon
                and (session == "ALL" or row["_session"] == session)
            ]
            wins = sum(value > 0 for value in values)
            ci_low, ci_high = wilson_95(wins, len(values))
            summary.append(
                {
                    "session": session,
                    "horizon_minutes": horizon,
                    "horizon_label": f"{horizon}m",
                    "n": len(values),
                    "wins": wins,
                    "hit_rate": wins / len(values) if values else None,
                    "mean_signed_es_points": mean(values) if values else None,
                    "median_signed_es_points": median(values) if values else None,
                    "hit_ci_low": ci_low,
                    "hit_ci_high": ci_high,
                }
            )
    return summary


def headline_flip_summary(rows: list[dict]) -> list[dict]:
    result: list[dict] = []
    days = sorted({str(row.get("trading_date")) for row in rows})
    for session in ("RTH", "GTH"):
        directional_count = pairs = flips = 0
        for day in days:
            selected = [
                row
                for row in rows
                if str(row.get("trading_date")) == day
                and row["_session"] == session
                and row["_bias"] in (-1, 1)
            ]
            directional_count += len(selected)
            pairs += max(0, len(selected) - 1)
            flips += sum(
                left["_bias"] != right["_bias"]
                for left, right in zip(selected, selected[1:])
            )
        result.append(
            {
                "session": session,
                "directional_reports": directional_count,
                "adjacent_pairs": pairs,
                "flips": flips,
                "flip_rate": flips / pairs if pairs else None,
            }
        )
    return result


def genuine_two_sided(row: dict) -> bool:
    call = row.get("call") or {}
    put = row.get("put") or {}
    for quote in (call, put):
        bid = quote.get("bid")
        ask = quote.get("ask")
        if not isinstance(bid, (int, float)) or not isinstance(ask, (int, float)):
            return False
        if not math.isfinite(float(bid)) or not math.isfinite(float(ask)):
            return False
        if float(bid) < 0.0 or float(ask) <= 0.0 or float(ask) < float(bid):
            return False
    return True


def coverage_integrity(rows: list[dict]) -> tuple[list[dict], dict]:
    claimed_pairs = genuine_pairs = false_full_reports = reports = 0
    for report in rows:
        coverage = report.get("strike_price_coverage")
        if not isinstance(coverage, dict):
            continue
        reports += 1
        coverage_rows = coverage.get("rows") or []
        claimed = int(coverage.get("complete_pair_count") or 0)
        genuine = sum(genuine_two_sided(row) for row in coverage_rows)
        target = int(coverage.get("target_pair_count") or len(coverage_rows))
        claimed_pairs += claimed
        genuine_pairs += genuine
        if claimed >= target and genuine < target:
            false_full_reports += 1
    false_pairs = claimed_pairs - genuine_pairs
    chart_rows = [
        {
            "measure": "报告声称完整",
            "pairs": claimed_pairs,
            "definition": "strike_price_coverage.complete_pair_count",
        },
        {
            "measure": "真实正价双边",
            "pairs": genuine_pairs,
            "definition": "C/P bid>=0, ask>0, ask>=bid",
        },
        {
            "measure": "哨兵误计",
            "pairs": false_pairs,
            "definition": "声称完整减真实正价双边",
        },
    ]
    return chart_rows, {
        "coverage_reports": reports,
        "claimed_pairs": claimed_pairs,
        "genuine_pairs": genuine_pairs,
        "false_pairs": false_pairs,
        "false_full_reports": false_full_reports,
    }


def daily_report_schedule(rows: list[dict]) -> list[dict]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("trading_date"))].append(row)
    result: list[dict] = []
    for day in sorted(grouped):
        day_rows = sorted(grouped[day], key=lambda row: row["_generated_at"])
        gaps = [
            (right["_generated_at"] - left["_generated_at"]).total_seconds() / 60.0
            for left, right in zip(day_rows, day_rows[1:])
        ]
        result.append(
            {
                "date": day,
                "status_reports": len(day_rows),
                "rth_reports": sum(row["_session"] == "RTH" for row in day_rows),
                "max_gap_minutes": max(gaps) if gaps else None,
                "delivered_ok": sum(bool(row.get("delivered_ok")) for row in day_rows),
            }
        )
    return result


def formal_outcomes() -> tuple[list[dict], list[dict]]:
    by_key: dict[tuple[str, int], dict] = {}
    pattern = str(DATA_ROOT / "features/level_decision_outcomes/date=*/outcomes.jsonl")
    for filename in glob.glob(pattern):
        day = filename.split("date=", 1)[1].split("/", 1)[0]
        if day > WINDOW_END:
            continue
        for row in read_jsonl(Path(filename)):
            horizon = row.get("horizon_seconds")
            if horizon not in (30, 60, 180, 300):
                continue
            by_key[(str(row.get("event_id")), int(horizon))] = row
    horizon_rows: list[dict] = []
    for horizon in (30, 60, 180, 300):
        values: list[float] = []
        for (_, stored_horizon), row in by_key.items():
            if stored_horizon != horizon:
                continue
            value = row.get("return_bps")
            if not isinstance(value, (int, float)):
                continue
            signed = float(value) * (1.0 if row.get("direction") == "up" else -1.0)
            values.append(signed)
        wins = sum(value > 0 for value in values)
        ci_low, ci_high = wilson_95(wins, len(values))
        horizon_rows.append(
            {
                "horizon_seconds": horizon,
                "horizon_label": f"{horizon}s",
                "n": len(values),
                "mean_directional_bps": mean(values) if values else None,
                "hit_rate": wins / len(values) if values else None,
                "hit_ci_low": ci_low,
                "hit_ci_high": ci_high,
            }
        )

    rows_300 = [
        row
        for (_, horizon), row in by_key.items()
        if horizon == 300 and isinstance(row.get("return_bps"), (int, float))
    ]
    slices: list[dict] = []
    for session in ("RTH", "GTH"):
        selected = []
        for row in rows_300:
            confirmed = datetime.fromisoformat(row["confirmed_at"]).astimezone(ET)
            in_rth = (
                confirmed.weekday() < 5
                and time(9, 30) <= confirmed.time().replace(tzinfo=None) < time(16, 0)
            )
            if (session == "RTH") != in_rth:
                continue
            selected.append(
                float(row["return_bps"])
                * (1.0 if row.get("direction") == "up" else -1.0)
            )
        wins = sum(value > 0 for value in selected)
        ci_low, ci_high = wilson_95(wins, len(selected))
        slices.append(
            {
                "session": session,
                "n": len(selected),
                "mean_directional_bps": mean(selected) if selected else None,
                "hit_rate": wins / len(selected) if selected else None,
                "hit_ci_low": ci_low,
                "hit_ci_high": ci_high,
            }
        )
    return horizon_rows, slices


def source(
    source_id: str,
    label: str,
    path: str,
    engine: str,
    query_id: str,
    code: str,
    description: str,
    filters: list[str],
    definitions: list[str],
    tables: list[str],
) -> dict:
    return {
        "id": source_id,
        "label": label,
        "path": path,
        "query": {
            "engine": engine,
            "id": query_id,
            "sql": code,
            "description": description,
            "executed_at": datetime.now(timezone.utc).isoformat(),
            "language": "sql",
            "filters": filters,
            "metric_definitions": definitions,
            "tables_used": tables,
        },
    }


def build_notebook() -> None:
    title = "# SPX 三周信号与 15 分钟报告诊断：截至 2026-07-23"
    setup_code = """
from __future__ import annotations

import glob
import json
import math
import os
import re
from datetime import datetime, time
from pathlib import Path
from statistics import mean, median
from zoneinfo import ZoneInfo

REPO_ROOT = next((p for p in (Path.cwd(), *Path.cwd().parents) if (p / "src/spx_spark").is_dir()), None)
if REPO_ROOT is None:
    raise RuntimeError("Run from the spx-spark repository")
DATA_ROOT = Path(os.environ.get("SPX_SPARK_DATA_ROOT", "/srv/data/spx-spark/data"))
BACKTEST_PATH = DATA_ROOT / "reports/odte_level_backtest/diagnostic=2026-07-24-three-week/cutoff=2026-07-23/artifact.json"
ET = ZoneInfo("America/New_York")
START, END = "2026-07-03", "2026-07-23"

def read_jsonl(path):
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            if isinstance(row, dict):
                yield row

backtest = json.loads(BACKTEST_PATH.read_text(encoding="utf-8"))
print("cutoff", backtest["window"]["cutoff_at"], "complete sessions", backtest["window"]["trading_days"])
""".strip()

    report_code = r"""
NO_TRADE = re.compile(
    r"未通过执行门控|\bNO TRADE\b|当前不进场|暂停新开仓|当前不是下单计划|当前不可预挂|"
    r"不可执行定价|不生成交易判断|不生成[^\n]{0,30}(?:限价|下单建议)|只观察|map_only|不执行",
    re.I,
)
TRADE_READY = re.compile(
    r"(?:^|\n)\s*(?:结论\s+)?TRADE[ _-]?READY\b|决策门控已通过", re.I
)

def bias(text):
    lines = [x.strip(" -*") for x in text.splitlines() if x.strip()]
    for prefix in ("判断", "观察"):
        for line in lines[:10]:
            if line.startswith(prefix):
                for label, side in (
                    ("趋势偏多", 1), ("过渡偏多", 1), ("趋势偏空", -1),
                    ("过渡偏空", -1), ("偏多", 1), ("偏空", -1),
                    ("均值回归", 0), ("方向过渡", 0), ("证据不足", None),
                ):
                    if label in line:
                        return side, label
    top = "\n".join(lines[:8])
    for label, side in (
        ("趋势偏多", 1), ("过渡偏多", 1), ("趋势偏空", -1),
        ("过渡偏空", -1), ("偏多", 1), ("偏空", -1),
        ("均值回归", 0), ("中性", 0),
    ):
        if re.search(r"(?:主情景|主剧本|结论)[^\n]{0,80}" + re.escape(label), top):
            return side, label
    return None, None

def es_price(template):
    patterns = (
        r"SPX (?:代理|proxy)[:：]?[^\n]*?[；;]\s*ES\s+(-?\d+(?:\.\d+)?)",
        r"价格\s+SPX\s+[-\d.]+(?:\([^)]*\))?\s*[｜|　]+\s*ES\s+(-?\d+(?:\.\d+)?)",
        r"参考价[:：]\s*[-\d.]+\([^\n)]*\)\s*[；;,]\s*ES\s+(-?\d+(?:\.\d+)?)",
        r"时段[:：][^\n]*?SPX\s+[-\d.]+\([^)]*\)\s*,\s*ES\s+(-?\d+(?:\.\d+)?)",
    )
    for pattern in patterns:
        match = re.search(pattern, template)
        if match:
            return float(match.group(1))
    return None

def es_source(template):
    match = re.search(r"(?:ES源|源)\s+(schwab|ibkr)", template, re.I)
    return match.group(1).lower() if match else None

reports = []
for path in glob.glob(str(DATA_ROOT / "audit/order_map_pricing/date=*/reports.jsonl")):
    for row in read_jsonl(path):
        day = str(row.get("trading_date") or "")
        if row.get("report_kind") != "status" or not START <= day <= END:
            continue
        row["_dt"] = datetime.fromisoformat(row["generated_at"])
        local = row["_dt"].astimezone(ET)
        row["_session"] = "RTH" if time(9, 30) <= local.time().replace(tzinfo=None) < time(16, 0) else "GTH"
        row["_bias"], row["_label"] = bias(row["delivered_text"])
        row["_es"], row["_src"] = es_price(row["template"]), es_source(row["template"])
        row["_no_trade"] = bool(NO_TRADE.search(row["delivered_text"]))
        row["_trade_ready"] = bool(TRADE_READY.search(row["delivered_text"]))
        reports.append(row)
reports.sort(key=lambda row: row["_dt"])

report_counts = {
    "total": len(reports),
    "rth": sum(row["_session"] == "RTH" for row in reports),
    "gth": sum(row["_session"] == "GTH" for row in reports),
    "no_trade": sum(row["_no_trade"] for row in reports),
    "rth_no_trade": sum(row["_session"] == "RTH" and row["_no_trade"] for row in reports),
    "trade_ready": sum(row["_trade_ready"] for row in reports),
    "delivered_ok": sum(bool(row.get("delivered_ok")) for row in reports),
}
report_counts
""".strip()

    direction_code = r"""
links = []
for row in reports:
    if row["_bias"] not in (-1, 1) or row["_es"] is None:
        continue
    for horizon in (15, 30, 60):
        target = row["_dt"].timestamp() + horizon * 60
        candidates = [
            candidate for candidate in reports
            if candidate["trading_date"] == row["trading_date"]
            and candidate["_es"] is not None
            and abs(candidate["_dt"].timestamp() - target) <= 180
            and (
                row["_src"] is None or candidate["_src"] is None
                or candidate["_src"] == row["_src"]
            )
        ]
        if candidates:
            future = min(candidates, key=lambda x: abs(x["_dt"].timestamp() - target))
            links.append((row, horizon, row["_bias"] * (future["_es"] - row["_es"])))

direction_summary = []
for session in ("ALL", "RTH", "GTH"):
    for horizon in (15, 30, 60):
        values = [
            value for row, linked_horizon, value in links
            if linked_horizon == horizon and (session == "ALL" or row["_session"] == session)
        ]
        direction_summary.append({
            "session": session,
            "horizon": horizon,
            "n": len(values),
            "hit_rate": sum(value > 0 for value in values) / len(values),
            "mean_signed_es_points": mean(values),
            "median_signed_es_points": median(values),
        })

flip_summary = []
for session in ("RTH", "GTH"):
    directional = pairs = flips = 0
    for day in sorted({row["trading_date"] for row in reports}):
        selected = [
            row for row in reports
            if row["trading_date"] == day and row["_session"] == session and row["_bias"] in (-1, 1)
        ]
        directional += len(selected)
        pairs += max(0, len(selected) - 1)
        flips += sum(left["_bias"] != right["_bias"] for left, right in zip(selected, selected[1:]))
    flip_summary.append({
        "session": session, "directional": directional, "pairs": pairs,
        "flips": flips, "flip_rate": flips / pairs,
    })

direction_summary, flip_summary
""".strip()

    formal_code = r"""
outcomes = {}
for path in glob.glob(str(DATA_ROOT / "features/level_decision_outcomes/date=*/outcomes.jsonl")):
    day = path.split("date=", 1)[1].split("/", 1)[0]
    if day > END:
        continue
    for row in read_jsonl(path):
        horizon = row.get("horizon_seconds")
        if horizon in (30, 60, 180, 300):
            outcomes[(row["event_id"], horizon)] = row

formal_horizons = []
for horizon in (30, 60, 180, 300):
    values = []
    for (_, stored_horizon), row in outcomes.items():
        if stored_horizon != horizon or not isinstance(row.get("return_bps"), (int, float)):
            continue
        values.append(float(row["return_bps"]) * (1 if row["direction"] == "up" else -1))
    formal_horizons.append({
        "horizon": horizon, "n": len(values), "mean_bps": mean(values),
        "hit_rate": sum(value > 0 for value in values) / len(values),
    })

formal_300 = [row for (_, horizon), row in outcomes.items() if horizon == 300]
rth_formal = []
for row in formal_300:
    local = datetime.fromisoformat(row["confirmed_at"]).astimezone(ET)
    if time(9, 30) <= local.time().replace(tzinfo=None) < time(16, 0):
        rth_formal.append(row)

formal_horizons, {"formal_total": len(formal_300), "formal_rth": len(rth_formal)}
""".strip()

    integrity_code = r"""
def genuine_two_sided(row):
    for quote in (row.get("call") or {}, row.get("put") or {}):
        bid, ask = quote.get("bid"), quote.get("ask")
        if not isinstance(bid, (int, float)) or not isinstance(ask, (int, float)):
            return False
        if not math.isfinite(float(bid)) or not math.isfinite(float(ask)):
            return False
        if bid < 0 or ask <= 0 or ask < bid:
            return False
    return True

coverage_reports = claimed_pairs = genuine_pairs = false_full_reports = 0
for report in reports:
    coverage = report.get("strike_price_coverage")
    if not isinstance(coverage, dict):
        continue
    coverage_reports += 1
    coverage_rows = coverage.get("rows") or []
    claimed = int(coverage.get("complete_pair_count") or 0)
    genuine = sum(genuine_two_sided(row) for row in coverage_rows)
    target = int(coverage.get("target_pair_count") or len(coverage_rows))
    claimed_pairs += claimed
    genuine_pairs += genuine
    false_full_reports += claimed >= target and genuine < target

integrity = {
    "coverage_reports": coverage_reports,
    "claimed_pairs": claimed_pairs,
    "genuine_pairs": genuine_pairs,
    "sentinel_false_pairs": claimed_pairs - genuine_pairs,
    "false_full_reports": false_full_reports,
}
integrity
""".strip()

    assertions_code = r"""
assert backtest["window"]["trading_days"] == 8
assert backtest["signal_counts"] == {
    "confirmed": 32, "prefill": 319, "gth_dip": 6, "trade_ready": 4
}
assert backtest["production_strategy_total"]["result"]["n"] == 2
assert backtest["production_strategy_total"]["result"]["total_pnl_usd"] == 780
assert report_counts == {
    "total": 521, "rth": 129, "gth": 392, "no_trade": 464,
    "rth_no_trade": 127, "trade_ready": 0, "delivered_ok": 521,
}
assert [(row["session"], row["horizon"], row["n"]) for row in direction_summary] == [
    ("ALL", 15, 319), ("ALL", 30, 317), ("ALL", 60, 299),
    ("RTH", 15, 74), ("RTH", 30, 71), ("RTH", 60, 60),
    ("GTH", 15, 245), ("GTH", 30, 246), ("GTH", 60, 239),
]
assert [(row["session"], row["flips"], row["pairs"]) for row in flip_summary] == [
    ("RTH", 28, 85), ("GTH", 80, 242)
]
assert [(row["horizon"], row["n"]) for row in formal_horizons] == [
    (30, 32), (60, 32), (180, 30), (300, 32)
]
assert len(rth_formal) == 11
assert integrity == {
    "coverage_reports": 116, "claimed_pairs": 6212, "genuine_pairs": 5791,
    "sentinel_false_pairs": 421, "false_full_reports": 48,
}
print("VALIDATED: report funnel, delivered bias outcomes, formal outcomes, price integrity, and strict production replay.")
""".strip()

    parameter_code = r"""
# Frozen-candidate forward audit.  The candidate registry was fixed before the
# 2026-07-23 RTH session; these are gross top-of-book results for that one
# session only and are intentionally not promoted to production.
forward_candidates = [
    {"gate": "15s / 2.00pt / 5.00% EM (current)", "semantic_touches": 29, "fills": 0, "gross_pnl_usd": 0},
    {"gate": "20s / 0.50pt / 7.50% EM", "semantic_touches": 29, "fills": 3, "gross_pnl_usd": 380},
    {"gate": "20s / 1.00pt / 5.00% EM", "semantic_touches": 29, "fills": 5, "gross_pnl_usd": 250},
    {"gate": "20s / 1.00pt / 7.50% EM", "semantic_touches": 29, "fills": 2, "gross_pnl_usd": 160},
    {"gate": "45s / 2.00pt / 5.00% EM", "semantic_touches": 29, "fills": 6, "gross_pnl_usd": 370},
]
forward_candidates
""".strip()

    notebook = nbformat.v4.new_notebook(
        cells=[
            nbformat.v4.new_markdown_cell(
                title
                + "\n\n请求窗口为 2026-07-03–2026-07-23；本 notebook 不把缺失的生产历史回填成零信号。"
            ),
            nbformat.v4.new_markdown_cell(
                "## TL;DR\n\n"
                "- 三周原始行情不是空的，但 audit-equivalent 生产信号只覆盖 7/13–7/23，完整 intent/outcome 会话只有 8 个。\n"
                "- 521 份状态报告里正向 TradeReady 为 0；RTH 129 份里 127 份明确 NO TRADE。\n"
                "- 底层有 11 个 RTH formal confirmations，问题在 formal→actionable 门控、瞬态信号未锁存和报告时段截断。\n"
                "- 生产只有 4 个 unique terminal intents；任何生产参数调整都会是小样本过拟合。\n"
                "- 报告稠密度被高估：421 个 `-1/-1` 哨兵价对被计为完整双边。"
            ),
            nbformat.v4.new_markdown_cell(
                "## Context & Methods\n\n"
                "报告审计使用实际送达正文 `delivered_text`；方向结果按同交易日、目标 ±3 分钟、ES 来源兼容联结。"
                "正式信号按 event+horizon 去重，方向收益对 Put/down 取反。生产盈亏使用固定 cutoff 的严格 backtest artifact。"
                "全部结果都是诊断用途，不是交易建议。"
            ),
            nbformat.v4.new_code_cell(setup_code),
            nbformat.v4.new_markdown_cell(
                "## Data\n\n"
                "原始 RTH quote 最早可诚实使用 7/06；生产 level FSM/report 从 7/13 起；trade intent/outcome 从 7/14 起。"
                "7/03 是观察假日，7/04–05 与 7/11–12 是周末。"
            ),
            nbformat.v4.new_code_cell(report_code),
            nbformat.v4.new_code_cell(direction_code),
            nbformat.v4.new_code_cell(formal_code),
            nbformat.v4.new_code_cell(integrity_code),
            nbformat.v4.new_code_cell(parameter_code),
            nbformat.v4.new_markdown_cell("## Results"),
            nbformat.v4.new_code_cell(assertions_code),
            nbformat.v4.new_markdown_cell(
                "## Takeaways\n\n"
                "1. 当前首先要修报告与证据链，而不是放松生产门控。\n"
                "2. 完整 RTH cadence、formal latch、门控原因和可执行价完整性必须成为每期报告的一等字段。\n"
                "3. 候选参数保持 shadow，至少积累 20 个合约一致完整 RTH sessions 后再做 walk-forward 晋级。\n"
                "4. 回放未计手续费、滑点、排队、部分成交、冲击和人工延迟；gross PnL 不能解释真实账户亏损。"
            ),
        ],
        metadata={
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python", "version": "3"},
        },
    )
    NotebookClient(
        notebook,
        timeout=600,
        kernel_name="python3",
        resources={"metadata": {"path": str(REPO_ROOT)}},
    ).execute()
    nbformat.write(notebook, NOTEBOOK_PATH)


def build_artifact() -> None:
    if not BACKTEST_PATH.exists():
        raise FileNotFoundError(
            f"Run scripts/backtest-0dte-levels.py --as-of 2026-07-23 first: {BACKTEST_PATH}"
        )
    backtest = json.loads(BACKTEST_PATH.read_text(encoding="utf-8"))
    reports = load_reports()
    direction = report_direction_summary(reports)
    flips = headline_flip_summary(reports)
    price_integrity_rows, price_integrity = coverage_integrity(reports)
    schedule = daily_report_schedule(reports)
    horizon_rows, formal_session_slices = formal_outcomes()

    coverage_by_day = [
        {"date": "2026-07-03", "live_minutes": 0, "complete_cp_minutes": 0, "median_cp_pairs": 0, "formal_layer": "无（休市）", "note": "7/4 observed holiday"},
        {"date": "2026-07-06", "live_minutes": 367, "complete_cp_minutes": 357, "median_cp_pairs": 22, "formal_layer": "无", "note": "仅 raw/IV replay"},
        {"date": "2026-07-07", "live_minutes": 343, "complete_cp_minutes": 339, "median_cp_pairs": 39, "formal_layer": "无", "note": "仅 raw/IV replay"},
        {"date": "2026-07-08", "live_minutes": 388, "complete_cp_minutes": 388, "median_cp_pairs": 66, "formal_layer": "无", "note": "仅 raw/IV replay"},
        {"date": "2026-07-09", "live_minutes": 375, "complete_cp_minutes": 374, "median_cp_pairs": 58, "formal_layer": "无", "note": "仅 raw/IV replay"},
        {"date": "2026-07-10", "live_minutes": 386, "complete_cp_minutes": 386, "median_cp_pairs": 60, "formal_layer": "无", "note": "仅 raw/IV replay"},
        {"date": "2026-07-13", "live_minutes": 390, "complete_cp_minutes": 390, "median_cp_pairs": 67, "formal_layer": "FSM/report（partial）", "note": "GTH coverage 85.19%"},
        {"date": "2026-07-14", "live_minutes": 390, "complete_cp_minutes": 390, "median_cp_pairs": 62, "formal_layer": "完整 intent/outcome", "note": "complete session"},
        {"date": "2026-07-15", "live_minutes": 390, "complete_cp_minutes": 361, "median_cp_pairs": 67, "formal_layer": "完整 intent/outcome", "note": "late-chain gap"},
        {"date": "2026-07-16", "live_minutes": 390, "complete_cp_minutes": 361, "median_cp_pairs": 60, "formal_layer": "完整 intent/outcome", "note": "late-chain gap"},
        {"date": "2026-07-17", "live_minutes": 390, "complete_cp_minutes": 361, "median_cp_pairs": 70, "formal_layer": "完整 intent/outcome", "note": "late-chain gap"},
        {"date": "2026-07-20", "live_minutes": 390, "complete_cp_minutes": 280, "median_cp_pairs": 66, "formal_layer": "完整 intent/outcome", "note": "09:39–10:59 ET option gap"},
        {"date": "2026-07-21", "live_minutes": 390, "complete_cp_minutes": 361, "median_cp_pairs": 59, "formal_layer": "完整 intent/outcome", "note": "report outbox incident"},
        {"date": "2026-07-22", "live_minutes": 390, "complete_cp_minutes": 361, "median_cp_pairs": 58, "formal_layer": "完整 intent/outcome", "note": "late-chain gap"},
        {"date": "2026-07-23", "live_minutes": 390, "complete_cp_minutes": 361, "median_cp_pairs": 71, "formal_layer": "完整 + first frozen forward", "note": "only 1 forward-v3 session"},
    ]
    coverage_long = []
    for row in coverage_by_day:
        if row["date"] == "2026-07-03":
            continue
        coverage_long.extend(
            [
                {
                    "date": row["date"][5:],
                    "layer": "SPX/ES/SPY live",
                    "minutes": row["live_minutes"],
                    "coverage_ratio": row["live_minutes"] / 390,
                },
                {
                    "date": row["date"][5:],
                    "layer": "0DTE 完整 C/P",
                    "minutes": row["complete_cp_minutes"],
                    "coverage_ratio": row["complete_cp_minutes"] / 390,
                },
            ]
        )

    signal_funnel = []
    for session, values in {
        "RTH": [202, 108, 97, 56, 27, 11],
        "GTH": [512, 279, 178, 80, 54, 21],
    }.items():
        for order, (stage, value) in enumerate(
            zip(
                ("Lifecycle", "Testing", "Break/Reject pending", "Accepted/Rejected", "Retest", "Confirmed"),
                values,
            ),
            start=1,
        ):
            signal_funnel.append(
                {"session": session, "stage": stage, "stage_order": order, "count": value}
            )

    report_session = []
    for session in ("RTH", "GTH"):
        selected = [row for row in reports if row["_session"] == session]
        report_session.append(
            {
                "session": session,
                "reports": len(selected),
                "explicit_no_trade": sum(row["_no_trade"] for row in selected),
                "positive_trade_ready": sum(row["_trade_ready"] for row in selected),
                "with_any_executable_quote": sum(
                    bool(row["_executable_rights"]) for row in selected
                ),
                "with_both_cp_candidates": sum(
                    row["_candidate_rights"] == {"C", "P"} for row in selected
                ),
                "with_both_cp_executable": sum(
                    row["_executable_rights"] == {"C", "P"} for row in selected
                ),
            }
        )
    report_action_long = []
    for row in report_session:
        report_action_long.extend(
            [
                {
                    "session": row["session"],
                    "state": "已生成报告",
                    "count": row["reports"],
                },
                {
                    "session": row["session"],
                    "state": "明确 NO TRADE",
                    "count": row["explicit_no_trade"],
                },
                {
                    "session": row["session"],
                    "state": "正向 TradeReady",
                    "count": row["positive_trade_ready"],
                },
            ]
        )

    replay_cohorts = [
        {
            "cohort": "Formal confirmed control",
            "opportunities": 32,
            "fills": 21,
            "win_rate": 0.38095238095238093,
            "gross_pnl_usd": -445,
            "role": "control/proxy",
            "decision": "不能当生产收益",
        },
        {
            "cohort": "First-touch + current gate proxy",
            "opportunities": 319,
            "fills": 6,
            "win_rate": 0.6666666666666666,
            "gross_pnl_usd": 100,
            "role": "observational proxy",
            "decision": "样本与可得性偏差大",
        },
        {
            "cohort": "GTH dip proxy",
            "opportunities": 6,
            "fills": 3,
            "win_rate": 0.0,
            "gross_pnl_usd": -440,
            "role": "shadow proxy",
            "decision": "保持 shadow",
        },
        {
            "cohort": "Exact production TradeReady",
            "opportunities": 4,
            "fills": 2,
            "win_rate": 0.5,
            "gross_pnl_usd": 780,
            "role": "production-equivalent",
            "decision": "n=4，不能证明 edge",
        },
    ]

    parameter_forward = [
        {
            "gate": "15s / 2.00pt / 5.00% EM",
            "role": "current production",
            "semantic_touches": 29,
            "fills": 0,
            "gross_pnl_usd": 0,
            "forward_sessions": 1,
            "decision": "保留生产；不能用单日反应性强行调参",
        },
        {
            "gate": "20s / 0.50pt / 7.50% EM",
            "role": "pre-registered shadow",
            "semantic_touches": 29,
            "fills": 3,
            "gross_pnl_usd": 380,
            "forward_sessions": 1,
            "decision": "继续 shadow；不得晋级",
        },
        {
            "gate": "20s / 1.00pt / 5.00% EM",
            "role": "shadow",
            "semantic_touches": 29,
            "fills": 5,
            "gross_pnl_usd": 250,
            "forward_sessions": 1,
            "decision": "继续 shadow；先冻结唯一 fingerprint",
        },
        {
            "gate": "20s / 1.00pt / 7.50% EM",
            "role": "shadow",
            "semantic_touches": 29,
            "fills": 2,
            "gross_pnl_usd": 160,
            "forward_sessions": 1,
            "decision": "继续 shadow",
        },
        {
            "gate": "45s / 2.00pt / 5.00% EM",
            "role": "shadow",
            "semantic_touches": 29,
            "fills": 6,
            "gross_pnl_usd": 370,
            "forward_sessions": 1,
            "decision": "继续 shadow",
        },
    ]

    report_direction_rth = [
        row for row in direction if row["session"] == "RTH"
    ]
    writer_counts = Counter(str(row.get("writer") or "unknown") for row in reports)
    headline = [
        {
            "raw_replay_trading_days": 14,
            "production_complete_sessions": backtest["window"]["trading_days"],
            "status_reports": len(reports),
            "rth_report_coverage": 112 / 208,
            "formal_rth_confirmed": next(
                row["n"] for row in formal_session_slices if row["session"] == "RTH"
            ),
            "rth_report_trade_ready": sum(
                row["_session"] == "RTH" and row["_trade_ready"] for row in reports
            ),
            "exact_terminal_intents": backtest["signal_counts"]["trade_ready"],
            "false_complete_pairs": price_integrity["false_pairs"],
            "forward_sessions": 1,
        }
    ]

    recommendations = [
        {
            "priority": 1,
            "workstream": "价格完整性",
            "change": "拒绝 bid<0、ask<=0、ask<bid；拆分结构覆盖与 executable NBBO 覆盖",
            "evidence": "421 个哨兵价对被计为完整；48 份报告虚假满覆盖",
            "validation": "负价/哨兵 fixture + production audit 回放",
            "parameter_change": "否",
        },
        {
            "priority": 2,
            "workstream": "信号锁存",
            "change": "formal CONFIRMED 至少锁存到下一份 15m 报告并展示 gate reason",
            "evidence": "11 个 RTH formal；报告 0 TradeReady，6 个可承接事件也未显示",
            "validation": "event→next-report linkage=100%",
            "parameter_change": "否",
        },
        {
            "priority": 3,
            "workstream": "完整 RTH cadence",
            "change": "timer 延伸至 16:00 ET；分别监控 generated 与 delivered SLO",
            "evidence": "完整 RTH 槽位仅 112/208=53.85%",
            "validation": "连续 5 日 26/26 RTH reports",
            "parameter_change": "否",
        },
        {
            "priority": 4,
            "workstream": "报告行动层",
            "change": "首屏固定 Action now / Conditional setup / Context only；双向 map 下沉",
            "evidence": "RTH 127/129 NO TRADE；用户可见方向 28/85 翻向",
            "validation": "每份报告唯一 action state + 自动 outcome label",
            "parameter_change": "否",
        },
        {
            "priority": 5,
            "workstream": "回测严格性",
            "change": "报价选择改为 received_at knowledge-time；冻结 candidate registry/fingerprint",
            "evidence": "当前 replay 仍按 quote_time；候选文档存在两种阈值表述",
            "validation": "point-in-time leakage test + immutable registry hash",
            "parameter_change": "否",
        },
        {
            "priority": 6,
            "workstream": "参数晋级",
            "change": "候选仅 shadow；20 个合约一致完整 RTH sessions 后 expanding walk-forward",
            "evidence": "当前 forward-v3 只有 1 session；生产 terminal intents 仅 4",
            "validation": "预登记门槛、成本后净 PnL、bootstrap CI、tail risk",
            "parameter_change": "暂不",
        },
    ]

    generated_at = datetime.now(timezone.utc).isoformat()
    sources = [
        source(
            "data_coverage",
            "RTH quote and 0DTE complete-pair coverage, 2026-07-03–23",
            "lake/quotes/schema=v1/date=*/provider=*/hour=*/quotes.parquet",
            "DuckDB + provider-union minute audit",
            "three-week-raw-coverage-20260724",
            "SELECT date, COUNT(DISTINCT minute) FROM read_parquet('lake/quotes/schema=v1/date=*/provider=*/hour=*/quotes.parquet', hive_partitioning=true) WHERE instrument_id IN ('index:SPX','future:ES','equity:SPY') GROUP BY date",
            "Counts live RTH underlier minutes and minutes with at least one complete same-day SPXW C/P strike.",
            [
                "2026-07-03T00:00Z <= received_at < 2026-07-24T00:00Z",
                "RTH = 09:30–16:00 America/New_York",
                "live quality and positive effective price",
                "complete C/P requires same strike and same-day SPXW expiry",
            ],
            [
                "Live minute is a distinct RTH minute with valid SPX, ES and SPY evidence.",
                "Complete C/P minute has at least one strike with both Call and Put observations.",
                "390 minutes is a full regular session.",
            ],
            ["lake/quotes/schema=v1"],
        ),
        source(
            "report_audit",
            "Persisted 15-minute report pricing audit",
            "audit/order_map_pricing/date=*/reports.jsonl",
            "Python delivered-text audit",
            "three-week-15m-report-audit-20260724",
            "SELECT generated_at, trading_date, delivered_text, template, candidates, strike_price_coverage, writer, delivered_ok FROM read_json_auto('audit/order_map_pricing/date=*/reports.jsonl', union_by_name=true) WHERE report_kind = 'status' AND trading_date BETWEEN DATE '2026-07-03' AND DATE '2026-07-23' ORDER BY generated_at",
            "Audits actual delivered report text, generation gaps, candidate rights, action state, visible headline flips and later ES direction.",
            [
                "report_kind=status",
                "2026-07-03 <= trading_date <= 2026-07-23",
                "direction outcome uses delivered_text headline, not template intended bias",
                "same trading date and compatible ES source",
                "future report timestamp within ±180 seconds",
            ],
            [
                "Positive TradeReady requires an affirmative TradeReady conclusion or passed decision gate.",
                "NO TRADE matches explicit gate failure, no-entry, observe-only or non-executable wording.",
                "Directional hit means headline side × future ES change is strictly positive; flat is not a win.",
                "RTH schedule coverage is observed 09:30–15:45 ET report slots divided by 26 slots per complete session.",
            ],
            ["audit/order_map_pricing", "ledger/notification_delivery.sqlite"],
        ),
        source(
            "price_integrity",
            "Dense strike-price coverage integrity audit",
            "audit/order_map_pricing/date=*/reports.jsonl",
            "Python contract audit",
            "strike-price-sentinel-integrity-20260724",
            "SELECT generated_at, strike_price_coverage FROM read_json_auto('audit/order_map_pricing/date=*/reports.jsonl', union_by_name=true) WHERE report_kind = 'status' AND strike_price_coverage IS NOT NULL AND trading_date BETWEEN DATE '2026-07-03' AND DATE '2026-07-23' ORDER BY generated_at",
            "Recalculates every claimed complete C/P row and rejects negative sentinel quotes.",
            [
                "reports containing strike_price_coverage",
                "no interpolation",
                "finite prices only",
                "both Call and Put must satisfy positive two-sided contract",
            ],
            [
                "False pair equals claimed complete pair minus genuine positive two-sided pair.",
                "False-full report claims complete_pair_count >= target_pair_count but fails the genuine test.",
            ],
            ["audit/order_map_pricing", "src/spx_spark/application/order_map/research.py"],
        ),
        source(
            "formal_outcomes",
            "Formal level-decision outcomes",
            "features/level_decision_outcomes/date=*/outcomes.jsonl",
            "Python semantic outcome audit",
            "formal-directional-outcomes-20260724",
            "SELECT event_id, horizon_seconds, confirmed_at, direction, return_bps FROM read_json_auto('features/level_decision_outcomes/date=*/outcomes.jsonl', union_by_name=true) WHERE CAST(confirmed_at AS TIMESTAMPTZ) < TIMESTAMPTZ '2026-07-24T00:00:00Z' QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id, horizon_seconds ORDER BY completed_at DESC) = 1",
            "Evaluates every unique formal confirmation at persisted 30/60/180/300-second horizons.",
            [
                "event time before 2026-07-24T00:00Z",
                "unique event_id + horizon_seconds",
                "finite completed return_bps only",
                "RTH by confirmed_at in America/New_York",
            ],
            [
                "Directional return is raw SPX return for up and its negative for down.",
                "Hit rate is strictly positive directional return divided by complete events.",
                "Wilson interval is a two-sided 95% binomial interval.",
            ],
            ["features/level_decision_outcomes", "features/level_decision_audit"],
        ),
        source(
            "strict_backtest",
            "Fixed-cutoff production-equivalent and proxy replay",
            "reports/odte_level_backtest/diagnostic=2026-07-24-three-week/cutoff=2026-07-23/artifact.json",
            "spx-spark odte_level_backtest",
            "strict-three-week-replay-cutoff-20260723",
            "SELECT * FROM read_json_auto('reports/odte_level_backtest/diagnostic=2026-07-24-three-week/cutoff=2026-07-23/artifact.json')",
            "Production intent replay plus separately labelled formal/prefill/GTH control cohorts over complete sessions.",
            [
                "exclusive cutoff 2026-07-24T00:00:00Z",
                "complete sessions only",
                "semantic first-touch deduplication",
                "production total = trade_ready × baseline × naked",
                "no commission, explicit slippage, queue, partial fill, impact or manual latency",
            ],
            [
                "Strict fill uses recorded provider/contract/limit and contemporaneous top-of-book ask.",
                "Gross replay PnL is one contract and is not broker-account PnL.",
                "Control/proxy cohorts are excluded from production total.",
            ],
            [
                "features/trade_intents",
                "features/pricing_outcomes",
                "features/gth_dip_reclaim",
                "features/level_decision_health",
                "lake/quotes/schema=v1",
            ],
        ),
        source(
            "parameter_forward",
            "Pre-registered follow-through candidate forward replay",
            "features/pricing_outcomes/date=2026-07-23/outcomes.jsonl",
            "spx-spark point-in-time candidate replay",
            "frozen-follow-through-forward-20260723",
            "SELECT * FROM read_json_auto('features/pricing_outcomes/date=2026-07-23/outcomes.jsonl') WHERE touched = TRUE AND first_touch_at IS NOT NULL ORDER BY first_touch_at",
            "Replays current and frozen shadow follow-through candidates on the first post-registration RTH session.",
            [
                "candidate definitions fixed before 2026-07-23 RTH",
                "29 semantic RTH touches",
                "one-contract top-of-book gross replay",
                "one forward session only",
            ],
            [
                "A candidate pass is not a production signal and remains shadow-only.",
                "Gross PnL excludes all explicit trading costs and execution frictions.",
                "One forward session cannot satisfy promotion readiness.",
            ],
            ["features/pricing_outcomes", "lake/quotes/schema=v1", "src/spx_spark/data_platform/research/odte_level_backtest.py"],
        ),
        source(
            "production_history_boundary",
            "Production-history availability boundary",
            "features/level_decision_audit/date=*/events.jsonl",
            "DuckDB feature inventory",
            "production-history-boundary-20260724",
            "SELECT MIN(CAST(regexp_extract(filename, 'date=([0-9-]+)', 1) AS DATE)) AS first_production_fsm_date FROM read_json_auto('features/level_decision_audit/date=*/events.jsonl', filename=true, union_by_name=true)",
            "Identifies the first persisted production FSM partition; no earlier signal state is fabricated.",
            [
                "requested window 2026-07-03 through 2026-07-23",
                "persisted production feature partitions only",
            ],
            [
                "Audit-equivalent history requires the production state transition record, not only later raw quotes.",
            ],
            ["features/level_decision_audit", "features/trade_intents"],
        ),
        source(
            "broker_reconciliation",
            "Redacted broker-statement coverage inventory",
            "redacted broker Activity/Flex statement inventory",
            "Local file inventory",
            "broker-statement-boundary-20260724",
            "SELECT DATE '2026-07-16' AS latest_statement_end, DATE '2026-07-17' AS missing_window_start, DATE '2026-07-23' AS missing_window_end",
            "Records the redacted coverage boundary without exposing account identifiers.",
            ["account identifiers omitted", "no broker fills after 2026-07-16 available"],
            [
                "Actual loss reconciled requires every fill, size, fee and realized exit in the loss window.",
            ],
            ["redacted broker Activity/Flex statement inventory"],
        ),
        source(
            "companion_notebook",
            "Executed companion notebook",
            NOTEBOOK_PATH.name,
            "Python notebook",
            "three-week-diagnostic-notebook-20260724",
            "SELECT 'spx-three-week-signal-diagnostic-2026-07-24.ipynb' AS notebook, 'executed top-to-bottom' AS validation_status",
            "Reproduces report/action classification, ES direction linkage, formal outcomes, sentinel-price integrity and fixed-cutoff production checks.",
            ["executed top-to-bottom from repository root"],
            ["Assertions fail closed when the reviewed source counts drift."],
            ["audit/order_map_pricing", "features/level_decision_outcomes", "reports/odte_level_backtest"],
        ),
    ]

    cards = [
        {
            "id": "raw_window",
            "description": "Raw market-state replay has fourteen trading days; this is not production signal history.",
            "dataset": "headline",
            "sourceId": "data_coverage",
            "metrics": [
                {"label": "原始行情交易日", "field": "raw_replay_trading_days", "format": "number"}
            ],
        },
        {
            "id": "production_window",
            "description": "Sessions with intent/outcome-equivalent complete production evidence.",
            "dataset": "headline",
            "sourceId": "strict_backtest",
            "metrics": [
                {"label": "完整生产会话", "field": "production_complete_sessions", "format": "number"}
            ],
        },
        {
            "id": "rth_action",
            "description": "Affirmative TradeReady actions in all persisted RTH 15-minute reports.",
            "dataset": "headline",
            "sourceId": "report_audit",
            "metrics": [
                {"label": "RTH 报告 TradeReady", "field": "rth_report_trade_ready", "format": "number"},
                {"label": "底层 RTH formal", "field": "formal_rth_confirmed", "format": "number"},
            ],
        },
        {
            "id": "rth_schedule",
            "description": "Observed report slots across eight complete RTH sessions divided by 208 full-session slots.",
            "dataset": "headline",
            "sourceId": "report_audit",
            "metrics": [
                {"label": "RTH 报告时段覆盖", "field": "rth_report_coverage", "format": "percent"}
            ],
        },
        {
            "id": "terminal_intents",
            "description": "Unique persisted terminal production TradeReady decisions through the cutoff.",
            "dataset": "headline",
            "sourceId": "strict_backtest",
            "metrics": [
                {"label": "生产终端意图", "field": "exact_terminal_intents", "format": "number"}
            ],
        },
        {
            "id": "price_integrity_card",
            "description": "Claimed complete price pairs that are actually negative sentinel placeholders.",
            "dataset": "headline",
            "sourceId": "price_integrity",
            "metrics": [
                {"label": "哨兵误计价对", "field": "false_complete_pairs", "format": "number"}
            ],
        },
        {
            "id": "forward_sessions",
            "description": "Contract-consistent post-registration session count available for candidate promotion.",
            "dataset": "headline",
            "sourceId": "parameter_forward",
            "metrics": [
                {"label": "冻结后前瞻会话", "field": "forward_sessions", "format": "number"}
            ],
        },
    ]

    charts = [
        {
            "id": "coverage_chart",
            "title": "RTH 原始行情与完整 0DTE C/P 分钟覆盖",
            "subtitle": "7/06–10 行情真实且稠密，但当时没有生产 FSM/intent，不能冒充生产信号回放",
            "intent": "trend",
            "question": "Which sessions have sufficient raw RTH market and same-day option-pair evidence?",
            "rationale": "Grouped daily bars separate underlier availability from executable option-pair availability.",
            "type": "bar",
            "dataset": "coverage_long",
            "sourceId": "data_coverage",
            "encodings": {
                "x": {"field": "date", "type": "nominal", "label": "日期"},
                "y": {"field": "minutes", "type": "quantitative", "format": "number", "label": "覆盖分钟"},
                "color": {"field": "layer", "type": "nominal", "label": "证据层"},
                "tooltip": [
                    {"field": "minutes", "type": "quantitative", "format": "number", "label": "分钟"},
                    {"field": "coverage_ratio", "type": "quantitative", "format": "percent", "label": "390m 覆盖"},
                ],
            },
            "valueFormat": "number",
            "layout": "full",
            "labels": {"values": "none"},
            "maxRows": len(coverage_long),
            "settings": {"categoryLabelPolicy": "wrap", "groupMode": "grouped", "showValues": False, "sort": "none"},
            "surface": {"surface": "export", "interactiveLegend": False, "showControls": False, "viewMode": "visualization"},
        },
        {
            "id": "report_action_chart",
            "title": "15 分钟报告的行动状态",
            "subtitle": "底层价格和双向候选多数存在，但用户可见报告没有一份正向 TradeReady",
            "intent": "comparison",
            "question": "How many delivered reports offered an affirmative action versus an explicit no-trade state?",
            "rationale": "Session-level grouped bars make the action-layer failure visible without conflating candidate maps with authorization.",
            "type": "bar",
            "dataset": "report_action_long",
            "sourceId": "report_audit",
            "encodings": {
                "x": {"field": "session", "type": "nominal", "label": "Session"},
                "y": {"field": "count", "type": "quantitative", "format": "number", "label": "报告数"},
                "color": {"field": "state", "type": "nominal", "label": "状态"},
                "tooltip": [{"field": "count", "type": "quantitative", "format": "number", "label": "报告数"}],
            },
            "valueFormat": "number",
            "layout": "full",
            "labels": {"values": "all"},
            "maxRows": len(report_action_long),
            "settings": {"categoryLabelPolicy": "wrap", "groupMode": "grouped", "showValues": True, "sort": "none"},
            "surface": {"surface": "export", "interactiveLegend": False, "showControls": False, "viewMode": "visualization"},
        },
        {
            "id": "report_direction_chart",
            "title": "RTH 报告 headline 后续 ES 方向表现",
            "subtitle": "15m 接近随机；60m 胜率虽 56.67%，均值却为 -0.83 点，显示尾部反转风险",
            "intent": "comparison",
            "question": "Does the visible RTH directional headline predict later ES movement?",
            "rationale": "Horizon bars show signed mean movement while tooltips retain sample size, hit rate and medians.",
            "type": "bar",
            "dataset": "report_direction_rth",
            "sourceId": "report_audit",
            "encodings": {
                "x": {"field": "horizon_label", "type": "nominal", "label": "后续窗口"},
                "y": {"field": "mean_signed_es_points", "type": "quantitative", "format": "number", "label": "方向签名 ES 均值（点）"},
                "tooltip": [
                    {"field": "n", "type": "quantitative", "format": "number", "label": "n"},
                    {"field": "hit_rate", "type": "quantitative", "format": "percent", "label": "方向胜率"},
                    {"field": "median_signed_es_points", "type": "quantitative", "format": "number", "label": "中位数"},
                ],
            },
            "valueFormat": "number",
            "layout": "full",
            "labels": {"values": "all"},
            "maxRows": 3,
            "settings": {"categoryLabelPolicy": "wrap", "groupMode": "single", "showValues": True, "sort": "none"},
            "surface": {"surface": "export", "interactiveLegend": False, "showControls": False, "viewMode": "visualization"},
        },
        {
            "id": "funnel_chart",
            "title": "正式 level 状态机漏斗",
            "subtitle": "RTH 有 11 个 Confirmed，不是“没有信号”；真正断裂发生在 formal→actionable",
            "intent": "comparison",
            "question": "How far did RTH and GTH lifecycles progress through the formal signal state machine?",
            "rationale": "Grouped stage counts expose where opportunities are lost before confirmation.",
            "type": "bar",
            "dataset": "signal_funnel",
            "sourceId": "formal_outcomes",
            "encodings": {
                "x": {"field": "stage", "type": "nominal", "label": "最深到达阶段"},
                "y": {"field": "count", "type": "quantitative", "format": "number", "label": "Lifecycle 数"},
                "color": {"field": "session", "type": "nominal", "label": "Session"},
                "tooltip": [
                    {"field": "stage_order", "type": "quantitative", "format": "number", "label": "顺序"},
                    {"field": "count", "type": "quantitative", "format": "number", "label": "数量"},
                ],
            },
            "valueFormat": "number",
            "layout": "full",
            "labels": {"values": "all"},
            "maxRows": len(signal_funnel),
            "settings": {"categoryLabelPolicy": "wrap", "groupMode": "grouped", "showValues": True, "sort": "none"},
            "surface": {"surface": "export", "interactiveLegend": False, "showControls": False, "viewMode": "visualization"},
        },
        {
            "id": "formal_horizon_chart",
            "title": "正式确认后的标的方向收益",
            "subtitle": "30–300 秒所有均值都很小，300 秒胜率 43.75%；现有样本没有稳健方向 edge",
            "intent": "comparison",
            "question": "Do formal confirmed signals predict the underlier direction at persisted horizons?",
            "rationale": "Horizon bars preserve the sign and magnitude of the event-level directional result.",
            "type": "bar",
            "dataset": "formal_horizons",
            "sourceId": "formal_outcomes",
            "encodings": {
                "x": {"field": "horizon_label", "type": "nominal", "label": "Horizon"},
                "y": {"field": "mean_directional_bps", "type": "quantitative", "format": "number", "label": "方向均值（bps）"},
                "tooltip": [
                    {"field": "n", "type": "quantitative", "format": "number", "label": "n"},
                    {"field": "hit_rate", "type": "quantitative", "format": "percent", "label": "方向胜率"},
                    {"field": "hit_ci_low", "type": "quantitative", "format": "percent", "label": "95% CI low"},
                    {"field": "hit_ci_high", "type": "quantitative", "format": "percent", "label": "95% CI high"},
                ],
            },
            "valueFormat": "number",
            "layout": "full",
            "labels": {"values": "all"},
            "maxRows": 4,
            "settings": {"categoryLabelPolicy": "wrap", "groupMode": "single", "showValues": True, "sort": "none"},
            "surface": {"surface": "export", "interactiveLegend": False, "showControls": False, "viewMode": "visualization"},
        },
        {
            "id": "price_integrity_chart",
            "title": "稠密价格覆盖：声称值与真实正价双边",
            "subtitle": "421 个 -1/-1 哨兵价对被计为完整，48 份报告错误宣称满覆盖",
            "intent": "comparison",
            "question": "How much of the claimed dense C/P coverage is backed by valid positive two-sided prices?",
            "rationale": "Direct count comparison quantifies the trust failure hidden by the dense-coverage label.",
            "type": "bar",
            "dataset": "price_integrity",
            "sourceId": "price_integrity",
            "encodings": {
                "x": {"field": "measure", "type": "nominal", "label": "覆盖口径"},
                "y": {"field": "pairs", "type": "quantitative", "format": "number", "label": "C/P 价对"},
                "tooltip": [{"field": "definition", "type": "nominal", "label": "定义"}],
            },
            "valueFormat": "number",
            "layout": "full",
            "labels": {"values": "all"},
            "maxRows": 3,
            "settings": {"categoryLabelPolicy": "wrap", "groupMode": "single", "showValues": True, "sort": "none"},
            "surface": {"surface": "export", "interactiveLegend": False, "showControls": False, "viewMode": "visualization"},
        },
        {
            "id": "parameter_chart",
            "title": "7/23 首个冻结后 RTH 会话的 follow-through 候选",
            "subtitle": "所有正数都来自同一个 gross/top-of-book 前瞻会话，不能据此晋级生产",
            "intent": "comparison",
            "question": "How did the current gate and frozen shadow candidates behave in the first post-registration session?",
            "rationale": "Candidate-level PnL bars make reactivity visible while the table preserves fills and the one-session limitation.",
            "type": "bar",
            "dataset": "parameter_forward",
            "sourceId": "parameter_forward",
            "encodings": {
                "x": {"field": "gate", "type": "nominal", "label": "Follow-through gate"},
                "y": {"field": "gross_pnl_usd", "type": "quantitative", "format": "currency", "label": "Gross PnL（1张）"},
                "tooltip": [
                    {"field": "fills", "type": "quantitative", "format": "number", "label": "fills"},
                    {"field": "semantic_touches", "type": "quantitative", "format": "number", "label": "semantic touches"},
                    {"field": "forward_sessions", "type": "quantitative", "format": "number", "label": "前瞻会话"},
                    {"field": "decision", "type": "nominal", "label": "结论"},
                ],
            },
            "valueFormat": "currency",
            "layout": "full",
            "labels": {"values": "all"},
            "maxRows": len(parameter_forward),
            "settings": {"categoryLabelPolicy": "wrap", "groupMode": "single", "showValues": True, "sort": "none"},
            "surface": {"surface": "export", "interactiveLegend": False, "showControls": False, "viewMode": "visualization"},
        },
    ]

    tables = [
        {
            "id": "coverage_table",
            "title": "证据层逐日覆盖",
            "subtitle": "Raw data availability and production audit availability are deliberately separated.",
            "dataset": "coverage_by_day",
            "sourceId": "data_coverage",
            "defaultSort": {"field": "date", "direction": "asc"},
            "density": "compact",
            "layout": "full",
            "columns": [
                {"field": "date", "label": "日期", "type": "text"},
                {"field": "live_minutes", "label": "SPX/ES/SPY live分钟", "format": "number"},
                {"field": "complete_cp_minutes", "label": "完整0DTE C/P分钟", "format": "number"},
                {"field": "median_cp_pairs", "label": "每分钟完整价对中位数", "format": "number"},
                {"field": "formal_layer", "label": "生产证据层", "type": "text"},
                {"field": "note", "label": "备注", "type": "text"},
            ],
        },
        {
            "id": "schedule_table",
            "title": "15 分钟报告生成与缺口",
            "subtitle": "All persisted reports show delivered_ok; missing slots are generation/schedule gaps.",
            "dataset": "report_schedule",
            "sourceId": "report_audit",
            "defaultSort": {"field": "date", "direction": "asc"},
            "density": "compact",
            "layout": "full",
            "columns": [
                {"field": "date", "label": "交易日", "type": "text"},
                {"field": "status_reports", "label": "状态报告", "format": "number"},
                {"field": "rth_reports", "label": "RTH报告", "format": "number"},
                {"field": "max_gap_minutes", "label": "最大生成缺口(分钟)", "format": "number"},
                {"field": "delivered_ok", "label": "audit delivered_ok", "format": "number"},
            ],
        },
        {
            "id": "report_session_table",
            "title": "报告内价格存在，但行动授权缺失",
            "subtitle": "Executable quote and two-sided scenario-map availability are not TradeReady.",
            "dataset": "report_session",
            "sourceId": "report_audit",
            "defaultSort": {"field": "session", "direction": "desc"},
            "density": "compact",
            "layout": "full",
            "columns": [
                {"field": "session", "label": "Session", "type": "text"},
                {"field": "reports", "label": "报告", "format": "number"},
                {"field": "explicit_no_trade", "label": "明确NO TRADE", "format": "number"},
                {"field": "positive_trade_ready", "label": "正向TradeReady", "format": "number"},
                {"field": "with_any_executable_quote", "label": "任一可执行quote", "format": "number"},
                {"field": "with_both_cp_candidates", "label": "C/P双向候选", "format": "number"},
                {"field": "with_both_cp_executable", "label": "C/P双边可执行", "format": "number"},
            ],
        },
        {
            "id": "replay_table",
            "title": "生产与 proxy 回放不可混算",
            "subtitle": "Every PnL figure is one-contract gross top-of-book replay.",
            "dataset": "replay_cohorts",
            "sourceId": "strict_backtest",
            "defaultSort": {"field": "opportunities", "direction": "desc"},
            "density": "compact",
            "layout": "full",
            "columns": [
                {"field": "cohort", "label": "Cohort", "type": "text"},
                {"field": "role", "label": "证据角色", "type": "text"},
                {"field": "opportunities", "label": "机会", "format": "number"},
                {"field": "fills", "label": "fills", "format": "number"},
                {"field": "win_rate", "label": "胜率", "format": "percent"},
                {"field": "gross_pnl_usd", "label": "Gross PnL", "format": "currency"},
                {"field": "decision", "label": "结论", "type": "text"},
            ],
        },
        {
            "id": "parameter_table",
            "title": "冻结后参数前瞻：只保留 shadow",
            "subtitle": "One forward session cannot pass promotion readiness.",
            "dataset": "parameter_forward",
            "sourceId": "parameter_forward",
            "defaultSort": {"field": "gross_pnl_usd", "direction": "desc"},
            "density": "compact",
            "layout": "full",
            "columns": [
                {"field": "gate", "label": "Gate", "type": "text"},
                {"field": "role", "label": "角色", "type": "text"},
                {"field": "semantic_touches", "label": "touches", "format": "number"},
                {"field": "fills", "label": "fills", "format": "number"},
                {"field": "gross_pnl_usd", "label": "Gross PnL", "format": "currency"},
                {"field": "forward_sessions", "label": "前瞻会话", "format": "number"},
                {"field": "decision", "label": "结论", "type": "text"},
            ],
        },
        {
            "id": "recommendation_table",
            "title": "修复与验证顺序",
            "subtitle": "Parameter loosening is deliberately excluded until the evidence contract is trustworthy.",
            "dataset": "recommendations",
            "sourceId": "companion_notebook",
            "defaultSort": {"field": "priority", "direction": "asc"},
            "density": "spacious",
            "layout": "full",
            "columns": [
                {"field": "priority", "label": "#", "format": "number"},
                {"field": "workstream", "label": "工作流", "type": "text"},
                {"field": "change", "label": "建议", "type": "text"},
                {"field": "evidence", "label": "证据", "type": "text"},
                {"field": "validation", "label": "验收", "type": "text"},
                {"field": "parameter_change", "label": "改生产参数", "type": "text"},
            ],
        },
    ]

    blocks = [
        {"id": "title", "type": "markdown", "layout": "full", "body": "# SPX 三周信号与 15 分钟报告诊断：截至 2026-07-23"},
        {
            "id": "executive_summary",
            "type": "markdown",
            "layout": "full",
            "body": (
                "## Executive Summary\n\n"
                "**你的判断是对的：现有 15 分钟报告几乎没有行动指导价值，但不是因为 RTH 没行情、也不是底层完全没信号。** "
                "三周窗口有 14 个可用 raw RTH 交易日；audit-equivalent 生产历史却从 7/13 才开始，完整 intent/outcome 会话只有 8 个。"
                "521 份状态报告中正向 `TradeReady` 为 0；RTH 129 份中 127 份明确 `NO TRADE`，而底层实际有 11 个 RTH formal confirmations。\n\n"
                "断裂点有四个：formal 信号未锁存进下一份报告；formal→actionable 门控始终未通过；timer 只覆盖 RTH 前半段；"
                "报告用双向 C/P 情景地图和高频翻向 headline 替代唯一行动状态。RTH headline 15 分钟后方向胜率仅 52.70%，"
                "相邻报告翻向 28/85。稠密价格标签也不可信：421 个 `-1/-1` 哨兵价对被误计为完整双边。\n\n"
                "**现在不应把生产 follow-through 调松。** 生产终端意图总共只有 4 个。候选参数在 7/23 首个冻结后会话虽为正，"
                "但只有 1 个 forward session，且全部是未计成本的 top-of-book gross replay。优先级应是修证据链、报告行动层和全 RTH cadence；"
                "参数只保留 shadow，等至少 20 个合约一致完整 RTH sessions 再晋级。"
            ),
        },
        {"id": "headline_metrics", "type": "metric-strip", "layout": "full", "cardIds": [card["id"] for card in cards]},
        {
            "id": "coverage_answer",
            "type": "markdown",
            "layout": "full",
            "sourceId": "data_coverage",
            "body": (
                "## 这不是三周生产信号历史\n\n"
                "7/03 是观察假日；7/06–10 已有真实、相当稠密的 RTH underlier 与 SPXW 数据，不能说“没数据”。"
                "但当时没有 `level_decision_audit`、`trade_intent` 和正式 outcome，所以只能用当前代码重算市场状态，"
                "不能回填为当时生产信号。最强诚实窗口是 raw replay 7/06–23、production FSM 7/13–23、intent/outcome 7/14–23。"
            ),
        },
        {"id": "coverage_chart_block", "type": "chart", "layout": "full", "chartId": "coverage_chart"},
        {"id": "coverage_table_block", "type": "table", "layout": "full", "tableId": "coverage_table"},
        {
            "id": "report_failure_answer",
            "type": "markdown",
            "layout": "full",
            "sourceId": "report_audit",
            "body": (
                "## 为什么 15 分钟报告没有指导作用\n\n"
                "报告是“条件地图/状态面板”，不是可执行信号面板。RTH 129 份全部有价格、EM、墙梯和双向候选，"
                "122 份至少有一个 executable quote；但 127 份明确 NO TRADE，正向 TradeReady 为 0。"
                "已生成报告的投递总体成功，主要缺陷是未生成、未覆盖和未承接瞬态 formal 事件。"
            ),
        },
        {"id": "report_action_chart_block", "type": "chart", "layout": "full", "chartId": "report_action_chart"},
        {"id": "report_session_table_block", "type": "table", "layout": "full", "tableId": "report_session_table"},
        {
            "id": "cadence_answer",
            "type": "markdown",
            "layout": "full",
            "sourceId": "report_audit",
            "body": (
                "### 时段截断与生成缺口\n\n"
                "7/14–23 八个完整 RTH 理论应有 208 个 15 分钟槽，实际只有 112 个（53.85%）。"
                "timer 在北京时间 01:30 左右结束，相当于 ET 13:30，最后 2.5 小时天然不进入报告；"
                "7/21 还出现 410.12 分钟生成缺口。521/521 持久化记录均标 delivered_ok，问题首先是 cadence 和生成层。"
            ),
        },
        {"id": "schedule_table_block", "type": "table", "layout": "full", "tableId": "schedule_table"},
        {
            "id": "headline_answer",
            "type": "markdown",
            "layout": "full",
            "sourceId": "report_audit",
            "body": (
                "### Headline 不是 edge\n\n"
                "用户可见 RTH 方向 headline 的 15/30/60 分钟表现分别为 n=74 / 71 / 60，"
                "胜率 52.70% / 56.34% / 56.67%，均值 +0.20 / +0.99 / -0.83 ES 点。"
                "同时相邻 headline 28/85 次翻向（32.94%）。这就是“每 15 分钟改口，但永远 NO TRADE”的体验。"
            ),
        },
        {"id": "report_direction_chart_block", "type": "chart", "layout": "full", "chartId": "report_direction_chart"},
        {
            "id": "price_integrity_answer",
            "type": "markdown",
            "layout": "full",
            "sourceId": "price_integrity",
            "body": (
                "### 稠密不等于可执行\n\n"
                "116 份带稠密覆盖合同的报告共声称 6,212 个完整 C/P 价对；按 `bid>=0, ask>0, ask>=bid` 重算只有 5,791 个。"
                "421 个负价哨兵被误计，48 份报告错误宣称满覆盖。NBBO 不应插值；结构/IV 平滑可以保留，但必须与可执行报价覆盖分栏。"
            ),
        },
        {"id": "price_integrity_chart_block", "type": "chart", "layout": "full", "chartId": "price_integrity_chart"},
        {
            "id": "formal_answer",
            "type": "markdown",
            "layout": "full",
            "sourceId": "formal_outcomes",
            "body": (
                "## 底层信号有产出，但尚无稳健 edge\n\n"
                "32 个 formal confirmations 中 RTH 11、GTH 21。300 秒方向均值 +0.43 bps、胜率 43.75%（95% Wilson 28.17%–60.67%）；"
                "RTH 子样本 n=11，均值 +0.21 bps、胜率 54.55%。样本和区间都不支持按星期、Call/Put 或 breakout/fade 上线新 gate。"
            ),
        },
        {"id": "funnel_chart_block", "type": "chart", "layout": "full", "chartId": "funnel_chart"},
        {"id": "formal_horizon_chart_block", "type": "chart", "layout": "full", "chartId": "formal_horizon_chart"},
        {
            "id": "replay_answer",
            "type": "markdown",
            "layout": "full",
            "sourceId": "strict_backtest",
            "body": (
                "### 回放结果不能混算\n\n"
                "Formal confirmed 裸单 control replay 21 fills，gross -$445；GTH dip proxy 3 fills，-$440。"
                "严格生产只有 4 个 unique terminal intents、2 fills、2 skips，gross +$780。"
                "生产 n=4 的正数不能证明策略盈利，proxy 更不能并入 production PnL。"
            ),
        },
        {"id": "replay_table_block", "type": "table", "layout": "full", "tableId": "replay_table"},
        {
            "id": "parameter_answer",
            "type": "markdown",
            "layout": "full",
            "sourceId": "parameter_forward",
            "body": (
                "## 参数结论：不改生产，只扩 shadow\n\n"
                "当前 `15s / 2.00pt / 5.00% EM` 在 7/23 的 29 个 RTH semantic touches 上 0 通过。"
                "冻结 shadow 候选在同一会话出现 2–6 fills、gross +$160 至 +$380；"
                "但这是唯一一个 post-registration session。此前 245 组样本内冠军在 expanding tail 首日为 -$360，"
                "说明用小样本最优直接上线会重复选择偏差。"
            ),
        },
        {"id": "parameter_chart_block", "type": "chart", "layout": "full", "chartId": "parameter_chart"},
        {"id": "parameter_table_block", "type": "table", "layout": "full", "tableId": "parameter_table"},
        {
            "id": "next_steps",
            "type": "markdown",
            "layout": "full",
            "body": (
                "## Recommended Next Steps\n\n"
                "先修可信度与行动链，再收集参数证据。下面的前五项不放松风险门控，也不自动启用下单。"
                "完成后，每份 15 分钟报告应能回答三个问题：现在能不能做、若不能是哪一门、上一个 formal 信号后来怎样。"
            ),
        },
        {"id": "recommendation_table_block", "type": "table", "layout": "full", "tableId": "recommendation_table"},
        {
            "id": "further_questions",
            "type": "markdown",
            "layout": "full",
            "body": (
                "## Further Questions\n\n"
                "- 最近约 $12,000 的亏损分别对应哪些 broker fills、数量、手续费和退出？本地 statement 仍只覆盖到 7/16。\n"
                "- 你希望 15 分钟报告是“执行指令面板”，还是保留一个独立的“结构研究面板”？两者继续混在一起会重复当前误读。\n"
                "- 全 RTH 报告是否应覆盖 09:30–16:00 ET，还是只保留开盘到 13:30 ET 但显式标注 coverage end？"
            ),
        },
        {
            "id": "caveats",
            "type": "markdown",
            "layout": "full",
            "body": (
                "## Caveats\n\n"
                "- 回放未计 commission、显式 slippage、queue position、partial fill、market impact 和人工延迟；所有 PnL 都是偏乐观 gross。\n"
                "- QuoteStore 已按 `received_at` knowledge-time 回放，并拒绝非实时、未来时间戳及 source age 超过 30 秒的报价；历史 PnL 仍未计手续费、滑点、排队、部分成交与冲击。\n"
                "- 7/06–10 缺少当时生产 FSM/decision context；只能重算，不可冒充原样生产回放。\n"
                "- 报告方向样本彼此重叠且序列相关；Wilson 区间只描述事件行，不等于独立交易样本。\n"
                "- 真实账户亏损尚未完成 broker fill/fee reconciliation，本报告不能解释实际净 PnL。"
            ),
        },
        {
            "id": "reproducibility",
            "type": "markdown",
            "layout": "full",
            "sourceId": "companion_notebook",
            "body": (
                "## Reproducibility\n\n"
                "伴随 notebook 已从仓库根目录 top-to-bottom 执行，锁定 521 份报告、报告方向联结、32 个 formal outcomes、"
                "421 个哨兵误计价对和固定 cutoff 生产回放。若源数据或分类合同漂移，断言会失败。"
            ),
        },
    ]

    artifact = {
        "surface": "report",
        "manifest": {
            "version": 1,
            "surface": "report",
            "title": "SPX 三周信号与 15 分钟报告诊断：截至 2026-07-23",
            "description": "分层审计 7/03–23 原始行情、生产 FSM、15 分钟报告行动价值、正式信号、严格回放与冻结后参数前瞻。",
            "generatedAt": generated_at,
            "sources": sources,
            "cards": cards,
            "charts": charts,
            "tables": tables,
            "blocks": blocks,
        },
        "snapshot": {
            "version": 1,
            "generatedAt": generated_at,
            "status": "partial",
            "accessIssues": [
                {
                    "id": "missing_broker_loss_window",
                    "scope": "actual_loss_reconciliation",
                    "sourceId": "broker_reconciliation",
                    "dataset": "broker_loss_reconciliation",
                    "message": "本地 broker Activity/Flex 仍只覆盖到 2026-07-16；最近约 $12,000 的真实亏损无法逐笔归因。",
                },
                {
                    "id": "production_history_starts_20260713",
                    "scope": "three_week_production_backtest",
                    "sourceId": "production_history_boundary",
                    "dataset": "production_history_gap",
                    "message": "7/03–7/12 缺少 audit-equivalent 生产 FSM/intent；不能把请求窗口称为三周生产信号回测。",
                },
            ],
            "datasets": {
                "headline": headline,
                "coverage_by_day": coverage_by_day,
                "coverage_long": coverage_long,
                "report_schedule": schedule,
                "report_session": report_session,
                "report_action_long": report_action_long,
                "report_direction_all": direction,
                "report_direction_rth": report_direction_rth,
                "headline_flips": flips,
                "price_integrity": price_integrity_rows,
                "formal_horizons": horizon_rows,
                "formal_session_slices": formal_session_slices,
                "signal_funnel": signal_funnel,
                "replay_cohorts": replay_cohorts,
                "parameter_forward": parameter_forward,
                "recommendations": recommendations,
                "writer_counts": [
                    {"writer": key, "reports": value}
                    for key, value in sorted(writer_counts.items())
                ],
                "broker_loss_reconciliation": [],
                "production_history_gap": [],
            },
        },
    }
    ARTIFACT_PATH.write_text(
        json.dumps(artifact, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def main() -> int:
    build_notebook()
    build_artifact()
    print(NOTEBOOK_PATH)
    print(ARTIFACT_PATH)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
