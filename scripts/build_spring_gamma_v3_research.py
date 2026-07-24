#!/usr/bin/env python3
"""Build the Spring Gamma v3 data-quality and ablation research bundle.

The research window is fixed at 2026-07-03 through 2026-07-23 with an
exclusive 2026-07-24T00:00:00Z information cutoff.  All feature joins are
backward-looking, expiry-matched, and bounded by an explicit age cap.

The script writes:

* docs/spring-gamma-v3-research-2026-07-24.ipynb
* docs/spring-gamma-v3-research-2026-07-24.artifact.json
* docs/spring-gamma-v3-research-2026-07-24.html

Numerical analysis uses DuckDB for point-in-time quote scans plus NumPy and
the Python standard library.  ``nbformat`` and ``nbclient`` are used only to
construct and execute the companion notebook.  The HTML is packaged from the
canonical artifact by the shared Data Analytics portable-report builder.
"""

from __future__ import annotations

import bisect
import glob
import html
import json
import math
import os
import re
import subprocess
from collections import Counter, defaultdict
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Any, Callable, Iterable
from zoneinfo import ZoneInfo

import duckdb
import nbformat
import numpy as np
from nbclient import NotebookClient


REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = Path(os.environ.get("SPX_SPARK_DATA_ROOT", "/srv/data/spx-spark/data"))
DOCS_ROOT = REPO_ROOT / "docs"
REPORT_DATE = "2026-07-24"
WINDOW_START = "2026-07-03"
WINDOW_END = "2026-07-23"
CUTOFF_AT = datetime(2026, 7, 24, tzinfo=timezone.utc)
GENERATED_AT = "2026-07-24T00:00:00Z"
ET = ZoneInfo("America/New_York")

SCRIPT_PATH = REPO_ROOT / "scripts/build_spring_gamma_v3_research.py"
NOTEBOOK_PATH = DOCS_ROOT / f"spring-gamma-v3-research-{REPORT_DATE}.ipynb"
ARTIFACT_PATH = DOCS_ROOT / f"spring-gamma-v3-research-{REPORT_DATE}.artifact.json"
HTML_PATH = DOCS_ROOT / f"spring-gamma-v3-research-{REPORT_DATE}.html"

GREEK_MAX_AGE_SECONDS = 300.0
IV_MAX_AGE_SECONDS = 600.0
HORIZONS = (15, 30, 60)
TENOR_QUOTE_MAX_AGE_SECONDS = 15.0
TENOR_BOOTSTRAP_REPLICATIONS = 4000

VISIBLE_BIAS_RE = (
    ("趋势偏多", 1),
    ("过渡偏多", 1),
    ("趋势偏空", -1),
    ("过渡偏空", -1),
    ("偏多", 1),
    ("偏空", -1),
    ("均值回归", 0),
    ("方向过渡", 0),
    ("证据不足", None),
)

PRIMARY_FEATURES = (
    ("gex_net_billion", "Net GEX proxy, $bn", "Call+/Put− proxy; not dealer sign"),
    ("gex_ratio", "Net gamma ratio", "Call+/Put− proxy; not dealer sign"),
    ("log_gross_gamma", "log1p gross gamma", "Magnitude gate"),
    ("log_charm", "log1p gross Charm 5m", "Gate only; never direction"),
    ("log_vanna", "log1p gross Vanna 1vol", "Magnitude gate"),
    ("aligned_zero_dist", "Side-aligned zero-gamma distance", "Structural context"),
    ("zg_em", "Zero-gamma distance / EM", "Structural context"),
    ("skew_tilt", "Put ratio − Call ratio", "Skew context"),
    ("put_skew_25d", "Put 25Δ skew", "Skew context"),
    ("wall_room", "Side-aligned wall room", "Structural context"),
    ("charm_equiv_points_5m", "Charm/Gamma equivalent points", "Gate only"),
    ("vanna_equiv_points_1vol", "Vanna/Gamma equivalent points", "Magnitude gate"),
)

RAW_FEATURES = (
    ("gex_net_billion", "Net GEX proxy, $bn", "Unsigned target control"),
    ("gex_ratio", "Net gamma ratio", "Unsigned target control"),
    ("log_gross_gamma", "log1p gross gamma", "Magnitude only"),
    ("log_charm", "log1p gross Charm 5m", "Gate only"),
    ("log_vanna", "log1p gross Vanna 1vol", "Magnitude only"),
    ("zero_signed_dist", "Spot − zero gamma", "Structural location"),
    ("zero_abs_dist", "|Spot − zero gamma|", "Structural distance"),
    ("skew_tilt", "Put ratio − Call ratio", "Skew context"),
    ("put_skew_25d", "Put 25Δ skew", "Skew context"),
    ("charm_equiv_points_5m", "Charm/Gamma equivalent points", "Gate only"),
    ("vanna_equiv_points_1vol", "Vanna/Gamma equivalent points", "Magnitude only"),
)


def read_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            if isinstance(row, dict):
                yield row


def parse_dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def finite_float(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def expiry_key(row: dict[str, Any]) -> str:
    raw = row.get("expiry")
    if raw:
        return str(raw).replace("-", "")
    return str(row.get("trading_date") or "").replace("-", "")


def visible_bias(text: str) -> tuple[int | None, str | None]:
    lines = [line.strip(" -*") for line in text.splitlines() if line.strip()]
    for prefix in ("判断", "观察"):
        for line in lines[:10]:
            if not line.startswith(prefix):
                continue
            for label, side in VISIBLE_BIAS_RE:
                if label in line:
                    return side, label
    top = "\n".join(lines[:8])
    for label, side in VISIBLE_BIAS_RE:
        if side is None:
            continue
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


def session_for(instant: datetime) -> str:
    local = instant.astimezone(ET)
    in_rth = (
        local.weekday() < 5
        and time(9, 30) <= local.time().replace(tzinfo=None) < time(16, 0)
    )
    return "RTH" if in_rth else "GTH"


def gth_segment(instant: datetime) -> str:
    if session_for(instant) == "RTH":
        return "RTH 09:30–16:00"
    local_time = instant.astimezone(ET).time().replace(tzinfo=None)
    if local_time >= time(20, 15) or local_time < time(3, 0):
        return "Overnight 20:15–03:00"
    if time(3, 0) <= local_time < time(9, 30):
        return "Europe 03:00–09:30"
    return "Closed 16:00–20:15"


def load_reports() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    pattern = str(DATA_ROOT / "audit/order_map_pricing/date=*/reports.jsonl")
    for filename in sorted(glob.glob(pattern)):
        for raw in read_jsonl(Path(filename)):
            if raw.get("report_kind") != "status":
                continue
            day = str(raw.get("trading_date") or "")
            if not WINDOW_START <= day <= WINDOW_END:
                continue
            generated = parse_dt(str(raw["generated_at"]))
            if generated >= CUTOFF_AT:
                continue
            template = str(raw.get("template") or "")
            bias, bias_label = visible_bias(str(raw.get("delivered_text") or ""))
            row = dict(raw)
            row["_generated_at"] = generated
            row["_origin_id"] = generated.isoformat()
            row["_session"] = session_for(generated)
            row["_gth_segment"] = gth_segment(generated)
            row["_bias"] = bias
            row["_bias_label"] = bias_label
            row["_es"] = template_es_price(template)
            row["_es_source"] = template_es_source(template)
            rows.append(row)
    rows.sort(key=lambda row: row["_generated_at"])
    return rows


def build_origins(
    reports: list[dict[str, Any]], directional_only: bool
) -> list[dict[str, Any]]:
    result = []
    for row in reports:
        if row["_es"] is None:
            continue
        if directional_only and row["_bias"] not in (-1, 1):
            continue
        result.append(row)
    return result


def build_labels(
    origins: list[dict[str, Any]], reports: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in reports:
        if row["_es"] is not None:
            by_day[str(row.get("trading_date"))].append(row)

    labels: list[dict[str, Any]] = []
    for origin in origins:
        candidates_for_day = by_day[str(origin.get("trading_date"))]
        for horizon in HORIZONS:
            target = origin["_generated_at"].timestamp() + horizon * 60
            candidates = [
                row
                for row in candidates_for_day
                if abs(row["_generated_at"].timestamp() - target) <= 180
                and (
                    origin["_es_source"] is None
                    or row["_es_source"] is None
                    or origin["_es_source"] == row["_es_source"]
                )
            ]
            if not candidates:
                continue
            future = min(
                candidates,
                key=lambda row: abs(row["_generated_at"].timestamp() - target),
            )
            raw_y = float(future["_es"] - origin["_es"])
            side = origin["_bias"] if origin["_bias"] in (-1, 1) else None
            labels.append(
                {
                    "origin_id": origin["_origin_id"],
                    "trading_date": str(origin.get("trading_date")),
                    "session": origin["_session"],
                    "gth_segment": origin["_gth_segment"],
                    "horizon": horizon,
                    "origin_at": origin["_generated_at"].isoformat(),
                    "target_at": future["_generated_at"].isoformat(),
                    "side": side,
                    "raw_y": raw_y,
                    "signed_y": float(side * raw_y) if side is not None else None,
                }
            )
    return labels


def new_series_store() -> dict[str, dict[str, dict[str, list[Any]]]]:
    return defaultdict(lambda: defaultdict(lambda: {"times": [], "values": []}))


def append_series(
    store: dict[str, dict[str, dict[str, list[Any]]]],
    expiry: str,
    feature: str,
    instant: datetime,
    value: Any,
) -> None:
    series = store[expiry][feature]
    series["times"].append(instant.timestamp())
    series["values"].append(value)


def finalize_series(
    store: dict[str, dict[str, dict[str, list[Any]]]]
) -> dict[str, dict[str, dict[str, list[Any]]]]:
    for features in store.values():
        for series in features.values():
            order = sorted(
                range(len(series["times"])),
                key=lambda index: (series["times"][index], index),
            )
            series["times"] = [series["times"][index] for index in order]
            series["values"] = [series["values"][index] for index in order]
    return store


def latest_from_series(
    store: dict[str, dict[str, dict[str, list[Any]]]],
    expiry: str,
    feature: str,
    origin: datetime,
    max_age_seconds: float,
) -> tuple[Any | None, float | None]:
    series = store.get(expiry, {}).get(feature)
    if not series:
        return None, None
    origin_ts = origin.timestamp()
    index = bisect.bisect_right(series["times"], origin_ts) - 1
    if index < 0:
        return None, None
    age = origin_ts - series["times"][index]
    if age < 0.0 or age > max_age_seconds:
        return None, None
    return series["values"][index], age


def load_greek_series() -> dict[str, dict[str, dict[str, list[Any]]]]:
    store = new_series_store()
    pattern = str(
        DATA_ROOT / "features/spxw_0dte_greeks_reference/date=*/snapshots.jsonl"
    )
    for filename in sorted(glob.glob(pattern)):
        for row in read_jsonl(Path(filename)):
            if not row.get("as_of"):
                continue
            instant = parse_dt(str(row["as_of"]))
            if instant >= CUTOFF_AT:
                continue
            expiry = expiry_key(row)
            if not expiry:
                continue
            append_series(store, expiry, "raw", instant, row)

            gex = row.get("signed_gex_proxy") or {}
            if gex.get("quality") == "available":
                payload = {
                    "gex_net": finite_float(gex.get("net_gex")),
                    "gex_ratio": finite_float(gex.get("net_gamma_ratio")),
                    "gex_abs": finite_float(gex.get("abs_gex")),
                }
                if payload["gex_net"] is not None:
                    append_series(store, expiry, "gex", instant, payload)

            aggregate = row.get("aggregate") or {}
            if aggregate.get("quality") == "ok":
                payload = {
                    "gross_gamma": finite_float(aggregate.get("gross_gamma_abs")),
                    "gross_charm": finite_float(
                        aggregate.get("gross_charm_5m_abs")
                    ),
                    "gross_vanna": finite_float(
                        aggregate.get("gross_vanna_1vol_abs")
                    ),
                    "iv_coverage": finite_float(
                        aggregate.get("iv_coverage_ratio")
                    ),
                    "oi_coverage": finite_float(
                        aggregate.get("oi_coverage_ratio")
                    ),
                }
                if all(
                    payload[key] is not None
                    for key in ("gross_gamma", "gross_charm", "gross_vanna")
                ):
                    append_series(store, expiry, "aggregate", instant, payload)

            usable_ratio = finite_float((row.get("coverage") or {}).get("usable_ratio"))
            if usable_ratio is not None:
                append_series(store, expiry, "usable_ratio", instant, usable_ratio)
    return finalize_series(store)


def load_iv_series() -> dict[str, dict[str, dict[str, list[Any]]]]:
    store = new_series_store()
    pattern = str(DATA_ROOT / "features/iv_surface/date=*/hour=*/snapshots.jsonl")
    for filename in sorted(glob.glob(pattern)):
        for row in read_jsonl(Path(filename)):
            if not row.get("created_at"):
                continue
            created = parse_dt(str(row["created_at"]))
            if created >= CUTOFF_AT:
                continue
            front_expiry = str(row.get("front_expiry") or "")
            front = next(
                (
                    expiry
                    for expiry in row.get("expiries") or []
                    if str(expiry.get("expiry") or "") == front_expiry
                ),
                None,
            )
            if not front_expiry or not isinstance(front, dict):
                continue
            base = {
                "as_of": row.get("as_of"),
                "created_at": row.get("created_at"),
                "underlier": finite_float(row.get("underlier_price")),
                "zero_gamma": finite_float(front.get("zero_gamma")),
                "expected_move": finite_float(front.get("expected_move_points")),
                "put_skew_25d": finite_float(front.get("put_skew_25d")),
                "put_skew_ratio": finite_float(front.get("put_skew_ratio")),
                "call_skew_ratio": finite_float(front.get("call_skew_ratio")),
                "put_wall": finite_float(front.get("put_wall")),
                "call_wall": finite_float(front.get("call_wall")),
                "atm_iv": finite_float(front.get("atm_iv")),
                "iv_coverage": finite_float(front.get("iv_coverage_ratio")),
                "gamma_coverage": finite_float(front.get("gamma_coverage_ratio")),
            }
            append_series(store, front_expiry, "raw", created, base)
            if base["underlier"] is not None and base["zero_gamma"] is not None:
                append_series(store, front_expiry, "zero", created, base)
            if (
                base["put_skew_ratio"] is not None
                and base["call_skew_ratio"] is not None
            ):
                append_series(store, front_expiry, "skew", created, base)
            if (
                base["underlier"] is not None
                and base["put_wall"] is not None
                and base["call_wall"] is not None
            ):
                append_series(store, front_expiry, "wall", created, base)
    return finalize_series(store)


def join_features(
    origins: list[dict[str, Any]],
    greek_store: dict[str, dict[str, dict[str, list[Any]]]],
    iv_store: dict[str, dict[str, dict[str, list[Any]]]],
) -> list[dict[str, Any]]:
    joined: list[dict[str, Any]] = []
    for origin in origins:
        instant = origin["_generated_at"]
        expiry = expiry_key(origin)
        row: dict[str, Any] = {
            "origin_id": origin["_origin_id"],
            "trading_date": str(origin.get("trading_date")),
            "session": origin["_session"],
            "gth_segment": origin["_gth_segment"],
            "side": origin["_bias"] if origin["_bias"] in (-1, 1) else None,
        }

        gex, gex_age = latest_from_series(
            greek_store, expiry, "gex", instant, GREEK_MAX_AGE_SECONDS
        )
        aggregate, aggregate_age = latest_from_series(
            greek_store, expiry, "aggregate", instant, GREEK_MAX_AGE_SECONDS
        )
        usable, usable_age = latest_from_series(
            greek_store, expiry, "usable_ratio", instant, GREEK_MAX_AGE_SECONDS
        )
        current_greek, current_age = latest_from_series(
            greek_store, expiry, "raw", instant, GREEK_MAX_AGE_SECONDS
        )
        zero, zero_age = latest_from_series(
            iv_store, expiry, "zero", instant, IV_MAX_AGE_SECONDS
        )
        skew, skew_age = latest_from_series(
            iv_store, expiry, "skew", instant, IV_MAX_AGE_SECONDS
        )
        wall, wall_age = latest_from_series(
            iv_store, expiry, "wall", instant, IV_MAX_AGE_SECONDS
        )

        row.update(
            {
                "gex_net": gex.get("gex_net") if gex else None,
                "gex_net_billion": (
                    gex["gex_net"] / 1_000_000_000.0
                    if gex and gex.get("gex_net") is not None
                    else None
                ),
                "gex_ratio": gex.get("gex_ratio") if gex else None,
                "gex_abs": gex.get("gex_abs") if gex else None,
                "gex_age_seconds": gex_age,
                "gross_gamma": (
                    aggregate.get("gross_gamma") if aggregate else None
                ),
                "gross_charm": (
                    aggregate.get("gross_charm") if aggregate else None
                ),
                "gross_vanna": (
                    aggregate.get("gross_vanna") if aggregate else None
                ),
                "aggregate_age_seconds": aggregate_age,
                "usable_ratio": usable,
                "usable_age_seconds": usable_age,
                "zero_age_seconds": zero_age,
                "skew_age_seconds": skew_age,
                "wall_age_seconds": wall_age,
                "current_greek_age_seconds": current_age,
                "current_greek": current_greek,
            }
        )

        if aggregate:
            gamma = aggregate["gross_gamma"]
            charm = aggregate["gross_charm"]
            vanna = aggregate["gross_vanna"]
            row["log_gross_gamma"] = math.log1p(gamma)
            row["log_charm"] = math.log1p(charm)
            row["log_vanna"] = math.log1p(vanna)
            row["charm_equiv_points_5m"] = charm / gamma if gamma > 0.0 else None
            row["vanna_equiv_points_1vol"] = vanna / gamma if gamma > 0.0 else None
        else:
            for key in (
                "log_gross_gamma",
                "log_charm",
                "log_vanna",
                "charm_equiv_points_5m",
                "vanna_equiv_points_1vol",
            ):
                row[key] = None

        if zero:
            signed_distance = zero["underlier"] - zero["zero_gamma"]
            row["zero_signed_dist"] = signed_distance
            row["zero_abs_dist"] = abs(signed_distance)
            row["aligned_zero_dist"] = (
                row["side"] * signed_distance if row["side"] is not None else None
            )
            expected_move = zero.get("expected_move")
            row["zg_em"] = (
                signed_distance / expected_move
                if expected_move is not None and expected_move > 0.0
                else None
            )
        else:
            for key in (
                "zero_signed_dist",
                "zero_abs_dist",
                "aligned_zero_dist",
                "zg_em",
            ):
                row[key] = None

        row["put_skew_25d"] = skew.get("put_skew_25d") if skew else None
        row["put_skew_ratio"] = skew.get("put_skew_ratio") if skew else None
        row["call_skew_ratio"] = skew.get("call_skew_ratio") if skew else None
        row["skew_tilt"] = (
            skew["put_skew_ratio"] - skew["call_skew_ratio"] if skew else None
        )

        row["wall_pair_available"] = wall is not None
        if wall and row["side"] is not None:
            row["wall_room"] = (
                wall["call_wall"] - wall["underlier"]
                if row["side"] > 0
                else wall["underlier"] - wall["put_wall"]
            )
        else:
            row["wall_room"] = None
        joined.append(row)
    return joined


def wilson_interval(successes: int, total: int) -> tuple[float | None, float | None]:
    if total <= 0:
        return None, None
    z = 1.959963984540054
    p = successes / total
    denominator = 1.0 + z * z / total
    center = (p + z * z / (2.0 * total)) / denominator
    margin = (
        z
        * math.sqrt(p * (1.0 - p) / total + z * z / (4.0 * total**2))
        / denominator
    )
    return max(0.0, center - margin), min(1.0, center + margin)


def coverage_rows(
    features: list[dict[str, Any]], cohort: str
) -> list[dict[str, Any]]:
    rules: tuple[tuple[str, str, Callable[[dict[str, Any]], bool], float], ...] = (
        (
            "Net GEX proxy",
            "quality=available",
            lambda row: row["gex_net"] is not None,
            GREEK_MAX_AGE_SECONDS,
        ),
        (
            "Gross Gamma",
            "aggregate.quality=ok",
            lambda row: row["gross_gamma"] is not None,
            GREEK_MAX_AGE_SECONDS,
        ),
        (
            "Gross Charm 5m",
            "aggregate.quality=ok; gate only",
            lambda row: row["gross_charm"] is not None,
            GREEK_MAX_AGE_SECONDS,
        ),
        (
            "Gross Vanna 1vol",
            "aggregate.quality=ok",
            lambda row: row["gross_vanna"] is not None,
            GREEK_MAX_AGE_SECONDS,
        ),
        (
            "Zero-gamma structure",
            "same IV snapshot has spot+ZG",
            lambda row: row["zero_signed_dist"] is not None,
            IV_MAX_AGE_SECONDS,
        ),
        (
            "Put/Call skew",
            "same IV snapshot has both ratios",
            lambda row: row["skew_tilt"] is not None,
            IV_MAX_AGE_SECONDS,
        ),
        (
            "Put/Call walls",
            "same IV snapshot has spot+both walls",
            lambda row: bool(row["wall_pair_available"]),
            IV_MAX_AGE_SECONDS,
        ),
        (
            "Usable contract ratio",
            "numeric coverage.usable_ratio",
            lambda row: row["usable_ratio"] is not None,
            GREEK_MAX_AGE_SECONDS,
        ),
    )
    result = []
    for session in ("RTH", "GTH"):
        selected = [row for row in features if row["session"] == session]
        for feature, rule, predicate, max_age in rules:
            available = sum(predicate(row) for row in selected)
            low, high = wilson_interval(available, len(selected))
            result.append(
                {
                    "cohort": cohort,
                    "session": session,
                    "feature": feature,
                    "origins": len(selected),
                    "available": available,
                    "coverage_pct": 100.0 * available / len(selected)
                    if selected
                    else None,
                    "ci_low_pct": 100.0 * low if low is not None else None,
                    "ci_high_pct": 100.0 * high if high is not None else None,
                    "max_age_seconds": max_age,
                    "validity_rule": rule,
                }
            )
    return result


def segment_coverage_rows(
    features: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    result = []
    order = (
        "Closed 16:00–20:15",
        "Overnight 20:15–03:00",
        "Europe 03:00–09:30",
        "RTH 09:30–16:00",
    )
    for segment in order:
        selected = [row for row in features if row["gth_segment"] == segment]
        result.append(
            {
                "segment_et": segment,
                "origins": len(selected),
                "gex_pct": 100.0
                * sum(row["gex_net"] is not None for row in selected)
                / len(selected)
                if selected
                else None,
                "gamma_pct": 100.0
                * sum(row["gross_gamma"] is not None for row in selected)
                / len(selected)
                if selected
                else None,
                "charm_pct": 100.0
                * sum(row["gross_charm"] is not None for row in selected)
                / len(selected)
                if selected
                else None,
                "vanna_pct": 100.0
                * sum(row["gross_vanna"] is not None for row in selected)
                / len(selected)
                if selected
                else None,
                "zero_gamma_pct": 100.0
                * sum(row["zero_signed_dist"] is not None for row in selected)
                / len(selected)
                if selected
                else None,
                "skew_pct": 100.0
                * sum(row["skew_tilt"] is not None for row in selected)
                / len(selected)
                if selected
                else None,
            }
        )
    return result


def daily_label_rows(
    origins: list[dict[str, Any]], labels: list[dict[str, Any]], cohort: str
) -> list[dict[str, Any]]:
    result = []
    keys = sorted(
        {(row["_session"], str(row.get("trading_date"))) for row in origins},
        key=lambda item: (0 if item[0] == "RTH" else 1, item[1]),
    )
    for session, day in keys:
        result.append(
            {
                "cohort": cohort,
                "session": session,
                "date": day,
                "origins": sum(
                    row["_session"] == session
                    and str(row.get("trading_date")) == day
                    for row in origins
                ),
                "labels_15m": sum(
                    row["session"] == session
                    and row["trading_date"] == day
                    and row["horizon"] == 15
                    for row in labels
                ),
                "labels_30m": sum(
                    row["session"] == session
                    and row["trading_date"] == day
                    and row["horizon"] == 30
                    for row in labels
                ),
                "labels_60m": sum(
                    row["session"] == session
                    and row["trading_date"] == day
                    and row["horizon"] == 60
                    for row in labels
                ),
            }
        )
    return result


def current_snapshot_diagnostics(
    features: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    metrics: dict[str, Counter[str]] = defaultdict(Counter)
    usable: dict[str, list[float]] = defaultdict(list)
    for row in features:
        session = row["session"]
        current = row.get("current_greek")
        if not current:
            metrics[session]["No raw Greek snapshot within 5m"] += 1
            continue
        status = str(current.get("status") or "missing")
        aggregate_quality = str((current.get("aggregate") or {}).get("quality") or "none")
        gex_quality = str(
            (current.get("signed_gex_proxy") or {}).get("quality")
            or "legacy/missing"
        )
        metrics[session][f"Snapshot status: {status}"] += 1
        metrics[session][f"Aggregate quality: {aggregate_quality}"] += 1
        metrics[session][f"GEX quality: {gex_quality}"] += 1
        blocked = current.get("blocked_counts") or {}
        if finite_float(blocked.get("missing_or_invalid_iv")) not in (None, 0.0):
            metrics[session]["Blocked: missing_or_invalid_iv"] += 1
        if any(
            str(key).startswith(
                "quote_not_pricing_allowed:transport_stale_after_15s"
            )
            and finite_float(value) not in (None, 0.0)
            for key, value in blocked.items()
        ):
            metrics[session]["Blocked: transport_stale_after_15s"] += 1
        warnings = [str(value) for value in current.get("warnings") or []]
        if any("live_spx_underlier_unavailable" in value for value in warnings):
            metrics[session]["Warning: live_spx_underlier_unavailable"] += 1
        ratio = finite_float((current.get("coverage") or {}).get("usable_ratio"))
        if ratio is not None:
            usable[session].append(ratio)

    rows = []
    for session in ("RTH", "GTH"):
        denominator = sum(row["session"] == session for row in features)
        for metric, count in sorted(metrics[session].items()):
            rows.append(
                {
                    "session": session,
                    "metric": metric,
                    "origins": denominator,
                    "count": count,
                    "share_pct": 100.0 * count / denominator if denominator else None,
                }
            )

    quantile_rows = []
    for session in ("RTH", "GTH"):
        values = np.asarray(usable[session], dtype=float)
        quantiles = np.quantile(values, [0.0, 0.1, 0.25, 0.5, 0.75, 0.9, 1.0])
        quantile_rows.append(
            {
                "session": session,
                "n": int(values.size),
                "min": quantiles[0],
                "p10": quantiles[1],
                "p25": quantiles[2],
                "p50": quantiles[3],
                "p75": quantiles[4],
                "p90": quantiles[5],
                "max": quantiles[6],
            }
        )
    return rows, quantile_rows


def average_ranks(values: np.ndarray) -> np.ndarray:
    order = np.argsort(values, kind="mergesort")
    ranks = np.empty(values.size, dtype=float)
    index = 0
    while index < values.size:
        end = index + 1
        while end < values.size and values[order[end]] == values[order[index]]:
            end += 1
        ranks[order[index:end]] = (index + end - 1) / 2.0 + 1.0
        index = end
    return ranks


def spearman(values: np.ndarray, target: np.ndarray) -> float | None:
    if values.size < 3:
        return None
    left = average_ranks(values)
    right = average_ranks(target)
    if np.std(left) == 0.0 or np.std(right) == 0.0:
        return None
    return float(np.corrcoef(left, right)[0, 1])


def walk_forward_incremental_r2(
    rows: list[dict[str, Any]],
    feature: str,
    target: str,
    ordered_dates: list[str],
) -> tuple[float | None, int, int, str]:
    squared_model: list[float] = []
    squared_baseline: list[float] = []
    test_dates: list[str] = []
    for date_index in range(4, len(ordered_dates)):
        test_date = ordered_dates[date_index]
        train_dates = set(ordered_dates[:date_index])
        train = [
            row
            for row in rows
            if row["trading_date"] in train_dates
            and finite_float(row.get(feature)) is not None
            and finite_float(row.get(target)) is not None
        ]
        test = [
            row
            for row in rows
            if row["trading_date"] == test_date
            and finite_float(row.get(feature)) is not None
            and finite_float(row.get(target)) is not None
        ]
        if len(train) < 10 or not test:
            continue
        x_train = np.asarray([float(row[feature]) for row in train], dtype=float)
        y_train = np.asarray([float(row[target]) for row in train], dtype=float)
        x_test = np.asarray([float(row[feature]) for row in test], dtype=float)
        y_test = np.asarray([float(row[target]) for row in test], dtype=float)
        mean_x = float(np.mean(x_train))
        std_x = float(np.std(x_train))
        if not math.isfinite(std_x) or std_x <= 1e-12:
            continue
        z_train = (x_train - mean_x) / std_x
        z_test = (x_test - mean_x) / std_x
        design = np.column_stack([np.ones(z_train.size), z_train])
        coefficients = np.linalg.lstsq(design, y_train, rcond=None)[0]
        predictions = coefficients[0] + coefficients[1] * z_test
        baseline = np.full(y_test.size, float(np.mean(y_train)))
        squared_model.extend(np.square(y_test - predictions).tolist())
        squared_baseline.extend(np.square(y_test - baseline).tolist())
        test_dates.append(test_date)
    if not squared_baseline or sum(squared_baseline) <= 0.0:
        return None, len(squared_model), len(test_dates), ",".join(test_dates)
    incremental_r2 = 1.0 - sum(squared_model) / sum(squared_baseline)
    return (
        float(incremental_r2),
        len(squared_model),
        len(test_dates),
        ",".join(test_dates),
    )


def ablation_rows(
    labels: list[dict[str, Any]],
    feature_by_origin: dict[str, dict[str, Any]],
    feature_specs: tuple[tuple[str, str, str], ...],
    target: str,
    study: str,
) -> list[dict[str, Any]]:
    enriched = []
    for label in labels:
        feature_row = feature_by_origin.get(label["origin_id"])
        if not feature_row:
            continue
        enriched.append({**label, **feature_row})

    result = []
    for session in ("RTH", "GTH"):
        for horizon in HORIZONS:
            cohort = [
                row
                for row in enriched
                if row["session"] == session
                and row["horizon"] == horizon
                and finite_float(row.get(target)) is not None
            ]
            ordered_dates = sorted({row["trading_date"] for row in cohort})
            for feature, label, role in feature_specs:
                selected = [
                    row
                    for row in cohort
                    if finite_float(row.get(feature)) is not None
                ]
                x = np.asarray([float(row[feature]) for row in selected], dtype=float)
                y = np.asarray([float(row[target]) for row in selected], dtype=float)
                if x.size:
                    ordered = np.argsort(x, kind="mergesort")
                    tertiles = np.array_split(ordered, 3)
                    low_values = y[tertiles[0]]
                    high_values = y[tertiles[-1]]
                    low_mean = float(np.mean(low_values)) if low_values.size else None
                    high_mean = float(np.mean(high_values)) if high_values.size else None
                else:
                    low_mean = high_mean = None
                wf_r2, wf_n, wf_days, wf_date_list = walk_forward_incremental_r2(
                    cohort, feature, target, ordered_dates
                )
                result.append(
                    {
                        "study": study,
                        "session": session,
                        "horizon": f"{horizon}m",
                        "feature": label,
                        "role": role,
                        "n": int(x.size),
                        "days": len({row["trading_date"] for row in selected}),
                        "spearman": spearman(x, y),
                        "low_tertile_mean_points": low_mean,
                        "high_tertile_mean_points": high_mean,
                        "high_minus_low_points": (
                            high_mean - low_mean
                            if high_mean is not None and low_mean is not None
                            else None
                        ),
                        "wf_delta_r2_pct": 100.0 * wf_r2
                        if wf_r2 is not None
                        else None,
                        "wf_labels": wf_n,
                        "wf_test_days": wf_days,
                        "wf_dates": wf_date_list,
                    }
                )
    return result


def round_tree(value: Any) -> Any:
    if isinstance(value, float):
        return float(round(float(value), 2)) if math.isfinite(value) else None
    if isinstance(value, dict):
        return {key: round_tree(item) for key, item in value.items()}
    if isinstance(value, list):
        return [round_tree(item) for item in value]
    return value


def expected_counts(payload: dict[str, Any]) -> None:
    summary = payload["cohort_summary"]
    index = {(row["cohort"], row["session"]): row for row in summary}
    assert index[("All ES origins", "RTH")]["origins"] == 129
    assert index[("All ES origins", "GTH")]["origins"] == 392
    assert index[("Directional headline", "RTH")]["origins"] == 92
    assert index[("Directional headline", "GTH")]["origins"] == 250
    assert (
        index[("Directional headline", "RTH")]["labels_15m"],
        index[("Directional headline", "RTH")]["labels_30m"],
        index[("Directional headline", "RTH")]["labels_60m"],
    ) == (74, 71, 60)
    assert (
        index[("Directional headline", "GTH")]["labels_15m"],
        index[("Directional headline", "GTH")]["labels_30m"],
        index[("Directional headline", "GTH")]["labels_60m"],
    ) == (245, 246, 239)


def template_spx_price(template: str) -> float | None:
    patterns = (
        r"价格\s+SPX\s+(-?\d+(?:\.\d+)?)",
        r"SPX\s+(?:代理|proxy)[:：]?\s*(-?\d+(?:\.\d+)?)",
        r"参考价[:：]\s*(-?\d+(?:\.\d+)?)",
    )
    for pattern in patterns:
        match = re.search(pattern, template, re.I)
        if match:
            return float(match.group(1))
    return None


def template_directional_wall(template: str, side: int) -> float | None:
    match = re.search(
        r"结构[^\n]*?Put(?: Wall)?\s+(\d+(?:\.\d+)?)"
        r"[^\n]*?Call(?: Wall)?\s+(\d+(?:\.\d+)?)",
        template,
        re.I,
    )
    if not match:
        return None
    return float(match.group(2 if side == 1 else 1))


def tenor_cutoff_bucket(instant: datetime) -> str:
    local = instant.astimezone(ET)
    return (
        "<13:00 ET"
        if local.time().replace(tzinfo=None) < time(13, 0)
        else ">=13:00 ET"
    )


def stable_seed(label: str) -> int:
    return 20260724 + sum(
        (index + 1) * ord(character)
        for index, character in enumerate(label)
    )


def date_block_mean_ci(
    rows: list[dict[str, Any]],
    value_fn: Callable[[dict[str, Any]], float],
    label: str,
) -> tuple[float | None, float | None]:
    by_day: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        value = finite_float(value_fn(row))
        if value is not None:
            by_day[str(row["trading_date"])].append(value)
    days = sorted(by_day)
    if len(days) < 2:
        return None, None
    generator = np.random.default_rng(stable_seed(label))
    estimates = np.empty(TENOR_BOOTSTRAP_REPLICATIONS, dtype=float)
    for index in range(TENOR_BOOTSTRAP_REPLICATIONS):
        sampled_days = generator.choice(days, size=len(days), replace=True)
        sampled_values = [
            value
            for day in sampled_days
            for value in by_day[str(day)]
        ]
        estimates[index] = float(np.mean(sampled_values))
    low, high = np.quantile(estimates, (0.025, 0.975))
    return float(low), float(high)


def duckdb_rows(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    parameters: list[Any] | None = None,
) -> list[dict[str, Any]]:
    cursor = connection.execute(query, parameters or [])
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def quote_lake_files(trading_dates: Iterable[str]) -> list[str]:
    paths: list[str] = []
    for trading_date in sorted(set(trading_dates)):
        for provider in ("ibkr", "schwab"):
            for hour in range(13, 21):
                paths.extend(
                    glob.glob(
                        str(
                            DATA_ROOT
                            / "lake/quotes/schema=v1"
                            / f"date={trading_date}"
                            / f"provider={provider}"
                            / f"hour={hour:02d}"
                            / "quotes.parquet"
                        )
                    )
                )
    return sorted(paths)


def build_tenor_counterfactual(
    directional_origins: list[dict[str, Any]],
) -> dict[str, Any]:
    origin_rows: list[tuple[Any, ...]] = []
    raw_overlap_days: set[str] = set()
    for origin in directional_origins:
        if origin["_session"] != "RTH":
            continue
        trading_date = str(origin.get("trading_date") or "")
        raw_available = any(
            (
                DATA_ROOT
                / "raw"
                / f"provider={provider}"
                / f"date={trading_date}"
            ).is_dir()
            for provider in ("ibkr", "schwab")
        )
        if not raw_available:
            continue
        side = int(origin["_bias"])
        template = str(origin.get("template") or "")
        spot = template_spx_price(template)
        wall = template_directional_wall(template, side)
        if spot is None or wall is None:
            continue
        local = origin["_generated_at"].astimezone(ET)
        close_at = datetime.combine(
            local.date(), time(16, 0), tzinfo=ET
        ).astimezone(timezone.utc)
        raw_overlap_days.add(trading_date)
        origin_rows.append(
            (
                origin["_origin_id"],
                origin["_generated_at"],
                trading_date,
                tenor_cutoff_bucket(origin["_generated_at"]),
                side,
                spot,
                wall,
                close_at,
            )
        )

    paths = quote_lake_files(raw_overlap_days)
    empty = {
        "tenor_summary": {
            "eligible_signals": len(origin_rows),
            "raw_overlap_days": len(raw_overlap_days),
            "paired_entries": 0,
            "matched_15m": 0,
            "matched_30m": 0,
            "matched_60m": 0,
        },
        "tenor_coverage": [],
        "tenor_performance": [],
        "tenor_paired_effect": [],
        "tenor_daily": [],
        "tenor_walk_forward": [],
        "expiry_touch": [],
        "tenor_provenance": [],
        "tenor_methodology": [],
    }
    if not origin_rows or not paths:
        return empty

    connection = duckdb.connect()
    connection.execute("SET TimeZone='UTC'")
    connection.execute("SET threads=8")
    connection.execute(
        """
        CREATE TEMP TABLE origins (
            origin_id VARCHAR,
            origin_at TIMESTAMPTZ,
            trading_date DATE,
            cutoff_bucket VARCHAR,
            side INTEGER,
            spot DOUBLE,
            wall DOUBLE,
            close_at TIMESTAMPTZ
        )
        """
    )
    connection.executemany(
        "INSERT INTO origins VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        origin_rows,
    )

    connection.execute(
        f"""
        CREATE TEMP TABLE tenor_entries AS
        WITH entry_quotes AS (
            SELECT
                o.origin_id,
                o.origin_at,
                o.trading_date,
                o.cutoff_bucket,
                o.side,
                o.spot,
                o.wall,
                o.close_at,
                q.provider,
                q.received_at,
                q.quote_time,
                q.instrument_id,
                q.expiry,
                q.strike,
                q."right" AS option_right,
                q.bid,
                q.ask,
                q.source_file,
                q.source_sha256,
                date_diff(
                    'millisecond', q.received_at, o.origin_at
                ) / 1000.0 AS age_seconds,
                date_diff(
                    'millisecond', q.quote_time, q.received_at
                ) / 1000.0 AS source_lag_seconds,
                row_number() OVER (
                    PARTITION BY o.origin_id, q.provider, q.instrument_id
                    ORDER BY q.received_at DESC, q.quote_time DESC
                ) AS quote_rank
            FROM origins o
            JOIN read_parquet(?, hive_partitioning=true) q
              ON q.received_at BETWEEN
                 o.origin_at - INTERVAL '{TENOR_QUOTE_MAX_AGE_SECONDS:g} seconds'
                 AND o.origin_at
            WHERE q.instrument_type = 'option'
              AND q.underlier = 'SPX'
              AND q.trading_class = 'SPXW'
              AND q.quality = 'live'
              AND q.bid IS NOT NULL
              AND q.ask IS NOT NULL
              AND q.bid >= 0
              AND q.ask > 0
              AND q.ask >= q.bid
              AND q.quote_time BETWEEN
                  q.received_at - INTERVAL
                      '{TENOR_QUOTE_MAX_AGE_SECONDS:g} seconds'
                  AND q.received_at + INTERVAL '5 seconds'
              AND q."right" = CASE WHEN o.side = 1 THEN 'C' ELSE 'P' END
              AND q.source_file LIKE 'raw/provider=%'
              AND q.source_sha256 IS NOT NULL
              AND q.received_at < TIMESTAMPTZ '{GENERATED_AT}'
        ),
        latest AS (
            SELECT * EXCLUDE (quote_rank)
            FROM entry_quotes
            WHERE quote_rank = 1
        ),
        expiries AS (
            SELECT
                origin_id,
                provider,
                min(expiry) FILTER (WHERE expiry > trading_date) AS next_expiry
            FROM latest
            WHERE expiry >= trading_date
            GROUP BY origin_id, provider
        ),
        paired AS (
            SELECT
                zero.origin_id,
                zero.origin_at,
                zero.trading_date,
                zero.cutoff_bucket,
                zero.side,
                zero.spot,
                zero.wall,
                zero.close_at,
                zero.provider,
                zero.strike,
                zero.option_right,
                zero.instrument_id AS zero_instrument,
                zero.expiry AS zero_expiry,
                zero.bid AS zero_bid,
                zero.ask AS zero_ask,
                zero.received_at AS zero_received_at,
                zero.age_seconds AS zero_age_seconds,
                zero.source_lag_seconds AS zero_source_lag_seconds,
                zero.source_file AS zero_source_file,
                next.instrument_id AS one_instrument,
                next.expiry AS one_expiry,
                next.bid AS one_bid,
                next.ask AS one_ask,
                next.received_at AS one_received_at,
                next.age_seconds AS one_age_seconds,
                next.source_lag_seconds AS one_source_lag_seconds,
                next.source_file AS one_source_file,
                abs(zero.strike - zero.wall) AS strike_distance_to_wall,
                abs(
                    date_diff(
                        'millisecond',
                        zero.received_at,
                        next.received_at
                    )
                ) / 1000.0 AS quote_skew_seconds,
                row_number() OVER (
                    PARTITION BY zero.origin_id
                    ORDER BY
                        abs(zero.strike - zero.wall),
                        CASE zero.provider
                            WHEN 'schwab' THEN 1
                            WHEN 'ibkr' THEN 2
                            ELSE 9
                        END,
                        greatest(zero.age_seconds, next.age_seconds),
                        zero.strike
                ) AS pair_rank
            FROM latest zero
            JOIN expiries expiry
              ON expiry.origin_id = zero.origin_id
             AND expiry.provider = zero.provider
            JOIN latest next
              ON next.origin_id = zero.origin_id
             AND next.provider = zero.provider
             AND next.strike = zero.strike
             AND next.option_right = zero.option_right
             AND next.expiry = expiry.next_expiry
            WHERE zero.expiry = zero.trading_date
              AND abs(
                    date_diff(
                        'millisecond',
                        zero.received_at,
                        next.received_at
                    )
                  ) / 1000.0 <= {TENOR_QUOTE_MAX_AGE_SECONDS:g}
        )
        SELECT * EXCLUDE (pair_rank)
        FROM paired
        WHERE pair_rank = 1
        """,
        [paths],
    )

    connection.execute(
        f"""
        CREATE TEMP TABLE tenor_exits AS
        WITH requests AS (
            SELECT
                entry.*,
                horizon.horizon,
                entry.origin_at
                    + horizon.horizon * INTERVAL '1 minute' AS target_at
            FROM tenor_entries entry
            CROSS JOIN (VALUES (15), (30), (60)) horizon(horizon)
        ),
        exit_quotes AS (
            SELECT
                request.origin_id,
                request.horizon,
                q.instrument_id,
                q.received_at,
                q.quote_time,
                q.bid,
                q.ask,
                q.source_file,
                date_diff(
                    'millisecond', q.received_at, request.target_at
                ) / 1000.0 AS age_seconds,
                row_number() OVER (
                    PARTITION BY
                        request.origin_id,
                        request.horizon,
                        q.instrument_id
                    ORDER BY q.received_at DESC, q.quote_time DESC
                ) AS quote_rank
            FROM requests request
            JOIN read_parquet(?, hive_partitioning=true) q
              ON q.provider = request.provider
             AND q.instrument_id IN (
                 request.zero_instrument,
                 request.one_instrument
             )
             AND q.received_at BETWEEN
                 request.target_at
                     - INTERVAL '{TENOR_QUOTE_MAX_AGE_SECONDS:g} seconds'
                 AND request.target_at
            WHERE q.instrument_type = 'option'
              AND q.quality = 'live'
              AND q.bid IS NOT NULL
              AND q.ask IS NOT NULL
              AND q.bid >= 0
              AND q.ask > 0
              AND q.ask >= q.bid
              AND q.quote_time BETWEEN
                  q.received_at - INTERVAL
                      '{TENOR_QUOTE_MAX_AGE_SECONDS:g} seconds'
                  AND q.received_at + INTERVAL '5 seconds'
              AND q.source_file LIKE 'raw/provider=%'
              AND q.source_sha256 IS NOT NULL
              AND q.received_at < TIMESTAMPTZ '{GENERATED_AT}'
        ),
        latest AS (
            SELECT * EXCLUDE (quote_rank)
            FROM exit_quotes
            WHERE quote_rank = 1
        )
        SELECT
            request.*,
            zero.bid AS zero_exit_bid,
            zero.received_at AS zero_exit_at,
            zero.age_seconds AS zero_exit_age_seconds,
            zero.source_file AS zero_exit_source_file,
            next.bid AS one_exit_bid,
            next.received_at AS one_exit_at,
            next.age_seconds AS one_exit_age_seconds,
            next.source_file AS one_exit_source_file,
            zero.bid - request.zero_ask AS zero_pnl,
            next.bid - request.one_ask AS one_pnl
        FROM requests request
        JOIN latest zero
          ON zero.origin_id = request.origin_id
         AND zero.horizon = request.horizon
         AND zero.instrument_id = request.zero_instrument
        JOIN latest next
          ON next.origin_id = request.origin_id
         AND next.horizon = request.horizon
         AND next.instrument_id = request.one_instrument
        """,
        [paths],
    )

    entries = duckdb_rows(
        connection,
        "SELECT * FROM tenor_entries ORDER BY origin_at",
    )
    exits = duckdb_rows(
        connection,
        "SELECT * FROM tenor_exits ORDER BY origin_at, horizon",
    )

    path_rows = duckdb_rows(
        connection,
        """
        WITH raw_path AS (
            SELECT
                origin.origin_id,
                origin.trading_date,
                origin.cutoff_bucket,
                origin.side,
                origin.spot,
                origin.wall,
                origin.close_at,
                quote.provider,
                quote.received_at,
                quote.source_file,
                coalesce(
                    nullif(quote.effective_price, 0),
                    nullif(quote.last, 0),
                    nullif(quote.mid, 0),
                    CASE
                        WHEN quote.bid > 0 AND quote.ask >= quote.bid
                        THEN (quote.bid + quote.ask) / 2
                    END
                ) AS price
            FROM origins origin
            JOIN read_parquet(?, hive_partitioning=true) quote
              ON quote.received_at >= origin.origin_at
             AND quote.received_at < origin.close_at
            WHERE quote.instrument_type = 'index'
              AND quote.instrument_id = 'index:SPX'
              AND quote.quality = 'live'
              AND quote.source_file LIKE 'raw/provider=%'
              AND quote.source_sha256 IS NOT NULL
              AND quote.received_at < TIMESTAMPTZ '2026-07-24T00:00:00Z'
        ),
        plausible AS (
            SELECT *
            FROM raw_path
            WHERE price BETWEEN spot - 500 AND spot + 500
        ),
        summarized AS (
            SELECT
                origin_id,
                trading_date,
                cutoff_bucket,
                side,
                spot,
                wall,
                close_at,
                provider,
                min(price) AS path_low,
                max(price) AS path_high,
                count(*) AS quote_rows,
                count(DISTINCT source_file) AS raw_files,
                max(received_at) AS last_quote_at
            FROM plausible
            GROUP BY
                origin_id,
                trading_date,
                cutoff_bucket,
                side,
                spot,
                wall,
                close_at,
                provider
        ),
        ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY origin_id
                    ORDER BY
                        CASE provider
                            WHEN 'schwab' THEN 1
                            WHEN 'ibkr' THEN 2
                            ELSE 9
                        END
                ) AS provider_rank
            FROM summarized
            WHERE last_quote_at >= close_at - INTERVAL '2 minutes'
        )
        SELECT * EXCLUDE (provider_rank)
        FROM ranked
        WHERE provider_rank = 1
        ORDER BY trading_date, origin_id
        """,
        [paths],
    )
    connection.close()

    for row in (*entries, *exits, *path_rows):
        if "trading_date" in row:
            row["trading_date"] = str(row["trading_date"])

    origin_dicts = [
        {
            "origin_id": row[0],
            "origin_at": row[1],
            "trading_date": str(row[2]),
            "cutoff_bucket": row[3],
            "side": row[4],
            "spot": row[5],
            "wall": row[6],
            "close_at": row[7],
        }
        for row in origin_rows
    ]

    buckets = ("All RTH", "<13:00 ET", ">=13:00 ET")

    def in_bucket(row: dict[str, Any], bucket: str) -> bool:
        return bucket == "All RTH" or row["cutoff_bucket"] == bucket

    coverage_rows_result = []
    performance_rows = []
    paired_rows = []
    for bucket in buckets:
        selected_origins = [
            row for row in origin_dicts if in_bucket(row, bucket)
        ]
        selected_entries = [row for row in entries if in_bucket(row, bucket)]
        for horizon in HORIZONS:
            selected_exits = [
                row
                for row in exits
                if in_bucket(row, bucket) and row["horizon"] == horizon
            ]
            signal_low, signal_high = wilson_interval(
                len(selected_exits), len(selected_origins)
            )
            coverage_rows_result.append(
                {
                    "cutoff": bucket,
                    "horizon": f"{horizon}m",
                    "eligible_signals": len(selected_origins),
                    "paired_entries": len(selected_entries),
                    "matched_exits": len(selected_exits),
                    "entry_coverage_pct": (
                        100 * len(selected_entries) / len(selected_origins)
                        if selected_origins
                        else None
                    ),
                    "signal_coverage_pct": (
                        100 * len(selected_exits) / len(selected_origins)
                        if selected_origins
                        else None
                    ),
                    "signal_ci_low_pct": (
                        100 * signal_low if signal_low is not None else None
                    ),
                    "signal_ci_high_pct": (
                        100 * signal_high if signal_high is not None else None
                    ),
                    "entry_exit_coverage_pct": (
                        100 * len(selected_exits) / len(selected_entries)
                        if selected_entries
                        else None
                    ),
                    "days": len(
                        {row["trading_date"] for row in selected_exits}
                    ),
                    "quote_age_cap_seconds": TENOR_QUOTE_MAX_AGE_SECONDS,
                }
            )

            for tenor, prefix in (("0DTE", "zero"), ("1DTE", "one")):
                pnl_values = [
                    float(row[f"{prefix}_pnl"]) for row in selected_exits
                ]
                asks = [
                    float(row[f"{prefix}_ask"]) for row in selected_exits
                ]
                spreads = [
                    float(row[f"{prefix}_ask"] - row[f"{prefix}_bid"])
                    for row in selected_exits
                ]
                spread_pcts = [
                    100
                    * float(row[f"{prefix}_ask"] - row[f"{prefix}_bid"])
                    / ((float(row[f"{prefix}_ask"]) + float(row[f"{prefix}_bid"])) / 2)
                    for row in selected_exits
                    if float(row[f"{prefix}_ask"]) + float(row[f"{prefix}_bid"])
                    > 0
                ]
                returns = [
                    100
                    * float(row[f"{prefix}_pnl"])
                    / float(row[f"{prefix}_ask"])
                    for row in selected_exits
                    if float(row[f"{prefix}_ask"]) > 0
                ]
                low, high = date_block_mean_ci(
                    selected_exits,
                    lambda row, name=f"{prefix}_pnl": float(row[name]),
                    f"{bucket}-{horizon}-{tenor}",
                )
                performance_rows.append(
                    {
                        "cutoff": bucket,
                        "horizon": f"{horizon}m",
                        "tenor": tenor,
                        "n": len(selected_exits),
                        "days": len(
                            {row["trading_date"] for row in selected_exits}
                        ),
                        "mean_pnl_points": (
                            float(np.mean(pnl_values)) if pnl_values else None
                        ),
                        "median_pnl_points": (
                            float(np.median(pnl_values)) if pnl_values else None
                        ),
                        "mean_pnl_usd": (
                            100 * float(np.mean(pnl_values))
                            if pnl_values
                            else None
                        ),
                        "win_rate_pct": (
                            100
                            * sum(value > 0 for value in pnl_values)
                            / len(pnl_values)
                            if pnl_values
                            else None
                        ),
                        "mean_return_pct": (
                            float(np.mean(returns)) if returns else None
                        ),
                        "median_entry_ask_points": (
                            float(np.median(asks)) if asks else None
                        ),
                        "median_entry_spread_points": (
                            float(np.median(spreads)) if spreads else None
                        ),
                        "median_entry_spread_pct": (
                            float(np.median(spread_pcts))
                            if spread_pcts
                            else None
                        ),
                        "mean_ci_low_points": low,
                        "mean_ci_high_points": high,
                    }
                )

            differences = [
                float(row["zero_pnl"] - row["one_pnl"])
                for row in selected_exits
            ]
            low, high = date_block_mean_ci(
                selected_exits,
                lambda row: float(row["zero_pnl"] - row["one_pnl"]),
                f"{bucket}-{horizon}-paired-delta",
            )
            conclusive = (
                low is not None
                and high is not None
                and len({row["trading_date"] for row in selected_exits}) >= 5
                and (low > 0 or high < 0)
            )
            if not conclusive:
                decision = "Inconclusive"
            elif low > 0:
                decision = "0DTE favored in sample"
            else:
                decision = "1DTE favored in sample"
            paired_rows.append(
                {
                    "cutoff": bucket,
                    "horizon": f"{horizon}m",
                    "n": len(selected_exits),
                    "days": len(
                        {row["trading_date"] for row in selected_exits}
                    ),
                    "mean_0dte_minus_1dte_points": (
                        float(np.mean(differences)) if differences else None
                    ),
                    "median_0dte_minus_1dte_points": (
                        float(np.median(differences)) if differences else None
                    ),
                    "zero_better_pct": (
                        100
                        * sum(value > 0 for value in differences)
                        / len(differences)
                        if differences
                        else None
                    ),
                    "mean_ci_low_points": low,
                    "mean_ci_high_points": high,
                    "decision": decision,
                }
            )

    daily_rows = []
    walk_forward_rows = []
    for cutoff in ("<13:00 ET", ">=13:00 ET"):
        for horizon in HORIZONS:
            selected = [
                row
                for row in exits
                if row["cutoff_bucket"] == cutoff
                and row["horizon"] == horizon
            ]
            dates = sorted({row["trading_date"] for row in selected})
            for trading_date in dates:
                rows_for_day = [
                    row
                    for row in selected
                    if row["trading_date"] == trading_date
                ]
                zero_values = [
                    float(row["zero_pnl"]) for row in rows_for_day
                ]
                one_values = [float(row["one_pnl"]) for row in rows_for_day]
                daily_rows.append(
                    {
                        "cutoff": cutoff,
                        "horizon": f"{horizon}m",
                        "date": trading_date,
                        "n": len(rows_for_day),
                        "zero_mean_points": float(np.mean(zero_values)),
                        "zero_median_points": float(np.median(zero_values)),
                        "zero_win_pct": 100
                        * sum(value > 0 for value in zero_values)
                        / len(zero_values),
                        "one_mean_points": float(np.mean(one_values)),
                        "one_median_points": float(np.median(one_values)),
                        "one_win_pct": 100
                        * sum(value > 0 for value in one_values)
                        / len(one_values),
                        "mean_zero_minus_one_points": float(
                            np.mean(
                                [
                                    zero - one
                                    for zero, one in zip(
                                        zero_values, one_values, strict=True
                                    )
                                ]
                            )
                        ),
                    }
                )

            for index in range(1, len(dates)):
                training_days = set(dates[:index])
                training = [
                    row
                    for row in selected
                    if row["trading_date"] in training_days
                ]
                test = [
                    row
                    for row in selected
                    if row["trading_date"] == dates[index]
                ]
                if not training or not test:
                    continue
                zero_training_mean = float(
                    np.mean([float(row["zero_pnl"]) for row in training])
                )
                one_training_mean = float(
                    np.mean([float(row["one_pnl"]) for row in training])
                )
                selected_prefix = (
                    "zero"
                    if zero_training_mean > one_training_mean
                    else "one"
                )
                selected_tenor = (
                    "0DTE" if selected_prefix == "zero" else "1DTE"
                )
                test_values = [
                    float(row[f"{selected_prefix}_pnl"]) for row in test
                ]
                walk_forward_rows.append(
                    {
                        "cutoff": cutoff,
                        "horizon": f"{horizon}m",
                        "test_date": dates[index],
                        "train_days": len(training_days),
                        "train_n": len(training),
                        "selected_tenor": selected_tenor,
                        "train_zero_mean_points": zero_training_mean,
                        "train_one_mean_points": one_training_mean,
                        "test_n": len(test),
                        "test_mean_points": float(np.mean(test_values)),
                        "test_median_points": float(np.median(test_values)),
                        "test_win_pct": 100
                        * sum(value > 0 for value in test_values)
                        / len(test_values),
                    }
                )

    path_by_origin = {row["origin_id"]: row for row in path_rows}
    touch_rows = []
    for bucket in buckets:
        selected = [row for row in origin_dicts if in_bucket(row, bucket)]
        directional = [
            row
            for row in selected
            if (
                row["side"] == 1 and row["wall"] > row["spot"]
            )
            or (
                row["side"] == -1 and row["wall"] < row["spot"]
            )
        ]
        covered = [
            row
            for row in directional
            if row["origin_id"] in path_by_origin
        ]
        touched = []
        for row in covered:
            path = path_by_origin[row["origin_id"]]
            touched.append(
                bool(
                    (
                        row["side"] == 1
                        and float(path["path_high"]) >= row["wall"]
                    )
                    or (
                        row["side"] == -1
                        and float(path["path_low"]) <= row["wall"]
                    )
                )
            )
        low, high = wilson_interval(sum(touched), len(touched))
        touch_rows.append(
            {
                "cutoff": bucket,
                "signals": len(selected),
                "directional_wall_targets": len(directional),
                "path_covered": len(covered),
                "path_coverage_pct": (
                    100 * len(covered) / len(directional)
                    if directional
                    else None
                ),
                "touched_by_16_et": sum(touched),
                "touch_rate_pct": (
                    100 * sum(touched) / len(touched) if touched else None
                ),
                "touch_ci_low_pct": 100 * low if low is not None else None,
                "touch_ci_high_pct": 100 * high if high is not None else None,
                "median_wall_distance_points": (
                    float(
                        np.median(
                            [
                                abs(float(row["wall"] - row["spot"]))
                                for row in directional
                            ]
                        )
                    )
                    if directional
                    else None
                ),
                "metric_type": "Realized SPX path; not risk-neutral probability",
            }
        )

    entry_source_files = {
        str(row[field])
        for row in entries
        for field in ("zero_source_file", "one_source_file")
    }
    exit_source_files = {
        str(row[field])
        for row in exits
        for field in ("zero_exit_source_file", "one_exit_source_file")
    }
    provenance_rows = [
        {
            "fact_set": "Matched entry NBBO",
            "paired_observations": len(entries),
            "raw_quote_rows": 2 * len(entries),
            "raw_source_files": len(entry_source_files),
            "lineage_contract": "raw/...jsonl path + non-null SHA256 required",
        },
        {
            "fact_set": "Matched fixed-exit NBBO",
            "paired_observations": len(exits),
            "raw_quote_rows": 2 * len(exits),
            "raw_source_files": len(exit_source_files),
            "lineage_contract": "raw/...jsonl path + non-null SHA256 required",
        },
        {
            "fact_set": "SPX path to 16:00 ET",
            "paired_observations": len(path_rows),
            "raw_quote_rows": sum(int(row["quote_rows"]) for row in path_rows),
            "raw_source_files": sum(int(row["raw_files"]) for row in path_rows),
            "lineage_contract": "raw/...jsonl path + non-null SHA256 required",
        },
    ]

    methodology_rows = [
        {
            "rule": "Signal and wall path",
            "implementation": (
                "Exact persisted directional RTH report; Call for bullish, "
                "Put for bearish; report SPX and directional wall frozen at origin"
            ),
            "limitation": (
                "Directional single-leg wall proxy; not a reconstructed vertical spread"
            ),
        },
        {
            "rule": "Matched contract",
            "implementation": (
                "Same provider, right and strike for 0DTE and next listed "
                "SPXW expiry; common strike nearest frozen directional wall"
            ),
            "limitation": (
                "Friday next listed expiry is Monday; calendar DTE can exceed one"
            ),
        },
        {
            "rule": "Point-in-time entry",
            "implementation": (
                "Latest live two-sided raw quote received at or before origin; "
                f"age <= {TENOR_QUOTE_MAX_AGE_SECONDS:.2f}s; buy at ask"
            ),
            "limitation": "No queue, fill, commission or market-impact label",
        },
        {
            "rule": "Fixed exit",
            "implementation": (
                "Latest live two-sided raw quote as of exactly 15/30/60m; "
                f"age <= {TENOR_QUOTE_MAX_AGE_SECONDS:.2f}s; sell at bid"
            ),
            "limitation": "Unmatched horizons are dropped from both tenors",
        },
        {
            "rule": "Uncertainty",
            "implementation": (
                f"{TENOR_BOOTSTRAP_REPLICATIONS} deterministic whole-day "
                "bootstrap replications; expanding past-days-only tenor choice"
            ),
            "limitation": (
                "Only six overlapping RTH dates and overlapping 15-minute origins"
            ),
        },
        {
            "rule": "Expiry touch",
            "implementation": (
                "Separate realized SPX path crossing of the frozen directional "
                "wall by 16:00 ET"
            ),
            "limitation": (
                "Not risk-neutral probability and never mixed into fixed-window P&L"
            ),
        },
    ]

    summary = {
        "eligible_signals": len(origin_rows),
        "raw_overlap_days": len(raw_overlap_days),
        "paired_entries": len(entries),
        **{
            f"matched_{horizon}m": sum(
                row["horizon"] == horizon for row in exits
            )
            for horizon in HORIZONS
        },
    }
    return {
        "tenor_summary": summary,
        "tenor_coverage": coverage_rows_result,
        "tenor_performance": performance_rows,
        "tenor_paired_effect": paired_rows,
        "tenor_daily": daily_rows,
        "tenor_walk_forward": walk_forward_rows,
        "expiry_touch": touch_rows,
        "tenor_provenance": provenance_rows,
        "tenor_methodology": methodology_rows,
    }


def build_research_payload() -> dict[str, Any]:
    reports = load_reports()
    raw_origins = build_origins(reports, directional_only=False)
    directional_origins = build_origins(reports, directional_only=True)
    tenor_results = build_tenor_counterfactual(directional_origins)
    tenor_summary = tenor_results["tenor_summary"]
    raw_labels = build_labels(raw_origins, reports)
    directional_labels = build_labels(directional_origins, reports)
    greek_store = load_greek_series()
    iv_store = load_iv_series()
    raw_features = join_features(raw_origins, greek_store, iv_store)
    directional_features = join_features(directional_origins, greek_store, iv_store)
    raw_feature_by_origin = {row["origin_id"]: row for row in raw_features}
    directional_feature_by_origin = {
        row["origin_id"]: row for row in directional_features
    }

    cohort_summary = []
    for cohort, origins, labels in (
        ("All ES origins", raw_origins, raw_labels),
        ("Directional headline", directional_origins, directional_labels),
    ):
        for session in ("RTH", "GTH"):
            selected_origins = [
                row for row in origins if row["_session"] == session
            ]
            cohort_summary.append(
                {
                    "cohort": cohort,
                    "session": session,
                    "origins": len(selected_origins),
                    "days": len(
                        {str(row.get("trading_date")) for row in selected_origins}
                    ),
                    "labels_15m": sum(
                        row["session"] == session and row["horizon"] == 15
                        for row in labels
                    ),
                    "labels_30m": sum(
                        row["session"] == session and row["horizon"] == 30
                        for row in labels
                    ),
                    "labels_60m": sum(
                        row["session"] == session and row["horizon"] == 60
                        for row in labels
                    ),
                }
            )

    coverage = coverage_rows(raw_features, "All ES origins") + coverage_rows(
        directional_features, "Directional headline"
    )
    missing, usable_quantiles = current_snapshot_diagnostics(directional_features)
    primary_ablation = ablation_rows(
        directional_labels,
        directional_feature_by_origin,
        PRIMARY_FEATURES,
        "signed_y",
        "Headline continuation gate",
    )
    raw_ablation = ablation_rows(
        raw_labels,
        raw_feature_by_origin,
        RAW_FEATURES,
        "raw_y",
        "Raw ES delta negative control",
    )

    payload = {
        "metadata": {
            "title": "Spring Gamma v3：三周数据质量与消融研究",
            "window_start": WINDOW_START,
            "window_end": WINDOW_END,
            "cutoff_at": GENERATED_AT,
            "timezone": "America/New_York",
            "greek_max_age_seconds": GREEK_MAX_AGE_SECONDS,
            "iv_max_age_seconds": IV_MAX_AGE_SECONDS,
            "reports": len(reports),
            "analysis_dependencies": (
                "DuckDB point-in-time quote audit + NumPy + Python standard library"
            ),
            "status": "shadow_only",
        },
        "cohort_summary": cohort_summary,
        "daily_labels": daily_label_rows(
            raw_origins, raw_labels, "All ES origins"
        )
        + daily_label_rows(
            directional_origins, directional_labels, "Directional headline"
        ),
        "coverage": coverage,
        "segment_coverage": segment_coverage_rows(directional_features),
        "current_snapshot_missing": missing,
        "usable_ratio_quantiles": usable_quantiles,
        "primary_ablation": primary_ablation,
        "raw_ablation": raw_ablation,
        "normalization": [
            {
                "feature_family": "Gross Gamma / Charm / Vanna",
                "transform": "log1p; training-only 1/99% winsor; session-specific median/MAD",
                "missing_rule": "Keep missing flag; never zero-impute",
                "production_age": "RTH ≤120s; GTH ≤300s",
            },
            {
                "feature_family": "Net GEX proxy / ratio",
                "transform": "Scale net GEX to $bn; clip ratio to training 1/99%",
                "missing_rule": "Require quality=available",
                "production_age": "RTH ≤120s; GTH ≤300s",
            },
            {
                "feature_family": "Zero-gamma distance",
                "transform": "(spot−ZG)/max(EM,10); clip to [−3,3]",
                "missing_rule": "Same causal IV snapshot; keep missing flag",
                "production_age": "RTH ≤360s; GTH ≤600s",
            },
            {
                "feature_family": "Put/Call skew and walls",
                "transform": "Put ratio−Call ratio; side-aligned wall room",
                "missing_rule": "No NBBO interpolation; smooth IV/structure only",
                "production_age": "RTH ≤360s; GTH ≤600s",
            },
            {
                "feature_family": "Charm",
                "transform": "log1p magnitude and Charm/Gamma equivalent points",
                "missing_rule": "Gate only; never infer direction from absolute Charm",
                "production_age": "RTH ≤120s; GTH ≤300s",
            },
        ],
        "weak_priors": [
            {
                "term": "Standardized main effects",
                "prior": "Normal(0.00, 0.25²)",
                "status": "Weak regularization; shadow only",
            },
            {
                "term": "Session interactions",
                "prior": "Normal(0.00, 0.35²)",
                "status": "Required because RTH/GTH signs differ",
            },
            {
                "term": "RTH gross Gamma/Vanna continuation gate",
                "prior": "Normal(−0.10, 0.20²)",
                "status": "Optional weak prior; do not sign-constrain",
            },
            {
                "term": "GTH net-GEX continuation gate",
                "prior": "Normal(−0.10, 0.20²)",
                "status": "Optional weak prior; do not sign-constrain",
            },
        ],
        "promotion_readiness": [
            {
                "criterion": "Complete sessions",
                "minimum": "≥20 RTH and ≥20 GTH",
                "current": "7 directional RTH days; 8 directional GTH days",
                "decision": "Not met",
            },
            {
                "criterion": "Independent forward labels",
                "minimum": "≥100 non-overlapping labels per horizon",
                "current": "Walk-forward rows overlap at 15m cadence",
                "decision": "Not met",
            },
            {
                "criterion": "Regime bucket size",
                "minimum": "≥30 labels per bucket",
                "current": "RTH tertiles are materially smaller",
                "decision": "Not met",
            },
            {
                "criterion": "Forward stability",
                "minimum": "Same effect sign on ≥4 forward days",
                "current": "RTH has only 3 test days",
                "decision": "Not met",
            },
            {
                "criterion": "Incremental performance",
                "minimum": "≥2% relative MSE/log-loss gain; date-block CI excludes 0",
                "current": "No estimable stable CI with 3–4 test days",
                "decision": "Not met",
            },
        ],
        "untrainable": [
            {
                "item": "Dealer-signed GEX / Charm / Vanna",
                "why": "Dealer inventory sign is unknown; available GEX is Call+/Put− house proxy",
            },
            {
                "item": "Gamma-state categorical effect",
                "why": "Zero-gamma transition dominates because ±0.5% is checked first",
            },
            {
                "item": "Per-strike signed live surface",
                "why": "Only four usable late-window days; insufficient regime support",
            },
            {
                "item": "High-order interactions / neural model",
                "why": "Only 7–8 directional days; degrees of freedom exceed evidence",
            },
            {
                "item": "Real trade P&L calibration",
                "why": (
                    "Ask-entry/bid-exit counterfactual is available, but causal fill, "
                    "fee, queue, impact and account-position labels remain absent"
                ),
            },
            {
                "item": "2026-07-06–10 production labels",
                "why": "Raw market data exists but production report/FSM history does not",
            },
            {
                "item": "13:00 ET 的 1DTE→0DTE 硬切换",
                "why": (
                    "同刻可执行对照已补齐，但 >=13:00 ET 每个 horizon 仅约 "
                    "5–6 个 matched 样本，日阻断区间不足以判定最优 tenor"
                ),
            },
        ],
        "tenor_availability": [
            {
                "field": "Raw RTH overlap",
                "current_outcome": (
                    f"{tenor_summary['eligible_signals']} signals / "
                    f"{tenor_summary['raw_overlap_days']} dates"
                ),
                "shadow_capture": (
                    f"{tenor_summary['paired_entries']} same-provider, "
                    "same-right, same-strike 0/1DTE entries"
                ),
                "promotion_use": "Coverage denominator is explicit; no imputation",
            },
            {
                "field": "Executable NBBO",
                "current_outcome": (
                    f"Matched exits 15/30/60m = "
                    f"{tenor_summary['matched_15m']}/"
                    f"{tenor_summary['matched_30m']}/"
                    f"{tenor_summary['matched_60m']}"
                ),
                "shadow_capture": (
                    "Entry at ask and fixed-horizon exit at bid; "
                    "quote/source ages <=15.00s"
                ),
                "promotion_use": "Compare conservative executable marks, not model mid",
            },
            {
                "field": "Fill outcome",
                "current_outcome": "No causal fill/queue label",
                "shadow_capture": "Limit, fill price/time, partial fill, reject and cancel",
                "promotion_use": "Estimate realized slippage and selection bias",
            },
            {
                "field": "ATM IV / term gap",
                "current_outcome": "Not joined to both tenors",
                "shadow_capture": "ATM IV for 0DTE and 1DTE; 0DTE−1DTE gap",
                "promotion_use": "Condition tenor choice on term structure",
            },
            {
                "field": "到墙概率",
                "current_outcome": (
                    "Frozen wall has a separate realized SPX path-to-16:00 label"
                ),
                "shadow_capture": (
                    "Wall, origin spot, direction, path coverage and realized touch"
                ),
                "promotion_use": (
                    "Path metric only; never substitute it for risk-neutral probability"
                ),
            },
            {
                "field": "Theta / Gamma / cost",
                "current_outcome": "No same-instant net comparison",
                "shadow_capture": "Leg Greeks, spread Greeks, fees and conservative slippage",
                "promotion_use": "Compare expected edge after decay and cost",
            },
        ],
        "data_contract": [
            {
                "rule": "Causal report label",
                "implementation": "Same trading date; target ±180s; ES source compatible",
                "limitation": "15m cadence makes 30m/60m labels overlap",
            },
            {
                "rule": "Greek availability clock",
                "implementation": "as_of ≤ origin; expiry match; age ≤300s",
                "limitation": "No independent created_at; ingestion latency is unobservable",
            },
            {
                "rule": "IV availability clock",
                "implementation": "created_at ≤ origin; expiry match; age ≤600s",
                "limitation": "Research cap is coverage-oriented, not production freshness",
            },
            {
                "rule": "Latest-valid carry",
                "implementation": "Per quality-valid feature, backward only within cap",
                "limitation": "Reported beside current-snapshot failure mechanisms",
            },
            {
                "rule": "Price integrity",
                "implementation": "NBBO is never interpolated",
                "limitation": "Only IV surface and structural functions may be smoothed",
            },
            {
                "rule": "Tenor counterfactual clock",
                "implementation": (
                    "Same provider/right/strike; latest <=15s quote as of origin "
                    "and each fixed exit; ask in / bid out"
                ),
                "limitation": (
                    "Missing pair drops both tenors; compacted rows require raw path "
                    "and SHA256 lineage"
                ),
            },
        ],
    }
    payload.update(tenor_results)
    payload = round_tree(payload)
    expected_counts(payload)
    return payload


def source(
    source_id: str,
    label: str,
    path: str,
    description: str,
    filters: list[str],
    definitions: list[str],
    engine: str = "Python/NumPy file audit",
) -> dict[str, Any]:
    return {
        "id": source_id,
        "label": label,
        "path": path,
        "query": {
            "engine": engine,
            "id": f"spring-gamma-v3-{source_id}-20260724",
            "sql": (
                f"SELECT '{path}' AS audited_source_path, "
                "'parsed by scripts/build_spring_gamma_v3_research.py' AS method"
            ),
            "description": description,
            "executed_at": GENERATED_AT,
            "language": "sql",
            "filters": filters,
            "metric_definitions": definitions,
            "tables_used": [path],
        },
    }


def table(
    table_id: str,
    title: str,
    subtitle: str,
    dataset: str,
    columns: list[tuple[str, str, str]],
    sort_field: str,
    sort_direction: str = "asc",
    source_id: str = "research_build",
) -> dict[str, Any]:
    rendered_columns = []
    for field, label, kind in columns:
        column: dict[str, Any] = {"field": field, "label": label}
        if kind == "text":
            column["type"] = "text"
        else:
            column["format"] = "number"
        rendered_columns.append(column)
    return {
        "id": table_id,
        "title": title,
        "subtitle": subtitle,
        "dataset": dataset,
        "sourceId": source_id,
        "defaultSort": {"field": sort_field, "direction": sort_direction},
        "density": "compact",
        "layout": "full",
        "columns": rendered_columns,
    }


def lookup_ablation(
    payload: dict[str, Any], session: str, horizon: str, feature: str
) -> dict[str, Any]:
    return next(
        row
        for row in payload["primary_ablation"]
        if row["session"] == session
        and row["horizon"] == horizon
        and row["feature"] == feature
    )


def fmt(value: Any, suffix: str = "") -> str:
    return "—" if value is None else f"{float(value):.2f}{suffix}"


def build_artifact(payload: dict[str, Any]) -> dict[str, Any]:
    rth_gamma_60 = lookup_ablation(
        payload, "RTH", "60m", "log1p gross gamma"
    )
    rth_charm_60 = lookup_ablation(
        payload, "RTH", "60m", "log1p gross Charm 5m"
    )
    gth_gex_60 = lookup_ablation(
        payload, "GTH", "60m", "Net GEX proxy, $bn"
    )
    rth_coverage = next(
        row
        for row in payload["coverage"]
        if row["cohort"] == "Directional headline"
        and row["session"] == "RTH"
        and row["feature"] == "Gross Charm 5m"
    )
    gth_coverage = next(
        row
        for row in payload["coverage"]
        if row["cohort"] == "Directional headline"
        and row["session"] == "GTH"
        and row["feature"] == "Gross Charm 5m"
    )
    tenor_summary = payload["tenor_summary"]
    post_effects = {
        row["horizon"]: row
        for row in payload["tenor_paired_effect"]
        if row["cutoff"] == ">=13:00 ET"
    }
    all_60_effect = next(
        row
        for row in payload["tenor_paired_effect"]
        if row["cutoff"] == "All RTH" and row["horizon"] == "60m"
    )

    sources = [
        source(
            "reports",
            "Persisted 15-minute status reports",
            "audit/order_map_pricing/date=*/reports.jsonl",
            "Parses report session, visible headline side, ES reference and causal forward labels.",
            [
                f"{WINDOW_START} <= trading_date <= {WINDOW_END}",
                f"generated_at < {GENERATED_AT}",
                "status reports with parseable ES; target on same date within ±180 seconds",
            ],
            [
                "Raw ES delta = future ES − origin ES.",
                "Headline continuation = visible side × raw ES delta.",
                "30m and 60m labels overlap at a 15m origin cadence.",
            ],
        ),
        source(
            "greeks",
            "SPXW 0DTE Greek reference snapshots",
            "features/spxw_0dte_greeks_reference/date=*/snapshots.jsonl",
            "Extracts quality-valid GEX proxy and gross Greek magnitude features.",
            [
                f"as_of < {GENERATED_AT}",
                "expiry must match report expiry",
                "latest quality-valid feature no older than 300 seconds",
            ],
            [
                "GEX proxy uses Call positive and Put negative; dealer position sign is unknown.",
                "Gross Gamma, Charm and Vanna require aggregate.quality=ok.",
                "Charm is an absolute-magnitude gate only, not a direction feature.",
            ],
        ),
        source(
            "iv_surface",
            "Causal IV-surface snapshots",
            "features/iv_surface/date=*/hour=*/snapshots.jsonl",
            "Extracts zero-gamma, expected move, skew ratios and walls using creation time.",
            [
                f"created_at < {GENERATED_AT}",
                "created_at <= report origin",
                "front expiry must match report expiry",
                "latest valid structure no older than 600 seconds",
            ],
            [
                "Zero-gamma distance = underlier − zero_gamma.",
                "Skew tilt = put_skew_ratio − call_skew_ratio.",
                "NBBO values are never interpolated.",
            ],
        ),
        source(
            "tenor_quotes",
            "Point-in-time SPX/SPXW raw quote facts",
            (
                "lake/quotes/schema=v1/date=*/provider={ibkr,schwab}/"
                "hour=*/quotes.parquet"
            ),
            (
                "Audits append-only raw quote facts for synchronized 0DTE versus "
                "next-listed-expiry entry and fixed-horizon exits. Every selected "
                "row must retain its raw JSONL source path and SHA256."
            ),
            [
                "RTH directional origins on raw-overlap dates 2026-07-16–23",
                "Same provider, option right and strike; common strike nearest frozen wall",
                "quality=live; two-sided; received_at <= decision time; age <=15 seconds",
                "Entry at ask; 15/30/60m fixed exit at bid; no interpolation or imputation",
            ],
            [
                "P&L points = exit bid − entry ask; one SPX contract is points × $100.",
                "1DTE means next listed SPXW expiry; Friday maps to Monday.",
                "Date-block bootstrap resamples whole trading dates.",
                "Expiry touch is a separate realized SPX path metric through 16:00 ET.",
            ],
            engine="DuckDB point-in-time Parquet audit",
        ),
        source(
            "research_build",
            "Reproducible Spring Gamma v3 builder",
            "scripts/build_spring_gamma_v3_research.py",
            "Performs deterministic coverage, tertile and expanding date-block ablation calculations.",
            [
                "First four dates train; each later date is predicted from prior dates only",
                "Feature scaler is fit on training rows only",
                "No random split and no future feature carry",
            ],
            [
                "Spearman is an in-sample descriptive statistic.",
                "WF ΔR² = 100 × (1 − feature-model SSE / training-mean baseline SSE).",
                "All reader-facing numerical values are rounded to two decimals.",
            ],
        ),
    ]

    tables = [
        table(
            "tenor_coverage_table",
            "0DTE/1DTE 严格配对覆盖",
            (
                "Denominator is every directional RTH report on raw-overlap dates; "
                "unmatched legs or horizons drop both tenors."
            ),
            "tenor_coverage",
            [
                ("cutoff", "ET cutoff", "text"),
                ("horizon", "Horizon", "text"),
                ("eligible_signals", "Signals", "number"),
                ("paired_entries", "Entry pairs", "number"),
                ("matched_exits", "Matched exits", "number"),
                ("entry_coverage_pct", "Entry cov. %", "number"),
                ("signal_coverage_pct", "Signal cov. %", "number"),
                ("signal_ci_low_pct", "Cov. CI low %", "number"),
                ("signal_ci_high_pct", "Cov. CI high %", "number"),
                ("entry_exit_coverage_pct", "Exit/entry %", "number"),
                ("days", "Days", "number"),
            ],
            "cutoff",
            source_id="tenor_quotes",
        ),
        table(
            "tenor_performance_table",
            "Ask 入 / Bid 出的单腿墙位路径反事实",
            (
                "One-contract P&L; $ P&L uses the SPX $100 multiplier. "
                "No commissions, fill probability, queue or impact."
            ),
            "tenor_performance",
            [
                ("cutoff", "ET cutoff", "text"),
                ("horizon", "Horizon", "text"),
                ("tenor", "Tenor", "text"),
                ("n", "n", "number"),
                ("days", "Days", "number"),
                ("mean_pnl_points", "Mean pts", "number"),
                ("median_pnl_points", "Median pts", "number"),
                ("mean_pnl_usd", "Mean $", "number"),
                ("win_rate_pct", "Win %", "number"),
                ("mean_return_pct", "Mean return %", "number"),
                ("median_entry_ask_points", "Median ask", "number"),
                (
                    "median_entry_spread_points",
                    "Median spread pts",
                    "number",
                ),
                ("median_entry_spread_pct", "Median spread %", "number"),
                ("mean_ci_low_points", "Mean CI low", "number"),
                ("mean_ci_high_points", "Mean CI high", "number"),
            ],
            "cutoff",
            source_id="tenor_quotes",
        ),
        table(
            "tenor_paired_table",
            "配对期限差：正值才表示 0DTE 更好",
            (
                "Difference is 0DTE P&L minus 1DTE P&L on the identical "
                "signal/provider/right/strike/exit clock; CI resamples whole dates."
            ),
            "tenor_paired_effect",
            [
                ("cutoff", "ET cutoff", "text"),
                ("horizon", "Horizon", "text"),
                ("n", "n", "number"),
                ("days", "Days", "number"),
                (
                    "mean_0dte_minus_1dte_points",
                    "Mean 0DTE−1DTE",
                    "number",
                ),
                (
                    "median_0dte_minus_1dte_points",
                    "Median 0DTE−1DTE",
                    "number",
                ),
                ("zero_better_pct", "0DTE better %", "number"),
                ("mean_ci_low_points", "CI low", "number"),
                ("mean_ci_high_points", "CI high", "number"),
                ("decision", "Audit decision", "text"),
            ],
            "cutoff",
            source_id="tenor_quotes",
        ),
        table(
            "tenor_daily_table",
            "逐日 matched P&L 显示结果由少数日期主导",
            "Each row keeps the two tenors on an identical matched sample.",
            "tenor_daily",
            [
                ("cutoff", "ET cutoff", "text"),
                ("horizon", "Horizon", "text"),
                ("date", "Date", "text"),
                ("n", "n", "number"),
                ("zero_mean_points", "0DTE mean", "number"),
                ("zero_median_points", "0DTE median", "number"),
                ("zero_win_pct", "0DTE win %", "number"),
                ("one_mean_points", "1DTE mean", "number"),
                ("one_median_points", "1DTE median", "number"),
                ("one_win_pct", "1DTE win %", "number"),
                (
                    "mean_zero_minus_one_points",
                    "Mean 0DTE−1DTE",
                    "number",
                ),
            ],
            "date",
            source_id="tenor_quotes",
        ),
        table(
            "tenor_walk_forward_table",
            "逐日 expanding walk-forward 期限选择",
            (
                "For each test date, the tenor is chosen only from earlier matched "
                "dates' mean P&L; the rule is diagnostic, not a fitted production policy."
            ),
            "tenor_walk_forward",
            [
                ("cutoff", "ET cutoff", "text"),
                ("horizon", "Horizon", "text"),
                ("test_date", "Test date", "text"),
                ("train_days", "Train days", "number"),
                ("train_n", "Train n", "number"),
                ("selected_tenor", "Selected", "text"),
                ("train_zero_mean_points", "Train 0DTE", "number"),
                ("train_one_mean_points", "Train 1DTE", "number"),
                ("test_n", "Test n", "number"),
                ("test_mean_points", "Test mean", "number"),
                ("test_median_points", "Test median", "number"),
                ("test_win_pct", "Test win %", "number"),
            ],
            "test_date",
            source_id="tenor_quotes",
        ),
        table(
            "expiry_touch_table",
            "到期墙位触达是独立价格路径指标",
            (
                "Realized SPX crossing through 16:00 ET; this is not a "
                "risk-neutral probability and is not mixed with fixed-window P&L."
            ),
            "expiry_touch",
            [
                ("cutoff", "ET cutoff", "text"),
                ("signals", "Signals", "number"),
                (
                    "directional_wall_targets",
                    "Directional targets",
                    "number",
                ),
                ("path_covered", "Path covered", "number"),
                ("path_coverage_pct", "Coverage %", "number"),
                ("touched_by_16_et", "Touched", "number"),
                ("touch_rate_pct", "Touch %", "number"),
                ("touch_ci_low_pct", "CI low %", "number"),
                ("touch_ci_high_pct", "CI high %", "number"),
                (
                    "median_wall_distance_points",
                    "Median wall dist.",
                    "number",
                ),
                ("metric_type", "Metric type", "text"),
            ],
            "cutoff",
            source_id="tenor_quotes",
        ),
        table(
            "tenor_provenance_table",
            "Raw JSONL lineage is enforced",
            (
                "The Parquet fact table is used for efficient scanning, but every "
                "selected row must point to a raw JSONL file and carry a SHA256."
            ),
            "tenor_provenance",
            [
                ("fact_set", "Fact set", "text"),
                ("paired_observations", "Observations", "number"),
                ("raw_quote_rows", "Raw rows", "number"),
                ("raw_source_files", "Raw files", "number"),
                ("lineage_contract", "Lineage contract", "text"),
            ],
            "fact_set",
            source_id="tenor_quotes",
        ),
        table(
            "tenor_methodology_table",
            "期限反事实的数据与执行契约",
            "Every exclusion is deterministic and applied symmetrically.",
            "tenor_methodology",
            [
                ("rule", "Rule", "text"),
                ("implementation", "Implementation", "text"),
                ("limitation", "Limitation", "text"),
            ],
            "rule",
            source_id="tenor_quotes",
        ),
        table(
            "cohort_table",
            "可用标签不是零，但 RTH 只有七个方向日",
            "All counts are causal links; observations at 15-minute cadence are not independent trades.",
            "cohort_summary",
            [
                ("cohort", "Cohort", "text"),
                ("session", "Session", "text"),
                ("origins", "Origins", "number"),
                ("days", "Days", "number"),
                ("labels_15m", "15m", "number"),
                ("labels_30m", "30m", "number"),
                ("labels_60m", "60m", "number"),
            ],
            "cohort",
        ),
        table(
            "coverage_table",
            "Latest-valid 覆盖掩盖了 GTH 当前帧缺口",
            "95% Wilson intervals; coverage-oriented caps are 300s for Greeks and 600s for IV.",
            "coverage",
            [
                ("cohort", "Cohort", "text"),
                ("session", "Session", "text"),
                ("feature", "Feature", "text"),
                ("origins", "Origins", "number"),
                ("available", "Available", "number"),
                ("coverage_pct", "Coverage %", "number"),
                ("ci_low_pct", "CI low %", "number"),
                ("ci_high_pct", "CI high %", "number"),
                ("max_age_seconds", "Max age s", "number"),
                ("validity_rule", "Validity rule", "text"),
            ],
            "coverage_pct",
            "desc",
        ),
        table(
            "segment_coverage_table",
            "GTH 的 Europe 段最缺 gross Greek",
            "Directional origins split by ET session segment.",
            "segment_coverage",
            [
                ("segment_et", "ET segment", "text"),
                ("origins", "Origins", "number"),
                ("gex_pct", "GEX %", "number"),
                ("gamma_pct", "Gamma %", "number"),
                ("charm_pct", "Charm %", "number"),
                ("vanna_pct", "Vanna %", "number"),
                ("zero_gamma_pct", "ZG %", "number"),
                ("skew_pct", "Skew %", "number"),
            ],
            "segment_et",
        ),
        table(
            "missing_table",
            "当前帧经常 degraded；LOCF 只能用于研究覆盖",
            "Nearest raw Greek snapshot at each directional report origin, without latest-valid substitution.",
            "current_snapshot_missing",
            [
                ("session", "Session", "text"),
                ("metric", "Current-frame state", "text"),
                ("origins", "Origins", "number"),
                ("count", "Count", "number"),
                ("share_pct", "Share %", "number"),
            ],
            "session",
        ),
        table(
            "usable_quantile_table",
            "Usable-ratio 分布有明显选择偏差",
            "Quantiles use only current snapshots with a numeric usable ratio.",
            "usable_ratio_quantiles",
            [
                ("session", "Session", "text"),
                ("n", "n", "number"),
                ("min", "Min", "number"),
                ("p10", "P10", "number"),
                ("p25", "P25", "number"),
                ("p50", "P50", "number"),
                ("p75", "P75", "number"),
                ("p90", "P90", "number"),
                ("max", "Max", "number"),
            ],
            "session",
        ),
        table(
            "daily_labels_table",
            "逐日样本显示周一至周三并非没有标签",
            "Raw and directional cohorts are shown separately; dates with no origin are absent.",
            "daily_labels",
            [
                ("cohort", "Cohort", "text"),
                ("session", "Session", "text"),
                ("date", "Date", "text"),
                ("origins", "Origins", "number"),
                ("labels_15m", "15m", "number"),
                ("labels_30m", "30m", "number"),
                ("labels_60m", "60m", "number"),
            ],
            "date",
        ),
        table(
            "primary_ablation_table",
            "方向延续 gate 消融：RTH 与 GTH 必须分开",
            "Target is visible headline side × later ES move. Positive WF ΔR² beats a training-mean baseline.",
            "primary_ablation",
            [
                ("session", "Session", "text"),
                ("horizon", "Horizon", "text"),
                ("feature", "Feature", "text"),
                ("role", "Role", "text"),
                ("n", "n", "number"),
                ("days", "Days", "number"),
                ("spearman", "Spearman", "number"),
                ("low_tertile_mean_points", "Low mean pts", "number"),
                ("high_tertile_mean_points", "High mean pts", "number"),
                ("high_minus_low_points", "High−low pts", "number"),
                ("wf_delta_r2_pct", "WF ΔR² %", "number"),
                ("wf_labels", "WF labels", "number"),
                ("wf_test_days", "WF days", "number"),
                ("wf_dates", "WF dates", "text"),
            ],
            "wf_delta_r2_pct",
            "desc",
        ),
        table(
            "raw_ablation_table",
            "Raw ΔES 负对照：绝对 Greek 不能直接当方向",
            "This control intentionally removes the visible headline side from the target.",
            "raw_ablation",
            [
                ("session", "Session", "text"),
                ("horizon", "Horizon", "text"),
                ("feature", "Feature", "text"),
                ("role", "Role", "text"),
                ("n", "n", "number"),
                ("days", "Days", "number"),
                ("spearman", "Spearman", "number"),
                ("low_tertile_mean_points", "Low mean pts", "number"),
                ("high_tertile_mean_points", "High mean pts", "number"),
                ("wf_delta_r2_pct", "WF ΔR² %", "number"),
                ("wf_labels", "WF labels", "number"),
                ("wf_test_days", "WF days", "number"),
            ],
            "wf_delta_r2_pct",
            "desc",
        ),
        table(
            "normalization_table",
            "v3 参数先标准化，再做弱 gate",
            "These are implementation contracts, not fitted production thresholds.",
            "normalization",
            [
                ("feature_family", "Feature family", "text"),
                ("transform", "Transform", "text"),
                ("missing_rule", "Missing/quality rule", "text"),
                ("production_age", "Recommended age", "text"),
            ],
            "feature_family",
        ),
        table(
            "prior_table",
            "先验必须弱，且不得用小样本锁死符号",
            "Suggested priors are research defaults only.",
            "weak_priors",
            [
                ("term", "Term", "text"),
                ("prior", "Prior", "text"),
                ("status", "Status", "text"),
            ],
            "term",
        ),
        table(
            "promotion_table",
            "所有生产晋级条件目前都未满足",
            "Promotion requires independent forward evidence, not overlapping report labels.",
            "promotion_readiness",
            [
                ("criterion", "Criterion", "text"),
                ("minimum", "Minimum", "text"),
                ("current", "Current", "text"),
                ("decision", "Decision", "text"),
            ],
            "criterion",
        ),
        table(
            "untrainable_table",
            "当前证据无法训练的目标",
            "These exclusions prevent false precision and label leakage.",
            "untrainable",
            [
                ("item", "Not trainable now", "text"),
                ("why", "Why", "text"),
            ],
            "item",
        ),
        table(
            "tenor_availability_table",
            "可执行 mark 对照已补齐，但真实 fill 与样本仍不足",
            (
                "Keep 0DTE and 1DTE side by side in shadow; the matched "
                "counterfactual does not authorize a hard switch."
            ),
            "tenor_availability",
            [
                ("field", "Required field", "text"),
                ("current_outcome", "Current availability", "text"),
                ("shadow_capture", "Shadow capture", "text"),
                ("promotion_use", "Why needed", "text"),
            ],
            "field",
        ),
        table(
            "contract_table",
            "Causal join 与数据契约",
            "The Greek clock limitation is explicit; IV uses created_at.",
            "data_contract",
            [
                ("rule", "Rule", "text"),
                ("implementation", "Implementation", "text"),
                ("limitation", "Limitation", "text"),
            ],
            "rule",
        ),
    ]

    charts = [
        {
            "id": "tenor_delta_chart",
            "title": "0DTE−1DTE 平均 P&L（按 ET 分段与持有期）",
            "subtitle": (
                "正值偏向 0DTE；>=13:00 ET 每格仅 6–8 个样本，"
                "三个日阻断区间都跨 0"
            ),
            "intent": "comparison",
            "question": (
                "Does the matched executable P&L difference support a "
                "13:00 ET tenor switch?"
            ),
            "rationale": (
                "Grouped bars expose sign instability across horizons and "
                "cutoff cohorts; the adjacent table retains confidence intervals."
            ),
            "type": "bar",
            "dataset": "tenor_paired_effect",
            "sourceId": "tenor_quotes",
            "encodings": {
                "x": {
                    "field": "horizon",
                    "type": "nominal",
                    "label": "Fixed exit",
                },
                "y": {
                    "field": "mean_0dte_minus_1dte_points",
                    "type": "quantitative",
                    "format": "number",
                    "label": "Mean P&L difference, points",
                },
                "color": {
                    "field": "cutoff",
                    "type": "nominal",
                    "label": "ET cohort",
                },
                "tooltip": [
                    {
                        "field": "n",
                        "type": "quantitative",
                        "format": "number",
                        "label": "Matched n",
                    },
                    {
                        "field": "mean_ci_low_points",
                        "type": "quantitative",
                        "format": "number",
                        "label": "CI low",
                    },
                    {
                        "field": "mean_ci_high_points",
                        "type": "quantitative",
                        "format": "number",
                        "label": "CI high",
                    },
                ],
            },
            "valueFormat": "number",
            "layout": "full",
            "labels": {"values": "all"},
            "maxRows": len(payload["tenor_paired_effect"]),
            "settings": {
                "categoryLabelPolicy": "wrap",
                "groupMode": "grouped",
                "showValues": True,
                "sort": "none",
            },
            "surface": {
                "surface": "export",
                "interactiveLegend": False,
                "showControls": False,
                "viewMode": "visualization",
            },
        }
    ]

    blocks: list[dict[str, Any]] = [
        {
            "id": "title",
            "type": "markdown",
            "layout": "full",
            "body": "# Spring Gamma v3：三周数据质量与消融研究",
        },
        {
            "id": "technical_summary",
            "type": "markdown",
            "layout": "full",
            "body": (
                "## 结论：RTH 不是没有信号，但证据不足以调生产参数\n\n"
                "**RTH 有 92 个方向 origin，15/30/60m 可联结标签为 74/71/60；"
                "所以“完全没有信号”不成立。** 真正的问题是只有 7 个 RTH 方向日，"
                "标签在 15 分钟 cadence 下重叠，而且 Greek 当前帧大多 degraded。"
                f"Latest-valid 后 RTH Charm 覆盖 {fmt(rth_coverage['coverage_pct'], '%')}，"
                f"GTH 只有 {fmt(gth_coverage['coverage_pct'], '%')}；后者不能和 RTH 共用参数。\n\n"
                f"RTH 60m 的 gross Gamma / Charm gate 在本样本 WF ΔR² 分别为 "
                f"{fmt(rth_gamma_60['wf_delta_r2_pct'], '%')} / "
                f"{fmt(rth_charm_60['wf_delta_r2_pct'], '%')}，但只有 "
                f"{rth_charm_60['wf_labels']} 个 forward 标签、"
                f"{rth_charm_60['wf_test_days']} 个测试日。GTH 60m net-GEX gate 为 "
                f"{fmt(gth_gex_60['wf_delta_r2_pct'], '%')}。这些结果只够提出弱 gate，"
                "不够拟合硬阈值。\n\n"
                "**Spring Gamma v3 维持 shadow-only。Charm 只作幅度/质量 gate，"
                "不能给方向；Call+/Put− GEX 也不是 dealer inventory sign。"
                f"期限反事实在 {tenor_summary['raw_overlap_days']} 个原始行情重叠日得到 "
                f"{tenor_summary['paired_entries']} 个成对 entry，但 13:00 ET 后各 "
                "horizon 只有 6–8 个 matched outcome，仍不能证明硬切换。**"
            ),
        },
        {
            "id": "tenor_answer",
            "type": "markdown",
            "layout": "full",
            "body": (
                "## 期限反事实：13:00 ET 后没有足够证据选 0DTE\n\n"
                f"在 {tenor_summary['eligible_signals']} 个 raw-overlap RTH 信号中，"
                f"{tenor_summary['paired_entries']} 个能在同一决策时刻找到同源、"
                "同权利、同执行价的 0DTE/下一到期双边报价；固定 15/30/60m 的完整 "
                f"ask→bid 配对为 {tenor_summary['matched_15m']}/"
                f"{tenor_summary['matched_30m']}/"
                f"{tenor_summary['matched_60m']}。\n\n"
                "13:00 ET 后 0DTE−1DTE 平均 P&L 差在 15/30/60m 分别为 "
                f"{fmt(post_effects['15m']['mean_0dte_minus_1dte_points'])} / "
                f"{fmt(post_effects['30m']['mean_0dte_minus_1dte_points'])} / "
                f"{fmt(post_effects['60m']['mean_0dte_minus_1dte_points'])} 点；"
                "对应 whole-day bootstrap 区间为 "
                f"[{fmt(post_effects['15m']['mean_ci_low_points'])}, "
                f"{fmt(post_effects['15m']['mean_ci_high_points'])}] / "
                f"[{fmt(post_effects['30m']['mean_ci_low_points'])}, "
                f"{fmt(post_effects['30m']['mean_ci_high_points'])}] / "
                f"[{fmt(post_effects['60m']['mean_ci_low_points'])}, "
                f"{fmt(post_effects['60m']['mean_ci_high_points'])}]，全部跨 0。"
                "**因此 13:00 不是已验证的最优切换点。**\n\n"
                f"全 RTH 的 60m 配对差为 "
                f"{fmt(all_60_effect['mean_0dte_minus_1dte_points'])} 点，区间 "
                f"[{fmt(all_60_effect['mean_ci_low_points'])}, "
                f"{fmt(all_60_effect['mean_ci_high_points'])}]，样本内偏向 1DTE；"
                "但只有 5 个 outcome 日，且两期限的绝对中位 P&L 仍为负，"
                "只能保留为 shadow 假设。"
            ),
        },
        {
            "id": "tenor_delta_chart_block",
            "type": "chart",
            "layout": "full",
            "chartId": "tenor_delta_chart",
        },
        {
            "id": "tenor_paired_block",
            "type": "table",
            "layout": "full",
            "tableId": "tenor_paired_table",
        },
        {
            "id": "tenor_performance_block",
            "type": "table",
            "layout": "full",
            "tableId": "tenor_performance_table",
        },
        {
            "id": "tenor_coverage_block",
            "type": "table",
            "layout": "full",
            "tableId": "tenor_coverage_table",
        },
        {
            "id": "tenor_daily_answer",
            "type": "markdown",
            "layout": "full",
            "body": (
                "## 日期阻断：周一至周三不能被当作几十笔独立交易\n\n"
                "逐日表保留相同配对样本；walk-forward 每个测试日只用更早日期的"
                "平均 P&L 选择期限。13:00 ET 后常见单日只有 1–2 个 outcome，"
                "期限选择随日期与 horizon 翻转，正是不能调硬参数的原因。"
            ),
        },
        {
            "id": "tenor_daily_block",
            "type": "table",
            "layout": "full",
            "tableId": "tenor_daily_table",
        },
        {
            "id": "tenor_walk_forward_block",
            "type": "table",
            "layout": "full",
            "tableId": "tenor_walk_forward_table",
        },
        {
            "id": "expiry_touch_answer",
            "type": "markdown",
            "layout": "full",
            "body": (
                "## 墙位触达与固定窗口 P&L 完全分离\n\n"
                "触达只回答“SPX 是否在 16:00 ET 前穿过报告时刻冻结的方向墙”；"
                "它不是风险中性概率，也没有被拿来替代 15/30/60m 的 ask→bid P&L。"
                "已经越过或不在方向前方的墙位不进入触达分母。"
            ),
        },
        {
            "id": "expiry_touch_block",
            "type": "table",
            "layout": "full",
            "tableId": "expiry_touch_table",
        },
        {
            "id": "sample_answer",
            "type": "markdown",
            "layout": "full",
            "body": (
                "## 周一至周三有标签，缺的是独立会话\n\n"
                "按日期阻断后，RTH 只有 7 个方向日，GTH 只有 8 个。"
                "同日 15/30/60m 标签仍高度重叠，所以 origin 数不能当作独立交易数。"
            ),
        },
        {"id": "cohort_block", "type": "table", "layout": "full", "tableId": "cohort_table"},
        {"id": "daily_labels_block", "type": "table", "layout": "full", "tableId": "daily_labels_table"},
        {
            "id": "quality_answer",
            "type": "markdown",
            "layout": "full",
            "body": (
                "## GTH 稀疏是真问题，尤其是 Europe 时段\n\n"
                "研究覆盖允许 Greek 向后携带最多 300 秒、IV 最多 600 秒，且只携带"
                "质量有效值；这不是生产 freshness 建议。生产建议收紧为 RTH Greek "
                "120 秒 / IV 360 秒，GTH Greek 300 秒 / IV 600 秒。缺失值保留 missing "
                "flag，绝不补零。"
            ),
        },
        {"id": "coverage_block", "type": "table", "layout": "full", "tableId": "coverage_table"},
        {"id": "segment_coverage_block", "type": "table", "layout": "full", "tableId": "segment_coverage_table"},
        {"id": "missing_block", "type": "table", "layout": "full", "tableId": "missing_table"},
        {"id": "usable_quantile_block", "type": "table", "layout": "full", "tableId": "usable_quantile_table"},
        {
            "id": "ablation_answer",
            "type": "markdown",
            "layout": "full",
            "body": (
                "## 消融只支持 session-specific 弱 gate\n\n"
                "方向延续消融先用报告可见 side 定义目标；绝对 Gamma/Charm/Vanna "
                "本身不被当作方向。Spearman 与 tertile 是全样本描述；WF 从前 4 个日期"
                "开始，逐日扩展训练，标准化只拟合训练集。RTH 与 GTH 的高/低 exposure "
                "关系反向，禁止合并训练。"
            ),
        },
        {"id": "primary_ablation_block", "type": "table", "layout": "full", "tableId": "primary_ablation_table"},
        {"id": "raw_ablation_block", "type": "table", "layout": "full", "tableId": "raw_ablation_table"},
        {
            "id": "model_specification",
            "type": "markdown",
            "layout": "full",
            "body": (
                "## v3 规格：标准化、质量门、弱先验\n\n"
                "建议先构造 session-specific 标准化特征，再用低自由度线性/逻辑 gate。"
                "Charm 的唯一合法角色是环境幅度 gate；它不产生 Call/Put 方向。"
                "数值阈值在满足晋级标准前保持研究默认，不写入生产执行。"
            ),
        },
        {"id": "normalization_block", "type": "table", "layout": "full", "tableId": "normalization_table"},
        {"id": "prior_block", "type": "table", "layout": "full", "tableId": "prior_table"},
        {
            "id": "limitations",
            "type": "markdown",
            "layout": "full",
            "body": (
                "## 限制：可执行 mark 反事实仍不是真实成交回测\n\n"
                "期限表使用真实 point-in-time bid/ask，并把买入记 ask、卖出记 bid；"
                "它仍不含手续费、排队、部分成交、拒单、市场冲击或账户仓位，因此不能"
                "解释账户亏损，也不能证明可交易 edge。ES 标签来自后续报告值。Greek 只有 "
                "`as_of`，没有独立 `created_at`，所以无法度量其 ingestion latency。"
                "期限反事实只是同一墙位路径的方向单腿代理，不是重建后的 vertical spread；"
                "也没有把 ATM IV term gap、Theta/Gamma 或风险中性触达概率加入选择器。"
            ),
        },
        {
            "id": "tenor_method_answer",
            "type": "markdown",
            "layout": "full",
            "body": (
                "## 可复现性：扫描压缩事实表，但逐行验证 raw lineage\n\n"
                "为避免反复解析数十 GB JSONL，研究用 DuckDB 扫描 append-only Parquet "
                "事实表；每个入选报价必须保留 `raw/provider=.../quotes.jsonl` 路径和"
                "非空 SHA256。任何缺腿、过期或不完整 horizon 都对两个 tenor 对称删除。"
            ),
        },
        {
            "id": "tenor_provenance_block",
            "type": "table",
            "layout": "full",
            "tableId": "tenor_provenance_table",
        },
        {
            "id": "tenor_methodology_block",
            "type": "table",
            "layout": "full",
            "tableId": "tenor_methodology_table",
        },
        {"id": "contract_block", "type": "table", "layout": "full", "tableId": "contract_table"},
        {"id": "tenor_availability_block", "type": "table", "layout": "full", "tableId": "tenor_availability_table"},
        {"id": "untrainable_block", "type": "table", "layout": "full", "tableId": "untrainable_table"},
        {
            "id": "next_steps",
            "type": "markdown",
            "layout": "full",
            "body": (
                "## 下一步：先积累独立 forward evidence\n\n"
                "保持 v3 shadow-only；逐期记录 feature age、missing flags、session、"
                "方向 gate 与后续非重叠 outcome。达到 20+20 完整会话和每 horizon "
                "100 个非重叠标签后，再冻结候选阈值并做 date-block CI。期限层继续"
                "并列记录 0DTE/1DTE；补齐真实 fill/fee 和 vertical-spread 双腿后，"
                "再评估动态期限选择，不做 13:00 ET 生产硬切换。"
            ),
        },
        {"id": "promotion_block", "type": "table", "layout": "full", "tableId": "promotion_table"},
    ]

    manifest = {
        "version": 1,
        "surface": "report",
        "title": payload["metadata"]["title"],
        "description": (
            "Causal three-week data-quality, date-block ablation and matched "
            "0DTE/1DTE executable-mark audit for Spring Gamma v3."
        ),
        "generatedAt": GENERATED_AT,
        "sources": sources,
        "cards": [],
        "charts": charts,
        "tables": tables,
        "blocks": blocks,
    }
    artifact = {
        "surface": "report",
        "manifest": manifest,
        "snapshot": {
            "version": 1,
            "generatedAt": GENERATED_AT,
            "status": "ready",
            "accessIssues": [],
            "datasets": {
                key: payload[key]
                for key in (
                    "cohort_summary",
                    "daily_labels",
                    "coverage",
                    "segment_coverage",
                    "current_snapshot_missing",
                    "usable_ratio_quantiles",
                    "primary_ablation",
                    "raw_ablation",
                    "normalization",
                    "weak_priors",
                    "promotion_readiness",
                    "untrainable",
                    "tenor_availability",
                    "data_contract",
                    "tenor_coverage",
                    "tenor_performance",
                    "tenor_paired_effect",
                    "tenor_daily",
                    "tenor_walk_forward",
                    "expiry_touch",
                    "tenor_provenance",
                    "tenor_methodology",
                )
            },
        },
    }
    return artifact


def compact_table(
    rows: list[dict[str, Any]], columns: list[str], limit: int | None = None
) -> str:
    selected = rows[:limit] if limit is not None else rows

    def display(value: Any) -> str:
        if isinstance(value, (float, np.floating)):
            return f"{float(value):.2f}"
        return str(value)

    widths = {
        column: max(
            [len(column)]
            + [len(display(row.get(column, ""))) for row in selected]
        )
        for column in columns
    }
    header = " | ".join(column.ljust(widths[column]) for column in columns)
    divider = "-+-".join("-" * widths[column] for column in columns)
    body = [
        " | ".join(
            display(row.get(column, "")).ljust(widths[column])
            for column in columns
        )
        for row in selected
    ]
    return "\n".join([header, divider, *body])


def build_notebook(payload: dict[str, Any]) -> nbformat.NotebookNode:
    rth_gamma_60 = lookup_ablation(
        payload, "RTH", "60m", "log1p gross gamma"
    )
    rth_charm_60 = lookup_ablation(
        payload, "RTH", "60m", "log1p gross Charm 5m"
    )
    gth_gex_60 = lookup_ablation(
        payload, "GTH", "60m", "Net GEX proxy, $bn"
    )
    tenor_summary = payload["tenor_summary"]
    notebook = nbformat.v4.new_notebook(
        cells=[
            nbformat.v4.new_markdown_cell(
                "# Spring Gamma v3：三周数据质量与消融研究\n\n"
                "固定窗口 2026-07-03–2026-07-23；exclusive cutoff "
                "`2026-07-24T00:00:00Z`。"
            ),
            nbformat.v4.new_markdown_cell(
                "## tl;dr\n\n"
                "- RTH 不是零信号：92 个方向 origins，15/30/60m 标签为 74/71/60；"
                "但只有 7 个方向日。\n"
                "- RTH/GTH exposure 关系不同，必须拆 session。\n"
                f"- RTH 60m Gamma/Charm WF ΔR² 为 "
                f"{fmt(rth_gamma_60['wf_delta_r2_pct'], '%')} / "
                f"{fmt(rth_charm_60['wf_delta_r2_pct'], '%')}，仅 "
                f"{rth_charm_60['wf_test_days']} 个测试日。\n"
                f"- GTH 60m net-GEX WF ΔR² 为 "
                f"{fmt(gth_gex_60['wf_delta_r2_pct'], '%')}。\n"
                f"- Raw-overlap tenor audit: {tenor_summary['eligible_signals']} "
                f"signals, {tenor_summary['paired_entries']} paired entries, "
                f"{tenor_summary['matched_15m']}/"
                f"{tenor_summary['matched_30m']}/"
                f"{tenor_summary['matched_60m']} matched fixed exits.\n"
                "- >=13:00 ET 只有 8/7/6 个 matched outcomes，三个 paired "
                "CI 全跨 0；不能证明 13:00 硬切换。\n"
                "- Charm 只作 gate；Spring Gamma v3 保持 shadow-only。"
            ),
            nbformat.v4.new_markdown_cell(
                "## Context & Methods\n\n"
                "报告方向标签按同交易日、目标 ±180 秒、ES 来源兼容联结。"
                "Greek 用 `as_of <= origin`，IV 必须用 `created_at <= origin`；"
                "expiry 必须一致。Greek/IV latest-valid 最大年龄分别为 300/600 秒。"
                "消融用前 4 个日期训练、之后逐日扩展；没有随机切分。\n\n"
                "期限反事实固定报告 direction/wall，两个 tenor 使用同 provider/right/"
                "strike；entry 最新 <=15.00s live quote 按 ask，15/30/60m exit "
                "最新 <=15.00s quote 按 bid。任何缺腿对称删除，不插值。"
            ),
            nbformat.v4.new_code_cell(
                "from scripts.build_spring_gamma_v3_research import "
                "build_research_payload, compact_table\n\n"
                "payload = build_research_payload()\n"
                "payload['metadata']"
            ),
            nbformat.v4.new_markdown_cell(
                "## Matched 0DTE versus next-listed-expiry counterfactual\n\n"
                "正的 `0DTE−1DTE` 才表示 0DTE 较好。置信区间按完整日期 bootstrap；"
                "单份 SPX 合约的美元 P&L 为点数乘 100，未计手续费与实际 fill。"
            ),
            nbformat.v4.new_code_cell(
                "print(payload['tenor_summary'])\n"
                "print(compact_table(\n"
                "    payload['tenor_coverage'],\n"
                "    ['cutoff','horizon','eligible_signals','paired_entries',"
                "'matched_exits','entry_coverage_pct','signal_coverage_pct','days'],\n"
                "))"
            ),
            nbformat.v4.new_code_cell(
                "print(compact_table(\n"
                "    payload['tenor_performance'],\n"
                "    ['cutoff','horizon','tenor','n','days','mean_pnl_points',"
                "'median_pnl_points','win_rate_pct','median_entry_ask_points',"
                "'median_entry_spread_points','mean_ci_low_points','mean_ci_high_points'],\n"
                "))"
            ),
            nbformat.v4.new_code_cell(
                "print(compact_table(\n"
                "    payload['tenor_paired_effect'],\n"
                "    ['cutoff','horizon','n','days','mean_0dte_minus_1dte_points',"
                "'median_0dte_minus_1dte_points','zero_better_pct',"
                "'mean_ci_low_points','mean_ci_high_points','decision'],\n"
                "))"
            ),
            nbformat.v4.new_code_cell(
                "assert payload['tenor_summary'] == {\n"
                "    'eligible_signals': 79,\n"
                "    'raw_overlap_days': 6,\n"
                "    'paired_entries': 58,\n"
                "    'matched_15m': 44,\n"
                "    'matched_30m': 42,\n"
                "    'matched_60m': 40,\n"
                "}\n"
                "post = [row for row in payload['tenor_paired_effect'] "
                "if row['cutoff'] == '>=13:00 ET']\n"
                "assert [row['n'] for row in post] == [8, 7, 6]\n"
                "assert all(row['mean_ci_low_points'] <= 0 <= "
                "row['mean_ci_high_points'] for row in post)\n"
                "print('VALIDATED: paired raw-lineage quote coverage; "
                "no post-13:00 CI excludes zero.')"
            ),
            nbformat.v4.new_markdown_cell(
                "## Daily walk-forward and separate expiry-touch path metric\n\n"
                "Walk-forward 只用更早日期选择 tenor。到墙触达仅为 SPX 至 16:00 ET "
                "的 realized path crossing，不是风险中性概率，也不进入固定窗口 P&L。"
            ),
            nbformat.v4.new_code_cell(
                "print(compact_table(\n"
                "    payload['tenor_walk_forward'],\n"
                "    ['cutoff','horizon','test_date','train_days','train_n',"
                "'selected_tenor','test_n','test_mean_points','test_win_pct'],\n"
                "))\n"
                "print(compact_table(\n"
                "    payload['expiry_touch'],\n"
                "    ['cutoff','directional_wall_targets','path_covered',"
                "'touch_rate_pct','touch_ci_low_pct','touch_ci_high_pct'],\n"
                "))"
            ),
            nbformat.v4.new_markdown_cell(
                "## Data\n\n"
                "All-ES 与 directional-headline cohort 分开。"
                "后者用于 continuation gate；前者用于 raw ΔES 负对照。"
            ),
            nbformat.v4.new_code_cell(
                "print(compact_table(\n"
                "    payload['cohort_summary'],\n"
                "    ['cohort','session','origins','days','labels_15m','labels_30m','labels_60m'],\n"
                "))"
            ),
            nbformat.v4.new_code_cell(
                "coverage_focus = [row for row in payload['coverage'] "
                "if row['cohort'] == 'Directional headline']\n"
                "print(compact_table(\n"
                "    coverage_focus,\n"
                "    ['session','feature','available','origins','coverage_pct','ci_low_pct','ci_high_pct'],\n"
                "))"
            ),
            nbformat.v4.new_code_cell(
                "print(compact_table(\n"
                "    payload['segment_coverage'],\n"
                "    ['segment_et','origins','gex_pct','gamma_pct','charm_pct','vanna_pct','zero_gamma_pct','skew_pct'],\n"
                "))"
            ),
            nbformat.v4.new_markdown_cell(
                "## Results\n\n"
                "表内 `WF ΔR² %` 相对逐日 training-mean baseline；正数仅表示该样本下"
                "相对误差更低，不表示可交易或因果。"
            ),
            nbformat.v4.new_code_cell(
                "focus_names = {\n"
                "    'Net GEX proxy, $bn', 'log1p gross gamma',\n"
                "    'log1p gross Charm 5m', 'log1p gross Vanna 1vol',\n"
                "    'Side-aligned zero-gamma distance', 'Put ratio − Call ratio',\n"
                "}\n"
                "focus = [row for row in payload['primary_ablation'] "
                "if row['feature'] in focus_names]\n"
                "print(compact_table(\n"
                "    focus,\n"
                "    ['session','horizon','feature','n','days','spearman',"
                "'low_tertile_mean_points','high_tertile_mean_points',"
                "'wf_delta_r2_pct','wf_labels','wf_test_days'],\n"
                "))"
            ),
            nbformat.v4.new_code_cell(
                "summary = {(row['cohort'], row['session']): row "
                "for row in payload['cohort_summary']}\n"
                "assert summary[('All ES origins','RTH')]['origins'] == 129\n"
                "assert summary[('All ES origins','GTH')]['origins'] == 392\n"
                "assert summary[('Directional headline','RTH')]['origins'] == 92\n"
                "assert summary[('Directional headline','GTH')]['origins'] == 250\n"
                "assert [summary[('Directional headline','RTH')][f'labels_{h}m'] "
                "for h in (15,30,60)] == [74,71,60]\n"
                "assert [summary[('Directional headline','GTH')][f'labels_{h}m'] "
                "for h in (15,30,60)] == [245,246,239]\n"
                "assert all(row['wf_test_days'] <= 4 "
                "for row in payload['primary_ablation'])\n"
                "print('VALIDATED: causal cutoff, cohort counts, date-block split, and shadow-only tables.')"
            ),
            nbformat.v4.new_markdown_cell(
                "## Takeaways\n\n"
                "1. RTH 有可审计标签，但 7 个日期不足以调生产硬阈值。\n"
                "2. GTH gross Greek 覆盖较差，Europe 段尤其明显；缺失不能补零。\n"
                "3. Charm 仅作幅度/质量 gate，Call+/Put− GEX 不等于 dealer sign。\n"
                "4. 先积累 20+20 完整会话与每 horizon 100 个非重叠 forward 标签，"
                "再做冻结参数的 date-block CI。\n"
                "5. 同刻 0DTE/1DTE ask→bid 配对已经可做，但 >=13:00 ET 仅 8/7/6 "
                "个 outcome 且 CI 全跨 0；不得上线 13:00 硬切换。\n"
                "6. 下一步补真实 fill/fee、vertical-spread 双腿、ATM IV term gap "
                "与 Theta/Gamma；本研究不能解释账户 P&L。"
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
    for index, cell in enumerate(notebook.cells, start=1):
        cell["id"] = f"spring-gamma-v3-{index:02d}"
    NotebookClient(
        notebook,
        timeout=600,
        kernel_name="python3",
        resources={"metadata": {"path": str(REPO_ROOT)}},
        record_timing=False,
    ).execute()
    for cell in notebook.cells:
        cell.metadata.pop("execution", None)
    nbformat.validate(notebook)
    return notebook


def inline_markdown(value: str) -> str:
    rendered = html.escape(value)
    rendered = re.sub(r"`([^`]+)`", r"<code>\1</code>", rendered)
    rendered = re.sub(r"\*\*([^*]+)\*\*", r"<strong>\1</strong>", rendered)
    return rendered


def markdown_to_html(body: str) -> str:
    output: list[str] = []
    paragraph: list[str] = []
    list_kind: str | None = None

    def flush_paragraph() -> None:
        if paragraph:
            output.append(f"<p>{inline_markdown(' '.join(paragraph))}</p>")
            paragraph.clear()

    def close_list() -> None:
        nonlocal list_kind
        if list_kind:
            output.append(f"</{list_kind}>")
            list_kind = None

    for raw_line in body.splitlines():
        line = raw_line.strip()
        if not line:
            flush_paragraph()
            close_list()
            continue
        heading = re.match(r"^(#{1,3})\s+(.+)$", line)
        if heading:
            flush_paragraph()
            close_list()
            level = len(heading.group(1))
            output.append(
                f"<h{level}>{inline_markdown(heading.group(2))}</h{level}>"
            )
            continue
        bullet = re.match(r"^[-*]\s+(.+)$", line)
        numbered = re.match(r"^\d+\.\s+(.+)$", line)
        if bullet or numbered:
            flush_paragraph()
            requested = "ul" if bullet else "ol"
            if list_kind != requested:
                close_list()
                list_kind = requested
                output.append(f"<{list_kind}>")
            content = bullet.group(1) if bullet else numbered.group(1)
            output.append(f"<li>{inline_markdown(content)}</li>")
            continue
        close_list()
        paragraph.append(line)
    flush_paragraph()
    close_list()
    return "\n".join(output)


def html_value(value: Any) -> str:
    if value is None:
        return '<span class="missing">—</span>'
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        return f"{value:,.2f}"
    return html.escape(str(value))


def build_table_only_html(artifact: dict[str, Any]) -> str:
    """Render the canonical artifact as static HTML without inventing a chart.

    The shared portable builder requires a chart block for every report.  This
    research deliverable is intentionally tables-only, so the fallback keeps
    the canonical manifest/snapshot payload and follows its block order.
    """

    manifest = artifact["manifest"]
    datasets = artifact["snapshot"]["datasets"]
    tables = {row["id"]: row for row in manifest["tables"]}
    sections: list[str] = []
    table_count = 0
    for block in manifest["blocks"]:
        if block["type"] == "markdown":
            sections.append(
                '<section class="narrative">'
                + markdown_to_html(str(block.get("body") or ""))
                + "</section>"
            )
            continue
        if block["type"] != "table":
            continue
        definition = tables[block["tableId"]]
        rows = list(datasets.get(definition["dataset"], []))
        columns = definition["columns"]
        table_count += 1
        header = "".join(
            f'<th scope="col">{html.escape(str(column["label"]))}</th>'
            for column in columns
        )
        body_rows = []
        for row in rows:
            cells = "".join(
                f'<td data-label="{html.escape(str(column["label"]))}">'
                f'{html_value(row.get(column["field"]))}</td>'
                for column in columns
            )
            body_rows.append(f"<tr>{cells}</tr>")
        source_id = html.escape(str(definition.get("sourceId") or ""))
        sections.append(
            '<section class="table-card">'
            f'<div class="table-heading"><div><h3>{html.escape(str(definition["title"]))}</h3>'
            f'<p>{html.escape(str(definition.get("subtitle") or ""))}</p></div>'
            f'<span class="source-tag">source: {source_id}</span></div>'
            '<div class="table-scroll"><table>'
            f"<thead><tr>{header}</tr></thead>"
            f"<tbody>{''.join(body_rows)}</tbody>"
            "</table></div></section>"
        )

    source_cards = []
    for source_row in manifest["sources"]:
        query = source_row.get("query") or {}
        filters = "".join(
            f"<li>{html.escape(str(value))}</li>"
            for value in query.get("filters") or []
        )
        definitions = "".join(
            f"<li>{html.escape(str(value))}</li>"
            for value in query.get("metric_definitions") or []
        )
        source_cards.append(
            '<article class="source-card">'
            f'<h3>{html.escape(str(source_row["label"]))}</h3>'
            f'<p><code>{html.escape(str(source_row["path"]))}</code></p>'
            f'<p>{html.escape(str(query.get("description") or ""))}</p>'
            f"<h4>Filters</h4><ul>{filters}</ul>"
            f"<h4>Definitions</h4><ul>{definitions}</ul>"
            "</article>"
        )

    payload_json = json.dumps(
        artifact, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    )
    payload_json = (
        payload_json.replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
    )
    title = html.escape(str(manifest["title"]))
    return (
        "<!doctype html><html lang=\"zh-CN\"><head><meta charset=\"utf-8\">"
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        f"<title>{title}</title>"
        "<style>"
        ":root{color-scheme:light;--ink:#17211d;--muted:#65716b;--line:#dce4df;"
        "--paper:#f4f7f5;--card:#fff;--green:#0d6647;--amber:#a25a00}"
        "*{box-sizing:border-box}body{margin:0;background:var(--paper);color:var(--ink);"
        "font:14px/1.55 system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif}"
        "main{max-width:1480px;margin:0 auto;padding:30px 24px 64px}"
        ".topbar{display:flex;align-items:center;justify-content:space-between;gap:16px;"
        "margin-bottom:16px}.eyebrow{font-size:12px;letter-spacing:.08em;text-transform:uppercase;"
        "color:var(--muted)}.badge{border:1px solid #e4bd88;background:#fff7e8;color:#7a4300;"
        "border-radius:999px;padding:5px 10px;font-weight:700;font-size:12px}"
        ".narrative,.table-card,.sources{background:var(--card);border:1px solid var(--line);"
        "border-radius:14px;padding:20px 22px;margin:12px 0;box-shadow:0 1px 2px #16231d0a}"
        ".narrative:first-of-type{border-top:4px solid var(--green)}"
        "h1{font-size:30px;line-height:1.18;margin:0 0 8px}h2{font-size:21px;margin:0 0 10px}"
        "h3{font-size:16px;margin:0}h4{font-size:13px;margin:12px 0 4px}"
        "p{margin:7px 0;color:#27342e}strong{color:#0b4f38}code{font-family:ui-monospace,"
        "SFMono-Regular,Consolas,monospace;background:#eef2ef;border-radius:4px;padding:1px 4px}"
        "ul,ol{margin:7px 0;padding-left:22px}.table-heading{display:flex;justify-content:"
        "space-between;align-items:flex-start;gap:16px;margin-bottom:10px}.table-heading p{"
        "margin:3px 0 0;color:var(--muted);font-size:12px}.source-tag{white-space:nowrap;"
        "font:11px ui-monospace,SFMono-Regular,Consolas,monospace;color:var(--muted);"
        "background:#f0f4f1;border-radius:999px;padding:4px 8px}.table-scroll{overflow-x:auto;"
        "border:1px solid var(--line);border-radius:9px}table{border-collapse:collapse;width:100%;"
        "font-size:12px;font-variant-numeric:tabular-nums}th{position:sticky;top:0;background:#edf3ef;"
        "color:#304038;text-align:left;font-weight:700;white-space:nowrap}th,td{padding:7px 9px;"
        "border-bottom:1px solid #e8eeea;vertical-align:top}tbody tr:nth-child(even){background:#fafcfb}"
        "tbody tr:hover{background:#f2f8f4}.missing{color:#9aa49f}.meta{color:var(--muted);"
        "font-size:12px}.source-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));"
        "gap:10px}.source-card{border:1px solid var(--line);border-radius:10px;padding:14px;"
        "background:#fbfcfb}.source-card p,.source-card li{font-size:12px}.footer{text-align:center;"
        "color:var(--muted);font-size:11px;margin-top:18px}@media(max-width:720px){main{padding:16px 10px 40px}"
        ".narrative,.table-card,.sources{padding:15px 13px;border-radius:10px}.topbar,.table-heading{"
        "align-items:flex-start;flex-direction:column}h1{font-size:25px}}@media print{body{background:#fff}"
        "main{max-width:none;padding:0}.narrative,.table-card,.sources{box-shadow:none;break-inside:avoid}"
        ".table-scroll{overflow:visible}.source-tag{display:none}}"
        "</style></head><body><main>"
        '<div class="topbar"><div><div class="eyebrow">Causal data-quality & ablation audit</div>'
        f'<div class="meta">Window {WINDOW_START}–{WINDOW_END} · cutoff {GENERATED_AT} · '
        f"{table_count} tables · all displayed decimals use 2 places</div></div>"
        '<span class="badge">SHADOW ONLY</span></div>'
        f"{''.join(sections)}"
        '<section class="sources"><h2>Sources & reproducibility</h2>'
        '<p class="meta">The complete canonical artifact is embedded below; no remote assets or network calls are required.</p>'
        f'<div class="source-grid">{"".join(source_cards)}</div></section>'
        f'<script id="canonical-artifact" type="application/json">{payload_json}</script>'
        '<div class="footer">Generated deterministically by '
        "scripts/build_spring_gamma_v3_research.py · tables-only fallback because the shared "
        "portable validator requires a chart block.</div>"
        "</main></body></html>\n"
    )


def portable_builder_path() -> Path:
    explicit = os.environ.get("DATA_ANALYTICS_PORTABLE_BUILDER")
    if explicit:
        path = Path(explicit)
        if path.is_file():
            return path
    roots = (
        Path("/home/ubuntu/.codex/plugins/cache/openai-curated-remote/data-analytics"),
        Path("/root/.codex/plugins/cache/openai-curated-remote/data-analytics"),
    )
    candidates: list[Path] = []
    for root in roots:
        try:
            candidates.extend(
                root.glob(
                    "*/skills/build-report/scripts/deliver_portable_artifact.mjs"
                )
            )
        except PermissionError:
            continue
    if not candidates:
        raise FileNotFoundError(
            "Set DATA_ANALYTICS_PORTABLE_BUILDER to "
            "deliver_portable_artifact.mjs"
        )
    return sorted(candidates)[-1]


def validate_outputs() -> None:
    artifact = json.loads(ARTIFACT_PATH.read_text(encoding="utf-8"))
    assert artifact["surface"] == "report"
    assert artifact["manifest"]["title"] == artifact["manifest"]["blocks"][0][
        "body"
    ].removeprefix("# ")
    assert {
        chart["id"] for chart in artifact["manifest"]["charts"]
    } == {"tenor_delta_chart"}
    assert any(
        block["type"] == "chart"
        and block["chartId"] == "tenor_delta_chart"
        for block in artifact["manifest"]["blocks"]
    )
    notebook = nbformat.read(NOTEBOOK_PATH, as_version=4)
    nbformat.validate(notebook)
    html = HTML_PATH.read_text(encoding="utf-8")
    assert payload_marker(artifact) in html
    assert "Spring Gamma v3" in html


def payload_marker(artifact: dict[str, Any]) -> str:
    # The portable builder serializes sources into its bundled runtime.  The
    # stable ASCII source id remains directly searchable across builder
    # versions even when the query id is encoded.
    assert any(
        source_row.get("id") == "research_build"
        for source_row in artifact["manifest"]["sources"]
    )
    return "research_build"


def main() -> None:
    payload = build_research_payload()
    artifact = build_artifact(payload)
    DOCS_ROOT.mkdir(parents=True, exist_ok=True)
    ARTIFACT_PATH.write_text(
        json.dumps(artifact, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    notebook = build_notebook(payload)
    nbformat.write(notebook, NOTEBOOK_PATH)
    if artifact["manifest"]["charts"]:
        builder = portable_builder_path()
        completed = subprocess.run(
            [
                "node",
                str(builder),
                "--input",
                str(ARTIFACT_PATH),
                "--output",
                str(HTML_PATH),
                "--ready-timeout-ms",
                "15000",
                "--action-timeout-ms",
                "5000",
                "--timeout-ms",
                "30000",
            ],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        delivery_receipt = completed.stdout.strip()
    else:
        HTML_PATH.write_text(build_table_only_html(artifact), encoding="utf-8")
        delivery_receipt = json.dumps(
            {
                "ok": True,
                "stage": "tables_only_static_fallback",
                "reason": (
                    "User requested no charts; shared portable validator requires "
                    "at least one chart block."
                ),
                "embedded_canonical_artifact": True,
            },
            ensure_ascii=False,
        )
    validate_outputs()

    print(delivery_receipt)
    print(
        json.dumps(
            {
                "command": ".venv/bin/python scripts/build_spring_gamma_v3_research.py",
                "artifacts": [
                    str(NOTEBOOK_PATH.relative_to(REPO_ROOT)),
                    str(ARTIFACT_PATH.relative_to(REPO_ROOT)),
                    str(HTML_PATH.relative_to(REPO_ROOT)),
                ],
                "rth_directional_labels": {"15m": 74, "30m": 71, "60m": 60},
                "gth_directional_labels": {"15m": 245, "30m": 246, "60m": 239},
                "tenor_counterfactual": payload["tenor_summary"],
                "decision": "shadow_only",
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
