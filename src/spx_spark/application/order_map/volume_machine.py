"""ES volume-break observation and watch state machine helpers."""

from __future__ import annotations

import json
import os
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

from spx_spark.analytics.options.pricing import finite_float
from spx_spark.config import NY_TZ, StorageSettings
from spx_spark.settings.order_map import DEFAULT_ORDER_MAP_POLICY, OrderMapPolicy


ES_VOLUME_SESSION_OPEN_ET = time(18, 0)
ES_VOLUME_MIN_WINDOW_MINUTES = 3.0
ES_VOLUME_MAX_WINDOW_MINUTES = 120.0
ES_VOLUME_ELEVATED_RATIO = 1.5
ES_VOLUME_QUIET_RATIO = 0.5
ES_VOLUME_MAX_SAMPLES = 16
ES_VOLUME_MAX_QUOTE_AGE_SECONDS = 900.0
# Direction flat band: moves smaller than this are noise for a 15-30m window.
ES_VOLUME_FLAT_POINTS = 3.0
# "Near a level" band for location classification.
ES_VOLUME_LEVEL_BAND_POINTS = 8.0
# Break watch: after a key level is crossed, wait at least this long before
# calling hold vs reclaim (avoids labeling a one-tick pierce).
ES_VOLUME_RECLAIM_MIN_MINUTES = 10.0
ES_VOLUME_RECLAIM_MAX_MINUTES = 90.0


def default_es_volume_sample_path(settings: StorageSettings) -> str:
    return os.getenv("SPX_ES_VOLUME_SAMPLE_PATH") or str(
        Path(settings.data_root) / "latest" / "es_volume_samples.json"
    )


def load_es_volume_samples(path: str) -> list[dict[str, Any]]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    samples = payload.get("samples") if isinstance(payload, dict) else None
    return [item for item in samples or [] if isinstance(item, dict)]


def load_es_volume_break_watch(path: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    watch = payload.get("break_watch") if isinstance(payload, dict) else None
    return watch if isinstance(watch, dict) else None


def save_es_volume_state(
    path: str,
    samples: list[dict[str, Any]],
    *,
    break_watch: dict[str, Any] | None = None,
    max_samples: int = ES_VOLUME_MAX_SAMPLES,
) -> None:
    file_path = Path(path)
    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        payload: dict[str, Any] = {"samples": samples[-max_samples:]}
        if break_watch is not None:
            payload["break_watch"] = break_watch
        file_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except OSError:
        pass


def save_es_volume_samples(path: str, samples: list[dict[str, Any]]) -> None:
    """Backward-compatible wrapper used by older tests."""
    save_es_volume_state(path, samples, break_watch=load_es_volume_break_watch(path))


def es_session_elapsed_minutes(now: datetime) -> float | None:
    """Minutes since the current Globex session opened (18:00 ET)."""
    local = now.astimezone(NY_TZ)
    session_open = local.replace(hour=18, minute=0, second=0, microsecond=0)
    if local.time() < ES_VOLUME_SESSION_OPEN_ET:
        session_open -= timedelta(days=1)
    elapsed = (local - session_open).total_seconds() / 60.0
    return elapsed if elapsed > 1.0 else None


def _parse_sample(sample: dict[str, Any]) -> tuple[datetime, float, float | None] | None:
    volume = finite_float(sample.get("volume"))
    at_raw = sample.get("at")
    if volume is None or volume <= 0 or not isinstance(at_raw, str):
        return None
    try:
        at = datetime.fromisoformat(at_raw)
    except ValueError:
        return None
    if at.tzinfo is None:
        at = at.replace(tzinfo=timezone.utc)
    price = finite_float(sample.get("price"))
    return at, volume, price


def _window_paces(
    points: list[tuple[datetime, float, float | None]],
    *,
    policy: OrderMapPolicy = DEFAULT_ORDER_MAP_POLICY,
) -> list[float]:
    """Contracts/minute for each valid consecutive sample pair."""
    paces: list[float] = []
    for (prev_at, prev_volume, _), (cur_at, cur_volume, _) in zip(points, points[1:]):
        minutes = (cur_at - prev_at).total_seconds() / 60.0
        if not (
            policy.es_volume_min_window_minutes
            <= minutes
            <= policy.es_volume_max_window_minutes
        ):
            continue
        if cur_volume < prev_volume:  # session rollover inside the pair
            continue
        paces.append((cur_volume - prev_volume) / minutes)
    return paces


def classify_price_direction(
    price_delta: float | None, *, flat_points: float = ES_VOLUME_FLAT_POINTS
) -> str | None:
    if price_delta is None:
        return None
    if price_delta >= flat_points:
        return "up"
    if price_delta <= -flat_points:
        return "down"
    return "flat"


def classify_spot_location(
    spot: float | None,
    *,
    put_wall: float | None,
    call_wall: float | None,
    flip_zone: list[float] | None,
    band: float = ES_VOLUME_LEVEL_BAND_POINTS,
) -> dict[str, Any]:
    """Where spot sits relative to the structural map."""
    result: dict[str, Any] = {
        "location": "unknown",
        "nearest_level": None,
        "distance_to_put_wall": None,
        "distance_to_call_wall": None,
        "distance_to_flip": None,
    }
    if spot is None:
        return result

    candidates: list[tuple[str, float, float]] = []  # kind, strike, signed distance
    if put_wall is not None:
        dist = spot - put_wall
        result["distance_to_put_wall"] = round(dist, 1)
        candidates.append(("put_wall", put_wall, dist))
    if call_wall is not None:
        dist = spot - call_wall
        result["distance_to_call_wall"] = round(dist, 1)
        candidates.append(("call_wall", call_wall, dist))

    flip_mid = None
    if isinstance(flip_zone, list) and len(flip_zone) >= 2:
        lo, hi = finite_float(flip_zone[0]), finite_float(flip_zone[1])
        if lo is not None and hi is not None:
            if lo > hi:
                lo, hi = hi, lo
            flip_mid = (lo + hi) / 2.0
            dist_flip = spot - flip_mid
            result["distance_to_flip"] = round(dist_flip, 1)
            # Strict inside the flip zone wins immediately; the band only
            # participates later via nearest-level selection so a put wall
            # sitting just under the flip is not swallowed by flip.
            if lo <= spot <= hi:
                result["location"] = "in_flip"
                result["nearest_level"] = {
                    "kind": "flip",
                    "strike": round(flip_mid, 1),
                    "distance": round(dist_flip, 1),
                }
                return result
            candidates.append(("flip", flip_mid, dist_flip))

    if put_wall is not None and spot < put_wall - band:
        result["location"] = "below_put_wall"
        result["nearest_level"] = {
            "kind": "put_wall",
            "strike": put_wall,
            "distance": round(spot - put_wall, 1),
        }
        return result
    if call_wall is not None and spot > call_wall + band:
        result["location"] = "above_call_wall"
        result["nearest_level"] = {
            "kind": "call_wall",
            "strike": call_wall,
            "distance": round(spot - call_wall, 1),
        }
        return result

    # Prefer the closest level within band.
    near = [(kind, strike, dist) for kind, strike, dist in candidates if abs(dist) <= band]
    if near:
        kind, strike, dist = min(near, key=lambda item: abs(item[2]))
        if kind == "put_wall":
            result["location"] = "at_put_wall"
        elif kind == "call_wall":
            result["location"] = "at_call_wall"
        else:
            result["location"] = "in_flip"
        result["nearest_level"] = {
            "kind": kind if kind != "flip" else "flip",
            "strike": strike,
            "distance": round(dist, 1),
        }
        return result

    if put_wall is not None and call_wall is not None and put_wall < spot < call_wall:
        result["location"] = "mid_range"
        if candidates:
            kind, strike, dist = min(candidates, key=lambda item: abs(item[2]))
            result["nearest_level"] = {
                "kind": kind if kind != "flip" else "flip",
                "strike": strike,
                "distance": round(dist, 1),
            }
        return result

    result["location"] = "mid_range"
    return result


def classify_volume_price_event(
    *,
    pace: str,
    direction: str | None,
    location: str,
    break_outcome: str | None = None,
) -> dict[str, Any]:
    """Map the four axes onto a single event_id + play hints."""
    event_id = "unclassified"
    sequence: str | None = None
    hints: list[str] = []

    if break_outcome == "reclaimed":
        event_id = "break_reclaimed"
        sequence = "break_reclaim"
        hints.append("破位后已收回：假破概率高，破位追单剧本降权，等站稳再论")
    elif break_outcome == "holds":
        if pace == "elevated":
            event_id = "elevated_break_holds"
            sequence = "break_hold"
            hints.append("放量破位后仍在破位侧：加速/弃守更可信，条件单可按剧本执行")
        elif pace == "quiet":
            event_id = "quiet_breakdown_holds"
            sequence = "break_hold"
            hints.append("缩量破位后仍在破位侧：共识一边倒，走得干净但回抽浅")
        else:
            event_id = "break_holds"
            sequence = "break_hold"

    elif pace == "elevated" and direction == "down" and location in {"at_put_wall", "in_flip"}:
        event_id = "elevated_sell_into_support"
        sequence = "wall_test"
        hints.append("放量砸向支撑/flip：墙在接还是弃守未定；反弹单等缩量收回，破位单等站不稳确认")
    elif pace == "elevated" and direction == "up" and location in {"at_call_wall", "in_flip"}:
        event_id = "elevated_buy_into_resistance"
        sequence = "wall_test"
        hints.append("放量撞阻力/flip：常先假突再回抽；fade 等滞涨，突破单等站稳")
    elif (
        pace == "quiet"
        and direction == "down"
        and location in {"at_put_wall", "in_flip", "below_put_wall"}
    ):
        event_id = "quiet_sell_near_support"
        sequence = "vacuum_or_abandon"
        hints.append("缩量靠近/跌破支撑：可能是弃守阴跌，也可能是真空漂移；站不稳才当破位")
    elif (
        pace == "quiet"
        and direction == "up"
        and location in {"at_call_wall", "in_flip", "above_call_wall"}
    ):
        event_id = "quiet_buy_near_resistance"
        sequence = "vacuum_or_abandon"
        hints.append("缩量靠近/越过阻力：可能是共识上移，也可能是真空；站稳才升级突破")
    elif pace == "quiet" and location == "mid_range":
        event_id = "quiet_mid_range"
        sequence = "vacuum_drift"
        hints.append("中间地带缩量：流动性真空漂移，不是突破信号，不追单")
    elif pace == "elevated" and location == "mid_range":
        event_id = "elevated_mid_range"
        sequence = "dispute"
        hints.append("中间地带放量：分歧对打，半路不追，等墙/flip")
    elif pace == "elevated":
        event_id = "elevated_move"
    elif pace == "quiet":
        event_id = "quiet_move"
    elif pace == "normal":
        event_id = "normal_pace"

    # Reclaim-after-test is layered on by the caller when previous sequence was wall_test.
    return {
        "event_id": event_id,
        "sequence": sequence,
        "play_hints": hints,
    }


def update_break_watch(
    previous: dict[str, Any] | None,
    *,
    spot: float | None,
    put_wall: float | None,
    call_wall: float | None,
    flip_zone: list[float] | None,
    pace: str,
    now: datetime,
    policy: OrderMapPolicy = DEFAULT_ORDER_MAP_POLICY,
) -> tuple[dict[str, Any] | None, str | None]:
    """Track whether a freshly broken key level holds or gets reclaimed.

    Returns (new_watch_or_none, outcome) where outcome is holds/reclaimed/pending/None.
    """
    if spot is None:
        return previous, None

    def flip_bounds() -> tuple[float, float] | None:
        if not isinstance(flip_zone, list) or len(flip_zone) < 2:
            return None
        lo, hi = finite_float(flip_zone[0]), finite_float(flip_zone[1])
        if lo is None or hi is None:
            return None
        return (lo, hi) if lo <= hi else (hi, lo)

    outcome: str | None = None
    watch = dict(previous) if isinstance(previous, dict) else None

    if watch is not None:
        level = finite_float(watch.get("level"))
        side = str(watch.get("broken_side") or "")
        broken_at_raw = watch.get("broken_at")
        if level is not None and isinstance(broken_at_raw, str):
            try:
                broken_at = datetime.fromisoformat(broken_at_raw)
                if broken_at.tzinfo is None:
                    broken_at = broken_at.replace(tzinfo=timezone.utc)
                age_min = (now - broken_at).total_seconds() / 60.0
            except ValueError:
                age_min = None
            if age_min is not None and age_min > policy.es_volume_reclaim_max_minutes:
                watch = None
            elif age_min is not None and age_min >= policy.es_volume_reclaim_min_minutes:
                if side == "below":
                    if spot >= level + policy.es_volume_flat_points:
                        outcome = "reclaimed"
                        watch = None
                    elif spot <= level - policy.es_volume_flat_points:
                        outcome = "holds"
                        # Keep watch so later windows can still say holds, but
                        # refresh timestamp so we don't spam forever.
                        watch["confirmed_at"] = now.isoformat()
                        watch["outcome"] = "holds"
                    else:
                        outcome = "pending"
                elif side == "above":
                    if spot <= level - policy.es_volume_flat_points:
                        outcome = "reclaimed"
                        watch = None
                    elif spot >= level + policy.es_volume_flat_points:
                        outcome = "holds"
                        watch["confirmed_at"] = now.isoformat()
                        watch["outcome"] = "holds"
                    else:
                        outcome = "pending"
            else:
                outcome = "pending"

    # Arm a new watch only when none is active / just cleared by reclaim.
    if watch is None or outcome == "reclaimed":
        bounds = flip_bounds()
        armed = None
        if put_wall is not None and spot < put_wall - policy.es_volume_flat_points:
            armed = {"level": put_wall, "kind": "put_wall", "broken_side": "below"}
        elif call_wall is not None and spot > call_wall + policy.es_volume_flat_points:
            armed = {"level": call_wall, "kind": "call_wall", "broken_side": "above"}
        elif bounds is not None and spot < bounds[0] - policy.es_volume_flat_points:
            armed = {"level": bounds[0], "kind": "flip_low", "broken_side": "below"}
        elif bounds is not None and spot > bounds[1] + policy.es_volume_flat_points:
            armed = {"level": bounds[1], "kind": "flip_high", "broken_side": "above"}
        if armed is not None:
            armed.update(
                {
                    "broken_at": now.isoformat(),
                    "pace_at_break": pace,
                    "spot_at_break": spot,
                }
            )
            watch = armed
            if outcome is None:
                outcome = "pending"

    return watch, outcome


def es_volume_signal(
    cumulative: float | None,
    samples: list[dict[str, Any]],
    *,
    now: datetime,
    spot: float | None = None,
    put_wall: float | None = None,
    call_wall: float | None = None,
    flip_zone: list[float] | None = None,
    break_watch: dict[str, Any] | None = None,
    policy: OrderMapPolicy = DEFAULT_ORDER_MAP_POLICY,
) -> dict[str, Any] | None:
    if cumulative is None or cumulative <= 0:
        return None
    signal: dict[str, Any] = {
        "cumulative": cumulative,
        "delta": None,
        "window_minutes": None,
        "recent_pace_per_min": None,
        "baseline_pace_per_min": None,
        "baseline": None,
        "pace_ratio": None,
        "label": "no_baseline",
        "price": spot,
        "price_delta": None,
        "direction": None,
        "location": "unknown",
        "nearest_level": None,
        "break_outcome": None,
        "break_watch": break_watch,
        "event_id": None,
        "sequence": None,
        "play_hints": [],
    }
    points = [parsed for sample in samples if (parsed := _parse_sample(sample)) is not None]
    points.sort(key=lambda item: item[0])
    if not points:
        loc = classify_spot_location(
            spot,
            put_wall=put_wall,
            call_wall=call_wall,
            flip_zone=flip_zone,
            band=policy.es_volume_level_band_points,
        )
        signal.update(loc)
        return signal
    last_at, last_volume, last_price = points[-1]
    if cumulative < last_volume:
        signal["label"] = "session_reset"
        return signal
    window_minutes = (now - last_at).total_seconds() / 60.0
    if not (
        policy.es_volume_min_window_minutes
        <= window_minutes
        <= policy.es_volume_max_window_minutes
    ):
        loc = classify_spot_location(
            spot,
            put_wall=put_wall,
            call_wall=call_wall,
            flip_zone=flip_zone,
            band=policy.es_volume_level_band_points,
        )
        signal.update(loc)
        return signal
    delta = cumulative - last_volume
    recent_pace = delta / window_minutes

    history_paces = _window_paces(points, policy=policy)
    if len(history_paces) >= 2:
        ordered = sorted(history_paces)
        mid = len(ordered) // 2
        baseline = (
            ordered[mid] if len(ordered) % 2 == 1 else (ordered[mid - 1] + ordered[mid]) / 2.0
        )
        baseline_name = "recent_windows"
    else:
        elapsed = es_session_elapsed_minutes(now)
        if elapsed is None:
            return signal
        baseline = cumulative / elapsed
        baseline_name = "session_average"
    if baseline <= 0:
        return signal

    ratio = recent_pace / baseline
    if ratio >= policy.es_volume_elevated_ratio:
        label = "elevated"
    elif ratio <= policy.es_volume_quiet_ratio:
        label = "quiet"
    else:
        label = "normal"

    price_delta = None
    if spot is not None and last_price is not None:
        price_delta = round(spot - last_price, 1)
    direction = classify_price_direction(price_delta, flat_points=policy.es_volume_flat_points)
    loc = classify_spot_location(
        spot,
        put_wall=put_wall,
        call_wall=call_wall,
        flip_zone=flip_zone,
        band=policy.es_volume_level_band_points,
    )
    new_watch, break_outcome = update_break_watch(
        break_watch,
        spot=spot,
        put_wall=put_wall,
        call_wall=call_wall,
        flip_zone=flip_zone,
        pace=label,
        now=now,
        policy=policy,
    )
    event = classify_volume_price_event(
        pace=label,
        direction=direction,
        location=str(loc.get("location") or "unknown"),
        break_outcome=break_outcome,
    )
    # Sequence upgrade: quiet reclaim after an elevated wall test.
    if (
        label == "quiet"
        and direction == "up"
        and loc.get("location") in {"mid_range", "at_put_wall", "in_flip"}
        and len(points) >= 2
    ):
        # Look at previous window direction via last two priced samples if present.
        prev_priced = [p for p in points[-3:] if p[2] is not None]
        if len(prev_priced) >= 2 and spot is not None:
            prev_delta = prev_priced[-1][2] - prev_priced[-2][2]  # type: ignore[operator]
            if prev_delta is not None and prev_delta <= -policy.es_volume_flat_points:
                event = {
                    "event_id": "quiet_reclaim_after_sell_test",
                    "sequence": "reclaim",
                    "play_hints": ["前窗下跌测试后本窗缩量收回：反弹剧本升温，破位追空降权"],
                }

    signal.update(
        {
            "delta": round(delta),
            "window_minutes": round(window_minutes, 1),
            "recent_pace_per_min": round(recent_pace, 1),
            "baseline_pace_per_min": round(baseline, 1),
            "baseline": baseline_name,
            "pace_ratio": round(ratio, 2),
            "label": label,
            "price": spot,
            "price_delta": price_delta,
            "direction": direction,
            "break_outcome": break_outcome,
            "break_watch": new_watch,
            "event_id": event["event_id"],
            "sequence": event["sequence"],
            "play_hints": event["play_hints"],
        }
    )
    signal.update(loc)
    return signal
