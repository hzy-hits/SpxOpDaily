"""Offline Steven episode forward-metrics and baseline validation.

Conclusions produced here apply only to this repository's house ``_proxy``
metrics and observe-only guidance contract. They do **not** validate the
original Steven SPX Options Framework or any vendor Net DEX / DAGEX product.
"""

from __future__ import annotations

import inspect
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from spx_spark.features.bar_builder import SpxBar, bar_hold, bar_to_dict
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET, MarketCalendar
from spx_spark.runtime_config import runtime_value

FORWARD_METRICS_DISCLAIMER = (
    "Results validate only house _proxy metrics / observe_only guidance; "
    "not a validation of the original Steven framework or vendor Net DEX/DAGEX."
)

_HORIZON_MINUTES = {
    "t_plus_5m": 5,
    "t_plus_15m": 15,
    "t_plus_30m": 30,
    "t_plus_60m": 60,
}
_DEFAULT_MAX_SAMPLE_DISTANCE_SECONDS = 30.0
_HOLD_BARS = 2


@dataclass(frozen=True)
class StevenValidationSettings:
    max_horizon_sample_distance_seconds: float = _DEFAULT_MAX_SAMPLE_DISTANCE_SECONDS

    @classmethod
    def from_env(cls) -> StevenValidationSettings:
        try:
            distance = float(
                runtime_value("steven.forward_max_horizon_sample_distance_seconds")
            )
        except Exception:
            distance = _DEFAULT_MAX_SAMPLE_DISTANCE_SECONDS
        return cls(max_horizon_sample_distance_seconds=distance)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return _as_utc(value)
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _as_utc(parsed)


def _finite(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    if not math.isfinite(number):
        return None
    return number


def return_bps(price: float, reference: float) -> float:
    return (price / reference - 1.0) * 10_000.0


def sort_bars(bars: Sequence[SpxBar]) -> tuple[SpxBar, ...]:
    return tuple(sorted(bars, key=lambda bar: (bar.bar_start, bar.interval_seconds)))


def spx_bar_from_dict(payload: Mapping[str, Any]) -> SpxBar:
    bar_start = _parse_datetime(payload.get("bar_start"))
    if bar_start is None:
        raise ValueError("bar_start is required")
    return SpxBar(
        bar_start=bar_start,
        interval_seconds=int(payload.get("interval_seconds") or 60),
        open=float(payload["open"]),
        high=float(payload["high"]),
        low=float(payload["low"]),
        close=float(payload["close"]),
        sample_count=int(payload.get("sample_count") or 0),
        quality=str(payload.get("quality") or "ok"),
        gap_before=bool(payload.get("gap_before", False)),
        provider=str(payload.get("provider") or "unknown"),
    )


def load_bars_jsonl(path: Path) -> tuple[SpxBar, ...]:
    if not path.exists():
        return ()
    bars: list[SpxBar] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        bars.append(spx_bar_from_dict(json.loads(text)))
    return sort_bars(bars)


def load_episode_events_jsonl(path: Path) -> tuple[dict[str, Any], ...]:
    if not path.exists():
        return ()
    events: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text:
            continue
        payload = json.loads(text)
        if isinstance(payload, dict):
            events.append(payload)
    return tuple(events)


def episode_paths(data_root: Path | str, trading_date: str) -> dict[str, Path]:
    root = Path(data_root)
    return {
        "episodes": root / "lake" / "steven" / "episodes" / f"date={trading_date}" / "episode.jsonl",
        "bars_1m": root / "lake" / "steven" / "bars" / f"date={trading_date}" / "spx_bars_1m.jsonl",
        "bars_5m": root / "lake" / "steven" / "bars" / f"date={trading_date}" / "spx_bars_5m.jsonl",
    }


def fold_episode_events(events: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    if not events:
        return None
    ordered = sorted(
        events,
        key=lambda item: (
            int(item.get("seq") or 0),
            str(item.get("recorded_at") or ""),
        ),
    )
    first = ordered[0]
    episode_id = str(first.get("episode_id") or "")
    trading_date = str(first.get("trading_date") or "")
    pre_market: dict[str, Any] | None = None
    triggers: list[dict[str, Any]] = []
    revisions: list[dict[str, Any]] = []
    final_state: str | None = None
    setup_count = 0

    for event in ordered:
        contract = event.get("contract") if isinstance(event.get("contract"), dict) else {}
        event_kind = str(event.get("event_kind") or "")
        seq = int(event.get("seq") or 0)
        if seq == 0 or event_kind == "pre_market_map":
            pre_market = {
                "map": contract.get("map"),
                "regime": contract.get("regime"),
                "data_quality": contract.get("data_quality"),
                "as_of": contract.get("as_of"),
                "contract": contract,
            }
        if event_kind in {"trigger", "state_transition"}:
            trigger = contract.get("trigger")
            if isinstance(trigger, dict) and trigger.get("confirmed") is True:
                triggers.append(dict(trigger))
        to_state = str(event.get("to_state") or contract.get("machine_state") or "")
        if to_state == "SETUP_CONFIRMED":
            setup_count += 1
        if event_kind in {"state_transition", "map_revision", "trigger", "final_state", "pre_market_map"}:
            revisions.append(
                {
                    "seq": seq,
                    "from_state": event.get("from_state"),
                    "to_state": event.get("to_state"),
                    "recorded_at": event.get("recorded_at"),
                    "event_kind": event_kind,
                }
            )
        if event_kind == "final_state" or to_state == "LOCKOUT_OR_REMAP":
            final_state = to_state or "LOCKOUT_OR_REMAP"

    return {
        "episode_id": episode_id,
        "trading_date": trading_date,
        "pre_market_map": pre_market,
        "triggers": triggers,
        "revisions": revisions,
        "final_state": final_state,
        "setup_count": setup_count,
        "forward_metrics": None,
        "events": list(ordered),
    }


def _setup_reference(
    episode: Mapping[str, Any],
) -> tuple[datetime | None, str, float | None, Mapping[str, Any] | None]:
    """Return (reference_at, direction_hypothesis, trigger_level, trigger)."""
    events = episode.get("events")
    if not isinstance(events, list):
        events = []
    for event in events:
        if not isinstance(event, Mapping):
            continue
        contract = event.get("contract") if isinstance(event.get("contract"), Mapping) else {}
        to_state = str(event.get("to_state") or "")
        machine_state = str(contract.get("machine_state") or "")
        trigger = contract.get("trigger") if isinstance(contract.get("trigger"), Mapping) else None
        if to_state == "SETUP_CONFIRMED" or machine_state == "SETUP_CONFIRMED":
            if trigger and trigger.get("confirmed") is True:
                confirmed_at = _parse_datetime(trigger.get("confirmed_at")) or _parse_datetime(
                    event.get("recorded_at")
                )
                direction = str(trigger.get("direction") or "none")
                hypothesis = direction if direction in {"up", "down"} else "range"
                level = _finite(trigger.get("level"))
                return confirmed_at, hypothesis, level, trigger

    # No setup: seq=0 as_of, range baseline only.
    for event in events:
        if not isinstance(event, Mapping):
            continue
        if int(event.get("seq") or -1) != 0:
            continue
        contract = event.get("contract") if isinstance(event.get("contract"), Mapping) else {}
        as_of = _parse_datetime(contract.get("as_of")) or _parse_datetime(event.get("recorded_at"))
        return as_of, "range", None, None

    pre = episode.get("pre_market_map")
    if isinstance(pre, Mapping):
        as_of = _parse_datetime(pre.get("as_of"))
        if as_of is not None:
            return as_of, "range", None, None
    return None, "range", None, None


def _reference_bar(bars: Sequence[SpxBar], reference_at: datetime) -> SpxBar | None:
    closed = [
        bar
        for bar in bars
        if bar.bar_start + timedelta(seconds=bar.interval_seconds) <= reference_at
    ]
    if not closed:
        # Fall back to latest bar that has started at or before reference_at.
        started = [bar for bar in bars if bar.bar_start <= reference_at]
        if not started:
            return None
        return started[-1]
    return closed[-1]


def _nearest_bar(
    bars: Sequence[SpxBar],
    target_at: datetime,
    *,
    max_distance_seconds: float,
) -> tuple[SpxBar | None, float | None]:
    if not bars:
        return None, None
    best: SpxBar | None = None
    best_distance = float("inf")
    for bar in bars:
        distance = abs((bar.bar_start - target_at).total_seconds())
        if distance < best_distance or (
            distance == best_distance and best is not None and bar.bar_start < best.bar_start
        ):
            best = bar
            best_distance = distance
    if best is None or best_distance > max_distance_seconds:
        return None, None
    return best, best_distance


def _null_horizon() -> dict[str, Any]:
    return {"price": None, "return_bps": None, "sample_gap_seconds": None}


def _horizon_payload(bar: SpxBar | None, distance: float | None, reference: float) -> dict[str, Any]:
    if bar is None or distance is None:
        return _null_horizon()
    return {
        "price": bar.close,
        "return_bps": return_bps(bar.close, reference),
        "sample_gap_seconds": float(distance),
    }


def _session_close_at(
    trading_date: str | date,
    *,
    calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
) -> datetime | None:
    if isinstance(trading_date, str):
        day = date.fromisoformat(trading_date)
    else:
        day = trading_date
    session = calendar.session(day)
    if session is None:
        return None
    return _as_utc(session.close_at)


def _mfe_mae(
    bars: Sequence[SpxBar],
    *,
    reference_price: float,
    direction_hypothesis: str,
) -> tuple[float | None, float | None]:
    if not bars:
        return None, None
    high_rets = [return_bps(bar.high, reference_price) for bar in bars]
    low_rets = [return_bps(bar.low, reference_price) for bar in bars]
    if direction_hypothesis == "up":
        return max(high_rets), min(low_rets)
    if direction_hypothesis == "down":
        # Mirror of up: favorable is downside excursion, adverse is upside.
        return max(-value for value in low_rets), min(-value for value in high_rets)
    # range: absolute envelope. mfe = max |deviation|; mae = -that max.
    # Sign convention: mae is non-positive mirror of the largest absolute excursion.
    abs_devs = [abs(value) for value in high_rets + low_rets]
    peak = max(abs_devs)
    return peak, -peak


def _bars_in_window(
    bars: Sequence[SpxBar],
    *,
    start: datetime,
    end: datetime,
) -> tuple[SpxBar, ...]:
    return tuple(bar for bar in bars if start <= bar.bar_start < end)


def _level_sides(direction_hypothesis: str) -> tuple[str, str]:
    """Return (original_side, breakthrough_side) for reclaim/accept.

    Support-style levels (up/range): original=above, breakthrough=below.
    Resistance-style levels (down): original=below, breakthrough=above.
    """
    if direction_hypothesis == "down":
        return "below", "above"
    return "above", "below"


def _first_touch(
    bars: Sequence[SpxBar],
    level: float,
) -> tuple[bool, datetime | None, int | None]:
    for index, bar in enumerate(bars):
        if bar.low <= level <= bar.high:
            touched_at = bar.bar_start + timedelta(seconds=bar.interval_seconds)
            return True, touched_at, index
    return False, None, None


def _first_hold_after(
    bars: Sequence[SpxBar],
    *,
    start_index: int,
    level: float,
    side: str,
    n: int = _HOLD_BARS,
) -> datetime | None:
    if start_index < 0:
        return None
    for end in range(start_index + n, len(bars) + 1):
        window = bars[:end]
        if bar_hold(window, level, side, n):
            last = bars[end - 1]
            return last.bar_start + timedelta(seconds=last.interval_seconds)
    return None


def evaluate_level_outcomes(
    bars: Sequence[SpxBar],
    *,
    level: float | None,
    reference_at: datetime,
    session_close: datetime | None,
    direction_hypothesis: str,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "trigger_level": level,
        "touched": False,
        "touched_at": None,
        "reclaimed": False,
        "reclaimed_at": None,
        "accepted": False,
        "accepted_at": None,
    }
    if level is None or not math.isfinite(level):
        return payload
    end = session_close or (reference_at + timedelta(hours=8))
    window = [bar for bar in bars if bar.bar_start >= reference_at and bar.bar_start < end]
    touched, touched_at, touch_index = _first_touch(window, level)
    payload["touched"] = touched
    payload["touched_at"] = touched_at.isoformat() if touched_at else None
    if not touched or touch_index is None:
        return payload
    original_side, breakthrough_side = _level_sides(direction_hypothesis)
    accepted_at = _first_hold_after(
        window, start_index=touch_index, level=level, side=breakthrough_side
    )
    reclaimed_at = _first_hold_after(
        window, start_index=touch_index, level=level, side=original_side
    )
    payload["accepted"] = accepted_at is not None
    payload["accepted_at"] = accepted_at.isoformat() if accepted_at else None
    payload["reclaimed"] = reclaimed_at is not None
    payload["reclaimed_at"] = reclaimed_at.isoformat() if reclaimed_at else None
    return payload


def compute_forward_metrics(
    episode: Mapping[str, Any],
    bars_1m: Sequence[SpxBar],
    *,
    bars_5m: Sequence[SpxBar] = (),
    calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
    settings: StevenValidationSettings | None = None,
    computed_at: datetime | None = None,
) -> dict[str, Any]:
    del bars_5m  # reserved for future 5m-path metrics; 1m drives v0.1 horizons.
    active = settings or StevenValidationSettings()
    bars = sort_bars(bars_1m)
    trading_date = str(episode.get("trading_date") or "")
    session_close = _session_close_at(trading_date, calendar=calendar) if trading_date else None
    reference_at, direction_hypothesis, trigger_level, _trigger = _setup_reference(episode)

    if reference_at is None or not bars:
        quality = "missing_bars"
        deterministic_computed = computed_at or session_close or datetime(1970, 1, 1, tzinfo=timezone.utc)
        return {
            "computed_at": _as_utc(deterministic_computed).isoformat(),
            "reference_price": None,
            "reference_at": reference_at.isoformat() if reference_at else None,
            "direction_hypothesis": direction_hypothesis,
            "horizons": {
                **{key: _null_horizon() for key in _HORIZON_MINUTES},
                "t_close": _null_horizon(),
            },
            "mfe_bps": None,
            "mae_bps": None,
            "level_outcomes": {
                "trigger_level": trigger_level,
                "touched": False,
                "touched_at": None,
                "reclaimed": False,
                "reclaimed_at": None,
                "accepted": False,
                "accepted_at": None,
            },
            "quality": quality,
            "disclaimer": FORWARD_METRICS_DISCLAIMER,
        }

    ref_bar = _reference_bar(bars, reference_at)
    if ref_bar is None:
        quality = "missing_bars"
        reference_price = None
    else:
        reference_price = ref_bar.close
        quality = "ok"

    horizons: dict[str, Any] = {}
    if reference_price is None:
        for key in _HORIZON_MINUTES:
            horizons[key] = _null_horizon()
        horizons["t_close"] = _null_horizon()
        quality = "missing_bars"
    else:
        any_null = False
        for key, minutes in _HORIZON_MINUTES.items():
            target = reference_at + timedelta(minutes=minutes)
            bar, distance = _nearest_bar(
                bars,
                target,
                max_distance_seconds=active.max_horizon_sample_distance_seconds,
            )
            horizons[key] = _horizon_payload(bar, distance, reference_price)
            if bar is None:
                any_null = True
        close_target = session_close or reference_at
        close_candidates = [bar for bar in bars if bar.bar_start < close_target] if session_close else list(bars)
        if close_candidates:
            close_bar = close_candidates[-1]
            close_distance = abs(
                (
                    close_bar.bar_start
                    + timedelta(seconds=close_bar.interval_seconds)
                    - close_target
                ).total_seconds()
            )
            # Session close bar is the last bar before session close; gap is informational.
            horizons["t_close"] = {
                "price": close_bar.close,
                "return_bps": return_bps(close_bar.close, reference_price),
                "sample_gap_seconds": float(close_distance),
            }
        else:
            horizons["t_close"] = _null_horizon()
            any_null = True
        if any_null and quality == "ok":
            quality = "partial_bars"

    window_end = reference_at + timedelta(minutes=60)
    if session_close is not None:
        window_end = min(window_end, session_close)
    mfe = mae = None
    if reference_price is not None:
        path_bars = _bars_in_window(bars, start=reference_at, end=window_end)
        # Include the reference bar itself when its start is before reference_at
        # but its close defines the entry (path extremes after entry still dominate).
        if not path_bars and ref_bar is not None:
            path_bars = (ref_bar,)
        mfe, mae = _mfe_mae(path_bars, reference_price=reference_price, direction_hypothesis=direction_hypothesis)

    level_outcomes = evaluate_level_outcomes(
        bars,
        level=trigger_level,
        reference_at=reference_at,
        session_close=session_close,
        direction_hypothesis=direction_hypothesis,
    )

    deterministic_computed = computed_at or session_close or max(
        (bar.bar_start for bar in bars),
        default=reference_at,
    )
    return {
        "computed_at": _as_utc(deterministic_computed).isoformat(),
        "reference_price": reference_price,
        "reference_at": reference_at.isoformat(),
        "direction_hypothesis": direction_hypothesis,
        "horizons": horizons,
        "mfe_bps": mfe,
        "mae_bps": mae,
        "level_outcomes": level_outcomes,
        "quality": quality,
        "disclaimer": FORWARD_METRICS_DISCLAIMER,
    }


def attach_forward_metrics(
    episode: Mapping[str, Any],
    bars_1m: Sequence[SpxBar],
    **kwargs: Any,
) -> dict[str, Any]:
    folded = dict(episode)
    folded["forward_metrics"] = compute_forward_metrics(folded, bars_1m, **kwargs)
    return folded


# --- Baselines -----------------------------------------------------------------


def baseline_unconditional_metrics(
    bars_1m: Sequence[SpxBar],
    *,
    trading_date: str | date,
    calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
    settings: StevenValidationSettings | None = None,
) -> dict[str, Any]:
    """09:35 ET buy hypothesis, direction always up."""
    day = date.fromisoformat(trading_date) if isinstance(trading_date, str) else trading_date
    entry = datetime.combine(day, datetime.strptime("09:35", "%H:%M").time(), tzinfo=ET)
    entry_utc = _as_utc(entry)
    synthetic = {
        "episode_id": f"baseline:unconditional:{day.isoformat()}",
        "trading_date": day.isoformat(),
        "events": [
            {
                "seq": 0,
                "event_kind": "pre_market_map",
                "recorded_at": entry_utc.isoformat(),
                "contract": {
                    "as_of": entry_utc.isoformat(),
                    "machine_state": "SETUP_CONFIRMED",
                    "trigger": {
                        "kind": "none",
                        "level": None,
                        "direction": "up",
                        "confirmed": True,
                        "confirmed_at": entry_utc.isoformat(),
                        "source_event_id": "baseline:unconditional",
                    },
                },
            }
        ],
    }
    return compute_forward_metrics(
        synthetic,
        bars_1m,
        calendar=calendar,
        settings=settings,
        computed_at=_session_close_at(day, calendar=calendar),
    )


def opening_range_direction(
    bars_1m: Sequence[SpxBar],
    *,
    trading_date: str | date,
    as_of: datetime,
) -> str:
    """Classify direction from 09:30–10:00 ET opening range break.

    Returns ``up`` (break above range high), ``down`` (break below range low),
    or ``range`` (still inside).
    """
    day = date.fromisoformat(trading_date) if isinstance(trading_date, str) else trading_date
    range_start = _as_utc(datetime.combine(day, datetime.strptime("09:30", "%H:%M").time(), tzinfo=ET))
    range_end = _as_utc(datetime.combine(day, datetime.strptime("10:00", "%H:%M").time(), tzinfo=ET))
    as_of_utc = _as_utc(as_of)
    bars = sort_bars(bars_1m)
    opening = [bar for bar in bars if range_start <= bar.bar_start < range_end]
    if not opening:
        return "range"
    high = max(bar.high for bar in opening)
    low = min(bar.low for bar in opening)
    after = [bar for bar in bars if range_end <= bar.bar_start and bar.bar_start <= as_of_utc]
    if not after:
        # If as_of falls inside the opening window, use last bar in window.
        probe = opening[-1].close
    else:
        probe = after[-1].close
    if probe > high:
        return "up"
    if probe < low:
        return "down"
    return "range"


def baseline_opening_range_metrics(
    bars_1m: Sequence[SpxBar],
    *,
    trading_date: str | date,
    as_of: datetime,
    calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
    settings: StevenValidationSettings | None = None,
) -> dict[str, Any]:
    day = date.fromisoformat(trading_date) if isinstance(trading_date, str) else trading_date
    direction = opening_range_direction(bars_1m, trading_date=day, as_of=as_of)
    entry = _as_utc(as_of)
    synthetic = {
        "episode_id": f"baseline:opening_range:{day.isoformat()}",
        "trading_date": day.isoformat(),
        "events": [
            {
                "seq": 0,
                "event_kind": "pre_market_map",
                "recorded_at": entry.isoformat(),
                "contract": {
                    "as_of": entry.isoformat(),
                    "machine_state": "SETUP_CONFIRMED",
                    "trigger": {
                        "kind": "none",
                        "level": None,
                        "direction": direction if direction in {"up", "down"} else "none",
                        "confirmed": True,
                        "confirmed_at": entry.isoformat(),
                        "source_event_id": "baseline:opening_range",
                    },
                },
            }
        ],
    }
    # Force range hypothesis when inside OR.
    metrics = compute_forward_metrics(
        synthetic,
        bars_1m,
        calendar=calendar,
        settings=settings,
        computed_at=_session_close_at(day, calendar=calendar),
    )
    if direction == "range":
        metrics = dict(metrics)
        metrics["direction_hypothesis"] = "range"
        if metrics.get("reference_price") is not None:
            window_end = entry + timedelta(minutes=60)
            session_close = _session_close_at(day, calendar=calendar)
            if session_close is not None:
                window_end = min(window_end, session_close)
            path = _bars_in_window(sort_bars(bars_1m), start=entry, end=window_end)
            mfe, mae = _mfe_mae(
                path,
                reference_price=float(metrics["reference_price"]),
                direction_hypothesis="range",
            )
            metrics["mfe_bps"] = mfe
            metrics["mae_bps"] = mae
    return metrics


def gex_only_direction(
    *,
    spot: float,
    put_walls: Sequence[float],
    call_walls: Sequence[float],
    max_distance_points: float = 25.0,
) -> str:
    """GEX-only map: near put wall → up; near call wall → down; else range.

    Intentionally walls-only: no DEX/regime inputs.
    """
    nearest_put = None
    nearest_call = None
    if put_walls:
        nearest_put = min(put_walls, key=lambda strike: abs(strike - spot))
    if call_walls:
        nearest_call = min(call_walls, key=lambda strike: abs(strike - spot))
    put_dist = abs(nearest_put - spot) if nearest_put is not None else float("inf")
    call_dist = abs(nearest_call - spot) if nearest_call is not None else float("inf")
    if put_dist <= max_distance_points and put_dist <= call_dist:
        return "up"
    if call_dist <= max_distance_points:
        return "down"
    return "range"


def gex_only_direction_from_walls_payload(payload: Mapping[str, Any], *, spot: float) -> str:
    """Accept a walls-like mapping; reads only put/call wall strikes."""
    put_walls = payload.get("put_walls") or payload.get("support") or ()
    call_walls = payload.get("call_walls") or payload.get("resistance") or ()
    put_levels = [
        float(item["strike"] if isinstance(item, Mapping) else item)
        for item in put_walls
    ]
    call_levels = [
        float(item["strike"] if isinstance(item, Mapping) else item)
        for item in call_walls
    ]
    return gex_only_direction(spot=spot, put_walls=put_levels, call_walls=call_levels)


def build_steven_episode_audit(
    events: Sequence[Mapping[str, Any]],
    bars_1m: Sequence[SpxBar],
    **kwargs: Any,
) -> dict[str, Any] | None:
    folded = fold_episode_events(events)
    if folded is None:
        return None
    return attach_forward_metrics(folded, bars_1m, **kwargs)


def build_replay_payload(
    *,
    trading_date: str,
    data_root: Path | str,
    calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
    settings: StevenValidationSettings | None = None,
) -> dict[str, Any]:
    paths = episode_paths(data_root, trading_date)
    events = load_episode_events_jsonl(paths["episodes"])
    bars = load_bars_jsonl(paths["bars_1m"])
    episode = build_steven_episode_audit(
        events,
        bars,
        calendar=calendar,
        settings=settings,
    )
    unconditional = baseline_unconditional_metrics(
        bars,
        trading_date=trading_date,
        calendar=calendar,
        settings=settings,
    )
    session = calendar.session(date.fromisoformat(trading_date))
    or_as_of = _as_utc(session.open_at + timedelta(minutes=35)) if session else None
    opening = (
        baseline_opening_range_metrics(
            bars,
            trading_date=trading_date,
            as_of=or_as_of,
            calendar=calendar,
            settings=settings,
        )
        if or_as_of is not None
        else None
    )
    return {
        "schema_version": "steven_replay.v0.1",
        "trading_date": trading_date,
        "disclaimer": FORWARD_METRICS_DISCLAIMER,
        "steven_episode": _public_episode(episode) if episode else None,
        "baselines": {
            "unconditional": unconditional,
            "opening_range": opening,
        },
        "bar_count_1m": len(bars),
        "paths": {key: str(value) for key, value in paths.items()},
    }


def _public_episode(episode: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if episode is None:
        return None
    return {
        "episode_id": episode.get("episode_id"),
        "trading_date": episode.get("trading_date"),
        "pre_market_map": episode.get("pre_market_map"),
        "triggers": episode.get("triggers"),
        "revisions": episode.get("revisions"),
        "final_state": episode.get("final_state"),
        "setup_count": episode.get("setup_count"),
        "forward_metrics": episode.get("forward_metrics"),
    }


def assert_gex_only_ignores_dex() -> None:
    """Test helper: gex-only helpers never take or read a DEX field."""
    banned = "net_dex_proxy"
    for fn in (gex_only_direction, gex_only_direction_from_walls_payload):
        assert banned not in inspect.signature(fn).parameters
        assert banned not in inspect.getsource(fn)


def make_bar(
    bar_start: datetime,
    *,
    open_: float | None = None,
    high: float | None = None,
    low: float | None = None,
    close: float,
    sample_count: int = 12,
    quality: str = "ok",
    gap_before: bool = False,
    provider: str = "ibkr",
    interval_seconds: int = 60,
) -> SpxBar:
    price = close
    return SpxBar(
        bar_start=_as_utc(bar_start),
        interval_seconds=interval_seconds,
        open=open_ if open_ is not None else price,
        high=high if high is not None else price,
        low=low if low is not None else price,
        close=close,
        sample_count=sample_count,
        quality=quality,
        gap_before=gap_before,
        provider=provider,
    )


def bars_to_jsonl_lines(bars: Sequence[SpxBar]) -> str:
    return "".join(json.dumps(bar_to_dict(bar), sort_keys=True) + "\n" for bar in bars)
