from __future__ import annotations

import argparse
import json
import math
import os
import urllib.error
import urllib.request
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from spx_spark.config import NotificationSettings, StorageSettings, env_bool, load_dotenv
from spx_spark.features.bar_builder import SpxBar
from spx_spark.greek_reference import (
    load_zero_dte_greeks_snapshots,
    summarize_zero_dte_greeks_session,
)
from spx_spark.iv_surface import (
    IvSurfaceExpiry,
    IvSurfaceSnapshot,
    raw_snapshot_paths_for_window,
    snapshot_from_dict,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET, MarketCalendar, MarketSession
from spx_spark.marketdata import (
    InstrumentType,
    MarketDataQuality,
    Quote,
    as_utc,
    quote_from_dict,
)
from spx_spark.notifier.llm_writer import DEFAULT_SYSTEM_PROMPT
from spx_spark.notifier.missed_queue import append_missed
from spx_spark.notifier.model import CommandRunner, default_runner
from spx_spark.notifier.sinks import (
    any_delivery_ok,
    deliver_trade_push,
    im_delivery_ok,
    run_openclaw_agent,
)
from spx_spark.runtime_config import runtime_value
from spx_spark.steven_validation import (
    FORWARD_METRICS_DISCLAIMER,
    build_steven_episode_audit,
    episode_paths,
    load_bars_jsonl,
    load_episode_events_jsonl,
)


MetricValue = bool | int | float | str | tuple[str, ...] | None


@dataclass(frozen=True)
class ReviewPaths:
    report_dir: Path
    markdown_path: Path
    json_path: Path
    latest_markdown_path: Path
    latest_json_path: Path
    hermes_markdown_path: Path | None
    hermes_latest_markdown_path: Path | None


@dataclass(frozen=True)
class ReviewLlmSettings:
    enabled: bool
    provider: str
    model: str
    url: str
    env_file: str
    timeout_seconds: float
    max_tokens: int

    @classmethod
    def from_env(cls) -> "ReviewLlmSettings":
        load_dotenv()
        return cls(
            enabled=env_bool("SPX_REVIEW_LLM_ENABLED", bool(runtime_value("review.llm_enabled"))),
            provider=os.getenv("SPX_REVIEW_LLM_PROVIDER", str(runtime_value("review.llm_provider"))).strip(),
            model=os.getenv("SPX_REVIEW_LLM_MODEL", str(runtime_value("review.llm_model"))).strip(),
            url=os.getenv(
                "SPX_REVIEW_LLM_URL",
                str(runtime_value("review.llm_url")),
            ).strip(),
            env_file=os.getenv(
                "SPX_REVIEW_LLM_ENV_FILE",
                str(runtime_value("review.llm_env_file")),
            ).strip(),
            timeout_seconds=float(
                os.getenv(
                    "SPX_REVIEW_LLM_TIMEOUT_SECONDS",
                    str(runtime_value("review.llm_timeout_seconds")),
                )
            ),
            max_tokens=int(
                os.getenv("SPX_REVIEW_LLM_MAX_TOKENS", str(runtime_value("review.llm_max_tokens")))
            ),
        )


@dataclass(frozen=True, slots=True)
class ReviewCompletenessPolicy:
    min_index_bucket_ratio: float = 0.90
    max_edge_gap_minutes: float = 15.0
    min_index_live_ratio: float = 0.95
    min_front_option_contracts: int = 20
    min_front_option_strikes: int = 10
    min_front_option_strike_span: float = 50.0
    min_option_usable_ratio: float = 0.90
    min_option_iv_ratio: float = 0.80
    min_surface_bucket_ratio: float = 0.60
    min_surface_iv_ratio: float = 0.50
    min_surface_gamma_ratio: float = 0.50

    def __post_init__(self) -> None:
        ratio_fields = (
            "min_index_bucket_ratio",
            "min_index_live_ratio",
            "min_option_usable_ratio",
            "min_option_iv_ratio",
            "min_surface_bucket_ratio",
            "min_surface_iv_ratio",
            "min_surface_gamma_ratio",
        )
        for field_name in ratio_fields:
            value = float(getattr(self, field_name))
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{field_name} must be between 0 and 1")
        if self.max_edge_gap_minutes < 0:
            raise ValueError("max_edge_gap_minutes must be non-negative")
        if self.min_front_option_contracts < 1:
            raise ValueError("min_front_option_contracts must be positive")
        if self.min_front_option_strikes < 1:
            raise ValueError("min_front_option_strikes must be positive")
        if self.min_front_option_strike_span < 0:
            raise ValueError("min_front_option_strike_span must be non-negative")

    @classmethod
    def from_env(cls) -> "ReviewCompletenessPolicy":
        load_dotenv()
        return cls(
            min_index_bucket_ratio=float(
                os.getenv(
                    "SPX_REVIEW_MIN_INDEX_BUCKET_RATIO",
                    str(runtime_value("review.min_index_bucket_ratio")),
                )
            ),
            max_edge_gap_minutes=float(
                os.getenv(
                    "SPX_REVIEW_MAX_EDGE_GAP_MINUTES",
                    str(runtime_value("review.max_edge_gap_minutes")),
                )
            ),
            min_index_live_ratio=float(
                os.getenv(
                    "SPX_REVIEW_MIN_INDEX_LIVE_RATIO",
                    str(runtime_value("review.min_index_live_ratio")),
                )
            ),
            min_front_option_contracts=int(
                os.getenv(
                    "SPX_REVIEW_MIN_FRONT_OPTION_CONTRACTS",
                    str(runtime_value("review.min_front_option_contracts")),
                )
            ),
            min_front_option_strikes=int(
                os.getenv(
                    "SPX_REVIEW_MIN_FRONT_OPTION_STRIKES",
                    str(runtime_value("review.min_front_option_strikes")),
                )
            ),
            min_front_option_strike_span=float(
                os.getenv(
                    "SPX_REVIEW_MIN_FRONT_OPTION_STRIKE_SPAN",
                    str(runtime_value("review.min_front_option_strike_span")),
                )
            ),
            min_option_usable_ratio=float(
                os.getenv(
                    "SPX_REVIEW_MIN_OPTION_USABLE_RATIO",
                    str(runtime_value("review.min_option_usable_ratio")),
                )
            ),
            min_option_iv_ratio=float(
                os.getenv(
                    "SPX_REVIEW_MIN_OPTION_IV_RATIO",
                    str(runtime_value("review.min_option_iv_ratio")),
                )
            ),
            min_surface_bucket_ratio=float(
                os.getenv(
                    "SPX_REVIEW_MIN_SURFACE_BUCKET_RATIO",
                    str(runtime_value("review.min_surface_bucket_ratio")),
                )
            ),
            min_surface_iv_ratio=float(
                os.getenv(
                    "SPX_REVIEW_MIN_SURFACE_IV_RATIO",
                    str(runtime_value("review.min_surface_iv_ratio")),
                )
            ),
            min_surface_gamma_ratio=float(
                os.getenv(
                    "SPX_REVIEW_MIN_SURFACE_GAMMA_RATIO",
                    str(runtime_value("review.min_surface_gamma_ratio")),
                )
            ),
        )


@dataclass(frozen=True, slots=True)
class ReviewCompletenessCheck:
    name: str
    measured: MetricValue
    threshold: MetricValue
    passed: bool
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "measured": self.measured,
            "threshold": self.threshold,
            "passed": self.passed,
            "reason": self.reason,
        }


def read_env_file_value(path: str, key: str) -> str:
    env_path = Path(path).expanduser()
    try:
        lines = env_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return ""
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        name, value = line.split("=", 1)
        if name.strip() == key:
            return value.strip().strip('"').strip("'")
    return ""


def deepseek_api_key(settings: ReviewLlmSettings) -> str:
    return os.getenv("DEEPSEEK_API_KEY", "").strip() or read_env_file_value(
        settings.env_file,
        "DEEPSEEK_API_KEY",
    )


def resolve_trading_date(
    raw: str | None,
    *,
    now: datetime | None = None,
    calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
) -> date:
    if raw and raw.lower() != "auto":
        return date.fromisoformat(raw)

    return calendar.completed_review_date(now or datetime.now(tz=timezone.utc))


def ready_auto_review_date(
    *,
    now: datetime,
    calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
) -> date | None:
    local_now = now.astimezone(ET)
    session = calendar.session(local_now.date())
    if session is None or local_now < session.review_ready_at:
        return None
    selected = calendar.completed_review_date(local_now)
    if selected != local_now.date():
        return None
    return selected


def session_window(
    trading_date: date,
    *,
    calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
) -> tuple[datetime, datetime, datetime]:
    session = calendar.session(trading_date)
    if session is None:
        raise ValueError(f"{trading_date.isoformat()} is not a trading day")
    return session.open_at, session.close_at, session.review_ready_at


def raw_quote_paths_for_window(
    settings: StorageSettings,
    *,
    start: datetime,
    end: datetime,
) -> list[Path]:
    root = Path(settings.data_root)
    start_utc = start.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    end_utc = end.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    paths: list[Path] = []
    current = start_utc
    while current <= end_utc:
        paths.extend(
            sorted(
                (root / "raw").glob(
                    "provider=*/"
                    f"date={current.strftime('%Y-%m-%d')}/"
                    f"hour={current.strftime('%H')}/"
                    f"{settings.raw_file_name}"
                )
            )
        )
        current += timedelta(hours=1)
    return sorted(dict.fromkeys(paths))


def is_spx_focus_quote(quote: Quote) -> bool:
    instrument = quote.instrument
    canonical = instrument.canonical_id
    if canonical == "index:SPX":
        return True
    if canonical.startswith("future:ES"):
        return True
    if canonical.startswith("future:MES"):
        return True
    if instrument.instrument_type == InstrumentType.OPTION:
        trading_class = (instrument.trading_class or "").upper()
        return (
            instrument.underlier or instrument.symbol
        ).upper() == "SPX" and trading_class.startswith("SPXW")
    return canonical.startswith("option:SPX:SPXW:")


def load_raw_quotes(
    settings: StorageSettings,
    *,
    start: datetime,
    end: datetime,
) -> tuple[Quote, ...]:
    start_utc = start.astimezone(timezone.utc)
    end_utc = end.astimezone(timezone.utc)
    quotes: list[Quote] = []
    for path in raw_quote_paths_for_window(settings, start=start, end=end):
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in lines:
            if not line.strip():
                continue
            try:
                quote = quote_from_dict(json.loads(line))
            except (json.JSONDecodeError, TypeError, ValueError):
                continue
            received = as_utc(quote.received_at)
            if start_utc <= received <= end_utc and is_spx_focus_quote(quote):
                quotes.append(quote)
    return tuple(sorted(quotes, key=lambda item: item.received_at))


def load_surface_snapshots(
    settings: StorageSettings,
    *,
    start: datetime,
    end: datetime,
) -> tuple[IvSurfaceSnapshot, ...]:
    class SurfaceSettings:
        data_root = settings.data_root
        raw_file_name = os.getenv(
            "IV_SURFACE_RAW_FILE_NAME",
            str(runtime_value("iv_surface.raw_file_name")),
        )

    snapshots: list[IvSurfaceSnapshot] = []
    start_utc = start.astimezone(timezone.utc)
    end_utc = end.astimezone(timezone.utc)
    for path in raw_snapshot_paths_for_window(SurfaceSettings(), start=start, end=end):
        if not path.exists():
            continue
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in lines:
            if not line.strip():
                continue
            try:
                snapshot = snapshot_from_dict(json.loads(line))
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                continue
            snapshot_time = snapshot.as_of.astimezone(timezone.utc)
            if start_utc <= snapshot_time <= end_utc:
                snapshots.append(snapshot)
    return tuple(sorted(snapshots, key=lambda item: item.as_of))


def finite_price(quote: Quote) -> float | None:
    price = quote.effective_price
    return price if price is not None and price > 0 else None


def series_stats(quotes: list[Quote]) -> dict[str, Any]:
    rows = [(quote.received_at, finite_price(quote), quote) for quote in quotes]
    rows = [(ts, price, quote) for ts, price, quote in rows if price is not None]
    if not rows:
        return {
            "count": len(quotes),
            "price_count": 0,
            "first": None,
            "last": None,
            "high": None,
            "low": None,
            "change_points": None,
            "change_bps": None,
        }

    first_ts, first_price, _first_quote = rows[0]
    last_ts, last_price, _last_quote = rows[-1]
    high_ts, high_price, _ = max(rows, key=lambda item: item[1])
    low_ts, low_price, _ = min(rows, key=lambda item: item[1])
    quality_counts = Counter(quote.quality.value for quote in quotes)
    provider_counts = Counter(quote.provider.value for quote in quotes)
    return {
        "count": len(quotes),
        "price_count": len(rows),
        "first": first_price,
        "first_at": first_ts.isoformat(),
        "last": last_price,
        "last_at": last_ts.isoformat(),
        "high": high_price,
        "high_at": high_ts.isoformat(),
        "low": low_price,
        "low_at": low_ts.isoformat(),
        "range_points": high_price - low_price,
        "change_points": last_price - first_price,
        "change_bps": (last_price / first_price - 1.0) * 10_000.0 if first_price else None,
        "qualities": dict(sorted(quality_counts.items())),
        "providers": dict(sorted(provider_counts.items())),
    }


def option_quote_summary(quotes: list[Quote]) -> dict[str, Any]:
    option_quotes = [
        quote for quote in quotes if quote.instrument.instrument_type == InstrumentType.OPTION
    ]
    expiries = sorted({quote.instrument.expiry or "unknown" for quote in option_quotes})
    contracts = {quote.instrument.canonical_id for quote in option_quotes}
    spreads = [quote.spread_bps for quote in option_quotes if quote.spread_bps is not None]
    strikes = [
        quote.instrument.strike for quote in option_quotes if quote.instrument.strike is not None
    ]
    return {
        "rows": len(option_quotes),
        "unique_contracts": len(contracts),
        "expiries": expiries,
        "min_strike": min(strikes) if strikes else None,
        "max_strike": max(strikes) if strikes else None,
        "with_iv": sum(
            1 for quote in option_quotes if quote.greeks and quote.greeks.implied_vol is not None
        ),
        "with_gamma": sum(
            1 for quote in option_quotes if quote.greeks and quote.greeks.gamma is not None
        ),
        "with_open_interest": sum(1 for quote in option_quotes if quote.open_interest is not None),
        "avg_spread_bps": sum(spreads) / len(spreads) if spreads else None,
        "quality_counts": dict(
            sorted(Counter(quote.quality.value for quote in option_quotes).items())
        ),
    }


def metric_change(first: float | None, last: float | None) -> dict[str, float | None]:
    return {
        "first": first,
        "last": last,
        "change": last - first if first is not None and last is not None else None,
    }


def expiry_surface_summary(expiry: str, rows: list[IvSurfaceExpiry]) -> dict[str, Any]:
    if not rows:
        return {}
    first = rows[0]
    last = rows[-1]
    return {
        "expiry": expiry,
        "snapshot_count": len(rows),
        "first_as_of": None,
        "last_as_of": None,
        "atm_iv": metric_change(first.atm_iv, last.atm_iv),
        "expected_move_points": metric_change(
            first.expected_move_points, last.expected_move_points
        ),
        "put_skew_ratio": metric_change(first.put_skew_ratio, last.put_skew_ratio),
        "call_skew_ratio": metric_change(first.call_skew_ratio, last.call_skew_ratio),
        "iv_surface_level": metric_change(first.iv_surface_level, last.iv_surface_level),
        "smile_curvature": metric_change(first.smile_curvature, last.smile_curvature),
        "gamma_state_first": first.gamma_state,
        "gamma_state_last": last.gamma_state,
        "zero_gamma_first": first.zero_gamma,
        "zero_gamma_last": last.zero_gamma,
        "put_wall_first": first.put_wall,
        "put_wall_last": last.put_wall,
        "call_wall_first": first.call_wall,
        "call_wall_last": last.call_wall,
        "quality_counts": dict(sorted(Counter(item.surface_fit_quality for item in rows).items())),
        "avg_spread_bps_last": last.avg_spread_bps,
        "iv_coverage_ratio_last": last.iv_coverage_ratio,
        "gamma_coverage_ratio_last": last.gamma_coverage_ratio,
    }


def surface_summary(snapshots: tuple[IvSurfaceSnapshot, ...]) -> dict[str, Any]:
    if not snapshots:
        return {"snapshot_count": 0, "expiries": []}
    by_expiry: dict[str, list[IvSurfaceExpiry]] = {}
    first_seen: dict[str, datetime] = {}
    last_seen: dict[str, datetime] = {}
    for snapshot in snapshots:
        for expiry in snapshot.expiries:
            by_expiry.setdefault(expiry.expiry, []).append(expiry)
            first_seen.setdefault(expiry.expiry, snapshot.as_of)
            last_seen[expiry.expiry] = snapshot.as_of

    last_expiry_order = [expiry.expiry for expiry in snapshots[-1].expiries[:2]]
    ordered_expiries = last_expiry_order + [
        expiry for expiry in sorted(by_expiry) if expiry not in last_expiry_order
    ]
    expiry_rows = []
    for expiry in ordered_expiries[:4]:
        row = expiry_surface_summary(expiry, by_expiry[expiry])
        row["first_as_of"] = first_seen[expiry].isoformat()
        row["last_as_of"] = last_seen[expiry].isoformat()
        expiry_rows.append(row)

    return {
        "snapshot_count": len(snapshots),
        "first_as_of": snapshots[0].as_of.isoformat(),
        "last_as_of": snapshots[-1].as_of.isoformat(),
        "underlier_first": snapshots[0].underlier_price,
        "underlier_last": snapshots[-1].underlier_price,
        "front_vs_next_atm_iv_gap": metric_change(
            snapshots[0].front_vs_next_atm_iv_gap,
            snapshots[-1].front_vs_next_atm_iv_gap,
        ),
        "expiries": expiry_rows,
        "warnings": sorted({warning for snapshot in snapshots for warning in snapshot.warnings}),
    }


def _inside_session(value: datetime, session: MarketSession) -> bool:
    observed = value.astimezone(ET)
    return session.open_at <= observed <= session.close_at


def _greek_snapshot_inside_session(row: dict[str, Any], session: MarketSession) -> bool:
    raw = row.get("as_of")
    if not isinstance(raw, str) or not raw:
        return False
    try:
        observed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return False
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=timezone.utc)
    return _inside_session(observed, session)


def _usable_quote(quote: Quote) -> bool:
    return (
        quote.quality in {MarketDataQuality.LIVE, MarketDataQuality.FROZEN}
        and finite_price(quote) is not None
    )


def _five_minute_bucket_count(values: list[datetime], session: MarketSession) -> int:
    expected = session.expected_five_minute_buckets
    if expected <= 0:
        return 0
    buckets: set[int] = set()
    for value in values:
        observed = value.astimezone(ET)
        if not session.open_at <= observed <= session.close_at:
            continue
        offset = int((observed - session.open_at).total_seconds() // 300)
        buckets.add(min(offset, expected - 1))
    return len(buckets)


def _ratio(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator > 0 else 0.0


def _gap_minutes(first: datetime | None, second: datetime | None) -> float | None:
    if first is None or second is None:
        return None
    return max((second - first).total_seconds() / 60.0, 0.0)


def _ratio_check(
    *,
    name: str,
    numerator: int,
    denominator: int,
    threshold: float,
    label: str,
) -> ReviewCompletenessCheck:
    measured = _ratio(numerator, denominator)
    passed = denominator > 0 and measured >= threshold
    return ReviewCompletenessCheck(
        name=name,
        measured=round(measured, 6),
        threshold=threshold,
        passed=passed,
        reason=(
            f"{label}: {numerator}/{denominator} ({measured:.1%}); required >= {threshold:.1%}"
        ),
    )


def evaluate_review_completeness(
    *,
    session: MarketSession,
    spx_quotes: list[Quote],
    es_quotes: list[Quote],
    quotes: tuple[Quote, ...],
    snapshots: tuple[IvSurfaceSnapshot, ...],
    policy: ReviewCompletenessPolicy,
) -> tuple[ReviewCompletenessCheck, ...]:
    checks: list[ReviewCompletenessCheck] = []
    expected_buckets = session.expected_five_minute_buckets

    for label, series in (("SPX", spx_quotes), ("ES", es_quotes)):
        price_rows = [quote for quote in series if finite_price(quote) is not None]
        usable = [quote for quote in price_rows if _usable_quote(quote)]
        usable_times = sorted(quote.received_at for quote in usable)
        bucket_count = _five_minute_bucket_count(usable_times, session)
        checks.append(
            _ratio_check(
                name=f"{label.lower()}_five_minute_bucket_coverage",
                numerator=bucket_count,
                denominator=expected_buckets,
                threshold=policy.min_index_bucket_ratio,
                label=f"{label} five-minute buckets",
            )
        )

        first_at = usable_times[0].astimezone(ET) if usable_times else None
        first_gap = _gap_minutes(session.open_at, first_at)
        first_passed = (
            first_gap is not None
            and first_at is not None
            and session.open_at <= first_at <= session.close_at
            and first_gap <= policy.max_edge_gap_minutes
        )
        checks.append(
            ReviewCompletenessCheck(
                name=f"{label.lower()}_first_observation_gap_minutes",
                measured=round(first_gap, 6) if first_gap is not None else None,
                threshold=policy.max_edge_gap_minutes,
                passed=first_passed,
                reason=(
                    f"{label} first usable observation gap: "
                    f"{first_gap:.1f} minutes; required <= {policy.max_edge_gap_minutes:g}"
                    if first_gap is not None
                    else f"{label} has no usable live/frozen observation"
                ),
            )
        )

        last_at = usable_times[-1].astimezone(ET) if usable_times else None
        last_gap = _gap_minutes(last_at, session.close_at)
        last_passed = (
            last_gap is not None
            and last_at is not None
            and session.open_at <= last_at <= session.close_at
            and last_gap <= policy.max_edge_gap_minutes
        )
        checks.append(
            ReviewCompletenessCheck(
                name=f"{label.lower()}_last_observation_gap_minutes",
                measured=round(last_gap, 6) if last_gap is not None else None,
                threshold=policy.max_edge_gap_minutes,
                passed=last_passed,
                reason=(
                    f"{label} last usable observation gap: "
                    f"{last_gap:.1f} minutes; required <= {policy.max_edge_gap_minutes:g}"
                    if last_gap is not None
                    else f"{label} has no usable live/frozen observation"
                ),
            )
        )

        live_rows = sum(
            1
            for quote in series
            if quote.quality == MarketDataQuality.LIVE and finite_price(quote) is not None
        )
        checks.append(
            _ratio_check(
                name=f"{label.lower()}_live_ratio",
                numerator=live_rows,
                denominator=len(series),
                threshold=policy.min_index_live_ratio,
                label=f"{label} live observations",
            )
        )

    front_expiry = session.trading_date.strftime("%Y%m%d")
    front_rows = [
        quote
        for quote in quotes
        if quote.instrument.instrument_type == InstrumentType.OPTION
        and quote.instrument.expiry == front_expiry
        and _inside_session(quote.received_at, session)
    ]
    usable_front_rows = [quote for quote in front_rows if _usable_quote(quote)]
    contracts = {quote.instrument.canonical_id for quote in usable_front_rows}
    strikes = {
        float(quote.instrument.strike)
        for quote in usable_front_rows
        if quote.instrument.strike is not None
    }
    rights = {
        quote.instrument.right.value
        for quote in usable_front_rows
        if quote.instrument.right is not None
    }
    strike_span = max(strikes) - min(strikes) if strikes else 0.0

    option_count_metrics = (
        (
            "front_option_unique_contracts",
            len(contracts),
            policy.min_front_option_contracts,
            "unique front-expiry contracts",
        ),
        (
            "front_option_unique_strikes",
            len(strikes),
            policy.min_front_option_strikes,
            "unique front-expiry strikes",
        ),
        (
            "front_option_strike_span",
            strike_span,
            policy.min_front_option_strike_span,
            "front-expiry strike span",
        ),
    )
    for name, measured, threshold, label in option_count_metrics:
        checks.append(
            ReviewCompletenessCheck(
                name=name,
                measured=measured,
                threshold=threshold,
                passed=measured >= threshold,
                reason=f"{label}: {measured:g}; required >= {threshold:g}",
            )
        )

    both_rights = {"C", "P"}.issubset(rights)
    checks.append(
        ReviewCompletenessCheck(
            name="front_option_call_put_coverage",
            measured=tuple(sorted(rights)),
            threshold=("C", "P"),
            passed=both_rights,
            reason=(
                f"front-expiry rights present: {','.join(sorted(rights)) or 'none'}; "
                "required C and P"
            ),
        )
    )
    checks.append(
        _ratio_check(
            name="front_option_usable_ratio",
            numerator=len(usable_front_rows),
            denominator=len(front_rows),
            threshold=policy.min_option_usable_ratio,
            label="usable live/frozen front-expiry option rows",
        )
    )
    iv_rows = sum(
        1
        for quote in usable_front_rows
        if quote.greeks is not None
        and quote.greeks.implied_vol is not None
        and math.isfinite(quote.greeks.implied_vol)
        and quote.greeks.implied_vol > 0
    )
    checks.append(
        _ratio_check(
            name="front_option_iv_coverage_ratio",
            numerator=iv_rows,
            denominator=len(front_rows),
            threshold=policy.min_option_iv_ratio,
            label="front-expiry option rows with usable IV",
        )
    )
    last_option_at = (
        max(quote.received_at for quote in usable_front_rows).astimezone(ET)
        if usable_front_rows
        else None
    )
    last_option_gap = _gap_minutes(last_option_at, session.close_at)
    checks.append(
        ReviewCompletenessCheck(
            name="front_option_last_observation_gap_minutes",
            measured=round(last_option_gap, 6) if last_option_gap is not None else None,
            threshold=policy.max_edge_gap_minutes,
            passed=(
                last_option_gap is not None
                and last_option_at is not None
                and session.open_at <= last_option_at <= session.close_at
                and last_option_gap <= policy.max_edge_gap_minutes
            ),
            reason=(
                f"front-expiry last usable option gap: {last_option_gap:.1f} minutes; "
                f"required <= {policy.max_edge_gap_minutes:g}"
                if last_option_gap is not None
                else "front expiry has no usable option observation"
            ),
        )
    )

    front_surfaces: list[tuple[datetime, IvSurfaceExpiry]] = []
    for snapshot in snapshots:
        if not _inside_session(snapshot.as_of, session):
            continue
        expiry = next((item for item in snapshot.expiries if item.expiry == front_expiry), None)
        if expiry is not None:
            front_surfaces.append((snapshot.as_of, expiry))
    front_surfaces.sort(key=lambda item: item[0])
    surface_buckets = _five_minute_bucket_count(
        [observed_at for observed_at, _expiry in front_surfaces],
        session,
    )
    checks.append(
        _ratio_check(
            name="front_iv_surface_five_minute_bucket_coverage",
            numerator=surface_buckets,
            denominator=expected_buckets,
            threshold=policy.min_surface_bucket_ratio,
            label="front-expiry IV surface five-minute buckets",
        )
    )
    last_surface_at = front_surfaces[-1][0].astimezone(ET) if front_surfaces else None
    last_surface_gap = _gap_minutes(last_surface_at, session.close_at)
    checks.append(
        ReviewCompletenessCheck(
            name="front_iv_surface_last_observation_gap_minutes",
            measured=round(last_surface_gap, 6) if last_surface_gap is not None else None,
            threshold=policy.max_edge_gap_minutes,
            passed=(
                last_surface_gap is not None
                and last_surface_at is not None
                and session.open_at <= last_surface_at <= session.close_at
                and last_surface_gap <= policy.max_edge_gap_minutes
            ),
            reason=(
                f"front-expiry IV surface last gap: {last_surface_gap:.1f} minutes; "
                f"required <= {policy.max_edge_gap_minutes:g}"
                if last_surface_gap is not None
                else "front expiry has no IV surface observation"
            ),
        )
    )

    latest_surface = front_surfaces[-1][1] if front_surfaces else None
    latest_ratios = (
        (
            "latest_front_iv_coverage_ratio",
            latest_surface.iv_coverage_ratio if latest_surface else None,
            policy.min_surface_iv_ratio,
            "latest front-expiry surface IV coverage",
        ),
        (
            "latest_front_gamma_coverage_ratio",
            latest_surface.gamma_coverage_ratio if latest_surface else None,
            policy.min_surface_gamma_ratio,
            "latest front-expiry surface gamma coverage",
        ),
    )
    for name, measured, threshold, label in latest_ratios:
        passed = measured is not None and math.isfinite(measured) and measured >= threshold
        checks.append(
            ReviewCompletenessCheck(
                name=name,
                measured=round(measured, 6) if measured is not None else None,
                threshold=threshold,
                passed=passed,
                reason=(
                    f"{label}: {measured:.1%}; required >= {threshold:.1%}"
                    if measured is not None
                    else f"{label}: missing; required >= {threshold:.1%}"
                ),
            )
        )

    return tuple(checks)


def build_review_payload(
    *,
    trading_date: date,
    settings: StorageSettings,
    now: datetime | None = None,
    policy: ReviewCompletenessPolicy | None = None,
    calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
) -> dict[str, Any]:
    start, end, _ready = session_window(trading_date, calendar=calendar)
    quotes = load_raw_quotes(settings, start=start, end=end)
    snapshots = load_surface_snapshots(settings, start=start, end=end)
    greek_snapshots = load_zero_dte_greeks_snapshots(
        data_root=settings.data_root,
        trading_date=trading_date.isoformat(),
    )
    paths = episode_paths(settings.data_root, trading_date.isoformat())
    steven_events = load_episode_events_jsonl(paths["episodes"])
    steven_bars = load_bars_jsonl(paths["bars_1m"])
    return build_review_payload_from_data(
        trading_date=trading_date,
        quotes=quotes,
        snapshots=snapshots,
        greek_snapshots=greek_snapshots,
        steven_episode_events=steven_events or None,
        steven_bars_1m=steven_bars or None,
        now=now,
        policy=policy,
        calendar=calendar,
    )


def build_review_payload_from_data(
    *,
    trading_date: date,
    quotes: tuple[Quote, ...],
    snapshots: tuple[IvSurfaceSnapshot, ...],
    greek_snapshots: tuple[dict[str, Any], ...] = (),
    steven_episode_events: tuple[dict[str, Any], ...] | None = None,
    steven_bars_1m: tuple[SpxBar, ...] | None = None,
    now: datetime | None = None,
    policy: ReviewCompletenessPolicy | None = None,
    calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
) -> dict[str, Any]:
    session = calendar.session(trading_date)
    if session is None:
        raise ValueError(f"{trading_date.isoformat()} is not a trading day")
    start, end, ready = session.open_at, session.close_at, session.review_ready_at
    quotes = tuple(
        sorted(
            (
                quote
                for quote in quotes
                if _inside_session(quote.received_at, session) and is_spx_focus_quote(quote)
            ),
            key=lambda item: item.received_at,
        )
    )
    snapshots = tuple(
        sorted(
            (snapshot for snapshot in snapshots if _inside_session(snapshot.as_of, session)),
            key=lambda item: item.as_of,
        )
    )
    exact_expiry = trading_date.strftime("%Y%m%d")
    greek_snapshots = tuple(
        row
        for row in greek_snapshots
        if row.get("expiry") == exact_expiry and _greek_snapshot_inside_session(row, session)
    )
    spx_quotes = [quote for quote in quotes if quote.instrument.canonical_id == "index:SPX"]
    es_quotes = [quote for quote in quotes if quote.instrument.canonical_id.startswith("future:ES")]
    mes_quotes = [
        quote for quote in quotes if quote.instrument.canonical_id.startswith("future:MES")
    ]
    active_policy = policy or ReviewCompletenessPolicy.from_env()
    checks = evaluate_review_completeness(
        session=session,
        spx_quotes=spx_quotes,
        es_quotes=es_quotes,
        quotes=quotes,
        snapshots=snapshots,
        policy=active_policy,
    )
    payload = {
        "created_at": (now or datetime.now(tz=timezone.utc)).isoformat(),
        "trading_date": trading_date.isoformat(),
        "session": {
            "timezone": str(ET),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "review_ready_after": ready.isoformat(),
            "early_close": session.early_close,
            "expected_five_minute_buckets": session.expected_five_minute_buckets,
        },
        "coverage": {
            "raw_quote_rows": len(quotes),
            "iv_surface_snapshots": len(snapshots),
            "zero_dte_greeks_snapshots": len(greek_snapshots),
            "spx_rows": len(spx_quotes),
            "es_rows": len(es_quotes),
            "mes_rows": len(mes_quotes),
        },
        "spx": series_stats(spx_quotes),
        "es": series_stats(es_quotes),
        "mes": series_stats(mes_quotes),
        "spxw_options": option_quote_summary(list(quotes)),
        "iv_surface": surface_summary(snapshots),
        "spxw_0dte_greeks_reference": summarize_zero_dte_greeks_session(
            greek_snapshots,
            expiry=exact_expiry,
        ),
        "completeness": {
            "policy": asdict(active_policy),
            "checks": [check.to_dict() for check in checks],
        },
    }
    if steven_episode_events:
        audit = build_steven_episode_audit(
            steven_episode_events,
            steven_bars_1m or (),
            calendar=calendar,
            computed_at=end,
        )
        if audit is not None:
            payload["steven_episode"] = {
                "episode_id": audit.get("episode_id"),
                "trading_date": audit.get("trading_date"),
                "pre_market_map": audit.get("pre_market_map"),
                "triggers": audit.get("triggers"),
                "revisions": audit.get("revisions"),
                "final_state": audit.get("final_state"),
                "setup_count": audit.get("setup_count"),
                "forward_metrics": audit.get("forward_metrics"),
            }
    payload["verdict"] = review_verdict(payload, checks=checks)
    return payload


def review_verdict(
    payload: dict[str, Any],
    *,
    checks: tuple[ReviewCompletenessCheck, ...] | None = None,
) -> dict[str, Any]:
    if checks is None:
        raw_checks = payload.get("completeness", {}).get("checks", [])
        failures = [item for item in raw_checks if not bool(item.get("passed"))]
        warnings = [f"{item.get('name')}: {item.get('reason')}" for item in failures]
        check_count = len(raw_checks)
        passed_count = check_count - len(failures)
    else:
        failures = [check for check in checks if not check.passed]
        warnings = [f"{check.name}: {check.reason}" for check in failures]
        check_count = len(checks)
        passed_count = check_count - len(failures)
    return {
        "status": "complete" if check_count > 0 and not warnings else "degraded",
        "warnings": warnings,
        "required_checks": check_count,
        "passed_checks": passed_count,
    }


def fmt(value: Any, digits: int = 2) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def fmt_bps(value: Any) -> str:
    return "-" if value is None else f"{float(value):+.1f} bps"


def change_cell(metric: dict[str, Any], digits: int = 4) -> str:
    return f"{fmt(metric.get('first'), digits)} -> {fmt(metric.get('last'), digits)} ({fmt(metric.get('change'), digits)})"


def completeness_value(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.4f}"
    if isinstance(value, (list, tuple)):
        return ", ".join(str(item) for item in value) or "-"
    return str(value)


def render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        f"# SPX/SPXW Post-Close Review - {payload['trading_date']}",
        "",
        "Scope: SPX, SPXW option structure, and ES confirmation only. This is a post-session review, not an order recommendation.",
        "",
        "## Summary",
        "",
        f"- Status: `{payload['verdict']['status']}`",
        f"- Raw quote rows: {payload['coverage']['raw_quote_rows']}; IV surface snapshots: {payload['coverage']['iv_surface_snapshots']}",
        f"- SPX rows: {payload['coverage']['spx_rows']}; ES rows: {payload['coverage']['es_rows']}; SPXW contracts: {payload['spxw_options']['unique_contracts']}",
        f"- SPX change: {fmt(payload['spx'].get('change_points'))} pts / {fmt_bps(payload['spx'].get('change_bps'))}; range: {fmt(payload['spx'].get('range_points'))} pts",
        "",
    ]
    warnings = payload["verdict"].get("warnings") or []
    if warnings:
        lines.extend(["## Data Warnings", ""])
        lines.extend(f"- {warning}" for warning in warnings)
        lines.append("")

    checks = payload.get("completeness", {}).get("checks", [])
    lines.extend(
        [
            "## Data Completeness",
            "",
            "| Check | Measured | Threshold | Pass | Reason |",
            "|---|---:|---:|:---:|---|",
        ]
    )
    for check in checks:
        reason = str(check.get("reason") or "-").replace("|", "\\|")
        lines.append(
            f"| {check.get('name', '-')} | {completeness_value(check.get('measured'))} | "
            f"{completeness_value(check.get('threshold'))} | "
            f"{'yes' if check.get('passed') else 'no'} | {reason} |"
        )
    lines.append("")

    lines.extend(
        [
            "## Price Path",
            "",
            "| Instrument | First | Last | Change | High | Low | Rows |",
            "|---|---:|---:|---:|---:|---:|---:|",
            price_row("SPX", payload["spx"]),
            price_row("ES", payload["es"]),
            price_row("MES", payload["mes"]),
            "",
            "## SPXW Quote Coverage",
            "",
            f"- Rows: {payload['spxw_options']['rows']}; unique contracts: {payload['spxw_options']['unique_contracts']}",
            f"- Expiries: {', '.join(payload['spxw_options']['expiries']) if payload['spxw_options']['expiries'] else '-'}",
            f"- Strike window: {fmt(payload['spxw_options']['min_strike'], 0)} - {fmt(payload['spxw_options']['max_strike'], 0)}",
            f"- With IV: {payload['spxw_options']['with_iv']}; with gamma: {payload['spxw_options']['with_gamma']}; with OI: {payload['spxw_options']['with_open_interest']}",
            f"- Average spread: {fmt(payload['spxw_options']['avg_spread_bps'], 1)} bps",
            "",
            "## IV Surface And Walls",
            "",
        ]
    )
    surface = payload["iv_surface"]
    if surface["snapshot_count"] == 0:
        lines.extend(["- No SPXW IV surface snapshots were available.", ""])
    else:
        lines.extend(
            [
                f"- Snapshots: {surface['snapshot_count']} ({surface['first_as_of']} -> {surface['last_as_of']})",
                f"- Underlier: {fmt(surface['underlier_first'])} -> {fmt(surface['underlier_last'])}",
                f"- 0DTE vs next ATM IV gap: {change_cell(surface['front_vs_next_atm_iv_gap'], 4)}",
                "",
                "| Expiry | ATM IV | Exp move | Put skew | Call skew | Gamma | Put wall | Call wall | Quality |",
                "|---|---:|---:|---:|---:|---|---:|---:|---|",
            ]
        )
        for expiry in surface["expiries"][:4]:
            lines.append(surface_row(expiry))
        lines.append("")

    greeks = payload.get("spxw_0dte_greeks_reference")
    lines.extend(["## 0DTE Greeks Reference", ""])
    if not isinstance(greeks, dict) or greeks.get("snapshot_count", 0) == 0:
        lines.extend(
            [
                "- No persisted same-day SPXW Greeks reference snapshots were available.",
                "- This optional shadow layer does not change the review completeness verdict.",
                "",
            ]
        )
    else:
        lines.extend(
            [
                "- Mode: `reference_only`; position sign/direction: `unknown` / `unknown`.",
                f"- Snapshots: {greeks['snapshot_count']} total; {greeks.get('comparison_snapshot_count', 0)} share the comparison universe ({greeks.get('first_as_of')} -> {greeks.get('last_as_of')}).",
                "- Gross values are OI-only absolute sensitivities, not signed dealer exposure or directional signals.",
                f"- Coverage usable ratio: {greeks.get('coverage', {}).get('usable_ratio')}; OI ratio: {greeks.get('coverage', {}).get('oi_ratio')}.",
                "",
                "| Metric | First | Last | Peak |",
                "|---|---:|---:|---:|",
            ]
        )
        for name, metric in (greeks.get("metrics") or {}).items():
            if not isinstance(metric, dict):
                continue
            lines.append(
                f"| {name} | {fmt(metric.get('first'), 6)} | "
                f"{fmt(metric.get('last'), 6)} | {fmt(metric.get('peak'), 6)} |"
            )
        if not greeks.get("metrics"):
            lines.append("| No comparable same-universe pair | - | - | - |")
        lines.append("")

    steven = payload.get("steven_episode")
    if isinstance(steven, dict):
        metrics = steven.get("forward_metrics") if isinstance(steven.get("forward_metrics"), dict) else {}
        lines.extend(
            [
                "## Steven Episode (observe_only audit)",
                "",
                f"- Episode: `{steven.get('episode_id')}`; setups: {steven.get('setup_count')}; "
                f"final_state: `{steven.get('final_state') or '-'}`.",
                f"- Forward quality: `{metrics.get('quality')}`; direction hypothesis: "
                f"`{metrics.get('direction_hypothesis')}`; reference: {fmt(metrics.get('reference_price'))}.",
                f"- MFE/MAE (bps): {fmt_bps(metrics.get('mfe_bps'))} / {fmt_bps(metrics.get('mae_bps'))}.",
                f"- {FORWARD_METRICS_DISCLAIMER}",
                "",
            ]
        )

    lines.extend(
        [
            "## Hermes Attachment Note",
            "",
            "This section is designed to be appended to the local Hermes daily report after the post-close delay. Hidden cross-market inputs remain outside the human-facing SPX review.",
            "",
        ]
    )
    return "\n".join(lines)


def build_llm_writer_prompt(payload: dict[str, Any], deterministic_markdown: str) -> str:
    compact = {
        "trading_date": payload.get("trading_date"),
        "coverage": payload.get("coverage"),
        "verdict": payload.get("verdict"),
        "spx": payload.get("spx"),
        "es": payload.get("es"),
        "spxw_options": payload.get("spxw_options"),
        "iv_surface": payload.get("iv_surface"),
        "spxw_0dte_greeks_reference": payload.get("spxw_0dte_greeks_reference"),
        "completeness": payload.get("completeness"),
    }
    return "\n".join(
        (
            "你是做了十几年 SPX 期权的自营交易员，收盘后给搭档写当日复盘。搭档只做 SPX/SPXW 0DTE/1DTE 买方"
            "(call/put/垂直价差)，他要的不是当日行情回放，而是对账：盘前那张地图说的墙位/gamma/预期波幅，"
            "今天市场兑现了多少、哪里打脸了、明天的剧本要改哪里。",
            "写之前先想清楚(不写出来)：今天价格是被墙拦住的还是根本没碰到墙？pin 是 gamma 压出来的还是碰巧？"
            "预期波幅是高估还是低估了，错在 vol 定价还是错在事件？模型今天哪里说对了、哪里说错了，都要点名。",
            "只允许使用给定 JSON 和模板报告里的事实；不编造价格、新闻、仓位。",
            "搭档只交易 SPX/SPXW；正文只提 SPX、SPXW、ES、IV surface、期权墙、gamma 和数据质量。",
            "输出中文 Markdown。第一行必须是：",
            f"# SPX/SPXW Post-Close Review - {payload.get('trading_date')}",
            "结构紧凑：摘要(第一句话就给结论：今天价格路径相对墙位/预期波幅的表现)、价格路径、SPXW 报价覆盖、"
            "IV 曲面与期权墙、下一交易日检查点。",
            "摘要必须有量化对照：实际波动占预期波幅的比例、收盘相对墙位/zero gamma 的位置(引用具体数字)，"
            "并且明说今天的地形判断是兑现还是被证伪。",
            "下一交易日检查点写成双向 if/then：明早价格在哪些位置之上/之下，分别先看什么、动哪张单。",
            "数据 degraded 时只说明覆盖质量，不给方向判断。",
            "spxw_0dte_greeks_reference 是同日到期的 reference-only 影子层；position_sign/direction 永远 unknown。"
            "负 gamma 不等于看跌，绝不能据此改写 Call/Put 方向；next expiry 只能用于已有 ATM IV gap 对照。",
            "",
            "JSON:",
            json.dumps(compact, ensure_ascii=False, sort_keys=True),
            "",
            "模板报告:",
            deterministic_markdown,
        )
    )


def call_deepseek_writer(
    payload: dict[str, Any],
    deterministic_markdown: str,
    settings: ReviewLlmSettings,
) -> tuple[str | None, str | None]:
    api_key = deepseek_api_key(settings)
    if not api_key:
        return None, "missing DEEPSEEK_API_KEY"
    body = {
        "model": settings.model,
        "messages": [
            {
                "role": "system",
                # Same master-to-apprentice doctrine as the intraday writers so the
                # review carries the identical trading philosophy and voice.
                "content": DEFAULT_SYSTEM_PROMPT,
            },
            {"role": "user", "content": build_llm_writer_prompt(payload, deterministic_markdown)},
        ],
        "temperature": 0.2,
        "max_tokens": settings.max_tokens,
        "stream": False,
    }
    request = urllib.request.Request(
        settings.url,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=settings.timeout_seconds) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:300]
        return None, f"http={exc.code}: {detail}"
    except OSError as exc:
        return None, str(exc)
    try:
        content = json.loads(raw)["choices"][0]["message"]["content"].strip()
    except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
        return None, f"bad response shape: {exc}"
    if not content:
        return None, "empty response"
    expected_title = f"# SPX/SPXW Post-Close Review - {payload.get('trading_date')}"
    if not content.lstrip().startswith(expected_title):
        content = expected_title + "\n\n" + content.lstrip("# \n")
    return content, None


def maybe_write_llm_review(
    payload: dict[str, Any],
    deterministic_markdown: str,
    settings: ReviewLlmSettings | None = None,
) -> str:
    settings = settings or ReviewLlmSettings.from_env()
    payload["llm_writer"] = {
        "enabled": settings.enabled,
        "provider": settings.provider,
        "model": settings.model,
        "status": "disabled",
    }
    if not settings.enabled:
        return deterministic_markdown
    if settings.provider.lower() != "deepseek":
        payload["llm_writer"]["status"] = "fallback_template"
        payload["llm_writer"]["error"] = f"unsupported provider: {settings.provider}"
        return deterministic_markdown
    markdown, error = call_deepseek_writer(payload, deterministic_markdown, settings)
    if error or not markdown:
        payload["llm_writer"]["status"] = "fallback_template"
        payload["llm_writer"]["error"] = error or "empty response"
        return deterministic_markdown
    payload["llm_writer"]["status"] = "ok"
    expected_title = f"# SPX/SPXW Post-Close Review - {payload.get('trading_date')}"
    narrative = markdown.strip()
    if narrative.startswith(expected_title):
        narrative = narrative[len(expected_title) :].lstrip()
    return deterministic_markdown.rstrip() + "\n\n## LLM Commentary\n\n" + narrative + "\n"


def price_row(label: str, stats: dict[str, Any]) -> str:
    return (
        f"| {label} | {fmt(stats.get('first'))} | {fmt(stats.get('last'))} | "
        f"{fmt(stats.get('change_points'))} / {fmt_bps(stats.get('change_bps'))} | "
        f"{fmt(stats.get('high'))} | {fmt(stats.get('low'))} | {stats.get('count', 0)} |"
    )


def surface_row(expiry: dict[str, Any]) -> str:
    qualities = expiry.get("quality_counts") or {}
    quality_text = ", ".join(f"{key}:{value}" for key, value in qualities.items()) or "-"
    return (
        f"| {expiry['expiry']} | {change_cell(expiry['atm_iv'], 4)} | "
        f"{change_cell(expiry['expected_move_points'], 2)} | "
        f"{change_cell(expiry['put_skew_ratio'], 3)} | "
        f"{change_cell(expiry['call_skew_ratio'], 3)} | "
        f"{expiry.get('gamma_state_first')} -> {expiry.get('gamma_state_last')} | "
        f"{fmt(expiry.get('put_wall_first'), 0)} -> {fmt(expiry.get('put_wall_last'), 0)} | "
        f"{fmt(expiry.get('call_wall_first'), 0)} -> {fmt(expiry.get('call_wall_last'), 0)} | "
        f"{quality_text} |"
    )


def default_output_dir(settings: StorageSettings) -> Path:
    return Path(
        os.getenv("SPX_REVIEW_OUTPUT_DIR")
        or Path(settings.data_root) / str(runtime_value("review.output_dir_name"))
    )


def default_latest_markdown_path(settings: StorageSettings) -> Path:
    return Path(
        os.getenv("SPX_REVIEW_LATEST_MARKDOWN_PATH")
        or Path(settings.data_root) / "latest" / "spx_options_review.md"
    )


def default_hermes_export_dir() -> Path:
    return Path(
        os.getenv("SPX_REVIEW_HERMES_EXPORT_DIR")
        or str(runtime_value("review.hermes_export_dir"))
    )


def review_paths(
    *,
    trading_date: date,
    settings: StorageSettings,
    output_dir: Path | None = None,
    latest_markdown_path: Path | None = None,
    hermes_export_dir: Path | None = None,
) -> ReviewPaths:
    root = output_dir or default_output_dir(settings)
    report_dir = root / f"date={trading_date.isoformat()}"
    latest_md = latest_markdown_path or default_latest_markdown_path(settings)
    hermes_path = None
    hermes_latest = None
    if hermes_export_dir is not None:
        hermes_path = hermes_export_dir / f"{trading_date.isoformat()}-spx-options-review.md"
        hermes_latest = hermes_export_dir / "latest-spx-options-review.md"
    return ReviewPaths(
        report_dir=report_dir,
        markdown_path=report_dir / "review.md",
        json_path=report_dir / "review.json",
        latest_markdown_path=latest_md,
        latest_json_path=latest_md.with_suffix(".json"),
        hermes_markdown_path=hermes_path,
        hermes_latest_markdown_path=hermes_latest,
    )


def write_outputs(payload: dict[str, Any], markdown: str, paths: ReviewPaths) -> dict[str, str]:
    paths.report_dir.mkdir(parents=True, exist_ok=True)
    paths.markdown_path.write_text(markdown, encoding="utf-8")
    paths.json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    paths.latest_markdown_path.parent.mkdir(parents=True, exist_ok=True)
    paths.latest_markdown_path.write_text(markdown, encoding="utf-8")
    paths.latest_json_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
    )
    result = {
        "markdown_path": str(paths.markdown_path),
        "json_path": str(paths.json_path),
        "latest_markdown_path": str(paths.latest_markdown_path),
        "latest_json_path": str(paths.latest_json_path),
    }
    if paths.hermes_markdown_path is not None and paths.hermes_latest_markdown_path is not None:
        paths.hermes_markdown_path.parent.mkdir(parents=True, exist_ok=True)
        paths.hermes_markdown_path.write_text(markdown, encoding="utf-8")
        paths.hermes_latest_markdown_path.write_text(markdown, encoding="utf-8")
        result["hermes_markdown_path"] = str(paths.hermes_markdown_path)
        result["hermes_latest_markdown_path"] = str(paths.hermes_latest_markdown_path)
    return result


def build_push_summary(payload: dict[str, Any], *, latest_markdown_path: str) -> str:
    trading_date = payload.get("trading_date", "-")
    spx = payload.get("spx") if isinstance(payload.get("spx"), dict) else {}
    first = spx.get("first")
    last = spx.get("last")
    change_points = spx.get("change_points")
    change_bps = spx.get("change_bps")
    range_points = spx.get("range_points")
    low = spx.get("low")
    high = spx.get("high")

    iv_surface = payload.get("iv_surface") if isinstance(payload.get("iv_surface"), dict) else {}
    expiries = iv_surface.get("expiries") if isinstance(iv_surface.get("expiries"), list) else []
    expected_front_expiry = str(trading_date).replace("-", "")
    front = next(
        (
            item
            for item in expiries
            if isinstance(item, dict) and item.get("expiry") == expected_front_expiry
        ),
        {},
    )

    put_wall_last = front.get("put_wall_last")
    call_wall_last = front.get("call_wall_last")
    zero_gamma_last = front.get("zero_gamma_last")
    gamma_state_last = front.get("gamma_state_last")

    atm_iv = front.get("atm_iv") if isinstance(front.get("atm_iv"), dict) else {}
    put_skew = front.get("put_skew_ratio") if isinstance(front.get("put_skew_ratio"), dict) else {}
    atm_iv_text = f"{fmt(atm_iv.get('first'), 4)}→{fmt(atm_iv.get('last'), 4)}"
    put_skew_text = f"{fmt(put_skew.get('first'), 3)}→{fmt(put_skew.get('last'), 3)}"

    verdict = payload.get("verdict") if isinstance(payload.get("verdict"), dict) else {}
    status = verdict.get("status", "-")
    warnings = verdict.get("warnings") if isinstance(verdict.get("warnings"), list) else []
    warning_text = f" ({', '.join(str(item) for item in warnings)})" if warnings else ""

    change_points_text = "-" if change_points is None else f"{float(change_points):+.1f}"
    change_bps_text = "-" if change_bps is None else f"{float(change_bps):+.0f}"
    range_text = "-" if range_points is None else f"{float(range_points):.1f}"

    lines = [
        f"【盘后复盘 {trading_date}】",
        (
            f"SPX: {fmt(first)}→{fmt(last)}({change_points_text} 点/{change_bps_text}bp), "
            f"区间 {range_text} 点(低 {fmt(low)} 高 {fmt(high)})"
        ),
        (
            f"0DTE 收盘墙位: put {fmt(put_wall_last, 0)} call {fmt(call_wall_last, 0)}, "
            f"zero gamma {fmt(zero_gamma_last, 0)}, gamma {fmt(gamma_state_last)}"
        ),
        f"ATM IV: {atm_iv_text}, put skew: {put_skew_text}",
        f"数据: {status}{warning_text}",
        f"完整报告: {latest_markdown_path}",
    ]
    return "\n".join(lines)


def build_review_push_prompt(payload: dict[str, Any], summary: str) -> str:
    return "\n".join(
        (
            "收盘了，给刚睡醒或还没睡的搭档发一条当日收盘便签。他只做 SPX/SPXW 0DTE 买方，凌晨挂的单已经了结或作废，"
            "他现在想知道的是：今天的地形判断靠不靠谱、明天开盘前要先看什么。",
            "只依据 JSON 与摘要事实。输出中文最多 12 行，第一行必须是摘要第一行。",
            "必须覆盖：当日价格路径一句话(相对预期波幅走了多少)、墙位/zero gamma/gamma state 的收盘位、IV 与 skew 当日变化；",
            "然后 2-3 句结构点评，要下判断不要罗列：pin 是 gamma 压出来的还是碰巧、墙被打穿过没有、IV 是 crush 还是抬升、"
            "今天地图哪里说对了哪里说错了；",
            "最后 2-3 条『下一交易日开盘前检查项』，写成看什么、到什么位置意味着什么。数据 degraded 时如实说明。",
            "JSON:" + json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            "摘要:" + summary,
        )
    )


def push_review(
    payload: dict[str, Any],
    *,
    latest_markdown_path: str,
    runner: CommandRunner = default_runner,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(tz=timezone.utc)
    if not env_bool("SPX_REVIEW_PUSH_ENABLED", bool(runtime_value("review.push_enabled"))):
        return {"skipped": True, "reason": "push_disabled"}

    settings = NotificationSettings.from_env()
    summary = build_push_summary(payload, latest_markdown_path=latest_markdown_path)
    used_agent = False
    text = summary

    if settings.openclaw_agent_enabled:
        sink, reply = run_openclaw_agent(
            settings,
            build_review_push_prompt(payload, summary),
            runner=runner,
        )
        if reply and sink.ok:
            text = reply
            used_agent = True

    delivery_sinks = deliver_trade_push(
        settings,
        title="盘后复盘",
        text=text,
        kind="post_close_review",
        lane="trade",
        friend=True,
        runner=runner,
    )
    delivered_ok = any_delivery_ok(delivery_sinks)
    if not im_delivery_ok(delivery_sinks):
        append_missed(settings.missed_queue_path, text, kind="post_close_review", at=now)

    return {
        "text": text,
        "used_agent": used_agent,
        "im_ok": any(s.sink == "feishu" and s.ok for s in delivery_sinks),
        "bark_ok": any(s.sink == "bark" and s.ok for s in delivery_sinks),
        "feishu_ok": any(s.sink == "feishu" and s.ok for s in delivery_sinks),
        "delivered_ok": delivered_ok,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build an SPX/SPXW post-close daily review.")
    parser.add_argument("--date", default="auto", help="NY trading date, YYYY-MM-DD, or auto.")
    parser.add_argument("--json", action="store_true", help="Print JSON payload.")
    parser.add_argument("--markdown", action="store_true", help="Print Markdown report.")
    parser.add_argument("--no-write", action="store_true", help="Do not write report artifacts.")
    parser.add_argument("--output-dir", help="Report output directory.")
    parser.add_argument("--latest-markdown-path", help="Latest Markdown path.")
    parser.add_argument("--hermes-export-dir", help="Hermes daily attachment export directory.")
    parser.add_argument(
        "--no-hermes-export", action="store_true", help="Do not write Hermes export files."
    )
    parser.add_argument(
        "--llm", action="store_true", help="Force-enable the configured LLM writer."
    )
    parser.add_argument(
        "--no-llm", action="store_true", help="Disable the LLM writer for this run."
    )
    parser.add_argument(
        "--quiet-if-empty",
        action="store_true",
        help="Suppress stdout when there are no raw rows or snapshots.",
    )
    parser.add_argument(
        "--no-push", action="store_true", help="Do not push review summary to Feishu/Bark."
    )
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = StorageSettings.from_env()
    run_now = datetime.now(tz=timezone.utc)
    if args.date.lower() == "auto":
        selected_date = ready_auto_review_date(now=run_now)
        if selected_date is None:
            return 0
        trading_date = selected_date
    else:
        trading_date = resolve_trading_date(args.date, now=run_now)
    payload = build_review_payload(trading_date=trading_date, settings=settings, now=run_now)
    markdown = render_markdown(payload)
    llm_settings = ReviewLlmSettings.from_env()
    if args.llm:
        llm_settings = ReviewLlmSettings(
            enabled=True,
            provider=llm_settings.provider,
            model=llm_settings.model,
            url=llm_settings.url,
            env_file=llm_settings.env_file,
            timeout_seconds=llm_settings.timeout_seconds,
            max_tokens=llm_settings.max_tokens,
        )
    if args.no_llm:
        llm_settings = ReviewLlmSettings(
            enabled=False,
            provider=llm_settings.provider,
            model=llm_settings.model,
            url=llm_settings.url,
            env_file=llm_settings.env_file,
            timeout_seconds=llm_settings.timeout_seconds,
            max_tokens=llm_settings.max_tokens,
        )
    markdown = maybe_write_llm_review(payload, markdown, llm_settings)
    paths_payload = None
    if not args.no_write:
        hermes_export_dir = (
            None
            if args.no_hermes_export
            else (
                Path(args.hermes_export_dir)
                if args.hermes_export_dir
                else default_hermes_export_dir()
            )
        )
        paths = review_paths(
            trading_date=trading_date,
            settings=settings,
            output_dir=Path(args.output_dir) if args.output_dir else None,
            latest_markdown_path=Path(args.latest_markdown_path)
            if args.latest_markdown_path
            else None,
            hermes_export_dir=hermes_export_dir,
        )
        paths_payload = write_outputs(payload, markdown, paths)
        payload["paths"] = paths_payload

    latest_markdown_path = str(
        paths_payload["latest_markdown_path"]
        if paths_payload
        else default_latest_markdown_path(settings)
    )
    if not args.no_push:
        coverage = payload["coverage"]
        if not (coverage["raw_quote_rows"] == 0 and coverage["iv_surface_snapshots"] == 0):
            payload["push"] = push_review(payload, latest_markdown_path=latest_markdown_path)

    if (
        args.quiet_if_empty
        and payload["coverage"]["raw_quote_rows"] == 0
        and payload["coverage"]["iv_surface_snapshots"] == 0
    ):
        return 0
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(markdown)
        if paths_payload:
            print(f"\nWrote review: {paths_payload['markdown_path']}")
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
