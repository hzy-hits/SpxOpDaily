from __future__ import annotations

import json
import os
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from spx_spark.config import NotificationSettings, StorageSettings, load_dotenv
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
from spx_spark.post_close_render import fmt, render_markdown
from spx_spark.post_close_quality import review_verdict
from spx_spark.settings import settings_value
from spx_spark.steven_validation import (
    build_steven_episode_audit,
    episode_paths,
    load_bars_jsonl,
    load_episode_events_jsonl,
)


MetricValue = bool | int | float | str | tuple[str, ...] | None


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
                    str(settings_value("review.min_index_bucket_ratio")),
                )
            ),
            max_edge_gap_minutes=float(
                os.getenv(
                    "SPX_REVIEW_MAX_EDGE_GAP_MINUTES",
                    str(settings_value("review.max_edge_gap_minutes")),
                )
            ),
            min_index_live_ratio=float(
                os.getenv(
                    "SPX_REVIEW_MIN_INDEX_LIVE_RATIO",
                    str(settings_value("review.min_index_live_ratio")),
                )
            ),
            min_front_option_contracts=int(
                os.getenv(
                    "SPX_REVIEW_MIN_FRONT_OPTION_CONTRACTS",
                    str(settings_value("review.min_front_option_contracts")),
                )
            ),
            min_front_option_strikes=int(
                os.getenv(
                    "SPX_REVIEW_MIN_FRONT_OPTION_STRIKES",
                    str(settings_value("review.min_front_option_strikes")),
                )
            ),
            min_front_option_strike_span=float(
                os.getenv(
                    "SPX_REVIEW_MIN_FRONT_OPTION_STRIKE_SPAN",
                    str(settings_value("review.min_front_option_strike_span")),
                )
            ),
            min_option_usable_ratio=float(
                os.getenv(
                    "SPX_REVIEW_MIN_OPTION_USABLE_RATIO",
                    str(settings_value("review.min_option_usable_ratio")),
                )
            ),
            min_option_iv_ratio=float(
                os.getenv(
                    "SPX_REVIEW_MIN_OPTION_IV_RATIO",
                    str(settings_value("review.min_option_iv_ratio")),
                )
            ),
            min_surface_bucket_ratio=float(
                os.getenv(
                    "SPX_REVIEW_MIN_SURFACE_BUCKET_RATIO",
                    str(settings_value("review.min_surface_bucket_ratio")),
                )
            ),
            min_surface_iv_ratio=float(
                os.getenv(
                    "SPX_REVIEW_MIN_SURFACE_IV_RATIO",
                    str(settings_value("review.min_surface_iv_ratio")),
                )
            ),
            min_surface_gamma_ratio=float(
                os.getenv(
                    "SPX_REVIEW_MIN_SURFACE_GAMMA_RATIO",
                    str(settings_value("review.min_surface_gamma_ratio")),
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
            str(settings_value("iv_surface.raw_file_name")),
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


from spx_spark.post_close_completeness import evaluate_review_completeness  # noqa: E402


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


from spx_spark.post_close_runtime import (  # noqa: E402
    ReviewLlmSettings,
    ReviewPaths,
    build_llm_writer_prompt,
    build_push_summary,
    build_review_push_prompt,
    call_deepseek_writer,
    deepseek_api_key,
    default_hermes_export_dir,
    default_latest_markdown_path,
    default_output_dir,
    main,
    maybe_write_llm_review,
    parse_args,
    push_review,
    ready_auto_review_date,
    read_env_file_value,
    resolve_trading_date,
    review_paths,
    run,
    write_outputs,
)

__all__ = [
    "NotificationSettings",
    "ReviewLlmSettings",
    "ReviewPaths",
    "build_llm_writer_prompt",
    "build_push_summary",
    "build_review_push_prompt",
    "call_deepseek_writer",
    "deepseek_api_key",
    "default_hermes_export_dir",
    "default_latest_markdown_path",
    "default_output_dir",
    "fmt",
    "main",
    "maybe_write_llm_review",
    "parse_args",
    "push_review",
    "ready_auto_review_date",
    "read_env_file_value",
    "render_markdown",
    "review_paths",
    "review_verdict",
    "resolve_trading_date",
    "run",
    "write_outputs",
]

if __name__ == "__main__":
    main()
