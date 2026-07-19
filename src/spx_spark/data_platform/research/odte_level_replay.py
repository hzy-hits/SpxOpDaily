"""Replay the operator's real IBKR spread rounds against exit-rule counterfactuals.

Parses the Chinese IBKR activity statement (交易 section), reconstructs
same-second two-leg SPXW spread rounds (call/put debit and credit), rebuilds
each round's holding path from the quote lake and compares the realized net
PnL against three counterfactual exit rules. Quote paths are point-in-time:
both legs must have a recent quote at or before the mark, and asynchronous
legs outside the configured skew are rejected. Rules decide their own exit
from the actual entry time; the replayed clock rule carries no invalidation
line because the statement records no per-round trough.
"""

from __future__ import annotations

import csv
import logging
import re
from bisect import bisect_left
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Sequence
from zoneinfo import ZoneInfo

from .odte_level_quotes import QuoteStore
from .odte_level_signals import (
    MAX_ENTRY_QUOTE_AGE,
    POINTS_PER_CONTRACT,
    PROVIDERS,
    SATURATION_FRACTION,
    TRAIL33_ARM_FRACTION,
    TRAILING_GIVEBACK_FRACTION,
    OptionTick,
)

logger = logging.getLogger(__name__)

STATEMENT_TZ = ZoneInfo("America/New_York")  # IBKR activity statement local time
RULES = ("sat85", "trail33", "clock")
MAX_MARK_QUOTE_AGE = timedelta(seconds=30)
MAX_LEG_SKEW = timedelta(seconds=5)
_EXIT_CLOCK_ET = time(9, 45)
_EXPIRY_CLOSE_ET = time(16, 0)
_RTH_OPEN_ET = time(9, 30)
_SYMBOL_RE = re.compile(r"^SPXW (\d{2})([A-Z]{3})(\d{2}) ([\d.]+) ([CP])$")
_MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


@dataclass(frozen=True)
class Fill:
    """One SPXW option fill from the statement's 交易 section."""

    expiry: date
    strike: float
    right: str
    at: datetime  # UTC
    qty: int
    price: float
    proceeds: float
    commission: float
    realized: float
    flags: str

    @property
    def contract(self) -> tuple[date, float, str]:
        return (self.expiry, self.strike, self.right)

    @property
    def is_open(self) -> bool:
        # the statement's O/C code column is unreliable (O;P/C;P combos); the
        # practical rule: open rows carry zero realized PnL, else trust the flag
        if self.realized == 0:
            return True
        return "O" in self.flags and "C" not in self.flags


@dataclass(frozen=True)
class SpreadRound:
    """One two-leg same-second spread opening plus its closing fills."""

    round_id: str
    expiry: date
    right: str
    kind: str  # "debit" | "credit"
    pos_strike: float  # strike of the qty>0 leg
    neg_strike: float  # strike of the qty<0 leg
    width: float
    units: int
    open_at: datetime  # UTC
    entry_per_unit: float  # debit paid / credit received per unit
    close_at: datetime
    actual_pnl: float  # statement realized, net of commissions
    commissions: float
    fills: tuple[Fill, ...]

    @property
    def structure(self) -> str:
        label = "借记" if self.kind == "debit" else "卖方"
        return (
            f"{self.expiry:%m-%d} {self.right} {self.pos_strike:g}/{self.neg_strike:g} "
            f"{label} w{self.width:g} ×{self.units}"
        )


@dataclass(frozen=True)
class RuleExit:
    rule: str
    exit_at: datetime
    pnl_per_unit: float
    reason: str  # saturation | trailing_tp | time_stop


@dataclass(frozen=True)
class ReplayAttempt:
    """One rule attempt, including a stable reason when it is not replayable."""

    exit: RuleExit | None
    skip_reason: str | None = None


def _parse_symbol(symbol: str) -> tuple[date, float, str] | None:
    match = _SYMBOL_RE.match(symbol.strip())
    if not match:
        return None
    day, month, year, strike, right = match.groups()
    expiry = date(2000 + int(year), _MONTHS[month], int(day))
    return expiry, float(strike), right


def _parse_ts(raw: str) -> datetime:
    local = datetime.strptime(raw.strip(), "%Y-%m-%d, %H:%M:%S")
    return local.replace(tzinfo=STATEMENT_TZ).astimezone(timezone.utc)


def next_replay_exit_clock(at: datetime) -> datetime:
    """Next 09:45 America/New_York clock strictly after ``at``.

    The UTC instant is 13:45 during EDT and 14:45 during EST. Constructing the
    clock in the market timezone avoids silently shifting the winter exit by
    one hour.
    """
    local = at.astimezone(STATEMENT_TZ)
    candidate = datetime.combine(local.date(), _EXIT_CLOCK_ET, tzinfo=STATEMENT_TZ)
    if candidate <= local:
        candidate = datetime.combine(
            local.date() + timedelta(days=1), _EXIT_CLOCK_ET, tzinfo=STATEMENT_TZ
        )
    return candidate.astimezone(timezone.utc)


def replay_expiry_close(expiry: date) -> datetime:
    """The expiry date's 16:00 America/New_York close, expressed in UTC."""
    return datetime.combine(expiry, _EXPIRY_CLOSE_ET, tzinfo=STATEMENT_TZ).astimezone(timezone.utc)


def parse_statement_fills(path: Path) -> list[Fill]:
    """Parse SPXW option fills from the 交易 section (utf-8-sig CSV)."""
    fills: list[Fill] = []
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.reader(handle):
            if len(row) < 16 or row[0] != "交易" or row[1] != "Data":
                continue
            if row[3] != "股票和指数期权" or not row[5].strip().startswith("SPXW"):
                continue
            parsed = _parse_symbol(row[5])
            if parsed is None:
                continue
            expiry, strike, right = parsed
            try:
                fills.append(
                    Fill(
                        expiry=expiry,
                        strike=strike,
                        right=right,
                        at=_parse_ts(row[6]),
                        qty=int(float(row[7])),
                        price=float(row[8]),
                        proceeds=float(row[10]),
                        commission=float(row[11]),
                        realized=float(row[13]),
                        flags=row[15].strip(),
                    )
                )
            except ValueError:
                logger.warning("skipping unparsable fill row: %s", row[:9])
    return fills


def _avg_price(fills: Sequence[Fill]) -> float:
    """Weighted average price of one contract's fills (proceeds/qty based)."""
    qty = sum(fill.qty for fill in fills)
    if qty == 0:
        return 0.0
    return -sum(fill.proceeds for fill in fills) / (qty * POINTS_PER_CONTRACT)


def pair_spread_rounds(fills: Sequence[Fill]) -> tuple[list[SpreadRound], int]:
    """Pair same-second two-leg openings with their closing fills.

    Returns (rounds, naked_groups) where naked_groups counts same-second
    single-contract groups (naked positions, excluded from the replay).
    """
    groups: dict[tuple[datetime, date], list[Fill]] = {}
    for fill in fills:
        groups.setdefault((fill.at, fill.expiry), []).append(fill)
    rounds: list[SpreadRound] = []
    naked_groups = 0
    # in-progress rounds: list of [round_meta, remaining qty per contract, fills]
    open_rounds: list[dict] = []
    for (at, expiry), group in sorted(groups.items()):
        by_contract: dict[tuple[date, float, str], list[Fill]] = {}
        for fill in group:
            by_contract.setdefault(fill.contract, []).append(fill)
        qtys = {
            contract: sum(fill.qty for fill in fills) for contract, fills in by_contract.items()
        }
        signs = {(qty > 0) - (qty < 0) for qty in qtys.values()}
        is_spread_open = (
            len(by_contract) >= 2 and signs == {-1, 1} and all(fill.is_open for fill in group)
        )
        if is_spread_open:
            pos = [contract for contract, qty in qtys.items() if qty > 0][0]
            neg = [contract for contract, qty in qtys.items() if qty < 0][0]
            units = min(abs(qtys[pos]), abs(qtys[neg]))
            entry_cash = -sum(fill.proceeds for fill in group) / (units * POINTS_PER_CONTRACT)
            # entry_cash > 0 => net premium paid (debit); < 0 => received (credit)
            open_rounds.append(
                {
                    "expiry": expiry,
                    "right": pos[2],
                    "kind": "debit" if entry_cash > 0 else "credit",
                    "pos_strike": pos[1],
                    "neg_strike": neg[1],
                    "units": units,
                    "open_at": at,
                    "entry_per_unit": abs(entry_cash),
                    "remaining": dict(qtys),
                    "fills": list(group),
                    "close_at": at,
                }
            )
            continue
        # otherwise: closing activity for in-progress rounds (or naked flow)
        matched = False
        for fill in group:
            for open_round in open_rounds:
                remaining = open_round["remaining"]
                if fill.contract in remaining and remaining[fill.contract] != 0:
                    remaining[fill.contract] += fill.qty
                    open_round["fills"].append(fill)
                    open_round["close_at"] = max(open_round["close_at"], fill.at)
                    matched = True
                    break
        if not matched and len(by_contract) < 2 and all(fill.is_open for fill in group):
            naked_groups += 1  # single-contract opening group (naked position)
        still_open = []
        for open_round in open_rounds:
            if any(qty != 0 for qty in open_round["remaining"].values()):
                still_open.append(open_round)
            else:
                _finalize(rounds, open_round)
        open_rounds = still_open
    # rounds left open at statement end are still reported (close_at = last fill)
    for open_round in open_rounds:
        _finalize(rounds, open_round)
    return rounds, naked_groups


def _finalize(rounds: list[SpreadRound], open_round: dict) -> bool:
    fills = open_round["fills"]
    actual = sum(fill.realized for fill in fills)
    rounds.append(
        SpreadRound(
            round_id=f"{open_round['open_at']:%Y%m%d%H%M%S}:{open_round['expiry']:%Y%m%d}",
            expiry=open_round["expiry"],
            right=open_round["right"],
            kind=open_round["kind"],
            pos_strike=open_round["pos_strike"],
            neg_strike=open_round["neg_strike"],
            width=abs(open_round["pos_strike"] - open_round["neg_strike"]),
            units=open_round["units"],
            open_at=open_round["open_at"],
            entry_per_unit=open_round["entry_per_unit"],
            close_at=open_round["close_at"],
            actual_pnl=actual,
            commissions=sum(fill.commission for fill in fills),
            fills=tuple(fills),
        )
    )
    return True


def _mid(tick: OptionTick) -> float | None:
    if tick.mid is not None:
        return tick.mid
    if tick.bid is not None and tick.ask is not None:
        return (tick.bid + tick.ask) / 2.0
    return None


def _tick_at_or_before(
    series: Sequence[OptionTick], times: Sequence[datetime], at: datetime
) -> OptionTick | None:
    """Latest quote at or before ``at``; never substitute a future quote."""
    index = bisect_left(times, at)
    if index < len(series) and series[index].at == at:
        return series[index]
    if index > 0:
        return series[index - 1]
    return None


def _seconds(value: timedelta) -> str:
    return f"{value.total_seconds():.3f}s"


def _entry_quote_gate(
    *,
    entry_at: datetime,
    long_series: Sequence[OptionTick],
    short_series: Sequence[OptionTick],
    max_entry_quote_age: timedelta,
    max_leg_skew: timedelta,
) -> str | None:
    """Return an auditable reason when the actual entry has no coherent quote pair."""
    long_times = [tick.at for tick in long_series]
    short_times = [tick.at for tick in short_series]
    long_tick = _tick_at_or_before(long_series, long_times, entry_at)
    short_tick = _tick_at_or_before(short_series, short_times, entry_at)
    if long_tick is None:
        return "entry_long_missing_at_or_before"
    if short_tick is None:
        return "entry_short_missing_at_or_before"
    long_age = entry_at - long_tick.at
    short_age = entry_at - short_tick.at
    if long_age > max_entry_quote_age:
        return f"entry_long_stale(age={_seconds(long_age)},max={_seconds(max_entry_quote_age)})"
    if short_age > max_entry_quote_age:
        return f"entry_short_stale(age={_seconds(short_age)},max={_seconds(max_entry_quote_age)})"
    skew = abs(long_tick.at - short_tick.at)
    if skew > max_leg_skew:
        return f"entry_leg_skew(skew={_seconds(skew)},max={_seconds(max_leg_skew)})"
    if _mid(long_tick) is None:
        return "entry_long_unpriced"
    if _mid(short_tick) is None:
        return "entry_short_unpriced"
    return None


def _quality_skip_reason(*, rejected_stale: int, rejected_skew: int, rejected_unpriced: int) -> str:
    return (
        "no_fresh_executable_path("
        f"stale={rejected_stale},skew={rejected_skew},unpriced={rejected_unpriced})"
    )


def replay_spread_rule_attempt(
    *,
    rule: str,
    kind: str,
    width: float,
    entry_per_unit: float,
    entry_at: datetime,
    stop_at: datetime,
    long_series: Sequence[OptionTick],
    short_series: Sequence[OptionTick],
    max_entry_quote_age: timedelta,
    max_mark_quote_age: timedelta,
    max_leg_skew: timedelta,
) -> ReplayAttempt:
    if rule not in RULES:
        raise ValueError(f"unknown replay rule: {rule}")
    if kind not in {"debit", "credit"}:
        raise ValueError(f"unknown spread kind: {kind}")
    if width <= 0 or entry_per_unit < 0:
        raise ValueError("width must be positive and entry_per_unit non-negative")
    if any(value < timedelta(0) for value in (max_entry_quote_age, max_mark_quote_age)):
        raise ValueError("quote ages must be non-negative")
    if max_leg_skew < timedelta(0):
        raise ValueError("leg skew must be non-negative")

    long_series = tuple(sorted(long_series, key=lambda tick: tick.at))
    short_series = tuple(sorted(short_series, key=lambda tick: tick.at))
    entry_skip = _entry_quote_gate(
        entry_at=entry_at,
        long_series=long_series,
        short_series=short_series,
        max_entry_quote_age=max_entry_quote_age,
        max_leg_skew=max_leg_skew,
    )
    if entry_skip is not None:
        return ReplayAttempt(exit=None, skip_reason=entry_skip)

    long_times = [tick.at for tick in long_series]
    short_times = [tick.at for tick in short_series]
    hold_until = stop_at + timedelta(minutes=15)
    event_times = {entry_at}
    event_times.update(tick.at for tick in long_series if entry_at <= tick.at <= hold_until)
    event_times.update(tick.at for tick in short_series if entry_at <= tick.at <= hold_until)
    peak_pnl: float | None = None
    armed = False
    valid_marks = 0
    rejected_stale = 0
    rejected_skew = 0
    rejected_unpriced = 0
    last_valid_at: datetime | None = None

    for mark_at in sorted(event_times):
        long_tick = _tick_at_or_before(long_series, long_times, mark_at)
        short_tick = _tick_at_or_before(short_series, short_times, mark_at)
        if long_tick is None or short_tick is None:
            # The entry gate normally makes this unreachable; keep it explicit
            # in case a caller passes inconsistent/non-monotonic timestamps.
            rejected_unpriced += 1
            continue
        long_age = mark_at - long_tick.at
        short_age = mark_at - short_tick.at
        if long_age > max_mark_quote_age or short_age > max_mark_quote_age:
            rejected_stale += 1
            continue
        if abs(long_tick.at - short_tick.at) > max_leg_skew:
            rejected_skew += 1
            continue
        long_mid, short_mid = _mid(long_tick), _mid(short_tick)
        # Closing either debit or credit vertical requires selling the positive
        # leg at bid and buying the negative leg at ask.
        if long_mid is None or short_mid is None or long_tick.bid is None or short_tick.ask is None:
            rejected_unpriced += 1
            continue
        value = long_mid - short_mid if kind == "debit" else short_mid - long_mid
        exit_value = (
            long_tick.bid - short_tick.ask if kind == "debit" else short_tick.ask - long_tick.bid
        )
        pnl = value - entry_per_unit if kind == "debit" else entry_per_unit - value
        valid_marks += 1
        last_valid_at = mark_at

        if rule == "sat85":
            # Debit sat85 retains the production definition (spread value at
            # 85% of width). For a credit spread the attainable max profit is
            # the entry credit, so 85% means realizing 85% of that credit.
            saturated = (
                value >= SATURATION_FRACTION * width
                if kind == "debit"
                else pnl >= SATURATION_FRACTION * entry_per_unit
            )
            if saturated:
                return ReplayAttempt(
                    exit=_rule_exit(rule, kind, entry_per_unit, mark_at, exit_value, "saturation")
                )
        elif rule == "trail33":
            peak_pnl = pnl if peak_pnl is None else max(peak_pnl, pnl)
            # Credit-spread progress is PnL / entry credit (its max profit),
            # not remaining liability / spread width.
            armed_now = (
                value >= TRAIL33_ARM_FRACTION * width
                if kind == "debit"
                else pnl >= TRAIL33_ARM_FRACTION * entry_per_unit
            )
            armed = armed or armed_now
            if armed and peak_pnl > 0 and pnl <= peak_pnl * (1.0 - TRAILING_GIVEBACK_FRACTION):
                return ReplayAttempt(
                    exit=_rule_exit(rule, kind, entry_per_unit, mark_at, exit_value, "trailing_tp")
                )
        # clock has no profit rule; sat85/trail33 share the same hard clock.
        if mark_at >= stop_at:
            return ReplayAttempt(
                exit=_rule_exit(rule, kind, entry_per_unit, mark_at, exit_value, "time_stop")
            )

    if valid_marks == 0:
        return ReplayAttempt(
            exit=None,
            skip_reason=_quality_skip_reason(
                rejected_stale=rejected_stale,
                rejected_skew=rejected_skew,
                rejected_unpriced=rejected_unpriced,
            ),
        )
    return ReplayAttempt(
        exit=None,
        skip_reason=(
            "no_fresh_exit_mark_before_grace("
            f"last={last_valid_at.isoformat() if last_valid_at else '-'},"
            f"stop={stop_at.isoformat()},stale={rejected_stale},"
            f"skew={rejected_skew},unpriced={rejected_unpriced})"
        ),
    )


def replay_spread_rule(
    *,
    rule: str,
    kind: str,
    width: float,
    entry_per_unit: float,
    entry_at: datetime,
    stop_at: datetime,
    long_series: Sequence[OptionTick],
    short_series: Sequence[OptionTick],
    max_entry_quote_age: timedelta = MAX_ENTRY_QUOTE_AGE,
    max_mark_quote_age: timedelta = MAX_MARK_QUOTE_AGE,
    max_leg_skew: timedelta = MAX_LEG_SKEW,
) -> RuleExit | None:
    """Counterfactual exit for one spread round on the lake quote path.

    Marks: debit rounds track long_mid - short_mid, credit rounds track the
    liability short_mid - long_mid. Exits pay the adverse side (debit: long
    bid - short ask; credit: short ask - long bid). Every pair is composed
    only from quotes at or before the mark and must pass age/skew gates.
    """
    return replay_spread_rule_attempt(
        rule=rule,
        kind=kind,
        width=width,
        entry_per_unit=entry_per_unit,
        entry_at=entry_at,
        stop_at=stop_at,
        long_series=long_series,
        short_series=short_series,
        max_entry_quote_age=max_entry_quote_age,
        max_mark_quote_age=max_mark_quote_age,
        max_leg_skew=max_leg_skew,
    ).exit


def _rule_exit(
    rule: str,
    kind: str,
    entry_per_unit: float,
    exit_at: datetime,
    exit_value: float,
    reason: str,
) -> RuleExit:
    pnl = (exit_value - entry_per_unit) if kind == "debit" else (entry_per_unit - exit_value)
    return RuleExit(rule=rule, exit_at=exit_at, pnl_per_unit=pnl, reason=reason)


def _point_in_time_provider_series(
    store: QuoteStore,
    *,
    expiry: date,
    strike: float,
    right: str,
    entry_at: datetime,
    end: datetime,
) -> list[OptionTick]:
    """Choose a provider using only its latest priced quote at/before entry."""
    start = entry_at - timedelta(minutes=5)
    chosen: list[OptionTick] = []
    chosen_entry_at: datetime | None = None
    for provider in PROVIDERS:
        candidate = store.option_series(
            provider=provider,
            expiry=expiry,
            strike=strike,
            right=right,
            start=start,
            end=end,
        )
        times = [tick.at for tick in candidate]
        entry_tick = _tick_at_or_before(candidate, times, entry_at)
        if entry_tick is None or _mid(entry_tick) is None:
            continue
        if chosen_entry_at is None or entry_tick.at > chosen_entry_at:
            chosen = candidate
            chosen_entry_at = entry_tick.at
    return chosen


def replay_round(
    store: QuoteStore,
    spread_round: SpreadRound,
    *,
    max_entry_quote_age: timedelta = MAX_ENTRY_QUOTE_AGE,
    max_mark_quote_age: timedelta = MAX_MARK_QUOTE_AGE,
    max_leg_skew: timedelta = MAX_LEG_SKEW,
) -> tuple[dict[str, RuleExit], dict[str, str]]:
    """Run all three rules for one round; returns (exits, skip_reasons)."""
    long_leg = (spread_round.expiry, spread_round.pos_strike, spread_round.right)
    short_leg = (spread_round.expiry, spread_round.neg_strike, spread_round.right)
    clock_stop = min(
        next_replay_exit_clock(spread_round.open_at),
        replay_expiry_close(spread_round.expiry),
    )
    series: dict[tuple[date, float, str], list[OptionTick]] = {}
    for leg in (long_leg, short_leg):
        series[leg] = _point_in_time_provider_series(
            store,
            expiry=leg[0],
            strike=leg[1],
            right=leg[2],
            entry_at=spread_round.open_at,
            end=clock_stop + timedelta(minutes=20),
        )
    exits: dict[str, RuleExit] = {}
    skips: dict[str, str] = {}
    for rule in RULES:
        attempt = replay_spread_rule_attempt(
            rule=rule,
            kind=spread_round.kind,
            width=spread_round.width,
            entry_per_unit=spread_round.entry_per_unit,
            entry_at=spread_round.open_at,
            stop_at=clock_stop,
            long_series=series[long_leg],
            short_series=series[short_leg],
            max_entry_quote_age=max_entry_quote_age,
            max_mark_quote_age=max_mark_quote_age,
            max_leg_skew=max_leg_skew,
        )
        if attempt.exit is None:
            skips[rule] = attempt.skip_reason or "unknown_skip"
        else:
            exits[rule] = attempt.exit
    return exits, skips


def _fmt_pnl(value: float | None) -> str:
    return "-" if value is None else f"{value:+.0f}"


def _is_gth(spread_round: SpreadRound) -> bool:
    local_time = spread_round.open_at.astimezone(STATEMENT_TZ).time()
    return not (_RTH_OPEN_ET <= local_time < _EXPIRY_CLOSE_ET)


def _is_zero_dte(spread_round: SpreadRound) -> bool:
    return spread_round.expiry == spread_round.open_at.astimezone(STATEMENT_TZ).date()


def _cohorts(rounds: Sequence[SpreadRound]) -> tuple[tuple[str, list[SpreadRound]], ...]:
    return (
        (
            "GTH Call debit 0DTE",
            [
                item
                for item in rounds
                if _is_gth(item)
                and item.right == "C"
                and item.kind == "debit"
                and _is_zero_dte(item)
            ],
        ),
        ("GTH debit", [item for item in rounds if _is_gth(item) and item.kind == "debit"]),
        ("all", list(rounds)),
    )


def render_replay_report(
    rounds: Sequence[SpreadRound],
    outcomes: dict[str, dict[str, RuleExit]],
    skips: dict[str, dict[str, str]],
    naked_groups: int,
    *,
    max_entry_quote_age: timedelta = MAX_ENTRY_QUOTE_AGE,
    max_mark_quote_age: timedelta = MAX_MARK_QUOTE_AGE,
    max_leg_skew: timedelta = MAX_LEG_SKEW,
) -> str:
    """Chinese per-round audit table plus matched-cohort comparisons."""
    lines = [
        "# 实盘价差回合回放:实际纪律 vs 三种出场规则",
        "",
        "- 数据:IBKR 活动账单(交易节)重建的两腿 SPXW 价差回合 + lake 报价路径。",
        "- 时钟:美东 09:45 严格之后的下一个出场钟,并以到期日美东 16:00 封顶;"
        "America/New_York 自动处理 DST(夏令时 13:45 UTC,冬令时 14:45 UTC)。",
        "- 反事实:sat85 借记=价差价值≥85%宽度,信用=已实现≥85%入场 credit;"
        "trail33 在相同 50% 进度激活后、峰值浮盈回撤 1/3 出场。",
        f"- 报价门:入场两腿 age≤{_seconds(max_entry_quote_age)},"
        f"持仓 mark age≤{_seconds(max_mark_quote_age)},"
        f"leg skew≤{_seconds(max_leg_skew)};只使用 mark 时刻或之前的报价,"
        "不使用未来 short,不无限 ffill。",
        "- 佣金:实际盈亏=账单净盈亏(已含佣金);规则盈亏先按退出 bid/ask 计算毛值;"
        "汇总也列出按同回合账单总佣金调整的估算净值(非仿真佣金)。",
        "- clock 规则在回放中没有每回合失效线(账单不记录 trough),仅纯时钟。",
        f"- 另有 {naked_groups} 个单腿(裸仓)开仓组未纳入回放。",
        "",
        "| 建仓(UTC) | 结构 | 实际净盈亏 | 账单佣金 | sat85 毛值 | trail33 毛值 | clock 毛值 |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for spread_round in rounds:
        exits = outcomes.get(spread_round.round_id, {})
        round_skips = skips.get(spread_round.round_id, {})
        cells = []
        for rule in RULES:
            if rule in exits:
                pnl = exits[rule].pnl_per_unit * spread_round.units * POINTS_PER_CONTRACT
                cells.append(
                    f"{_fmt_pnl(pnl)}({exits[rule].exit_at:%d日%H:%M}/{exits[rule].reason})"
                )
            else:
                cells.append(f"跳过({round_skips.get(rule, '?')})")
        lines.append(
            f"| {spread_round.open_at:%m-%d %H:%M} | {spread_round.structure} | "
            f"{_fmt_pnl(spread_round.actual_pnl)} | {_fmt_pnl(spread_round.commissions)} | "
            f"{cells[0]} | {cells[1]} | {cells[2]} |"
        )

    actual_total = sum(item.actual_pnl for item in rounds)
    commission_total = sum(item.commissions for item in rounds)
    lines.extend(
        [
            "",
            "## 同窗可比 cohort 汇总",
            "",
            "只有 sat85、trail33、clock 三规则都有合格退出的回合才进入比较;"
            "实际值也限定在同一批 common 回合。",
            "",
            "| cohort | 全部回合 | common | 实际净值(common) | 账单佣金(common) | "
            "sat85 毛值 / 佣后估算 | trail33 毛值 / 佣后估算 | "
            "clock 毛值 / 佣后估算 |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for cohort_name, cohort_rounds in _cohorts(rounds):
        common = [
            item
            for item in cohort_rounds
            if all(rule in outcomes.get(item.round_id, {}) for rule in RULES)
        ]
        if not common:
            lines.append(f"| {cohort_name} | {len(cohort_rounds)} | 0 | - | - | - | - | - |")
            continue
        actual = sum(item.actual_pnl for item in common)
        commissions = sum(item.commissions for item in common)
        rule_cells: list[str] = []
        for rule in RULES:
            gross = sum(
                outcomes[item.round_id][rule].pnl_per_unit * item.units * POINTS_PER_CONTRACT
                for item in common
            )
            rule_cells.append(f"{_fmt_pnl(gross)} / {_fmt_pnl(gross + commissions)}")
        lines.append(
            f"| {cohort_name} | {len(cohort_rounds)} | {len(common)} | "
            f"{_fmt_pnl(actual)} | {_fmt_pnl(commissions)} | "
            f"{rule_cells[0]} | {rule_cells[1]} | {rule_cells[2]} |"
        )

    reason_counts: dict[str, int] = {}
    for round_skips in skips.values():
        for reason in round_skips.values():
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
    lines.extend(
        [
            "",
            "## 审计与覆盖",
            "",
            f"- 账单全部 {len(rounds)} 价差回合的实际净盈亏 "
            f"{_fmt_pnl(actual_total)}$,账单佣金 {_fmt_pnl(commission_total)}$;"
            "该数字仅作覆盖审计,不与子集规则总额直接比较。",
        ]
    )
    if reason_counts:
        lines.extend(["", "| skip reason | rule-round 次数 |", "|---|---:|"])
        lines.extend(
            f"| `{reason}` | {count} |"
            for reason, count in sorted(reason_counts.items(), key=lambda item: (-item[1], item[0]))
        )
    else:
        lines.append("- 所有回合的三种规则均有合格路径,无 skip。")
    return "\n".join(lines) + "\n"


def run(
    statement_path: Path,
    data_root: Path,
    output_dir: Path,
    *,
    max_entry_quote_age: timedelta = MAX_ENTRY_QUOTE_AGE,
    max_mark_quote_age: timedelta = MAX_MARK_QUOTE_AGE,
    max_leg_skew: timedelta = MAX_LEG_SKEW,
) -> Path:
    """Parse the statement, replay all rounds and write report-account-spreads.md."""
    fills = parse_statement_fills(Path(statement_path))
    rounds, naked_groups = pair_spread_rounds(fills)
    logger.info("fills=%d rounds=%d naked_groups=%d", len(fills), len(rounds), naked_groups)
    store = QuoteStore(Path(data_root))
    outcomes: dict[str, dict[str, RuleExit]] = {}
    skips: dict[str, dict[str, str]] = {}
    try:
        for spread_round in rounds:
            exits, round_skips = replay_round(
                store,
                spread_round,
                max_entry_quote_age=max_entry_quote_age,
                max_mark_quote_age=max_mark_quote_age,
                max_leg_skew=max_leg_skew,
            )
            outcomes[spread_round.round_id] = exits
            skips[spread_round.round_id] = round_skips
    finally:
        store.close()
    skip_total = sum(len(round_skips) for round_skips in skips.values())
    logger.info("rule skips=%d", skip_total)
    target = Path(output_dir).expanduser().resolve()
    target.mkdir(parents=True, exist_ok=True)
    report = render_replay_report(
        rounds,
        outcomes,
        skips,
        naked_groups,
        max_entry_quote_age=max_entry_quote_age,
        max_mark_quote_age=max_mark_quote_age,
        max_leg_skew=max_leg_skew,
    )
    (target / "report-account-spreads.md").write_text(report, encoding="utf-8")
    return target
