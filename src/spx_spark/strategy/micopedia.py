from __future__ import annotations

import argparse
import json
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from spx_spark.config import StorageSettings
from spx_spark.storage import LatestState, LatestStateStore


VALID_BIASES = {"bullish", "bearish", "mixed_tactical", "neutral_unclear"}
VALID_GAMMA_STATES = {"positive", "negative", "pin", "transition", "unknown"}
VALID_TIME_PHASES = {"premarket", "open", "midday", "late", "closed", "unknown"}

EVENT_ALIASES = {
    "fed": "fomc",
    "nonfarm": "nfp",
    "payroll": "nfp",
    "headline_risk": "headline",
    "collar": "jpm_collar",
    "jpm": "jpm_collar",
    "quad_witching": "opex",
    "monthend": "month_end",
    "quarterend": "quarter_end",
    "window": "window_dressing",
    "flow": "systematic_flow",
}


@dataclass(frozen=True)
class KeyLevelDistance:
    level: float
    distance_points: float
    distance_bps: float | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MicopediaInputs:
    created_at: datetime
    underlier_price: float | None = None
    vix1d: float | None = None
    vix: float | None = None
    gamma_state: str = "unknown"
    directional_bias: str = "neutral_unclear"
    time_phase: str = "unknown"
    event_tags: tuple[str, ...] = ()
    key_levels: tuple[float, ...] = ()
    has_option_chain: bool = False
    has_es_data: bool = False
    source_notes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "created_at", as_utc(self.created_at))
        object.__setattr__(self, "gamma_state", normalize_choice(self.gamma_state, VALID_GAMMA_STATES))
        object.__setattr__(
            self,
            "directional_bias",
            normalize_choice(self.directional_bias, VALID_BIASES),
        )
        object.__setattr__(self, "time_phase", normalize_choice(self.time_phase, VALID_TIME_PHASES))
        object.__setattr__(self, "event_tags", normalize_tags(self.event_tags))
        object.__setattr__(self, "key_levels", tuple(float(level) for level in self.key_levels))
        object.__setattr__(self, "source_notes", tuple(note for note in self.source_notes if note))

    def nearest_key_level(self) -> KeyLevelDistance | None:
        if self.underlier_price is None or not self.key_levels:
            return None
        level = min(self.key_levels, key=lambda item: abs(item - self.underlier_price))
        distance = self.underlier_price - level
        distance_bps = distance / self.underlier_price * 10_000 if self.underlier_price else None
        return KeyLevelDistance(level=level, distance_points=distance, distance_bps=distance_bps)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["created_at"] = self.created_at.isoformat()
        return payload


@dataclass(frozen=True)
class MicopediaSignal:
    created_at: datetime
    source: str
    underlier_price: float | None
    regime: str
    directional_bias: str
    confidence: str
    nearest_key_level: KeyLevelDistance | None
    decision_stack: tuple[str, ...]
    map_focus: tuple[str, ...]
    trigger_watchlist: tuple[str, ...]
    candidate_expression: str
    risk_policy: tuple[str, ...]
    invalidation_checks: tuple[str, ...]
    data_warnings: tuple[str, ...]
    suggested_sampling_mode: str
    focus_instruments: tuple[str, ...]
    next_checks: tuple[str, ...]
    inputs: MicopediaInputs

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["created_at"] = self.created_at.isoformat()
        payload["nearest_key_level"] = (
            self.nearest_key_level.to_dict() if self.nearest_key_level else None
        )
        payload["inputs"] = self.inputs.to_dict()
        return payload


def as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def normalize_choice(value: str, valid: set[str]) -> str:
    normalized = value.strip().lower().replace("-", "_")
    if normalized not in valid:
        raise ValueError(f"Unsupported value {value!r}; expected one of {sorted(valid)}")
    return normalized


def normalize_tags(values: Iterable[str]) -> tuple[str, ...]:
    tags: set[str] = set()
    for value in values:
        for part in str(value).split(","):
            normalized = part.strip().lower().replace("-", "_")
            if not normalized:
                continue
            tags.add(EVENT_ALIASES.get(normalized, normalized))
    return tuple(sorted(tags))


def classify_regime(inputs: MicopediaInputs) -> str:
    tags = set(inputs.event_tags)
    if inputs.gamma_state == "pin" or {"opex", "jpm_collar"} & tags:
        return "opex_gamma_pin"
    if {"fomc", "cpi", "nfp", "pce", "headline"} & tags:
        return "high_vol_event"
    if {"month_end", "quarter_end", "tga", "liquidity", "systematic_flow", "cta"} & tags:
        return "liquidity_systematic_flow"
    if {"holiday", "window_dressing"} & tags:
        return "holiday_liquidity"
    if inputs.vix1d is not None and inputs.vix1d >= 25:
        return "high_vol_event"
    if inputs.vix1d is not None and inputs.vix1d < 10:
        return "low_vol_difficult"
    if inputs.gamma_state in {"negative", "transition"}:
        return "negative_gamma_trend"
    if inputs.gamma_state == "positive":
        return "positive_gamma_mean_reversion"
    return "ordinary_rth"


def classify_confidence(inputs: MicopediaInputs, regime: str) -> str:
    score = 0
    if inputs.underlier_price is not None:
        score += 1
    if inputs.vix1d is not None:
        score += 1
    if inputs.gamma_state != "unknown":
        score += 1
    if inputs.key_levels:
        score += 1
    if inputs.event_tags:
        score += 1
    if inputs.has_option_chain:
        score += 1
    if inputs.has_es_data:
        score += 1
    if regime in {"ordinary_rth", "low_vol_difficult"} and not inputs.has_option_chain:
        score -= 1
    if score >= 6:
        return "high_observational"
    if score >= 3:
        return "medium_observational"
    return "low_observational"


def build_micopedia_signal(inputs: MicopediaInputs) -> MicopediaSignal:
    regime = classify_regime(inputs)
    return MicopediaSignal(
        created_at=datetime.now(tz=timezone.utc),
        source="mr_micopedia_distilled_framework",
        underlier_price=inputs.underlier_price,
        regime=regime,
        directional_bias=inputs.directional_bias,
        confidence=classify_confidence(inputs, regime),
        nearest_key_level=inputs.nearest_key_level(),
        decision_stack=decision_stack(),
        map_focus=map_focus(inputs, regime),
        trigger_watchlist=trigger_watchlist(inputs, regime),
        candidate_expression=candidate_expression(inputs, regime),
        risk_policy=risk_policy(),
        invalidation_checks=invalidation_checks(inputs, regime),
        data_warnings=data_warnings(inputs),
        suggested_sampling_mode=suggested_sampling_mode(inputs, regime),
        focus_instruments=focus_instruments(regime),
        next_checks=next_checks(inputs, regime),
        inputs=inputs,
    )


def decision_stack() -> tuple[str, ...]:
    return (
        "Regime first: classify vol, event, liquidity, OPEX/gamma, holiday, and flow mode.",
        "Map second: locate SPX key levels, 0DTE walls, JPM collar zones, IV/VIX1D, and timing windows.",
        "Trigger third: require price action at the mapped level before treating a thesis as active.",
        "Tool fourth: prefer defined-risk SPX/SPXW vertical structures for mature 0DTE expression.",
        "Risk always: protect gains quickly, invalidate fast, and avoid undefined overnight exposure.",
        "Audit after: separate pre-trade thesis, intraday revision, and post-trade review.",
    )


def map_focus(inputs: MicopediaInputs, regime: str) -> tuple[str, ...]:
    focus = [
        "SPX price map: integer levels, prior high/low, opening range, VWAP, and supplied key levels.",
        "0DTE option map: ATM straddle, OI/gamma concentration, call wall, put wall, and max-payoff strikes.",
        "Vol map: VIX1D, VIX9D, VIX, IV crush risk, and realized-vs-implied range.",
    ]
    if regime == "opex_gamma_pin":
        focus.append("Pin map: test whether price is being attracted to or rejected from a wall/collar zone.")
    if regime == "liquidity_systematic_flow":
        focus.append("Flow map: check TGA/liquidity, CTA/systematic flow, pensions, and month-end effects.")
    if regime == "high_vol_event":
        focus.append("Event map: separate headline reaction, IV reset, and post-event second move.")
    if inputs.key_levels:
        focus.append("Nearest supplied key level is used only as a conditional branch, not as a trade order.")
    return tuple(focus)


def trigger_watchlist(inputs: MicopediaInputs, regime: str) -> tuple[str, ...]:
    triggers: list[str] = []
    if inputs.time_phase == "premarket":
        triggers.append("Premarket: build scenarios and max-loss structures; do not convert macro opinion into an order.")
    elif inputs.time_phase == "open":
        triggers.append("Open: wait for the first response to gap, flush, squeeze, or opening-range failure.")
    elif inputs.time_phase == "late":
        triggers.append("Late session: evaluate close distribution, pin risk, and whether spread max payoff is realistic.")
    else:
        triggers.append("RTH: require a level reaction or failed breakout before activating any directional read.")

    if inputs.gamma_state == "transition":
        triggers.append(
            "Zero-gamma transition: dealer hedging flips near this zone; "
            "a break can expand volatility and accelerate—do not treat it as a pin or fade toward walls."
        )
    elif inputs.gamma_state == "negative" or regime == "negative_gamma_trend":
        triggers.append("Negative gamma: a clean break can accelerate; avoid fading until reclaim or exhaustion is visible.")
    elif inputs.gamma_state in {"positive", "pin"} or regime == "opex_gamma_pin":
        triggers.append("Positive/pin gamma: expect mean reversion near walls unless price accepts beyond the level.")

    if inputs.directional_bias == "bullish":
        triggers.append("Bullish branch: look for dip/flush hold, reclaim, or supportive relative strength before expression.")
    elif inputs.directional_bias == "bearish":
        triggers.append("Bearish branch: look for failed reclaim, wall rejection, or support break before expression.")
    elif inputs.directional_bias == "mixed_tactical":
        triggers.append("Mixed branch: prefer range/pin hypotheses and avoid forcing a one-way read.")
    else:
        triggers.append("Neutral branch: keep the map live and wait for trigger confirmation.")
    return tuple(triggers)


def candidate_expression(inputs: MicopediaInputs, regime: str) -> str:
    prefix = "Observational candidate only, not an order: "
    if inputs.directional_bias == "bullish":
        return prefix + "defined-risk SPX/SPXW 0DTE call spread after trigger confirmation."
    if inputs.directional_bias == "bearish":
        return prefix + "defined-risk SPX/SPXW 0DTE put spread after trigger confirmation."
    if inputs.directional_bias == "mixed_tactical" or regime == "opex_gamma_pin":
        return prefix + "bounded spread or no-trade stance around the pin/range until acceptance changes."
    if regime == "low_vol_difficult":
        return prefix + "reduce 0DTE aggression; require cheap convexity or a clear pin/range edge."
    return prefix + "no default expression; complete map and trigger checks first."


def risk_policy() -> tuple[str, ...]:
    return (
        "Use defined maximum loss before any strategy is considered actionable.",
        "Protect green quickly; do not optimize for catching the full move.",
        "Keep single-leg options as exception cases because direction, IV, and time decay can all be right against you.",
        "Do not carry undefined overnight risk; any overnight exposure must be explicit, bounded, and cheap.",
        "Do not treat a post-trade review tweet as a pre-trade signal.",
    )


def invalidation_checks(inputs: MicopediaInputs, regime: str) -> tuple[str, ...]:
    checks = [
        "Reject the thesis if the mapped level accepts on the wrong side and does not reclaim quickly.",
        "Reject the thesis if VIX1D/IV behavior contradicts the expected range or crush scenario.",
    ]
    if inputs.directional_bias == "bullish":
        checks.append("Bullish invalidation: dip is not bought, relative strength fails, or support turns into resistance.")
    elif inputs.directional_bias == "bearish":
        checks.append("Bearish invalidation: breakdown is bought, wall is reclaimed, or squeeze broadens.")
    if regime == "high_vol_event":
        checks.append("Event invalidation: first reaction reverses after IV reset or a second headline changes the regime.")
    if regime == "opex_gamma_pin":
        checks.append("Pin invalidation: price accepts beyond the wall/collar and forces hedging in the other direction.")
    return tuple(checks)


def data_warnings(inputs: MicopediaInputs) -> tuple[str, ...]:
    warnings = list(inputs.source_notes)
    if inputs.underlier_price is None:
        warnings.append("Missing SPX underlier price; signal cannot be tied to a live map.")
    if inputs.vix1d is None:
        warnings.append("Missing VIX1D; vol-regime and range assumptions are low confidence.")
    if not inputs.has_option_chain:
        warnings.append("Missing SPXW option-chain/gamma map; wall and spread logic is only a framework.")
    if not inputs.has_es_data:
        warnings.append("Missing ES/SPX 1-minute validation feed; MFE/MAE and timing edge remain unvalidated.")
    warnings.append("Hyperliquid SP500 can be context only and must not be treated as CME ES or official SPX.")
    return tuple(dict.fromkeys(warnings))


def suggested_sampling_mode(inputs: MicopediaInputs, regime: str) -> str:
    if inputs.underlier_price is None:
        return "degraded"
    if inputs.time_phase in {"open", "late"}:
        return "execution_monitor"
    if regime in {"high_vol_event", "negative_gamma_trend", "opex_gamma_pin"}:
        return "execution_monitor"
    return "human_alert"


def focus_instruments(regime: str) -> tuple[str, ...]:
    instruments = [
        "index:SPX",
        "future:ES",
        "equity:SPY",
        "index:VIX1D",
        "index:VIX9D",
        "index:VIX",
        "index:VVIX",
        "index:SKEW",
        "SPXW 0DTE ATM +/- hot window",
        "SPXW next expiry ATM +/- hot window",
        "crypto_perp:xyz:SP500 context only",
    ]
    if regime == "liquidity_systematic_flow":
        instruments.extend(["equity:HYG", "equity:LQD", "equity:TLT", "equity:QQQ", "equity:IWM"])
    return tuple(instruments)


def next_checks(inputs: MicopediaInputs, regime: str) -> tuple[str, ...]:
    checks = [
        "Load current SPX, VIX1D, VIX9D/VIX, and SPXW 0DTE option chain.",
        "Build the current-day map before asking an LLM for narrative interpretation.",
        "Record any generated signal with timestamp, inputs, and later MFE/MAE validation fields.",
    ]
    if not inputs.has_option_chain:
        checks.append("Collect option chain before treating wall, gamma, or spread-payoff language as high confidence.")
    if not inputs.has_es_data:
        checks.append("Collect ES/SPX 1-minute bars before ranking timing windows or fixed thresholds.")
    if regime == "low_vol_difficult":
        checks.append("Low VIX1D: test whether realized range is too small for 0DTE premium risk.")
    return tuple(checks)


def effective_price_for(state: LatestState, instrument_id: str) -> float | None:
    quote = state.best_quote(instrument_id)
    return quote.effective_price if quote else None


def inputs_from_latest_state(
    state: LatestState,
    *,
    gamma_state: str = "unknown",
    directional_bias: str = "neutral_unclear",
    time_phase: str = "unknown",
    event_tags: Iterable[str] = (),
    key_levels: Iterable[float] = (),
    has_option_chain: bool = False,
    has_es_data: bool = False,
    overrides: dict[str, float | None] | None = None,
) -> MicopediaInputs:
    overrides = overrides or {}
    source_notes: list[str] = []
    underlier = overrides.get("underlier_price")
    if underlier is None:
        underlier = effective_price_for(state, "index:SPX")
        if underlier is not None:
            source_notes.append("Underlier from latest state index:SPX.")
    if underlier is None:
        underlier = effective_price_for(state, "crypto_perp:xyz:SP500")
        if underlier is not None:
            source_notes.append("Underlier fallback from Hyperliquid xyz:SP500 context, not official SPX.")

    vix1d = overrides.get("vix1d")
    if vix1d is None:
        vix1d = effective_price_for(state, "index:VIX1D")
    vix = overrides.get("vix")
    if vix is None:
        vix = effective_price_for(state, "index:VIX")

    return MicopediaInputs(
        created_at=state.as_of,
        underlier_price=underlier,
        vix1d=vix1d,
        vix=vix,
        gamma_state=gamma_state,
        directional_bias=directional_bias,
        time_phase=time_phase,
        event_tags=tuple(event_tags),
        key_levels=tuple(key_levels),
        has_option_chain=has_option_chain,
        has_es_data=has_es_data,
        source_notes=tuple(source_notes),
    )


def print_signal(signal: MicopediaSignal) -> None:
    print("MrMicopedia SPX/0DTE guidance")
    print(f"Created: {signal.created_at.isoformat()}")
    print(f"Regime: {signal.regime}")
    print(f"Bias: {signal.directional_bias}")
    print(f"Confidence: {signal.confidence}")
    print(f"Underlier: {format_optional_number(signal.underlier_price)}")
    if signal.nearest_key_level:
        print(
            "Nearest key level: "
            f"{signal.nearest_key_level.level:.2f} "
            f"distance={signal.nearest_key_level.distance_points:.2f}pt"
        )
    print(f"Suggested sampling mode: {signal.suggested_sampling_mode}")
    print(f"Candidate: {signal.candidate_expression}")
    print_items("Map focus", signal.map_focus)
    print_items("Triggers", signal.trigger_watchlist)
    print_items("Risk policy", signal.risk_policy)
    print_items("Data warnings", signal.data_warnings)
    print_items("Next checks", signal.next_checks)


def print_items(title: str, items: Iterable[str]) -> None:
    print(f"\n{title}:")
    for item in items:
        print(f"- {item}")


def format_optional_number(value: float | None) -> str:
    if value is None:
        return "-"
    if abs(value) >= 100:
        return f"{value:.2f}"
    return f"{value:.4f}".rstrip("0").rstrip(".")


def parse_float_list(values: list[str] | None) -> tuple[float, ...]:
    if not values:
        return ()
    result: list[float] = []
    for value in values:
        for part in value.split(","):
            stripped = part.strip()
            if stripped:
                result.append(float(stripped))
    return tuple(result)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build an observational MrMicopedia SPX/0DTE guidance signal."
    )
    parser.add_argument("--from-latest-state", action="store_true", help="Read missing prices from latest state.")
    parser.add_argument("--underlier", type=float, help="Current SPX reference price override.")
    parser.add_argument("--vix1d", type=float, help="Current VIX1D override.")
    parser.add_argument("--vix", type=float, help="Current VIX override.")
    parser.add_argument("--gamma-state", choices=sorted(VALID_GAMMA_STATES), default="unknown")
    parser.add_argument("--bias", choices=sorted(VALID_BIASES), default="neutral_unclear")
    parser.add_argument("--time-phase", choices=sorted(VALID_TIME_PHASES), default="unknown")
    parser.add_argument("--event", action="append", default=[], help="Event tag; may be repeated or comma-separated.")
    parser.add_argument("--key-level", action="append", default=[], help="Key SPX level; may be repeated or comma-separated.")
    parser.add_argument("--has-option-chain", action="store_true")
    parser.add_argument("--has-es-data", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    key_levels = parse_float_list(args.key_level)
    should_load_latest = args.from_latest_state or args.underlier is None or args.vix1d is None or args.vix is None
    overrides = {
        "underlier_price": args.underlier,
        "vix1d": args.vix1d,
        "vix": args.vix,
    }
    if should_load_latest:
        state = LatestStateStore(StorageSettings.from_env()).load()
        inputs = inputs_from_latest_state(
            state,
            gamma_state=args.gamma_state,
            directional_bias=args.bias,
            time_phase=args.time_phase,
            event_tags=args.event,
            key_levels=key_levels,
            has_option_chain=args.has_option_chain,
            has_es_data=args.has_es_data,
            overrides=overrides,
        )
    else:
        inputs = MicopediaInputs(
            created_at=datetime.now(tz=timezone.utc),
            underlier_price=args.underlier,
            vix1d=args.vix1d,
            vix=args.vix,
            gamma_state=args.gamma_state,
            directional_bias=args.bias,
            time_phase=args.time_phase,
            event_tags=tuple(args.event),
            key_levels=key_levels,
            has_option_chain=args.has_option_chain,
            has_es_data=args.has_es_data,
        )

    signal = build_micopedia_signal(inputs)
    if args.json:
        print(json.dumps(signal.to_dict(), indent=2, sort_keys=True))
    else:
        print_signal(signal)
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()

