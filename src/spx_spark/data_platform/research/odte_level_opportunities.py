"""Opportunity-level evidence and costed quote-replay artifacts."""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Mapping, Sequence

from .odte_level_signals import (
    POINTS_PER_CONTRACT,
    SET_GTH_LEVEL_CANDIDATE,
    SET_TRADE_READY,
    Signal,
    Skip,
    Trade,
    _iter_jsonl,
    _parse_ts,
)

OPPORTUNITY_SCHEMA_VERSION = "odte_opportunity_replay.v1"
OPPORTUNITY_KEY_VERSION = "production_signal_identity.v1"
LATENCY_SENSITIVITY_SECONDS = (0, 5, 10, 20, 30)
SLIPPAGE_PER_LEG_SIDE_POINTS = (0.0, 0.05, 0.10, 0.20)
REFERENCE_SLIPPAGE_PER_LEG_SIDE_POINTS = 0.05


@dataclass(frozen=True, slots=True)
class OpportunityCostModel:
    """Explicit contract-side cost grid for shadow quote replay."""

    commission_per_contract_side_usd: float = 1.25
    slippage_per_leg_side_points: tuple[float, ...] = SLIPPAGE_PER_LEG_SIDE_POINTS
    reference_slippage_per_leg_side_points: float = REFERENCE_SLIPPAGE_PER_LEG_SIDE_POINTS
    model_version: str = "spxw_top_of_book_contract_side_cost.v2"
    lineage: str = "strategy_policy.vNext/research_execution/v1"

    def __post_init__(self) -> None:
        if (
            not math.isfinite(self.commission_per_contract_side_usd)
            or self.commission_per_contract_side_usd < 0
        ):
            raise ValueError("commission_per_contract_side_usd must be finite and non-negative")
        if not self.slippage_per_leg_side_points or any(
            not math.isfinite(value) or value < 0
            for value in self.slippage_per_leg_side_points
        ):
            raise ValueError("slippage_per_leg_side_points must be finite and non-negative")
        if tuple(sorted(set(self.slippage_per_leg_side_points))) != (
            self.slippage_per_leg_side_points
        ):
            raise ValueError("slippage_per_leg_side_points must be unique and ascending")
        if self.reference_slippage_per_leg_side_points not in self.slippage_per_leg_side_points:
            raise ValueError("reference slippage must be present in the sensitivity grid")
        if not self.model_version or not self.lineage:
            raise ValueError("cost model identity must be non-empty")

    def to_payload(self) -> dict[str, object]:
        return {
            "model_version": self.model_version,
            "lineage": self.lineage,
            "commission_per_contract_side_usd": self.commission_per_contract_side_usd,
            "slippage_per_leg_side_points": list(self.slippage_per_leg_side_points),
            "reference_slippage_per_leg_side_points": (
                self.reference_slippage_per_leg_side_points
            ),
            "slippage_unit": "SPX_option_premium_points_per_contract_leg_side",
            "slippage_unit_definition": (
                "1.00 option point = 100 USD per contract; one leg side is one entry or "
                "exit transaction in one option leg"
            ),
            "total_slippage_formula": (
                "per_leg_side_points * contract_legs * 2_entry_exit_sides"
            ),
            "multiplier": POINTS_PER_CONTRACT,
        }


DEFAULT_OPPORTUNITY_COST_MODEL = OpportunityCostModel()
ReplayKey = tuple[str, str, int]
ReplayResult = Trade | Skip


def build_opportunity_artifacts(
    features_root: Path,
    signals: Sequence[Signal],
    replay_results: Mapping[ReplayKey, ReplayResult],
    *,
    cutoff_at: datetime,
    cost_model: OpportunityCostModel = DEFAULT_OPPORTUNITY_COST_MODEL,
) -> list[dict[str, object]]:
    """Join economic identity, raw occurrences, delivery, virtual episodes and replay."""

    selected = {
        (signal.set_name, signal.key): signal
        for signal in signals
        if signal.set_name in {SET_TRADE_READY, SET_GTH_LEVEL_CANDIDATE}
    }
    evidence = _load_evidence(Path(features_root), set(selected), cutoff_at=cutoff_at)
    artifacts: list[dict[str, object]] = []
    for identity, signal in sorted(selected.items(), key=lambda item: (item[1].at, item[0])):
        rows = evidence[identity]
        latency_rows = [
            _replay_payload(
                replay_results.get((signal.set_name, signal.key, latency)),
                latency_seconds=latency,
                cost_model=cost_model,
            )
            for latency in LATENCY_SENSITIVITY_SECONDS
        ]
        baseline = latency_rows[0]
        base_trade = replay_results.get((signal.set_name, signal.key, 0))
        contract_ids = [signal.contract_id] if signal.contract_id else []
        if isinstance(base_trade, Trade) and base_trade.short_contract_id:
            contract_ids.append(base_trade.short_contract_id)
        artifacts.append(
            {
                "schema_version": OPPORTUNITY_SCHEMA_VERSION,
                "opportunity_key_version": OPPORTUNITY_KEY_VERSION,
                "opportunity_id": signal.key,
                "set_name": signal.set_name,
                "session_id": (signal.expiry or signal.at.date()).isoformat(),
                "strategy_id": (
                    "rth_level_manual"
                    if signal.set_name == SET_TRADE_READY
                    else "gth_level_manual_candidate"
                ),
                "play": signal.thesis,
                "direction": signal.direction,
                "trigger_level": signal.level,
                "contract_ids": contract_ids,
                "provider": signal.entry_provider,
                "decision_at": signal.at.isoformat(),
                "entry_valid_until": (
                    signal.entry_expires_at.isoformat() if signal.entry_expires_at else None
                ),
                "occurrence_count": len(rows["occurrences"]),
                "occurrences": rows["occurrences"],
                "delivery_event_count": len(rows["delivery_events"]),
                "delivery_events": rows["delivery_events"],
                "virtual_episode_count": len(rows["virtual_episodes"]),
                "virtual_episodes": rows["virtual_episodes"],
                "baseline_status": baseline["status"],
                "execution_claim": "displayed_nbbo_shadow_no_fill_claim",
                "cost_model": cost_model.to_payload(),
                "latency_sensitivity": latency_rows,
            }
        )
    return artifacts


def _replay_payload(
    result: ReplayResult | None,
    *,
    latency_seconds: int,
    cost_model: OpportunityCostModel,
) -> dict[str, object]:
    if result is None:
        return {
            "latency_seconds": latency_seconds,
            "status": "not_evaluated",
            "skip_reason": "replay_result_unavailable",
            "entry": None,
            "exit": None,
            "cost": None,
        }
    if isinstance(result, Skip):
        return {
            "latency_seconds": latency_seconds,
            "status": "not_reached",
            "skip_reason": result.reason,
            "entry": None,
            "exit": None,
            "cost": None,
        }

    sides = result.executable_sides or (None, None, None, None)
    entry_long_ask, entry_short_bid, exit_long_bid, exit_short_ask = sides
    legs = 2 if result.short_contract_id else 1
    contract_sides = legs * 2
    commission_usd = cost_model.commission_per_contract_side_usd * contract_sides
    commission_points = commission_usd / POINTS_PER_CONTRACT
    gross_points = result.pnl_points
    slippage_sensitivity = [
        _slippage_scenario(
            gross_points=gross_points,
            entry_debit_points=result.entry_px,
            commission_points=commission_points,
            contract_sides=contract_sides,
            per_leg_side_points=per_leg_side_points,
        )
        for per_leg_side_points in cost_model.slippage_per_leg_side_points
    ]
    reference_cost = next(
        row
        for row in slippage_sensitivity
        if row["per_leg_side_slippage_points"]
        == cost_model.reference_slippage_per_leg_side_points
    )
    exact_sides = (
        entry_long_ask is not None
        and exit_long_bid is not None
        and (
            result.short_contract_id is None
            or (entry_short_bid is not None and exit_short_ask is not None)
        )
    )
    return {
        "latency_seconds": latency_seconds,
        "status": "quote_reached",
        "skip_reason": None,
        "entry": {
            "observed_at": result.entry_time,
            "natural_ask": result.entry_px,
            "long_ask": entry_long_ask,
            "short_bid": entry_short_bid,
            "price_source": result.entry_price_source,
        },
        "exit": {
            "observed_at": result.exit_time,
            "natural_bid": result.exit_px,
            "long_bid": exit_long_bid,
            "short_ask": exit_short_ask,
            "reason": result.exit_reason,
        },
        "cost": {
            "gross_points": round(gross_points, 4),
            "gross_pnl_usd": round(gross_points * POINTS_PER_CONTRACT, 2),
            "commission_usd": round(commission_usd, 2),
            "commission_points": round(commission_points, 4),
            "contract_legs": legs,
            "charged_contract_sides": contract_sides,
            "slippage_unit": "SPX_option_premium_points_per_contract_leg_side",
            "reference_slippage_per_leg_side_points": (
                cost_model.reference_slippage_per_leg_side_points
            ),
            "total_slippage_points": reference_cost["total_slippage_points"],
            "total_slippage_usd": reference_cost["total_slippage_usd"],
            "net_points": reference_cost["net_points"],
            "net_pnl_usd": reference_cost["net_pnl_usd"],
            "net_return_fraction": reference_cost["net_return_fraction"],
            "slippage_sensitivity": slippage_sensitivity,
        },
        "data_quality": {
            "exact_executable_sides": exact_sides,
            "censored": False,
        },
    }


def _slippage_scenario(
    *,
    gross_points: float,
    entry_debit_points: float,
    commission_points: float,
    contract_sides: int,
    per_leg_side_points: float,
) -> dict[str, object]:
    total_slippage_points = per_leg_side_points * contract_sides
    net_points = gross_points - commission_points - total_slippage_points
    net_return = net_points / entry_debit_points if entry_debit_points > 0 else None
    return {
        "per_leg_side_slippage_points": per_leg_side_points,
        "charged_contract_sides": contract_sides,
        "total_slippage_points": round(total_slippage_points, 4),
        "total_slippage_usd": round(total_slippage_points * POINTS_PER_CONTRACT, 2),
        "net_points": round(net_points, 4),
        "net_pnl_usd": round(net_points * POINTS_PER_CONTRACT, 2),
        "net_return_fraction": round(net_return, 6) if net_return is not None else None,
    }


def _load_evidence(
    features_root: Path,
    identities: set[tuple[str, str]],
    *,
    cutoff_at: datetime,
) -> dict[tuple[str, str], dict[str, list[dict[str, object]]]]:
    buckets = {
        identity: {"occurrences": {}, "delivery_events": {}, "virtual_episodes": {}}
        for identity in identities
    }
    _load_trade_ready_evidence(features_root, buckets, cutoff_at=cutoff_at)
    _load_gth_evidence(features_root, buckets, cutoff_at=cutoff_at)
    _load_delivery_evidence(features_root, buckets, cutoff_at=cutoff_at)
    _load_virtual_evidence(features_root, buckets, cutoff_at=cutoff_at)
    return {
        identity: {
            name: sorted(rows.values(), key=_evidence_sort_key)
            for name, rows in bucket.items()
        }
        for identity, bucket in buckets.items()
    }


def _load_trade_ready_evidence(features_root: Path, buckets: dict, *, cutoff_at: datetime) -> None:
    for path in sorted(features_root.glob("trade_intents/date=*/events.jsonl")):
        for record in _iter_jsonl(path):
            intent_id = str(record.get("opportunity_id") or record.get("intent_id") or "")
            identity = (SET_TRADE_READY, intent_id)
            if identity not in buckets or not _is_trade_ready(record):
                continue
            observed_at = _first_time(record, "evaluated_at", "recorded_at")
            if observed_at is None or observed_at >= cutoff_at:
                continue
            event_id = str(record.get("event_id") or "")
            if not event_id:
                continue
            _put(
                buckets[identity]["occurrences"],
                event_id,
                {
                    "occurrence_id": event_id,
                    "event_id": event_id,
                    "observed_at": observed_at.isoformat(),
                    "status": str(record.get("status") or "trade_ready"),
                    "entry_limit": record.get("entry_limit"),
                },
            )
            delivery_id = str(record.get("notification_event_id") or "")
            if delivery_id:
                _put_delivery(buckets[identity], delivery_id, event_id, observed_at, record)


def _load_gth_evidence(features_root: Path, buckets: dict, *, cutoff_at: datetime) -> None:
    for path in sorted(features_root.glob("gth_level_manual_candidates/date=*/events.jsonl")):
        for record in _iter_jsonl(path):
            candidate_id = str(record.get("candidate_id") or "")
            identity = (SET_GTH_LEVEL_CANDIDATE, candidate_id)
            if identity not in buckets or record.get("status") != "manual_ready":
                continue
            observed_at = _first_time(record, "evaluated_at", "recorded_at")
            if observed_at is None or observed_at >= cutoff_at:
                continue
            event_id = str(record.get("source_signal_id") or candidate_id)
            _put(
                buckets[identity]["occurrences"],
                event_id,
                {
                    "occurrence_id": event_id,
                    "event_id": event_id,
                    "candidate_id": candidate_id,
                    "observed_at": observed_at.isoformat(),
                    "status": "manual_ready",
                    "entry_limit": record.get("entry_limit"),
                },
            )
            _put_delivery(
                buckets[identity],
                f"{candidate_id}:ready",
                event_id,
                observed_at,
                {"notification_status": "notification_intent"},
            )


def _load_delivery_evidence(features_root: Path, buckets: dict, *, cutoff_at: datetime) -> None:
    pattern = "trade_intent_producer_ledger/date=*/events.jsonl"
    for path in sorted(features_root.glob(pattern)):
        for record in _iter_jsonl(path):
            if record.get("record_type") != "trade_ready_delivery_expectation":
                continue
            identity = (SET_TRADE_READY, str(record.get("intent_id") or ""))
            observed_at = _first_time(record, "observed_at")
            delivery_id = str(record.get("delivery_event_id") or "")
            if (
                identity not in buckets
                or observed_at is None
                or observed_at >= cutoff_at
                or not delivery_id
            ):
                continue
            _put_delivery(
                buckets[identity],
                delivery_id,
                str(record.get("intent_event_id") or ""),
                observed_at,
                {"notification_status": "delivery_expected"},
            )


def _load_virtual_evidence(features_root: Path, buckets: dict, *, cutoff_at: datetime) -> None:
    by_source = {identity[1]: identity for identity in buckets}
    for path in sorted(features_root.glob("virtual_strategy/date=*/events.jsonl")):
        for record in _iter_jsonl(path):
            identity = by_source.get(str(record.get("source_signal_id") or ""))
            observed_at = _first_time(
                record,
                "closed_at",
                "opened_at",
                "evaluated_at",
                "observed_at",
            )
            episode_id = str(record.get("episode_id") or "")
            if (
                identity is None
                or observed_at is None
                or observed_at >= cutoff_at
                or not episode_id
            ):
                continue
            event = str(record.get("event") or record.get("status") or "virtual_observation")
            episodes = buckets[identity]["virtual_episodes"]
            episode = episodes.setdefault(
                episode_id,
                {
                    "episode_id": episode_id,
                    "observed_at": observed_at.isoformat(),
                    "opened_at": None,
                    "closed_at": None,
                    "status": None,
                    "exit_reason": None,
                    "events": [],
                },
            )
            event_row = {
                "event": event,
                "observed_at": observed_at.isoformat(),
                "status": record.get("status"),
            }
            if event_row not in episode["events"]:
                episode["events"].append(event_row)
                episode["events"].sort(key=_evidence_sort_key)
            episode["observed_at"] = min(
                str(episode["observed_at"]), observed_at.isoformat()
            )
            if event == "virtual_opened":
                episode["opened_at"] = observed_at.isoformat()
            if event == "virtual_closed":
                episode["closed_at"] = observed_at.isoformat()
            if record.get("status") is not None:
                episode["status"] = record.get("status")
            if record.get("exit_reason") is not None:
                episode["exit_reason"] = record.get("exit_reason")


def _put_delivery(
    bucket: dict,
    delivery_id: str,
    source_event_id: str,
    observed_at: datetime,
    record: Mapping[str, object],
) -> None:
    _put(
        bucket["delivery_events"],
        delivery_id,
        {
            "delivery_event_id": delivery_id,
            "source_event_id": source_event_id or None,
            "observed_at": observed_at.isoformat(),
            "status": record.get("notification_status"),
        },
    )


def _put(rows: dict[str, dict[str, object]], key: str, payload: dict[str, object]) -> None:
    prior = rows.get(key)
    if prior is None:
        rows[key] = payload
        return
    rows[key] = {
        field: value if value is not None else prior.get(field)
        for field, value in {**prior, **payload}.items()
    }


def _first_time(record: Mapping[str, object], *fields: str) -> datetime | None:
    for field in fields:
        value = _parse_ts(record.get(field))
        if value is not None:
            return value
    return None


def _is_trade_ready(record: Mapping[str, object]) -> bool:
    return record.get("status") == SET_TRADE_READY or record.get("signal_status") == SET_TRADE_READY


def _evidence_sort_key(row: Mapping[str, object]) -> tuple[str, str]:
    identity = str(
        row.get("occurrence_id")
        or row.get("delivery_event_id")
        or row.get("episode_id")
        or hashlib.sha256(repr(sorted(row.items())).encode()).hexdigest()
    )
    return str(row.get("observed_at") or ""), identity
