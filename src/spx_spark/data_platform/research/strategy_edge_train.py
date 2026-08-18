"""Train candidate-level SPXW edge models from existing operational history.

This one-shot offline command replays conservative combination bids from the
quote lake, labels selected/no-trade/shadow candidates with a fixed 20-minute
debit management contract, performs session-purged walk-forward validation,
and emits the JSON artifact consumed by ``strategy_edge_model``.

The command never auto-promotes a weak model: each RTH/GTH structure family
must pass explicit out-of-sample PnL, drawdown, concentration, and coverage
gates before runtime may authorize a manual card.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import date, datetime, timezone
import json
import math
from pathlib import Path
import sqlite3
from typing import Any

import numpy as np

from spx_spark.analytics.options.strategy_payoff import (
    DEFAULT_MANAGEMENT_POLICY,
    policy_mark_horizon_end,
    simulate_management_policy,
)
from spx_spark.application.order_map.strategy_edge_model import (
    ARTIFACT_RELATIVE_PATH,
    FEATURE_NAMES,
    FEATURE_VERSION,
    SCHEMA_VERSION,
    candidate_edge_features,
    edge_model_key,
    feature_vector,
)
from spx_spark.data_platform.research.odte_level_quotes import QuoteStore
from spx_spark.data_platform.research.strategy_policy_backfill import (
    _candidate_legs,
    _combo_bid_marks,
    _entry_ask,
)


ENTRY_EDGE_POLICY = replace(
    DEFAULT_MANAGEMENT_POLICY,
    policy_version="management_policy.entry_edge_20m.v1",
    time_stop_minutes=20,
)
DEFAULT_THRESHOLDS = {
    "min_expected_pnl_points": 0.25,
    "min_expected_pnl_lcb_points": 0.10,
    "min_p_profit": 0.58,
    "max_p_stop_first_5m": 0.30,
    "min_return_on_risk": 0.08,
}
DEFAULT_PROMOTION_GATES = {
    "min_oof_trades": 60,
    "min_holdout_trades": 15,
    "min_profit_factor": 1.25,
    "min_average_pnl_points": 0.15,
    "min_positive_session_ratio": 0.55,
    "max_drawdown_r": 6.0,
    "max_top_session_profit_concentration": 0.35,
}


def load_candidate_labels(
    *,
    database_path: Path,
    data_root: Path,
    start_date: str | None = None,
    end_date: str | None = None,
    lookforward_minutes: int = 20,
) -> list[dict[str, Any]]:
    """Label all persisted candidate-bearing decisions from existing history."""

    decisions = _read_candidate_decisions(
        database_path,
        start_date=start_date,
        end_date=end_date,
    )
    # One causal row per candidate opportunity. Reprints of the same option
    # geometry on later ticks otherwise inflate both sample size and confidence.
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for decision in decisions:
        candidate = _candidate(decision)
        if not candidate:
            continue
        session = str(decision.get("session_date") or "")
        identity = str(candidate.get("opportunity_id") or candidate.get("candidate_id") or "")
        if not identity:
            identity = str(decision.get("decision_id") or "")
        key = (session, identity)
        known = deduped.get(key)
        if known is None or str(decision.get("decision_at") or "") < str(
            known.get("decision_at") or ""
        ):
            deduped[key] = decision

    store = QuoteStore(data_root)
    rows: list[dict[str, Any]] = []
    try:
        for decision in sorted(
            deduped.values(),
            key=lambda item: (
                str(item.get("session_date") or ""),
                str(item.get("decision_at") or ""),
            ),
        ):
            row = _label_decision(
                decision,
                store=store,
                lookforward_minutes=lookforward_minutes,
            )
            if row is not None:
                rows.append(row)
    finally:
        store.close()
    return rows


def _label_decision(
    decision: Mapping[str, Any],
    *,
    store: QuoteStore,
    lookforward_minutes: int,
) -> dict[str, Any] | None:
    candidate = _candidate(decision)
    decision_at = _time(decision.get("decision_at"))
    strategy_type = str(candidate.get("strategy_type") or "").upper()
    if (
        not candidate
        or decision_at is None
        or not strategy_type.endswith("_DEBIT_VERTICAL")
    ):
        return None
    facts = _map(decision.get("market_facts"))
    regime = _map(decision.get("regime"))
    session_date = _date(decision.get("session_date"))
    if session_date is None:
        return None
    legs = _candidate_legs(candidate)
    if len(legs) < 2:
        return None
    entry_ask = _entry_ask(legs)
    if entry_ask is None:
        return None
    provider = str(legs[0].get("provider") or "schwab")
    end = policy_mark_horizon_end(
        decision_at,
        ENTRY_EDGE_POLICY,
        session_date=session_date,
        lookforward_minutes=lookforward_minutes,
    )
    marks = _combo_bid_marks(
        store,
        legs=legs,
        provider=provider,
        start=decision_at,
        end=end,
    )
    if not marks:
        return None
    label = simulate_management_policy(
        marks,
        entry_ask=entry_ask,
        leg_count=len(legs),
        entry_at=decision_at,
        policy=ENTRY_EDGE_POLICY,
        session_date=session_date,
    )
    features = candidate_edge_features(
        candidate,
        facts,
        regime,
        now=decision_at,
    )
    exit_seconds = (
        (label.exit_at - decision_at).total_seconds()
        if label.exit_at is not None
        else None
    )
    max_loss = _number(_map(candidate.get("economics")).get("max_loss_points"))
    return {
        "schema_version": "strategy_edge_training_row.v1",
        "decision_id": decision.get("decision_id"),
        "session_date": session_date.isoformat(),
        "decision_at": decision_at.isoformat(),
        "model_key": edge_model_key(candidate, facts),
        "strategy_type": candidate.get("strategy_type"),
        "setup_kind": candidate.get("setup_kind"),
        "direction": candidate.get("direction"),
        "candidate_id": candidate.get("candidate_id"),
        "opportunity_id": candidate.get("opportunity_id"),
        "entry_ask": entry_ask,
        "max_loss_points": max_loss if max_loss is not None else entry_ask,
        "features": features,
        "policy_pnl_points": label.policy_pnl_points,
        "profit": int(label.policy_pnl_points > 0.0),
        "stop_first_5m": int(
            label.exit_reason == "premium_stop"
            and exit_seconds is not None
            and exit_seconds <= 300.0
        ),
        "exit_reason": label.exit_reason,
        "exit_seconds": exit_seconds,
        "mfe_points": label.mfe_points,
        "mae_points": label.mae_points,
        "policy_version": label.policy_version,
    }


def train_edge_artifact(
    rows: Sequence[Mapping[str, Any]],
    *,
    generated_at: datetime,
    holdout_sessions: int = 8,
    min_train_sessions: int = 12,
    thresholds: Mapping[str, float] = DEFAULT_THRESHOLDS,
    promotion_gates: Mapping[str, float] = DEFAULT_PROMOTION_GATES,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Train independent session/family models with expanding walk-forward OOF."""

    by_key: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for raw in rows:
        row = dict(raw)
        if _valid_training_row(row):
            by_key[str(row["model_key"])].append(row)

    models: dict[str, Any] = {}
    reports: dict[str, Any] = {}
    for model_key, group in sorted(by_key.items()):
        model, report = _train_group(
            model_key,
            group,
            holdout_sessions=holdout_sessions,
            min_train_sessions=min_train_sessions,
            thresholds=thresholds,
            promotion_gates=promotion_gates,
        )
        reports[model_key] = report
        if model is not None:
            models[model_key] = model

    generated = _utc(generated_at)
    artifact = {
        "schema_version": SCHEMA_VERSION,
        "feature_version": FEATURE_VERSION,
        "artifact_version": f"entry_edge.v1:{generated.strftime('%Y%m%dT%H%M%SZ')}",
        "generated_at": generated.isoformat(),
        "valid_days": 14,
        "feature_names": list(FEATURE_NAMES),
        "management_policy_version": ENTRY_EDGE_POLICY.policy_version,
        "models": models,
    }
    report = {
        "schema_version": "strategy_edge_training_report.v1",
        "generated_at": generated.isoformat(),
        "rows": len(rows),
        "sessions": sorted({str(row.get("session_date") or "") for row in rows}),
        "models": reports,
        "promoted_models": sorted(
            key for key, value in models.items() if value.get("promoted") is True
        ),
    }
    return artifact, report


def _train_group(
    model_key: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    holdout_sessions: int,
    min_train_sessions: int,
    thresholds: Mapping[str, float],
    promotion_gates: Mapping[str, float],
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    ordered = sorted(
        (dict(row) for row in rows),
        key=lambda item: (str(item["session_date"]), str(item["decision_at"])),
    )
    sessions = sorted({str(row["session_date"]) for row in ordered})
    if len(sessions) < max(min_train_sessions + 2, 6):
        return None, {
            "model_key": model_key,
            "promoted": False,
            "reason": "insufficient_sessions",
            "sessions": len(sessions),
            "rows": len(ordered),
        }
    holdout_count = min(max(holdout_sessions, 1), max(1, len(sessions) // 3))
    development_sessions = sessions[:-holdout_count]
    holdout_set = set(sessions[-holdout_count:])
    if len(development_sessions) <= min_train_sessions:
        return None, {
            "model_key": model_key,
            "promoted": False,
            "reason": "insufficient_development_sessions",
            "sessions": len(sessions),
            "rows": len(ordered),
        }

    oof: list[dict[str, Any]] = []
    for index in range(min_train_sessions, len(development_sessions)):
        train_set = set(development_sessions[:index])
        validation_session = development_sessions[index]
        train_rows = [row for row in ordered if row["session_date"] in train_set]
        validation_rows = [
            row for row in ordered if row["session_date"] == validation_session
        ]
        if len(train_rows) < 30 or not validation_rows:
            continue
        fitted = _fit_models(train_rows)
        oof.extend(_predict_rows(fitted, validation_rows))

    if len(oof) < 10:
        return None, {
            "model_key": model_key,
            "promoted": False,
            "reason": "insufficient_walk_forward_predictions",
            "sessions": len(sessions),
            "rows": len(ordered),
            "oof_rows": len(oof),
        }
    residual_q10 = float(
        np.quantile(
            np.asarray(
                [
                    row["policy_pnl_points"] - row["expected_pnl_points"]
                    for row in oof
                ],
                dtype=float,
            ),
            0.10,
        )
    )
    oof_metrics = _selection_metrics(
        oof,
        residual_q10=residual_q10,
        thresholds=thresholds,
    )

    development_rows = [
        row for row in ordered if row["session_date"] in set(development_sessions)
    ]
    holdout_rows = [row for row in ordered if row["session_date"] in holdout_set]
    development_fit = _fit_models(development_rows)
    holdout_predictions = _predict_rows(development_fit, holdout_rows)
    holdout_metrics = _selection_metrics(
        holdout_predictions,
        residual_q10=residual_q10,
        thresholds=thresholds,
    )
    promotion = _promotion_decision(
        oof_metrics,
        holdout_metrics,
        promotion_gates=promotion_gates,
    )

    final_fit = _fit_models(ordered)
    model_version = f"entry_edge_{model_key.replace('|', '_')}_v1"
    model = {
        **final_fit,
        "model_version": model_version,
        "promoted": promotion["promoted"],
        "promotion": promotion,
        "thresholds": dict(thresholds),
        "residual_q10_points": round(residual_q10, 8),
        "trained_from": sessions[0],
        "trained_through": sessions[-1],
        "training_rows": len(ordered),
        "training_sessions": len(sessions),
        "oof_metrics": oof_metrics,
        "holdout_metrics": holdout_metrics,
    }
    report = {
        "model_key": model_key,
        "model_version": model_version,
        "promoted": promotion["promoted"],
        "promotion": promotion,
        "rows": len(ordered),
        "sessions": sessions,
        "development_sessions": development_sessions,
        "holdout_sessions": sorted(holdout_set),
        "residual_q10_points": round(residual_q10, 8),
        "oof_metrics": oof_metrics,
        "holdout_metrics": holdout_metrics,
    }
    return model, report


def _fit_models(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    from sklearn.linear_model import LogisticRegression, Ridge
    from sklearn.preprocessing import StandardScaler

    x = np.asarray(
        [feature_vector(_map(row.get("features"))) for row in rows],
        dtype=float,
    )
    pnl = np.asarray([float(row["policy_pnl_points"]) for row in rows], dtype=float)
    profit = np.asarray([int(row["profit"]) for row in rows], dtype=int)
    stop = np.asarray([int(row["stop_first_5m"]) for row in rows], dtype=int)
    scaler = StandardScaler().fit(x)
    z = scaler.transform(x)
    ridge = Ridge(alpha=2.0).fit(z, pnl)
    profit_model = _fit_logistic(LogisticRegression, z, profit)
    stop_model = _fit_logistic(LogisticRegression, z, stop)
    max_z = np.max(np.abs(z), axis=1) if len(z) else np.asarray([0.0])
    return {
        "feature_mean": [round(float(value), 12) for value in scaler.mean_],
        "feature_scale": [round(float(value), 12) for value in scaler.scale_],
        "pnl": {
            "intercept": round(float(ridge.intercept_), 12),
            "coef": [round(float(value), 12) for value in ridge.coef_],
        },
        "profit": profit_model,
        "stop_first_5m": stop_model,
        "max_abs_z": round(
            max(3.0, float(np.quantile(max_z, 0.99)) + 0.25),
            8,
        ),
    }


def _fit_logistic(factory: Any, x: np.ndarray, labels: np.ndarray) -> dict[str, Any]:
    unique = np.unique(labels)
    if len(unique) < 2:
        # Laplace-smoothed constant probability remains valid and explicit.
        probability = (float(np.sum(labels)) + 1.0) / (float(len(labels)) + 2.0)
        intercept = math.log(probability / (1.0 - probability))
        return {
            "intercept": round(intercept, 12),
            "coef": [0.0] * x.shape[1],
            "constant": True,
        }
    model = factory(
        C=0.5,
        class_weight="balanced",
        max_iter=2_000,
        solver="liblinear",
        random_state=0,
    ).fit(x, labels)
    return {
        "intercept": round(float(model.intercept_[0]), 12),
        "coef": [round(float(value), 12) for value in model.coef_[0]],
        "constant": False,
    }


def _predict_rows(
    fitted: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    if not rows:
        return []
    mean = np.asarray(fitted["feature_mean"], dtype=float)
    scale = np.asarray(fitted["feature_scale"], dtype=float)
    x = np.asarray(
        [feature_vector(_map(row.get("features"))) for row in rows],
        dtype=float,
    )
    z = (x - mean) / scale
    expected = _linear_predictions(z, _map(fitted.get("pnl")))
    p_profit = _sigmoid_array(_linear_predictions(z, _map(fitted.get("profit"))))
    p_stop = _sigmoid_array(
        _linear_predictions(z, _map(fitted.get("stop_first_5m")))
    )
    result = []
    for row, pnl, profit, stop in zip(
        rows,
        expected,
        p_profit,
        p_stop,
        strict=True,
    ):
        result.append(
            {
                **dict(row),
                "expected_pnl_points": float(pnl),
                "p_profit": float(profit),
                "p_stop_first_5m": float(stop),
            }
        )
    return result


def _linear_predictions(x: np.ndarray, model: Mapping[str, Any]) -> np.ndarray:
    coefficients = np.asarray(model.get("coef"), dtype=float)
    intercept = float(model.get("intercept"))
    return x @ coefficients + intercept


def _sigmoid_array(values: np.ndarray) -> np.ndarray:
    clipped = np.clip(values, -50.0, 50.0)
    return 1.0 / (1.0 + np.exp(-clipped))


def _selection_metrics(
    rows: Sequence[Mapping[str, Any]],
    *,
    residual_q10: float,
    thresholds: Mapping[str, float],
) -> dict[str, Any]:
    selected: list[dict[str, Any]] = []
    for raw in rows:
        row = dict(raw)
        expected = float(row["expected_pnl_points"])
        lower = expected + residual_q10
        max_loss = max(float(row.get("max_loss_points") or 0.0), 1e-9)
        return_on_risk = lower / max_loss
        if (
            expected >= float(thresholds["min_expected_pnl_points"])
            and lower >= float(thresholds["min_expected_pnl_lcb_points"])
            and float(row["p_profit"]) >= float(thresholds["min_p_profit"])
            and float(row["p_stop_first_5m"])
            <= float(thresholds["max_p_stop_first_5m"])
            and return_on_risk >= float(thresholds["min_return_on_risk"])
        ):
            row["expected_pnl_lcb_points"] = lower
            row["return_on_risk"] = return_on_risk
            selected.append(row)
    selected.sort(
        key=lambda item: (str(item["session_date"]), str(item["decision_at"]))
    )
    pnl = [float(row["policy_pnl_points"]) for row in selected]
    gains = sum(value for value in pnl if value > 0)
    losses = -sum(value for value in pnl if value < 0)
    profit_factor = gains / losses if losses > 0 else (
        float("inf") if gains > 0 else 0.0
    )
    by_session: dict[str, float] = defaultdict(float)
    equity_r = 0.0
    peak_r = 0.0
    max_drawdown_r = 0.0
    for row in selected:
        session = str(row["session_date"])
        value = float(row["policy_pnl_points"])
        by_session[session] += value
        equity_r += value / max(float(row.get("max_loss_points") or 0.0), 1e-9)
        peak_r = max(peak_r, equity_r)
        max_drawdown_r = max(max_drawdown_r, peak_r - equity_r)
    positive_sessions = [value for value in by_session.values() if value > 0]
    positive_session_ratio = (
        len(positive_sessions) / len(by_session) if by_session else 0.0
    )
    total_positive_session_pnl = sum(positive_sessions)
    concentration = (
        max(positive_sessions) / total_positive_session_pnl
        if positive_sessions and total_positive_session_pnl > 0
        else 1.0
    )
    return {
        "candidate_rows": len(rows),
        "trades": len(selected),
        "sessions_traded": len(by_session),
        "net_pnl_points": round(sum(pnl), 8),
        "average_pnl_points": (
            round(sum(pnl) / len(pnl), 8) if pnl else 0.0
        ),
        "hit_rate": (
            round(sum(value > 0 for value in pnl) / len(pnl), 8)
            if pnl
            else 0.0
        ),
        "profit_factor": (
            "inf" if math.isinf(profit_factor) else round(profit_factor, 8)
        ),
        "positive_session_ratio": round(positive_session_ratio, 8),
        "max_drawdown_r": round(max_drawdown_r, 8),
        "top_session_profit_concentration": round(concentration, 8),
    }


def _promotion_decision(
    oof: Mapping[str, Any],
    holdout: Mapping[str, Any],
    *,
    promotion_gates: Mapping[str, float],
) -> dict[str, Any]:
    checks = {
        "oof_trade_count": int(oof.get("trades") or 0)
        >= int(promotion_gates["min_oof_trades"]),
        "holdout_trade_count": int(holdout.get("trades") or 0)
        >= int(promotion_gates["min_holdout_trades"]),
        "oof_positive_pnl": float(oof.get("net_pnl_points") or 0.0) > 0.0,
        "holdout_positive_pnl": float(holdout.get("net_pnl_points") or 0.0) > 0.0,
        "oof_profit_factor": _metric_float(oof.get("profit_factor"))
        >= float(promotion_gates["min_profit_factor"]),
        "holdout_profit_factor": _metric_float(holdout.get("profit_factor"))
        >= float(promotion_gates["min_profit_factor"]),
        "oof_average_pnl": float(oof.get("average_pnl_points") or 0.0)
        >= float(promotion_gates["min_average_pnl_points"]),
        "holdout_average_pnl": float(holdout.get("average_pnl_points") or 0.0)
        >= float(promotion_gates["min_average_pnl_points"]),
        "oof_positive_sessions": float(oof.get("positive_session_ratio") or 0.0)
        >= float(promotion_gates["min_positive_session_ratio"]),
        "holdout_positive_sessions": float(
            holdout.get("positive_session_ratio") or 0.0
        )
        >= float(promotion_gates["min_positive_session_ratio"]),
        "oof_drawdown": _metric_value(oof, "max_drawdown_r", float("inf"))
        <= float(promotion_gates["max_drawdown_r"]),
        "holdout_drawdown": _metric_value(
            holdout,
            "max_drawdown_r",
            float("inf"),
        )
        <= float(promotion_gates["max_drawdown_r"]),
        "oof_concentration": _metric_value(
            oof,
            "top_session_profit_concentration",
            1.0,
        )
        <= float(promotion_gates["max_top_session_profit_concentration"]),
        "holdout_concentration": _metric_value(
            holdout,
            "top_session_profit_concentration",
            1.0,
        )
        <= float(promotion_gates["max_top_session_profit_concentration"]),
    }
    return {
        "promoted": all(checks.values()),
        "checks": checks,
        "gates": dict(promotion_gates),
        "failed": [name for name, passed in checks.items() if not passed],
    }


def _read_candidate_decisions(
    database_path: Path,
    *,
    start_date: str | None,
    end_date: str | None,
) -> list[dict[str, Any]]:
    connection = sqlite3.connect(database_path)
    try:
        rows = connection.execute(
            """
            SELECT decision_id, event_key, session_date, decision_at, status, attributes_json
            FROM decisions
            WHERE strategy_name = 'strategy_signal_engine_v2'
              AND status IN ('selected', 'no_trade', 'shadow_candidate')
              AND (? IS NULL OR session_date >= ?)
              AND (? IS NULL OR session_date <= ?)
            ORDER BY decision_at, decision_id
            """,
            (start_date, start_date, end_date, end_date),
        ).fetchall()
    finally:
        connection.close()
    result = []
    for decision_id, event_key, session_date, decision_at, status, attributes_json in rows:
        try:
            payload = json.loads(attributes_json)
        except (TypeError, json.JSONDecodeError):
            continue
        result.append(
            {
                **dict(_map(payload)),
                "decision_id": decision_id,
                "event_key": event_key,
                "session_date": session_date,
                "decision_at": decision_at,
                "status": status,
            }
        )
    return result


def write_artifact(value: Mapping[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _candidate(decision: Mapping[str, Any]) -> Mapping[str, Any]:
    candidate = _map(decision.get("candidate"))
    if candidate:
        return candidate
    return _map(_map(decision.get("why_not")).get("nearest_candidate"))


def _valid_training_row(row: Mapping[str, Any]) -> bool:
    features = _map(row.get("features"))
    return (
        str(row.get("model_key") or "") != ""
        and all(name in features for name in FEATURE_NAMES)
        and _number(row.get("policy_pnl_points")) is not None
        and _number(row.get("max_loss_points")) is not None
        and float(row.get("max_loss_points") or 0.0) > 0
    )


def _metric_float(value: object) -> float:
    if value == "inf":
        return float("inf")
    return float(value or 0.0)


def _metric_value(
    metrics: Mapping[str, Any], key: str, default: float
) -> float:
    value = metrics.get(key)
    return default if value is None else float(value)


def _map(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    if not isinstance(value, int | float) or isinstance(value, bool):
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def _time(value: object) -> datetime | None:
    if not isinstance(value, str | datetime):
        return None
    try:
        parsed = (
            value
            if isinstance(value, datetime)
            else datetime.fromisoformat(value.replace("Z", "+00:00"))
        )
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _date(value: object) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("training timestamp must be timezone-aware")
    return value.astimezone(timezone.utc)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--start-date", type=str, default=None)
    parser.add_argument("--end-date", type=str, default=None)
    parser.add_argument("--holdout-sessions", type=int, default=8)
    parser.add_argument("--min-train-sessions", type=int, default=12)
    parser.add_argument("--lookforward-minutes", type=int, default=20)
    parser.add_argument("--artifact", type=Path, default=None)
    parser.add_argument("--report", type=Path, default=None)
    args = parser.parse_args(argv)

    rows = load_candidate_labels(
        database_path=args.database,
        data_root=args.data_root,
        start_date=args.start_date,
        end_date=args.end_date,
        lookforward_minutes=args.lookforward_minutes,
    )
    artifact, report = train_edge_artifact(
        rows,
        generated_at=datetime.now(tz=timezone.utc),
        holdout_sessions=args.holdout_sessions,
        min_train_sessions=args.min_train_sessions,
    )
    artifact_path = args.artifact or args.data_root.joinpath(*ARTIFACT_RELATIVE_PATH)
    report_path = (
        args.report
        or args.data_root / "research" / "strategy_edge_model.v1.report.json"
    )
    write_artifact(artifact, artifact_path)
    write_artifact(report, report_path)
    print(
        json.dumps(
            {
                "labeled_rows": len(rows),
                "artifact": str(artifact_path),
                "report": str(report_path),
                "promoted_models": report["promoted_models"],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
