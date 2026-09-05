"""Publish the fixed, account-free images linked from operator cards."""

from __future__ import annotations

import os
import hashlib
import json
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

from spx_spark.config import StorageSettings
from spx_spark.features.exposure_map import build_exposure_map
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.options_map import (
    group_spxw_option_quotes,
    write_open_interest_mirror_png,
    write_strategy_risk_png,
)
from spx_spark.options_map.net_premium_render import write_net_premium_flow_png
from spx_spark.options_map.orchestration import (
    cache_complete_open_interest_payload,
    gth_open_interest_payload,
)
from spx_spark.state_io import read_json_object
from spx_spark.storage import LatestStateStore


STRATEGY_RISK_IMAGE_PUBLIC_PATH = "/strategy-risk/latest.png"
STRATEGY_RISK_IMAGE_PUBLIC_URL = "https://spx.zh3nyu.com/strategy-risk/latest.png"
STRATEGY_RISK_GTH_IMAGE_PUBLIC_PATH = "/strategy-risk/gth-latest.png"
STRATEGY_RISK_GTH_IMAGE_PUBLIC_URL = "https://spx.zh3nyu.com/strategy-risk/gth-latest.png"
OPEN_INTEREST_IMAGE_PUBLIC_PATH = "/oi/latest.png"
OPEN_INTEREST_IMAGE_PUBLIC_URL = "https://spx.zh3nyu.com/oi/latest.png"
NET_PREMIUM_FLOW_IMAGE_PUBLIC_PATH = "/flow/latest.png"
NET_PREMIUM_FLOW_IMAGE_PUBLIC_URL = "https://spx.zh3nyu.com/flow/latest.png"


def publish_desk_map_images(
    storage_settings: StorageSettings,
    *,
    now: datetime,
) -> dict[str, dict[str, object]]:
    """Refresh independent OI and flow images for one visible Desk Map."""

    return {
        "oi_image": publish_open_interest_image(storage_settings, now=now),
        "net_premium_flow_image": publish_net_premium_flow_image(storage_settings, now=now),
    }


def publish_net_premium_flow_image(
    storage_settings: StorageSettings,
    *,
    now: datetime,
) -> dict[str, object]:
    """Refresh the fixed RTH captured-flow image without changing authority."""

    try:
        state = read_json_object(
            Path(storage_settings.data_root) / "latest" / "intraday_shock_state.json"
        )
        output = (
            Path(storage_settings.data_root) / "published" / "spxw-surface" / "flow" / "latest.png"
        )
        write_net_premium_flow_png(state, output)
        tape = state.get("captured_net_premium_divergence")
        updated_at = tape.get("updated_at") if isinstance(tape, Mapping) else None
    except Exception as exc:  # noqa: BLE001 - image failure cannot block delivery
        return {
            "status": "failed",
            "error": f"{type(exc).__name__}:{exc}",
            "public_path": NET_PREMIUM_FLOW_IMAGE_PUBLIC_PATH,
            "public_url": NET_PREMIUM_FLOW_IMAGE_PUBLIC_URL,
        }
    return {
        "status": "published",
        "as_of": str(updated_at or now.isoformat()),
        "public_path": NET_PREMIUM_FLOW_IMAGE_PUBLIC_PATH,
        "public_url": NET_PREMIUM_FLOW_IMAGE_PUBLIC_URL,
        "bytes": output.stat().st_size,
    }


def publish_open_interest_image(
    storage_settings: StorageSettings,
    *,
    now: datetime,
) -> dict[str, object]:
    """Refresh the stable OI image immediately before a human-visible card."""

    try:
        output = (
            Path(storage_settings.data_root) / "published" / "spxw-surface" / "oi" / "latest.png"
        )
        state = LatestStateStore(storage_settings).load()
        if DEFAULT_MARKET_CALENDAR.is_spx_gth_open(now):
            payload = gth_open_interest_payload(
                storage_settings,
                state=state,
                now=now,
            )
            front_payload = payload["expiries"][0]
            expiry = str(front_payload.get("expiry") or "unknown")
            rendered_as_of = now.isoformat()
        else:
            grouped = group_spxw_option_quotes(state, storage_settings=storage_settings)
            exposure = build_exposure_map(state, grouped_quotes=grouped)
            if not exposure.expiries:
                raise ValueError("front expiry exposure unavailable")
            front = exposure.expiries[0]
            if len(front.walls.put_walls) < 3 or len(front.walls.call_walls) < 3:
                raise ValueError("top-3 put/call walls unavailable")
            payload = exposure.to_dict()
            cache_complete_open_interest_payload(storage_settings, payload)
            expiry = front.expiry
            rendered_as_of = exposure.as_of.isoformat()
        write_open_interest_mirror_png(payload, output)
    except Exception as exc:  # noqa: BLE001 - card delivery must remain independent
        return {
            "status": "failed",
            "error": f"{type(exc).__name__}:{exc}",
            "public_path": OPEN_INTEREST_IMAGE_PUBLIC_PATH,
            "public_url": OPEN_INTEREST_IMAGE_PUBLIC_URL,
        }
    return {
        "status": "published",
        "as_of": rendered_as_of,
        "expiry": expiry,
        "public_path": OPEN_INTEREST_IMAGE_PUBLIC_PATH,
        "public_url": OPEN_INTEREST_IMAGE_PUBLIC_URL,
        "bytes": output.stat().st_size,
    }


def publish_strategy_risk_image(
    storage_settings: StorageSettings,
    *,
    decision: dict[str, Any],
    now: datetime,
) -> dict[str, object]:
    """Freeze the decision image and manifest; latest is a separate live alias."""

    facts = decision.get("market_facts") if isinstance(decision.get("market_facts"), dict) else {}
    session = facts.get("session") if isinstance(facts.get("session"), dict) else {}
    gth = str(session.get("mode") or "").lower() == "gth"
    snapshot = json.dumps(decision, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    artifact_id = hashlib.sha256(snapshot.encode("utf-8")).hexdigest()
    public_path = f"/strategy-risk/{artifact_id}.png"
    public_url = f"https://spx.zh3nyu.com{public_path}"
    try:
        output = (
            Path(storage_settings.data_root)
            / "published"
            / "spxw-surface"
            / "strategy-risk"
            / f"{artifact_id}.png"
        )
        if not decision:
            raise ValueError("strategy decision unavailable")
        decision_at = _timestamp(decision.get("decision_at"))
        facts_at = _timestamp(facts.get("decision_at"))
        if (
            decision_at is not None
            and facts_at is not None
            and abs((decision_at - facts_at).total_seconds()) > 1.0
        ):
            raise ValueError("strategy risk decision and market facts are not time-aligned")
        if not output.exists():
            write_strategy_risk_png(decision, output)
        manifest = output.with_suffix(".json")
        if not manifest.exists():
            with manifest.open("x", encoding="utf-8") as handle:
                json.dump({
                    "decision": decision,
                    "image_sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
                    "public_path": public_path,
                }, handle, ensure_ascii=False, sort_keys=True)
        for alias in ("latest.png", "gth-latest.png") if gth else ("latest.png",):
            session_output = output.with_name(alias)
            temporary = output.with_name(f".{alias}.{os.getpid()}.tmp")
            try:
                temporary.unlink(missing_ok=True)
                os.link(output, temporary)
                os.replace(temporary, session_output)
            finally:
                temporary.unlink(missing_ok=True)
    except Exception as exc:  # noqa: BLE001 - image failure must not block delivery
        return {
            "status": "failed",
            "error": f"{type(exc).__name__}:{exc}",
            "public_path": public_path,
            "public_url": public_url,
        }
    return {
        "status": "published",
        "as_of": str(decision.get("available_at") or now.isoformat()),
        "decision_id": decision.get("decision_id"),
        "strategy_type": decision.get("decision_type"),
        "manifest_url": public_url.removesuffix(".png") + ".json",
        "public_path": public_path,
        "public_url": public_url,
        "bytes": output.stat().st_size,
    }


def _timestamp(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
