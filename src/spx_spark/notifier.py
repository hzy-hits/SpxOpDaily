from __future__ import annotations

import json
import os
import subprocess
import tempfile
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from spx_spark.config import NotificationSettings


SEVERITY_RANK = {
    "info": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
    "critical": 4,
}

CommandRunner = Callable[[list[str], float], subprocess.CompletedProcess[str]]


@dataclass(frozen=True)
class SinkResult:
    sink: str
    attempted: bool
    ok: bool
    dry_run: bool = False
    exit_code: int | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class NotificationResult:
    enabled: bool
    selected_count: int
    sent_count: int
    skipped_reason: str | None
    sinks: tuple[SinkResult, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "enabled": self.enabled,
            "selected_count": self.selected_count,
            "sent_count": self.sent_count,
            "skipped_reason": self.skipped_reason,
            "sinks": [sink.to_dict() for sink in self.sinks],
        }


def default_runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )


def severity_value(value: object) -> int:
    return SEVERITY_RANK.get(str(value or "").lower(), -1)


def alert_key(alert: dict[str, object]) -> str:
    return "|".join(
        (
            str(alert.get("kind") or ""),
            str(alert.get("instrument_id") or ""),
            str(alert.get("title") or ""),
        )
    )


def load_sent_state(path: str) -> dict[str, float]:
    state_path = Path(path)
    if not state_path.exists():
        return {}
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    sent = payload.get("sent_at_by_key") if isinstance(payload, dict) else None
    if not isinstance(sent, dict):
        return {}
    return {str(key): float(value) for key, value in sent.items() if isinstance(value, int | float)}


def save_sent_state(path: str, sent_at_by_key: dict[str, float]) -> None:
    state_path = Path(path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = state_path.with_suffix(f"{state_path.suffix}.tmp")
    payload = {
        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
        "sent_at_by_key": dict(sorted(sent_at_by_key.items())),
    }
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(state_path)


def select_alerts_for_notification(
    payload: dict[str, object],
    settings: NotificationSettings,
    *,
    now: datetime | None = None,
) -> tuple[list[dict[str, object]], dict[str, float]]:
    alerts = payload.get("alerts")
    if not isinstance(alerts, list):
        return [], load_sent_state(settings.state_path)

    now = now or datetime.now(tz=timezone.utc)
    now_ts = now.timestamp()
    min_rank = severity_value(settings.min_severity)
    sent_at_by_key = load_sent_state(settings.state_path)

    selected: list[dict[str, object]] = []
    for alert in alerts:
        if not isinstance(alert, dict):
            continue
        if severity_value(alert.get("severity")) < min_rank:
            continue
        key = alert_key(alert)
        previous_ts = sent_at_by_key.get(key)
        if previous_ts is not None and now_ts - previous_ts < settings.cooldown_seconds:
            continue
        selected.append(alert)
    return selected, sent_at_by_key


def mark_alerts_sent(
    alerts: list[dict[str, object]],
    sent_at_by_key: dict[str, float],
    settings: NotificationSettings,
    *,
    now: datetime | None = None,
) -> None:
    now = now or datetime.now(tz=timezone.utc)
    now_ts = now.timestamp()
    for alert in alerts:
        sent_at_by_key[alert_key(alert)] = now_ts
    save_sent_state(settings.state_path, sent_at_by_key)


def format_alert_message(payload: dict[str, object], alerts: list[dict[str, object]]) -> str:
    window = payload.get("window")
    window_name = "unknown"
    priority = "unknown"
    if isinstance(window, dict):
        window_name = str(window.get("name") or "unknown")
        priority = str(window.get("priority") or "unknown")

    lines = [
        "SPX Spark alert",
        f"window: {window_name} priority={priority}",
        f"as_of: {payload.get('as_of')}",
        f"alerts: {len(alerts)}",
    ]
    for alert in alerts[:6]:
        severity = alert.get("severity")
        title = alert.get("title")
        detail = alert.get("detail")
        lines.append(f"- [{severity}] {title}")
        if detail:
            lines.append(f"  {detail}")
    if len(alerts) > 6:
        lines.append(f"... {len(alerts) - 6} more alerts suppressed in push body")
    return "\n".join(lines)


def build_agent_prompt(payload: dict[str, object], alerts: list[dict[str, object]]) -> str:
    compact_payload = {
        "as_of": payload.get("as_of"),
        "window": payload.get("window"),
        "alerts": alerts[:12],
        "options_map": payload.get("options_map"),
        "iv_surface": payload.get("iv_surface"),
    }
    return "\n".join(
        (
            "你是 SPX Spark 的盘中告警分析 agent。",
            "只根据下面的 JSON 做简短判断；不要给自动下单指令，不要假设缺失数据。",
            "输出结构：1. 发生了什么 2. 风险/数据质量 3. 人类需要看的检查项。",
            json.dumps(compact_payload, ensure_ascii=False, sort_keys=True),
        )
    )


def compact_analysis_payload(
    payload: dict[str, object],
    alerts: list[dict[str, object]],
) -> dict[str, object]:
    options_map = payload.get("options_map")
    compact_options: object = None
    if isinstance(options_map, dict):
        compact_options = {
            "underlier": options_map.get("underlier"),
            "expiries": [
                {
                    "expiry": expiry.get("expiry"),
                    "gamma_state": expiry.get("gamma_state"),
                    "zero_gamma": expiry.get("zero_gamma"),
                    "put_wall": expiry.get("put_wall"),
                    "call_wall": expiry.get("call_wall"),
                    "nearest_wall": expiry.get("nearest_wall"),
                    "nearest_wall_distance_points": expiry.get("nearest_wall_distance_points"),
                }
                for expiry in options_map.get("expiries", [])[:2]
                if isinstance(expiry, dict)
            ],
        }

    iv_surface = payload.get("iv_surface")
    compact_surface: object = None
    if isinstance(iv_surface, dict):
        compact_surface = {
            "front_expiry": iv_surface.get("front_expiry"),
            "next_expiry": iv_surface.get("next_expiry"),
            "front_vs_next_atm_iv_gap": iv_surface.get("front_vs_next_atm_iv_gap"),
            "expiries": [
                {
                    "expiry": expiry.get("expiry"),
                    "atm_iv": expiry.get("atm_iv"),
                    "atm_iv_jump_5m": expiry.get("atm_iv_jump_5m"),
                    "put_skew_steepening_5m": expiry.get("put_skew_steepening_5m"),
                    "iv_surface_shift_5m": expiry.get("iv_surface_shift_5m"),
                    "surface_fit_quality": expiry.get("surface_fit_quality"),
                }
                for expiry in iv_surface.get("expiries", [])[:2]
                if isinstance(expiry, dict)
            ],
        }

    return {
        "as_of": payload.get("as_of"),
        "window": payload.get("window"),
        "alerts": alerts[:8],
        "options_map": compact_options,
        "iv_surface": compact_surface,
    }


def build_codex_prompt(payload: dict[str, object], alerts: list[dict[str, object]]) -> str:
    compact_payload = compact_analysis_payload(payload, alerts)
    return "\n".join(
        (
            "你是 SPX Spark 的快速告警确认 agent。",
            "只根据下面的本机 JSON 判断是否需要推送给人类。不要给自动下单指令，不要编造缺失数据。",
            "输出中文，最多 6 行。必须包含：结论、原因、数据质量、需要人类看的检查项。",
            "如果数据质量不足，明确说 degraded；如果值得看盘，用 `需要看盘` 开头。",
            json.dumps(compact_payload, ensure_ascii=False, sort_keys=True),
        )
    )


def openclaw_state_dir() -> Path:
    return Path(os.getenv("OPENCLAW_STATE_DIR") or Path.home() / ".openclaw")


def resolve_default_weixin_delivery(
    *,
    account: str,
    target: str,
) -> tuple[str, str]:
    if account and target:
        return account, target

    state_dir = openclaw_state_dir()
    resolved_account = account
    if not resolved_account:
        accounts_path = state_dir / "openclaw-weixin" / "accounts.json"
        try:
            accounts = json.loads(accounts_path.read_text(encoding="utf-8"))
            if isinstance(accounts, list) and accounts:
                resolved_account = str(accounts[0])
        except (OSError, json.JSONDecodeError):
            resolved_account = ""

    resolved_target = target
    if not resolved_target and resolved_account:
        account_path = state_dir / "openclaw-weixin" / "accounts" / f"{resolved_account}.json"
        try:
            account_payload = json.loads(account_path.read_text(encoding="utf-8"))
            if isinstance(account_payload, dict):
                resolved_target = str(account_payload.get("userId") or "")
        except (OSError, json.JSONDecodeError):
            resolved_target = ""

    return resolved_account, resolved_target


def run_codex_exec(
    settings: NotificationSettings,
    prompt: str,
    *,
    runner: CommandRunner = default_runner,
) -> tuple[SinkResult, str]:
    with tempfile.NamedTemporaryFile("w+", encoding="utf-8", suffix=".txt", delete=True) as handle:
        command = [
            settings.codex_command,
            "exec",
            "-m",
            settings.codex_model,
            "-c",
            f'model_reasoning_effort="{settings.codex_reasoning_effort}"',
            "-c",
            'approval_policy="never"',
            "--sandbox",
            settings.codex_sandbox,
            "--ephemeral",
            "--skip-git-repo-check",
            "-C",
            settings.codex_cwd,
            "--output-last-message",
            handle.name,
            prompt,
        ]
        try:
            completed = runner(command, settings.codex_timeout_seconds)
            handle.seek(0)
            output = handle.read().strip()
        except Exception as exc:  # noqa: BLE001
            return (
                SinkResult(
                    sink="codex_exec",
                    attempted=True,
                    ok=False,
                    error=str(exc),
                ),
                "",
            )

    error = (completed.stderr or completed.stdout).strip() if completed.returncode else None
    if output and len(output) > settings.codex_output_max_chars:
        output = output[: settings.codex_output_max_chars].rstrip() + "\n..."
    return (
        SinkResult(
            sink="codex_exec",
            attempted=True,
            ok=completed.returncode == 0 and bool(output),
            exit_code=completed.returncode,
            error=error if completed.returncode else None,
        ),
        output,
    )


def send_openclaw_message(
    settings: NotificationSettings,
    message: str,
    *,
    runner: CommandRunner = default_runner,
) -> SinkResult:
    account = settings.openclaw_account
    target = settings.openclaw_target
    if settings.openclaw_channel == "openclaw-weixin":
        account, target = resolve_default_weixin_delivery(account=account, target=target)

    if not settings.openclaw_channel or not target:
        return SinkResult(
            sink="openclaw_message",
            attempted=False,
            ok=False,
            dry_run=settings.openclaw_dry_run,
            error="missing openclaw channel or target",
        )

    command = [
        settings.openclaw_command,
        "message",
        "send",
        "--channel",
        settings.openclaw_channel,
        "--target",
        target,
        "--message",
        message,
        "--json",
    ]
    if account:
        command.extend(["--account", account])
    if settings.openclaw_dry_run:
        command.append("--dry-run")

    try:
        completed = runner(command, settings.openclaw_timeout_seconds)
    except Exception as exc:  # noqa: BLE001
        return SinkResult(
            sink="openclaw_message",
            attempted=True,
            ok=False,
            dry_run=settings.openclaw_dry_run,
            error=str(exc),
        )
    return SinkResult(
        sink="openclaw_message",
        attempted=True,
        ok=completed.returncode == 0,
        dry_run=settings.openclaw_dry_run,
        exit_code=completed.returncode,
        error=(completed.stderr or completed.stdout).strip() if completed.returncode else None,
    )


def run_openclaw_agent(
    settings: NotificationSettings,
    prompt: str,
    *,
    runner: CommandRunner = default_runner,
) -> SinkResult:
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".md", delete=True) as handle:
        handle.write(prompt)
        handle.flush()
        command = [
            settings.openclaw_command,
            "agent",
            "--agent",
            settings.openclaw_agent_name,
            "--session-key",
            settings.openclaw_agent_session_key,
            "--thinking",
            settings.openclaw_agent_thinking,
            "--timeout",
            str(int(settings.openclaw_agent_timeout_seconds)),
            "--message-file",
            handle.name,
            "--json",
        ]
        if settings.openclaw_agent_model:
            command.extend(["--model", settings.openclaw_agent_model])
        if settings.openclaw_agent_deliver:
            command.append("--deliver")
            command.extend(["--reply-channel", settings.openclaw_channel])
            if settings.openclaw_account:
                command.extend(["--reply-account", settings.openclaw_account])
            if settings.openclaw_target:
                command.extend(["--reply-to", settings.openclaw_target])
        try:
            completed = runner(command, settings.openclaw_agent_timeout_seconds)
        except Exception as exc:  # noqa: BLE001
            return SinkResult(
                sink="openclaw_agent",
                attempted=True,
                ok=False,
                error=str(exc),
            )
    return SinkResult(
        sink="openclaw_agent",
        attempted=True,
        ok=completed.returncode == 0,
        exit_code=completed.returncode,
        error=(completed.stderr or completed.stdout).strip() if completed.returncode else None,
    )


def notify_payload(
    payload: dict[str, object],
    *,
    settings: NotificationSettings | None = None,
    runner: CommandRunner = default_runner,
    now: datetime | None = None,
) -> NotificationResult:
    settings = settings or NotificationSettings.from_env()
    if not settings.enabled:
        return NotificationResult(
            enabled=False,
            selected_count=0,
            sent_count=0,
            skipped_reason="disabled",
            sinks=(),
        )

    selected, sent_at_by_key = select_alerts_for_notification(payload, settings, now=now)
    if not selected:
        return NotificationResult(
            enabled=True,
            selected_count=0,
            sent_count=0,
            skipped_reason="no_alerts_after_severity_or_cooldown",
            sinks=(),
        )

    sinks: list[SinkResult] = []
    message = format_alert_message(payload, selected)
    if settings.openclaw_enabled:
        sinks.append(send_openclaw_message(settings, message, runner=runner))
    if settings.codex_enabled:
        codex_result, codex_message = run_codex_exec(
            settings,
            build_codex_prompt(payload, selected),
            runner=runner,
        )
        sinks.append(codex_result)
        if codex_result.ok and settings.codex_deliver:
            sinks.append(send_openclaw_message(settings, codex_message, runner=runner))
    if settings.openclaw_agent_enabled:
        sinks.append(run_openclaw_agent(settings, build_agent_prompt(payload, selected), runner=runner))

    sent_count = sum(1 for sink in sinks if sink.ok)
    if sent_count:
        mark_alerts_sent(selected, sent_at_by_key, settings, now=now)
    skipped_reason = None if sinks else "no_enabled_sinks"
    return NotificationResult(
        enabled=True,
        selected_count=len(selected),
        sent_count=sent_count,
        skipped_reason=skipped_reason,
        sinks=tuple(sinks),
    )
