from __future__ import annotations

import json
import os
import re
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

POSITIVE_DELIVERY_CUES = (
    "需要看盘",
    "需要人类",
    "需要关注",
    "需要立即",
    "高风险",
)

NEGATIVE_DELIVERY_CUES = (
    "不需要推送",
    "无需推送",
    "不要推送",
    "不推送",
    "不需要看盘",
    "无需看盘",
)

HUMAN_VISIBLE_ALERT_PREFIXES = (
    "index:SPX",
    "future:ES",
    "option:SPX:SPXW",
    "option_map:SPXW",
    "iv_surface:SPXW",
)

BLOCKED_HUMAN_MESSAGE_SYMBOLS = (
    "VIX",
    "VVIX",
    "SKEW",
    "SPY",
    "QQQ",
    "IWM",
    "DIA",
    "HYG",
    "LQD",
    "TLT",
    "IEF",
    "SHY",
    "UUP",
    "GLD",
    "USO",
    "RSP",
    "XLU",
    "NDX",
    "RUT",
    "DJX",
    "DJU",
)

BLOCKED_HUMAN_MESSAGE_PHRASES = (
    "hyperliquid",
    "polymarket",
    "crypto_perp",
    "prediction market",
)

SYSTEM_EVENT_ALERT_KINDS = {
    "ibkr_session_interrupted",
    "ibkr_session_restored",
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
    dedup_group = alert.get("dedup_group")
    return "|".join(
        (
            str(alert.get("kind") or ""),
            str(alert.get("instrument_id") or ""),
            "" if dedup_group is None else str(dedup_group),
        )
    )


def is_human_visible_alert(alert: dict[str, object]) -> bool:
    if alert.get("research_only") is True:
        return False
    kind = str(alert.get("kind") or "").lower()
    source_gate = str(alert.get("source_gate") or "").lower()
    blocked_terms = ("smart", "wallet", "onchain", "hyperliquid_proxy")
    if any(term in kind or term in source_gate for term in blocked_terms):
        return False
    instrument_id = str(alert.get("instrument_id") or "")
    return any(instrument_id.startswith(prefix) for prefix in HUMAN_VISIBLE_ALERT_PREFIXES)


def is_system_event_alert(alert: dict[str, object]) -> bool:
    return str(alert.get("kind") or "") in SYSTEM_EVENT_ALERT_KINDS


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
        if not is_human_visible_alert(alert):
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
        "SPX/SPXW alert",
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
        "window": compact_window(payload.get("window")),
        "alerts": alerts[:12],
        "human_focus_context": payload.get("human_focus_context"),
    }
    return "\n".join(
        (
            "你是 SPX Spark 的盘中告警分析 agent。",
            "只根据下面的 JSON 做简短判断；不要给自动下单指令，不要假设缺失数据。",
            "人类只交易 SPX/SPXW；输出只能提 SPX、SPXW、ES、期权墙、gamma、IV surface。",
            "如果 options_map 警告含 underlier_mismatch 或 gamma_state 以 unknown 开头，只说明数据降级，不下 wall/gamma 结论。",
            "输出结构：1. 发生了什么 2. 风险/数据质量 3. 人类需要看的 SPX/SPXW 检查项。",
            json.dumps(compact_payload, ensure_ascii=False, sort_keys=True),
        )
    )


def compact_window(window: object) -> dict[str, object] | None:
    if not isinstance(window, dict):
        return None
    return {
        "name": window.get("name"),
        "priority": window.get("priority"),
        "cadence_seconds": window.get("cadence_seconds"),
        "summary_cadence_seconds": window.get("summary_cadence_seconds"),
        "spxw_sampling_mode": window.get("spxw_sampling_mode"),
        "user_unattended": window.get("user_unattended"),
    }


def compact_analysis_payload(
    payload: dict[str, object],
    alerts: list[dict[str, object]],
) -> dict[str, object]:
    market_context = payload.get("market_context")
    algorithm_quality: object = None
    if isinstance(market_context, dict):
        algorithm_quality = {
            "quality_summary": market_context.get("quality_summary"),
            "note": (
                "Non-focus market context may be used only as hidden algorithm scoring input; "
                "never mention individual non-SPX/SPXW/ES instruments to the human."
            ),
        }

    return {
        "as_of": payload.get("as_of"),
        "window": compact_window(payload.get("window")),
        "visible_scope": ("SPX", "SPXW", "ES"),
        "human_focus_context": payload.get("human_focus_context"),
        "algorithm_quality": algorithm_quality,
        "alerts": alerts[:8],
    }


def build_codex_prompt(payload: dict[str, object], alerts: list[dict[str, object]]) -> str:
    compact_payload = compact_analysis_payload(payload, alerts)
    return "\n".join(
        (
            "你是 SPX Spark 的快速告警确认 agent。",
            "只根据下面的本机 JSON 判断是否需要推送给人类。不要给自动下单指令，不要编造缺失数据。",
            "人类只交易 SPX/SPXW；输出只能提 SPX、SPXW、ES、期权墙、gamma、IV surface。",
            "不要提任何非 SPX/SPXW/ES 标的名；隐藏算法上下文只能影响是否推送，不能进入人类可见解释。",
            "凡是 research_only、stale、missing、unknown、coverage 不足或 IV surface stale，默认不外发；只说明数据质量。",
            "带 source_gate 的告警默认不外发，唯一例外是 broker_unavailable_fallback、ibkr_session_state、ibkr_positions；"
            "ibkr_positions 表示 IBKR 实盘 SPXW 持仓变化或风险，应结合 Micopedia/wall/gamma 判断是否值得外发。",
            "如果 SPXW 期权 freshness gate 失败，不得基于 wall/gamma/IV 做看盘结论。",
            "如果 options_map 警告含 underlier_mismatch，或 gamma_state 以 unknown 开头，不得基于 wall/gamma 下结论，只能说明数据降级。",
            "gamma_state 为 zero_gamma_transition（micopedia 为 transition）表示零 gamma 交叉区：突破后波动可能放大，不得按 pin/均值回归解读。",
            "如果 ES/SPX anchor 缺失，不得把任何链上或 proxy 数据当作交易确认。",
            "如果 window.user_unattended 为 true，说明人类大概率在睡觉：只有 critical/high 且数据质量完好的 SPX/SPXW 风险才值得外发，其余一律不推送。",
            "发送决策必须优先参考 Micopedia、SPXW call wall/put wall/zero gamma、以及过去 1 小时 IV surface/期权变化。",
            "输出中文，最多 6 行。必须包含：结论、原因、数据质量、快照时间、需要人类看的 SPX/SPXW 检查项。",
            "如果数据质量不足，明确说 degraded。",
            "如果值得外发，第一行必须用 `需要看盘:` 开头；如果不值得外发，第一行必须用 `不需要推送:` 开头。",
            json.dumps(compact_payload, ensure_ascii=False, sort_keys=True),
        )
    )


def codex_message_requests_delivery(message: str) -> bool:
    normalized = message.strip().lower()
    if any(cue in normalized for cue in NEGATIVE_DELIVERY_CUES):
        return False
    first_line = normalized.splitlines()[0] if normalized else ""
    return any(first_line.startswith(cue) for cue in POSITIVE_DELIVERY_CUES)


def codex_message_respects_human_scope(message: str) -> bool:
    lowered = message.lower()
    if any(phrase in lowered for phrase in BLOCKED_HUMAN_MESSAGE_PHRASES):
        return False
    uppered = message.upper()
    return not any(
        re.search(rf"(?<![A-Z0-9]){re.escape(symbol)}(?![A-Z0-9])", uppered)
        for symbol in BLOCKED_HUMAN_MESSAGE_SYMBOLS
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
    delivery_error = openclaw_delivery_error(completed.stdout)
    ok = completed.returncode == 0 and delivery_error is None
    return SinkResult(
        sink="openclaw_message",
        attempted=True,
        ok=ok,
        dry_run=settings.openclaw_dry_run,
        exit_code=completed.returncode,
        error=delivery_error or ((completed.stderr or completed.stdout).strip() if completed.returncode else None),
    )


def openclaw_delivery_error(stdout: str) -> str | None:
    if not stdout.strip():
        return None
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        return None
    return openclaw_payload_error(payload)


def openclaw_payload_error(payload: object) -> str | None:
    if isinstance(payload, dict):
        for key in ("ok", "success"):
            if payload.get(key) is False:
                return f"openclaw returned {key}=false"
        for key in ("ret", "code", "errCode", "errno"):
            value = payload.get(key)
            if isinstance(value, int | float) and value != 0:
                return f"openclaw returned {key}={value:g}"
        status = str(payload.get("status") or "").lower()
        if status in {"error", "failed", "failure"}:
            return f"openclaw returned status={status}"
        for key in ("error", "err", "errMsg"):
            value = payload.get(key)
            if value:
                return f"openclaw returned {key}={value}"
        for value in payload.values():
            nested = openclaw_payload_error(value)
            if nested:
                return nested
    if isinstance(payload, list):
        for value in payload:
            nested = openclaw_payload_error(value)
            if nested:
                return nested
    return None


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
    else:
        system_alerts = [alert for alert in selected if is_system_event_alert(alert)]
        if system_alerts:
            sinks.append(
                send_openclaw_message(
                    settings,
                    format_alert_message(payload, system_alerts),
                    runner=runner,
                )
            )
    if settings.codex_enabled:
        codex_result, codex_message = run_codex_exec(
            settings,
            build_codex_prompt(payload, selected),
            runner=runner,
        )
        sinks.append(codex_result)
        if codex_result.ok and settings.codex_deliver:
            should_deliver = (
                codex_message_requests_delivery(codex_message)
                if settings.codex_require_delivery_cue
                else True
            )
            scope_ok = codex_message_respects_human_scope(codex_message)
            if should_deliver:
                if scope_ok:
                    sinks.append(send_openclaw_message(settings, codex_message, runner=runner))
                else:
                    sinks.append(
                        SinkResult(
                            sink="codex_scope_gate",
                            attempted=True,
                            ok=True,
                            error="codex output mentioned non-focus context",
                        )
                    )
            else:
                sinks.append(
                    SinkResult(
                        sink="codex_delivery_gate",
                        attempted=True,
                        ok=True,
                        error="codex output did not request delivery",
                    )
                )
    if settings.openclaw_agent_enabled:
        sinks.append(run_openclaw_agent(settings, build_agent_prompt(payload, selected), runner=runner))

    sent_count = sum(1 for sink in sinks if sink.sink == "openclaw_message" and sink.ok)
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
