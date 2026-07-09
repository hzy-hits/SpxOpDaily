from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Callable
from pathlib import Path

from spx_spark.config import NotificationSettings
from spx_spark.notifier.model import CommandRunner, SinkResult, default_runner


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


def post_bark(url: str, payload: dict[str, object], timeout_seconds: float) -> dict[str, object]:
    import urllib.request

    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        body = response.read().decode("utf-8", errors="replace")
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return {"code": response.status}
    return parsed if isinstance(parsed, dict) else {"code": response.status}


def send_bark_message(
    settings: NotificationSettings,
    title: str,
    body: str,
    *,
    group: str | None = None,
    markdown: str | None = None,
    poster: Callable[[str, dict[str, object], float], dict[str, object]] | None = None,
) -> SinkResult:
    if poster is None:
        poster = post_bark
    if not settings.bark_url:
        return SinkResult(
            sink="bark",
            attempted=False,
            ok=False,
            error="missing bark url",
        )
    payload: dict[str, object] = {
        "title": title,
        "body": body,
        "group": group or settings.bark_group,
    }
    if markdown and settings.bark_markdown_enabled:
        # Bark ignores body when markdown is set for the App detail view;
        # lockscreen still uses body from the notification service strip.
        payload["markdown"] = markdown
    if settings.bark_level:
        payload["level"] = settings.bark_level
    try:
        response = poster(settings.bark_url, payload, settings.bark_timeout_seconds)
    except Exception as exc:  # noqa: BLE001
        return SinkResult(sink="bark", attempted=True, ok=False, error=str(exc))
    code = response.get("code")
    ok = code in (200, "200")
    return SinkResult(
        sink="bark",
        attempted=True,
        ok=ok,
        error=None if ok else f"bark response code={code} message={response.get('message')}",
    )


def send_bark_friend_message(
    settings: NotificationSettings,
    title: str,
    body: str,
    *,
    poster: Callable[[str, dict[str, object], float], dict[str, object]] | None = None,
) -> SinkResult:
    """Friend channel: same Bark protocol, separate key, trading content only.

    Callers are responsible for only routing market-facing pushes here; this
    sink never sees ops/engineering messages.
    """
    if poster is None:
        poster = post_bark
    if not settings.bark_friend_url:
        return SinkResult(
            sink="bark_friend",
            attempted=False,
            ok=False,
            error="missing bark friend url",
        )
    payload: dict[str, object] = {
        "title": title,
        "body": body,
        "group": settings.bark_group,
    }
    if settings.bark_level:
        payload["level"] = settings.bark_level
    try:
        response = poster(settings.bark_friend_url, payload, settings.bark_timeout_seconds)
    except Exception as exc:  # noqa: BLE001
        return SinkResult(sink="bark_friend", attempted=True, ok=False, error=str(exc))
    code = response.get("code")
    ok = code in (200, "200")
    return SinkResult(
        sink="bark_friend",
        attempted=True,
        ok=ok,
        error=None if ok else f"bark response code={code} message={response.get('message')}",
    )


def feishu_sign(secret: str, timestamp: int) -> str:
    """Feishu custom-bot signature: HMAC-SHA256 of timestamp + '\\n' + secret."""
    import base64
    import hashlib
    import hmac

    string_to_sign = f"{timestamp}\n{secret}"
    digest = hmac.new(
        string_to_sign.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).digest()
    return base64.b64encode(digest).decode("utf-8")


def post_feishu(url: str, payload: dict[str, object], timeout_seconds: float) -> dict[str, object]:
    import urllib.request

    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        body = response.read().decode("utf-8", errors="replace")
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return {"code": response.status, "msg": body}
    return parsed if isinstance(parsed, dict) else {"code": response.status}


def send_feishu_card(
    settings: NotificationSettings,
    card: dict[str, object],
    *,
    poster: Callable[[str, dict[str, object], float], dict[str, object]] | None = None,
) -> SinkResult:
    """Send an interactive Feishu card via custom-bot webhook."""
    if poster is None:
        poster = post_feishu
    if not settings.feishu_enabled:
        return SinkResult(sink="feishu", attempted=False, ok=False, error="feishu disabled")
    if not settings.feishu_webhook_url:
        return SinkResult(
            sink="feishu",
            attempted=False,
            ok=False,
            error="missing feishu webhook url",
        )
    payload: dict[str, object] = {"msg_type": "interactive", "card": card}
    if settings.feishu_secret:
        import time

        timestamp = int(time.time())
        payload["timestamp"] = str(timestamp)
        payload["sign"] = feishu_sign(settings.feishu_secret, timestamp)
    try:
        response = poster(settings.feishu_webhook_url, payload, settings.feishu_timeout_seconds)
    except Exception as exc:  # noqa: BLE001
        return SinkResult(sink="feishu", attempted=True, ok=False, error=str(exc))
    # Feishu webhook success: {"code": 0, "msg": "success"} (also StatusCode=0 legacy).
    code = response.get("code", response.get("StatusCode"))
    ok = code in (0, "0")
    return SinkResult(
        sink="feishu",
        attempted=True,
        ok=ok,
        error=None if ok else f"feishu response code={code} msg={response.get('msg') or response.get('StatusMessage')}",
    )


def deliver_trade_push(
    settings: NotificationSettings,
    *,
    title: str,
    text: str,
    kind: str,
    lane: str = "trade",
    friend: bool = False,
    runner: CommandRunner = default_runner,
) -> list[SinkResult]:
    """Fan out one writer text across Feishu (trade) + Bark main (+ friend).

    - Feishu: interactive markdown card, trade lane only.
    - Bark main: always; ops use bark_ops_group and plain body; trade uses
      lockscreen summary + optional markdown detail.
    - Bark friend: only when friend=True (caller already filtered trade-only).
    - OpenClaw Weixin: still attempted when enabled (legacy parallel).
    """
    from spx_spark.notifier.format_push import (
        bark_groups_for_lane,
        bark_lockscreen_summary,
        build_feishu_card,
        strip_markdown_light,
    )

    sinks: list[SinkResult] = []
    is_trade = lane == "trade"

    if settings.feishu_enabled and is_trade:
        card = build_feishu_card(text, title=title, kind=kind, lane=lane)
        sinks.append(send_feishu_card(settings, card))

    # Legacy Weixin: historically delivered even when openclaw_enabled=false
    # (that flag only meant "raw dump without review"). Keep attempting so
    # existing .env setups keep working until Feishu fully replaces it.
    weixin = send_openclaw_message(settings, strip_markdown_light(text), runner=runner)
    if weixin.attempted or settings.openclaw_enabled:
        sinks.append(weixin)

    if settings.bark_enabled:
        group = bark_groups_for_lane(
            lane,
            trade_group=settings.bark_group,
            ops_group=settings.bark_ops_group,
        )
        if is_trade:
            body = bark_lockscreen_summary(text)
            markdown = text if settings.bark_markdown_enabled else None
        else:
            body = strip_markdown_light(text)
            markdown = None
        sinks.append(
            send_bark_message(
                settings,
                title,
                body,
                group=group,
                markdown=markdown,
            )
        )

    if friend and settings.bark_friend_enabled and is_trade:
        sinks.append(
            send_bark_friend_message(
                settings,
                title,
                bark_lockscreen_summary(text),
            )
        )

    return sinks


def any_delivery_ok(sinks: list[SinkResult]) -> bool:
    return any(
        sink.ok
        for sink in sinks
        if sink.sink in {"feishu", "bark", "openclaw_message"} and sink.attempted
    )


def im_delivery_ok(sinks: list[SinkResult]) -> bool:
    """True when Feishu or Weixin got the message. Bark alone does not count:

    the missed-queue digest is for recovering the IM reading surface after an
    outage; Bark already woke the phone, but the card/timeline still needs a
    later IM flush.
    """
    return any(
        sink.ok
        for sink in sinks
        if sink.sink in {"feishu", "openclaw_message"} and sink.attempted
    )


BARK_TITLE_CATEGORIES: tuple[tuple[str, frozenset[str]], ...] = (
    ("持仓事件", frozenset({
        "spxw_position_opened",
        "spxw_position_closed",
        "spxw_position_qty_changed",
        "spxw_position_book_pnl",
        "spxw_position_near_expiry",
    })),
    ("系统事件", frozenset({
        "ibkr_session_interrupted",
        "ibkr_session_restored",
    })),
    ("波动率信号", frozenset({
        "put_skew_steepening_5m",
        "atm_iv_jump_5m",
        "iv_surface_shift_5m",
        "iv_surface_shift_1h",
        "atm_iv_change_1h",
        "iv_term_gap",
    })),
    ("价格异动", frozenset({
        "price_move_from_close",
        "broker_unavailable_proxy_watch",
    })),
    ("结构信号", frozenset({
        "option_gamma_regime",
        "option_wall_proximity",
    })),
)


def bark_title_for_alerts(alerts: list[dict[str, object]]) -> str:
    top = alerts[0] if alerts else {}
    kind = str(top.get("kind", "")) or "alert"
    extra = f" +{len(alerts) - 1}" if len(alerts) > 1 else ""
    for label, kinds in BARK_TITLE_CATEGORIES:
        if kind in kinds:
            return f"SPX {label}{extra}"
    severity = str(top.get("severity", "")).upper() or "ALERT"
    return f"SPX Spark {severity} {kind}{extra}"


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


def extract_openclaw_agent_message(stdout: str) -> str:
    text = stdout.strip()
    if not text:
        return ""

    candidates = [text, *reversed(text.splitlines())]
    for chunk in candidates:
        try:
            payload = json.loads(chunk)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        for key in ("text", "message", "reply", "content"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        result = payload.get("result")
        if isinstance(result, dict):
            for key in ("text", "message", "reply", "content"):
                value = result.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        payloads = payload.get("payloads")
        if not isinstance(payloads, list) and isinstance(result, dict):
            payloads = result.get("payloads")
        if isinstance(payloads, list):
            parts = [
                str(item.get("text"))
                for item in payloads
                if isinstance(item, dict) and isinstance(item.get("text"), str) and item.get("text")
            ]
            if parts:
                return "\n".join(parts).strip()
    return text


def run_openclaw_agent(
    settings: NotificationSettings,
    prompt: str,
    *,
    runner: CommandRunner = default_runner,
) -> tuple[SinkResult, str]:
    """Run the OpenClaw agent for analysis only.

    Deliberately never passes ``--deliver``: with that flag OpenClaw pushes the
    agent's reply to the channel unconditionally, so even analyses starting
    with 不需要推送 reached the human. Delivery is decided by our gates and
    performed via ``openclaw message send`` afterwards.
    """
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
        try:
            completed = runner(command, settings.openclaw_agent_timeout_seconds)
        except Exception as exc:  # noqa: BLE001
            return (
                SinkResult(
                    sink="openclaw_agent",
                    attempted=True,
                    ok=False,
                    error=str(exc),
                ),
                "",
            )
    message = extract_openclaw_agent_message(completed.stdout or "")
    if message and len(message) > settings.codex_output_max_chars:
        message = message[: settings.codex_output_max_chars].rstrip() + "\n..."
    return (
        SinkResult(
            sink="openclaw_agent",
            attempted=True,
            ok=completed.returncode == 0 and bool(message),
            exit_code=completed.returncode,
            error=(completed.stderr or completed.stdout).strip() if completed.returncode else None,
        ),
        message,
    )
