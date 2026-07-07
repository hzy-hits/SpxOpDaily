#!/usr/bin/env python3
"""Sync the OpenAI codex access token from the codex CLI into OpenClaw.

The OpenClaw agent stores a static copy of the ChatGPT codex access token,
which expires after a few hours and cannot self-refresh. The codex CLI at
~/.codex/auth.json holds a refreshable token. This script copies the current
access token into the OpenClaw agent auth store when they differ, so the
agent review path never silently loses LLM access.

Intended to run from the openclaw-weixin keepalive timer (every 30 min).
"""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

CODEX_AUTH_PATH = Path.home() / ".codex" / "auth.json"
OPENCLAW_DB_PATH = Path.home() / ".openclaw" / "agents" / "main" / "agent" / "openclaw-agent.sqlite"
PROFILE_KEY = "openai:codex"


def main() -> int:
    try:
        codex_auth = json.loads(CODEX_AUTH_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(json.dumps({"ok": False, "reason": f"codex auth unreadable: {exc}"}))
        return 1
    access_token = (codex_auth.get("tokens") or {}).get("access_token")
    if not access_token:
        print(json.dumps({"ok": False, "reason": "no access_token in codex auth"}))
        return 1

    if not OPENCLAW_DB_PATH.exists():
        print(json.dumps({"ok": False, "reason": "openclaw agent db missing"}))
        return 1

    con = sqlite3.connect(OPENCLAW_DB_PATH)
    try:
        row = con.execute(
            "SELECT store_json FROM auth_profile_store WHERE store_key='primary'"
        ).fetchone()
        if row is None:
            print(json.dumps({"ok": False, "reason": "no primary auth profile row"}))
            return 1
        store = json.loads(row[0])
        profile = store.get("profiles", {}).get(PROFILE_KEY)
        if not isinstance(profile, dict):
            print(json.dumps({"ok": False, "reason": f"profile {PROFILE_KEY} missing"}))
            return 1
        if profile.get("token") == access_token:
            print(json.dumps({"ok": True, "updated": False}))
            return 0
        profile["token"] = access_token
        con.execute(
            "UPDATE auth_profile_store SET store_json=? WHERE store_key='primary'",
            (json.dumps(store),),
        )
        con.commit()
    finally:
        con.close()
    print(json.dumps({"ok": True, "updated": True}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
