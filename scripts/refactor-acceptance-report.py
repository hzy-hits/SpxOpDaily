#!/usr/bin/env python3
"""Generate artifacts/refactor-acceptance/<phase>/report.json."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _run(command: list[str]) -> dict:
    completed = subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    return {
        "command": command,
        "returncode": completed.returncode,
        "stdout_tail": completed.stdout[-4000:],
        "stderr_tail": completed.stderr[-2000:],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", required=True)
    parser.add_argument(
        "--go",
        action="store_true",
        help="Mark GO when checks pass; otherwise NO-GO.",
    )
    args = parser.parse_args()

    out_dir = ROOT / "artifacts" / "refactor-acceptance" / args.phase
    out_dir.mkdir(parents=True, exist_ok=True)

    git = _run(["git", "rev-parse", "HEAD"])
    pytest = _run(["uv", "run", "pytest", "-q"])
    ruff = _run(["uv", "run", "ruff", "check", "src", "tests"])
    arch = _run(["uv", "run", "pytest", "-q", "tests/architecture"])

    passed = all(item["returncode"] == 0 for item in (pytest, ruff, arch))
    decision = "GO" if args.go and passed else ("GO" if passed and args.go else "NO-GO")
    if passed and not args.go:
        decision = "GO-CANDIDATE"

    report = {
        "phase": args.phase,
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "git_commit": (git["stdout_tail"] or "").strip(),
        "python": sys.version,
        "checks": {
            "pytest": pytest,
            "ruff": ruff,
            "architecture": arch,
        },
        "decision": decision if passed else "NO-GO",
        "open_risks": [],
    }
    report_path = out_dir / "report.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(report_path)
    print(f"decision={report['decision']}")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
