from __future__ import annotations

import subprocess
import sys


def test_realtime_intraday_import_does_not_load_duckdb() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; import spx_spark.intraday_shock; "
                "raise SystemExit(1 if 'duckdb' in sys.modules else 0)"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_operational_facade_import_does_not_load_duckdb() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; import spx_spark.data_platform.facade; "
                "raise SystemExit(1 if 'duckdb' in sys.modules else 0)"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
