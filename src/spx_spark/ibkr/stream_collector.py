"""Compatibility facade for the IBKR persistent stream collector.

Implementation lives under ``spx_spark.ibkr.stream``. This module re-exports
the public API, console entrypoints, and patchable ``deps`` symbols used by tests.
"""

from __future__ import annotations

from spx_spark.ibkr.stream import *  # noqa: F403
from spx_spark.ibkr.stream import __all__ as _stream_all
from spx_spark.ibkr.stream.cli import main
from spx_spark.ibkr.stream.deps import *  # noqa: F403
from spx_spark.ibkr.stream.deps import __all__ as _deps_all

__all__ = sorted(set(_stream_all) | set(_deps_all))

if __name__ == "__main__":
    main()
