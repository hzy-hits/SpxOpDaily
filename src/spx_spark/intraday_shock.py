"""Compatibility facade for the intraday shock monitor.

Implementation lives under ``spx_spark.application.shock`` (plan §8.12).
This module only re-exports the documented public API and console entrypoint.
"""

from __future__ import annotations

from spx_spark.application.shock.exports import *  # noqa: F403
from spx_spark.application.shock.exports import __all__ as __all__  # noqa: F401
from spx_spark.application.shock.service import main

if __name__ == "__main__":
    main()
