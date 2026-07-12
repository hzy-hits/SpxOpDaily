"""Compatibility facade for the order-map application package.

Implementation lives under ``spx_spark.application.order_map``. This module
only re-exports the documented public API and console entrypoints.
"""

from __future__ import annotations

from spx_spark.application.order_map.exports import *  # noqa: F403
from spx_spark.application.order_map.exports import __all__ as __all__  # noqa: F401
from spx_spark.application.order_map.service import main

if __name__ == "__main__":
    main()
