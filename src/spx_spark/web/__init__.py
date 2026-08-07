"""FastAPI transport for the local SPX Spark control plane."""

from spx_spark.web.replay_api import create_app, create_default_app

__all__ = ["create_app", "create_default_app"]
