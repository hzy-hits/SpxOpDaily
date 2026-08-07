"""Single operator command tree for SPX Spark."""

import asyncio

import typer

from spx_spark import latest_state
from spx_spark.app_settings import get_settings
from spx_spark.logging_setup import configure_logging

app = typer.Typer(no_args_is_help=True)
core_app = typer.Typer(no_args_is_help=True)
app.add_typer(core_app, name="core")


@app.callback()
def _init() -> None:
    configure_logging("spx-cli", get_settings().log_level)


@app.command()
def status(all_providers: bool = False) -> None:
    """Inspect the normalized latest market-data state."""
    argv = ["--all-providers"] if all_providers else []
    raise typer.Exit(latest_state.run(argv))


@core_app.command("run")
def core_run() -> None:
    """Run the consolidated real-time Core process."""
    from spx_spark.core_main import main

    asyncio.run(main())
