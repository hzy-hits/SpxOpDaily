# Change Brief — P5-2 CLI consolidation and transitional TOML cutover

## User-visible goal

Use one explicit `spx ...` command tree instead of 26 separate
`spx-spark-*` console registrations, and remove the direct PyYAML dependency
without changing effective operational setting values.

## Existing owner and reuse

- `src/spx_spark/data_platform/cli.py` owns status, query, spool replay and manifest sync.
- `src/spx_spark/data_platform/lake/compact.py` owns compaction.
- `src/spx_spark/cli.py` already owns the Typer command tree.
- Reuse Typer and the existing `run(argv)` / `main(argv)` functions; add no parser or business layer.

## Delete

- Delete the two console-script registrations from `pyproject.toml`.
- Update `scripts/run-data-compact.sh` to call `spx data` for its three direct Python calls.
- Do not delete the compaction wrapper or timer in this slice; P7 moves that owner into Huey.

## Impact and acceptance

- Databases, config keys, services, timers and notification ownership: unchanged.
- `spx data status`, `spx data query ...`, `spx data replay-spool`,
  `spx data sync-manifests` and `spx data compact ...` must reach the existing owner.
- Relevant CLI, compaction, manifest and systemd tests pass; console scripts decrease from 27 to 1.

## Operator-tool follow-up in the same P5-2 card

The remaining non-service entrypoints move to explicit `spx ops`, `spx verify`,
`spx report` and `spx replay` branches. Each branch imports and calls the existing
`run(argv)` directly; no plugin discovery, command registry or subprocess IPC is added.
Existing shell wrappers are updated in place. Provider/service entrypoints are not
changed by this follow-up, so this is not an owner cutover.

## Provider and scheduled-job follow-up

The final console registrations move behind explicit `spx ibkr`, `spx schwab`
and `spx job` commands. Existing systemd units keep the same owner and call the
same Python `run(argv)` through their existing shell wrappers. This changes only
the executable name; provider process consolidation and timer-to-Huey cutovers
remain separate deployment actions. The final `[project.scripts]` contains only
`spx`.

## Transitional TOML cutover

The tracked `config/runtime.yaml`, local-overlay example, frozen test fixture and
macro-event calendar move to TOML. The existing loaders use Python `tomllib`;
the direct `pyyaml` project dependency is removed. PyYAML can still be installed
transitively by FastAPI's uvicorn standard extra and is not claimed to be absent
from the environment.

The conversion preserves every parsed setting `value`. Fourteen malformed YAML
description fragments had previously become bogus nested paths because their
unquoted punctuation was parsed as mapping syntax; those non-consumer nodes are
intentionally absent from TOML. No real setting path changed value.

Oracle deployment must convert an existing ignored
`config/runtime.local.yaml` to `config/runtime.local.toml` before the first
restart, without printing or committing its contents. The conversion occurs in
the approved weekend cutover window and preserves a rollback copy outside the
checkout.

This is explicitly an intermediate P5-2 slice. `settings/loader.py`,
`runtime_config.py`, `config.py` env helpers and three coordination scripts still
exist; P5-2 is not complete until their surviving consumers move individually
to the minimal pydantic-settings `AppSettings` and those legacy mechanisms are
deleted. No new setting may be added to `runtime.toml` in the meantime.

## Thin-wrapper deletion follow-up

Nineteen scripts whose only behavior was `cd` plus one Python/console dispatch
are deleted. The surviving IBKR, Schwab and scheduled-job systemd units call the
absolute `/home/ubuntu/spx-spark/.venv/bin/spx` entry directly, so unit startup
does not depend on an interactive PATH. Operator docs use `uv run spx ...`.

Scripts that still own real coordination are deliberately retained: compaction
and session finalization hold non-blocking `flock` locks, while weekly
maintenance applies the threshold/deletion sequence. Those owners leave only
when their P7/Huey replacement carries the same behavior.

This follow-up deletes 19 files and changes +116/-225 tracked lines
(production +16/-157). It changes no service/timer count, database, config key,
dependency or notification owner. Full validation: 2,973 tests passed; Import
Linter kept 2/2 contracts; Ruff and `git diff --check` passed.

## Complexity and validation

- Tracked diff: +2,026/-4,235 lines, net -2,209. The compact TOML files are
  750 lines each versus 2,052 lines for the tracked YAML and 1,922 for its fixture.
- Runtime dependencies: direct PyYAML -1; services/timers/databases/config keys: zero change.
- Console registrations: 27 -> 1; `src/spx_spark/cli.py`: 231 lines. This exceeds
  the initial P1 bootstrap budget because it now explicitly routes the surviving
  legacy commands; it adds no command registry, subprocess protocol or business owner.
- Validation: Ruff passed; Import Linter kept 2/2 contracts; `git diff --check`
  passed; pytest 2,972 passed with two upstream deprecation warnings.
