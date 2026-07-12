# SPX Spark Operations Acceptance - 2026-07-12

## Verdict

- Runtime and notification acceptance: **PASS**.
- Full refactor-plan completion: **NOT COMPLETE**.
- Acceptance context: Sunday/weekend maintenance mode. A real RTH market-data
  acceptance still requires the planned five-session shadow window.

## Verification Evidence

- `uv run pytest -q`: `1032 passed`, one upstream `websockets.legacy`
  deprecation warning.
- `uv run ruff check .`: passed.
- `uv build`: source distribution and wheel built successfully.
- Architecture/contracts/application/infrastructure/state-machine/E2E focused
  suite: `87 passed` before the runtime fixes; the final full suite includes
  those tests.
- Read-only operational commands passed: latest projection load, options map,
  alert engine with notifications disabled, data-platform status, and
  maintenance dry run.

## Runtime Evidence

The following user services were reloaded/restarted from the current worktree
and remained `active/running` with zero systemd restarts:

- `spx-spark-schwab-oauth.service`
- `spx-spark-ibkr-stream.service`
- `spx-spark-24h.service`
- `ibc-gateway.service`

Latest state contained 457 normalized rows from Schwab, Polymarket, and
Hyperliquid. Schwab was available; IBKR was intentionally connected in account
standby with market data inactive because weekend maintenance mode blocks the
IBKR market-data session.

The final heartbeat correctly reported `mode=blocked`, `ok=false`, with:

- `tradfi_anchor=false`
- `front_chain_fresh=false`
- `analytics_ok=true`
- `outbox_writable=true`
- `critical_tasks_ok=true`

This is the expected protected weekend state, not a task/process failure. All
scheduled tasks had zero consecutive failures.

## Bark End-to-End

One operations test was sent through:

`NotificationSettings -> ops lane router -> Bark HTTP -> api.day.app`

Result: `attempted=true`, `dry_run=false`, `ok=true`, `error=null`. The message
used the `spx-ops` group and did not fan out to the friend Bark or Feishu.

## Runtime Defects Fixed During Acceptance

1. Market snapshot duplicate detection used `option:<underlier>` instead of the
   full option contract canonical id, so a real multi-strike batch failed
   validation. It now uses the complete canonical id.
2. Realtime exceptions incorrectly marked a healthy outbox as unwritable.
   Outbox health is now probed independently.
3. Runtime task success was conflated with market readiness. `BLOCKED` and
   `DEGRADED` are now successful observations; only `FAILED` fails the task.
4. The service-loop heartbeat now consumes the realtime engine's latest health
   factors instead of reporting unconditional readiness.
5. TradFi anchor and SPXW chain freshness now require live usable quotes.

## Remaining Refactor Gates

- `runtime_value()` still appears 414 times under `src/spx_spark`; explicit
  settings injection is not complete.
- Several non-facade modules remain above the planned 600-line ceiling,
  including `strategy/steven.py` (1774), `post_close_review.py` (1698),
  `config.py` (1495), and `greek_reference.py` (1174).
- The five complete RTH-session density/data-quality shadow acceptance has not
  been performed.
- RTH-only SPX anchor, SPXW freshness, Greeks/density, and alert-delivery
  readiness cannot be proven from a Sunday run.

## Pre-RTH Execution Decision

周一首个实时样本前不进行 Rust/IPC/Kubernetes 或全仓大文件拆分。实施优先级为：

1. chain coverage/readiness 与 density 发布门槛；
2. production realtime composition 接入真实 options analytics kernel；
3. startup/readiness fail closed；
4. append-only shadow、日报和 new/legacy differential；
5. 离线 replay、全量 gate、systemd restart 和代码冻结。

详细模块、接口、测试和 T0 倒排见
`docs/pre-rth-refactor-implementation-plan.md`。完成这些项目只产生
`PRE-RTH-CODE-READY`，五个完整 RTH session 后才可能产生
`RTH-SHADOW-ACCEPTED`。
