# SPX Spark 架构简化执行方案 v1（面向执行 Agent 的施工图与硬约束）

状态：**执行基线。所有简化重构 PR 必须引用本文档中的 Phase 与任务卡编号。**
上位文档：`docs/architecture-simplification-blueprint-v1.md`（下称 blueprint）
事实核实基线：commit `2b96a2c2`（master），2026-08-07 由核实 Agent 在生产仓库逐项验证
执行者：GPT-5.6 Sol / Codex 等代码 Agent
设计者与仲裁者：项目维护者（用户）

本文档做四件事：

1. 把 blueprint 的断言锚定到仓库真实文件和数字（第 1 节）；
2. 记录 blueprint 与仓库现状的偏差及澄清决定（第 2 节）；
3. 给出执行 Agent 不得越界的硬约束与施工规范（第 3 节）；
4. 把 8 个 Phase 拆成施工图级任务卡：封闭的新文件清单、代码骨架、按序步骤、自检断言（第 4 节）。

文档优先级（冲突时从高到低）：

```text
本执行方案 > blueprint > docs/strategy-signal-engine-v2.md（S-track 范围内的实施合同）
          > AGENTS.md 通用工作流程
          > module-architecture.md / docs/refactor-architecture-acceptance-plan.md（冲突部分视为被取代）
          > rust/AGENTS.md、rust/docs/*（Rust 已冻结，只对故障修复有效）
```

---

## 1. 仓库现状核实基线（As-Is，已验证）

以下数字全部在 commit `2b96a2c2` 上实测，不是估计。执行 Agent 引用现状时以本节为准，不得重新盘点后自行修改目标。

### 1.1 规模

| 项 | 实测值 |
|---|---|
| Python 源文件（`src/`） | 452 个，约 147,700 行 |
| 测试文件（`tests/`） | 214 个 |
| console scripts（`pyproject.toml [project.scripts]`） | 43 个 |
| systemd units（`systemd/`） | 47 个（service + timer） |
| systemd units（`rust/systemd/`） | 9 个 |
| `config/runtime.yaml` | 108 KB |
| Rust crates | 6 个：`spx-bridge`、`spx-core`、`spx-delivery`、`spx-domain`、`spx-ledger`、`spx-report`（69 个 `.rs` 文件） |
| 生产依赖 | `duckdb`、`ib-async`、`numpy`、`pytz`、`pyyaml`、`schwab-py` |

### 1.2 blueprint 各项断言对应的真实文件

| blueprint 断言 | 仓库实体 |
|---|---|
| 自研线程池调度器 + 子进程 runner | `src/spx_spark/application/runtime/scheduler.py`（239 行）、`runner.py`（254 行）、`registry.py`、`tasks.py`、`supervisor.py`、顶层 `src/spx_spark/service_loop.py` |
| 多个手写 HTTP server | `src/spx_spark/schwab/oauth_service.py`、`src/spx_spark/surface_replay_http.py`、`src/spx_spark/surface_live_session_http.py`（共 3 处 `BaseHTTPRequestHandler`/`ThreadingHTTPServer`） |
| 手写配置体系 | `src/spx_spark/config.py`（1,000 行）、`src/spx_spark/settings/`（18 个文件，含 `loader.py`、`schema.py` 及按域拆分的 settings 模块）、`src/spx_spark/runtime_config.py` |
| 自研 SQLite outbox/claim/receipt | `src/spx_spark/notifier/delivery_outbox.py`、`delivery_outbox_claims.py`、`delivery_outbox_contract.py`、`delivery_outbox_read_model.py`、`delivery_worker.py`、`delivery_executor.py`、`receipts.py`、`receipt_mirror.py`；`src/spx_spark/application/notifications/outbox_consumer.py`；`src/spx_spark/infrastructure/ledger/outbox.py` |
| JSON 文件总线 | `src/spx_spark/storage.py`（868 行）+ `latest_state.py` + 各 feature 的 `latest/*.json` projection；`state_io.py`（135 行，保留） |
| 自研 AST 架构检查 | `tests/architecture/test_module_registry.py`、`tests/test_architecture.py`，登记表机制由 `module-architecture.md` 规定 |
| 手写交易日历 | `src/spx_spark/market_calendar.py` |
| 跨语言 contract | `contracts/golden/`、`tests/golden/`、`tests/contracts/`、`tests/test_shared_golden_contracts.py`、`tests/test_rust_operator_notification_ingress.py` |
| 顶层模块散乱 | 包根目录约 40 个孤立模块（`surface_*` 12 个、`post_close_*` 6 个、`greek_*`、`provider_failover*` 等），与 `application/`、`analytics/`、`domain/`、`infrastructure/` 分层并存 |
| 手写网络 backoff 代表点 | `src/spx_spark/schwab/collector.py`、`src/spx_spark/schwab/gateway.py`（含 `time.sleep` 重试循环） |

### 1.3 现有 56 个 systemd unit 到 4+1 服务的归属映射

这张表是 Phase 3/4/6 的 cutover 依据。执行 Agent 迁移某个 unit 时必须按此归属，不得自行发明第五个服务。

| 现有 unit（组） | 目标归属 |
|---|---|
| `ibc-gateway.service`、`ibc-watchdog.*`、`ibgateway-xvfb.service` | 外部依赖，保留原样（不计入应用 4 服务） |
| `spx-spark-ibkr-stream.service` | `spx-ibkr.service` |
| `spx-spark-schwab-marketdata.service`、`spx-spark-schwab-oauth.service` | `spx-schwab.service` |
| `spx-spark-24h.service`（service_loop） | 拆解：热任务入 `spx-core`，慢任务入 `spx-worker`，然后删除 |
| `spx-spark-es-bar-sampler`、`spx-spark-spx-minute-sampler`、`spx-spark-market-features-hot`、`spx-spark-intraday-shock-hot`、`spx-spark-market-regime-signal` | `spx-core` 内 `asyncio.TaskGroup` 任务 |
| `spx-spark-surface-dashboard`、`spx-spark-surface-live`、`spx-spark-surface-replay` | `spx-core` FastAPI app |
| `spx-spark-notification-delivery.service` | `spx-worker`（Huey consumer） |
| `spx-spark-morning-map.*`、`order-map.*`、`order-map-status.*`、`post-close-review.*`、`session-finalize.*`、`rth-daily-acceptance.*`、`backtest-weekly.*`、`data-compact.*`、`data-compact-weekend.*`、`maintenance-daily.*`、`maintenance-weekly.*`、`storage-pressure.*`、`surface-replay-warm.*`、`schwab-reauth-reminder.*`、`spx-ibkr-verifier.*` | `spx-worker` 内 Huey periodic task（timer 全部删除） |
| `rust/systemd/` 全部 9 个 unit | Phase 6 退役 |

### 1.4 43 个 console scripts 的处置原则

Phase 1 建立 `spx` Typer 命令树后：

- 人类会手工执行的入口（status、replay、report、data compact、notify test、ops）迁入 `spx` 子命令；
- 仅供 scheduler/systemd 调用的入口随 Phase 3/4 的宿主服务消亡而删除，不迁入 `spx`；
- 旧 `spx-spark-*` 名称最多以薄 wrapper 存活一个发布周期，Phase 5 完成前必须清零。

---

## 2. 与 blueprint 的偏差与澄清（执行时以本节为准）

核实中发现的偏差都不动摇 blueprint 结论，但执行 Agent 必须按以下澄清操作：

1. **AGENTS.md 存在失效引用。**（已于 2026-08-07 设计阶段修复）原 `AGENTS.md` 第 4 节引用的 `docs/unified-strategy-runtime-refactor-2026-07-31.md` 在仓库中不存在。
2. **手写 HTTP server 是 3 处不是 2 处。** blueprint 2.5 说“至少两套”；实际 `surface_replay_http.py` 与 `surface_live_session_http.py` 是两个独立实现，加 `schwab/oauth_service.py` 共 3 处。Phase 2 删除清单以本文档 1.2 为准。
3. **`docs/refactor-architecture-acceptance-plan.md` 部分被取代。** 该文档的状态机规格（§7）、模块级拆分计划（§8）、IPC 边界（§10）、Rust/NumPy 决策门（§11）、验收测试矩阵（§13）与 blueprint 冲突的部分全部失效；其中数据真实性语义、no-lookahead、fail-closed 等原则继续有效。Phase 0 在该文档头部加取代声明，不重写正文。
4. **`module-architecture.md` 的分层意图保留，登记机制废除。** 四层依赖方向继续有效并翻译成 Import Linter contract；“新增模块必须同步架构登记测试”的机制随 `tests/architecture/test_module_registry.py` 一起在 Phase 1 删除。
5. **Phase 4 依赖最小 Alembic 基线。** blueprint 把 SQLAlchemy/Alembic 全面统一放在 Phase 5，但 Phase 4 的 `notification_events`/`notification_attempts` 表需要先有 `spx.sqlite` 和 Alembic 初始 revision。澄清：Phase 4 交付一个只含通知两表的最小 Alembic 基线，Phase 5 在同一基线上追加其余表，不允许出现第二套 migration 机制。
6. **部署窗口比 blueprint 更严。** blueprint 19.4 只说“不在 RTH 中做部署迁移”；本项目 GTH（20:15–09:25 ET）同样产生可执行建议。所有 owner cutover 只允许在周末窗口（周五 GTH 收盘后至周日 GTH 开盘前）执行；平日只允许合入代码不切 owner。
7. **cutover 不得顺带转移 report/delivery owner。** 沿用 `AGENTS.md` 既有规则：owner 切换必须是 Change Brief 里明示的独立动作，不能作为合并或重构的副作用发生。
8. **`pytz` 不在本轮范围。** 迁移到 `zoneinfo` 是合理的，但不属于任何 Phase 的目标，禁止顺带做（防 scope creep）。白名单在 blueprint §17 基础上默认保留 `pytz`。
9. **PyYAML 的删除时点。** `config/macro_events.yaml` 也依赖 PyYAML。Phase 1 只要求新配置走 TOML/pydantic-settings；PyYAML 从生产依赖移除发生在 runtime.yaml 退役之后（Phase 5 收尾），macro_events 届时一并转 TOML 或入 DB，由 Change Brief 决定。
10. **HTTP 路径不改名。** blueprint §7.2 写的是 `/api/replay/*`；仓库现有对外契约是 `/api/v1/replay/*`、`/api/v1/live/*`。Phase 2 迁移**保持现有路径逐字不变**，契约保持优先于命名整洁。禁止借迁移做 URL 重设计。

以下第 11–16 条是策略引擎 v2（`docs/strategy-signal-engine-v2.md`）与本方案的协同裁决（2026-08-07）：

11. **策略引擎 v2 = Phase 8 内容提前，作为 S-track 与基础设施 Phase 并行。** S-track 开工前置门：P0-2、P0-3 完成，且 P1-4（Import Linter）已落地——否则每新增一个策略模块都要改 AST 登记表，白费工。S-track 与 Phase 2（HTTP）文件不相交，可并行；当 Phase 3/4 迁移 `order_map`/`market_features` 的宿主 unit 时，基础设施卡优先，S-track 在其上 rebase。周末 cutover 窗口两条线共享，先到先得、互不打包（C11/C13）。
12. **策略阈值不进配置。** v2 §21.3 的“新增配置项 ≤20”与 runtime.yaml 冻结（收口声明）冲突。裁决：v2 §7/§8/§11 的全部 bootstrap 阈值作为带 `policy_version` 字符串的**冻结常量集**放在策略代码内（单个常量 dataclass，随 `strategy_regime.py` 存在，不占新文件预算），不进 runtime.yaml、不进 AppSettings。“配置项预算 ≤20”理解为该常量集的字段数上限之外允许的真实部署键数（预期为 0）。改阈值 = 改代码 + policy_version 递增，天然进入 replay 对照。
13. **依赖白名单修订。** `scikit-learn` 加入白名单（限 analytics/research 层，S4 引入；理由：替代手写标准化与最近邻，符合 blueprint §1.3）；`scipy` 由“研究可选”升为 S4 的 analytics 生产依赖；`hypothesis` 由 P1-5 与 S1 中先落地者引入，另一方直接复用。`cvxpy`/`LightGBM`/`CatBoost`/`PyTorch` 维持禁止，解禁条件按 v2 §22.2。
14. **`strategy_decision` 不得进入冻结的跨语言契约。** `contracts/golden/` 已冻结。`payload["strategy_decision"]` 不得写入 `desk_map_projection.v1` 或任何 Rust 消费的投影；S1 的第 0 步必须核实 Rust bridge 对 payload 新增键的行为（fail-closed 拒绝还是忽略未知键），若拒绝则在 Python 投影导出处显式剥离该键并加一行测试。人类候选卡走现有 Python `trade_ready` lane（lane 语义仍有效，见 notification-architecture 收口声明），不走 Rust report lane。
15. **决策持久化时序。** Phase 5 之前，`StrategyDecision` 随现有 order-map payload artifact 持久化（复用既有 JSON artifact 机制，不新建存储，C6）；P5-1 的 `decisions`/`decision_legs` 表落地后，S-track 补一张收尾卡把持久化 owner 切到 `spx.sqlite`。`strategy_decision_replay` 只读现有 Parquet/DuckDB/JSONL lineage。
16. **PR 预算换算。** S-track 采用 v2 §21.3 总预算（生产 ≤1,200 行、测试 ≤600 行、新文件 ≤5 个），替代 C4 的单 PR 净增 ≤200 行上限；单 PR 新文件 ≤3、每 PR 复杂度记账、S5 必须删除/降级旧 owner 等规则不变。S4 需要同时引入 `scipy` 与 `scikit-learn` 两个依赖，是 C4“每 PR ≤1 依赖”的一次性已批准例外。

---

## 3. 执行 Agent 硬约束与施工规范（每张任务卡默认继承）

### 3.1 流程约束

- C1. 每张任务卡开工前先交 Change Brief（模板见 blueprint §15.1），用户确认后才改代码。
- C2. 一张任务卡 = 一个最小端到端结果。禁止“先搭好框架，下张卡再接业务”。
- C3. 完成报告必须含复杂度记账（blueprint §15.2），删除行数为 0 的“重构” PR 默认拒收。
- C4. 每张卡的 PR 遵守 blueprint §16.2 预算：新依赖 ≤1、新生产文件 ≤3、新 systemd unit = 0（Phase 3/4 建 4 个目标服务时除外，且需用户逐个批准）、净增生产代码 ≤200 行（除非同 PR 删除更多旧实现）。

### 3.2 技术约束

- C5. 通用基础设施只用白名单包（blueprint §17 + 本文档 2.8 澄清），一个职责一个包；禁止再手写 config loader、HTTP server、scheduler、retry loop、logger、migrator、CLI parser、架构 linter。
- C6. 禁止新建 JSON 文件 IPC、新 SQLite 文件（`spx.sqlite`/`huey.sqlite` 之外）、新状态机框架、新跨语言 contract。
- C7. Rust 目录冻结：只准删不准加；生产故障修复除外，且需用户批准。
- C8. `state_io.py` 保留但限用于：provider last-known snapshot、immutable export、本地 control file。任何新调用点超出这三类即违规。
- C9. 数据真实性语义（`source_at`/`received_at`、live/delayed/frozen/stale/missing、fail-closed、no-lookahead、manual-only）在任何迁移中不得弱化。这是验收一票否决项。
- C10. secrets 纪律沿用 `AGENTS.md`：不读、不打印、不提交 `.env`、token、私钥和 `/srv/data` 运行时 secrets；OAuth 相关迁移必须证明 secret 不进日志。

### 3.3 部署约束

- C11. owner cutover 只在周末窗口执行（见 2.6），且必须先在 replay/冻结 session 上验证新路径，写明回滚点。
- C12. 每次 cutover 后按 `AGENTS.md` §6 分别验证：unit 状态、restart count、日志、health endpoint、数据源时间戳/NBBO readiness、实际通知投递。`active` 不算验收。
- C13. 不双写、不双投递。过渡期读旧写新或读新写旧都需要 Change Brief 明示且限一个发布周期。

### 3.4 施工规范（防过度设计，逐条可机检）

- S1. **新文件清单是封闭的。** 每张任务卡列出的“新文件”是全集；要建清单之外的生产文件，先停下来问用户。
- S2. **骨架即抽象上限。** 任务卡里的代码骨架是允许的最复杂形态。执行 Agent 只能往骨架里填实现，不得在骨架之上加层：不加基类、不加 Protocol/interface、不加 registry、不加工厂的工厂、不加“为了可测试”的注入框架。函数参数直传就是合格的依赖注入。
- S3. **行数预算是硬的。** 每张卡给出的新文件行数上限包含空行和 docstring。超了先删自己的代码，不是来要豁免。
- S4. **步骤顺序是强制的。** 每张卡的施工步骤按序执行，每步结束时仓库可运行、相关测试通过。禁止跳步合并成一个大提交。
- S5. **自检断言必须出现在完成报告里。** 每张卡列出的 `rg`/命令断言，执行 Agent 要贴原始输出，不是口头声称。
- S6. **命名固定。** 配置环境变量前缀 `SPX_`；FastAPI 工厂函数一律 `create_app`；web 模块放 `src/spx_spark/web/`；Huey app 放 `src/spx_spark/infrastructure/jobs.py`；Typer 入口 `src/spx_spark/cli.py`。不要发明别的组织方式。
- S7. **不迁移 = 不碰。** 卡里没提到的模块，哪怕看起来“顺手能改”，也不碰。发现问题记到完成报告的 Remaining risks，不现场修。
- S8. **禁止预留。** 不为未来 Phase 写空壳、占位 enum、TODO 框架、`# will be used in Phase N` 的代码。未来 Phase 的代码未来再写。

---

## 4. Phase 任务卡（施工图）

每张卡结构：目标 / 新文件（封闭清单）/ 施工步骤（按序）/ 删除 / 自检断言 / 禁止。骨架代码是抽象上限（S2）。

### Phase 0：冻结与盘点

#### P0-1 旧文档取代声明

**已全部完成（2026-08-07，设计阶段收口）**，执行 Agent 无需再做。已收口的文档：

- `docs/refactor-architecture-acceptance-plan.md`、`module-architecture.md`、`docs/architecture-plan.md`、`docs/pre-rth-refactor-implementation-plan.md`：取代/历史记录声明；
- `docs/monorepo-layout.md`、`docs/runtime-configuration.md`、`docs/notification-architecture.md`：现状 vs 目标架构声明（Rust 冻结、YAML legacy、通知 lane 语义保留但实现冻结）；
- `rust/AGENTS.md`、`rust/README.md`、`contracts/README.md`：FROZEN 声明；
- `README.md`：新基线文档置顶；
- 顶层四份 design 规格（morning-map、quant-probability、slow-poll-lane、spy-lane）：历史记录声明；
- `AGENTS.md`：§8 工作合同 + 失效引用修复。

执行 Agent 若发现其他文档与新基线冲突：只报告，不自行加声明。

#### P0-2 冻结标记

`rust/AGENTS.md`、`rust/README.md`、`contracts/README.md` 的 FROZEN 声明已随 P0-1 完成。

剩余施工步骤：

1. 在以下 3 个文件的模块 docstring 第一行前插入一行（保持 docstring 其余内容不动）：
   - `src/spx_spark/service_loop.py`
   - `src/spx_spark/application/runtime/scheduler.py`
   - `src/spx_spark/notifier/delivery_outbox.py`

   插入的行逐字为：`FROZEN (2026-08): production-fault fixes only; see docs/architecture-simplification-execution-plan-v1.md.`

2. 运行 `uv run pytest -q tests/test_service_loop.py tests/test_notification_delivery_outbox.py` 确认无行为影响。

- 新文件：无。删除：无。
- 自检断言：`rg -c 'FROZEN \(2026-08\)' src/` 输出恰好 3 个文件各 1 次。
- 禁止：借机重构、格式化或“顺手清理”任何被标记模块。

#### P0-3 owner 盘点清单

施工步骤：

1. 用以下只读命令收集事实（生产主机上只查不改；`/srv/data` 下只列文件名，禁止读内容）：

```bash
ls systemd/ rust/systemd/
sed -n '/\[project.scripts\]/,/^\[/p' pyproject.toml
rg -l 'sqlite3|\.sqlite' src/spx_spark
rg -l "latest/" src/spx_spark
```

2. 产出 `docs/simplification-owner-inventory.md`，只允许一张表，列固定为：

```text
| 实体（unit/script/db/json） | 类型 | 当前 owner 模块 | 唯一消费者 | 目标归属（按 1.3/1.4） | 目标删除 Phase |
```

3. 消费者不明的行标 `TBD` 并在表下汇总，TBD 不得超过 5 行。

- 新文件：`docs/simplification-owner-inventory.md`（仅此一个）。删除：无。
- 自检断言：表行数 ≥ 47+9+43（unit + rust unit + script），每行目标归属非空。
- 禁止：盘点中发现“顺手能删”的东西直接删；给清单加第二张表或分析章节。

### Phase 1：开发基础设施统一

#### P1-1 最小 AppSettings + TOML

新文件（封闭清单）：`src/spx_spark/app_settings.py`（≤120 行）、`config/defaults.toml`、`config/production.toml.example`、`tests/test_app_settings.py`。

骨架（抽象上限）：

```python
"""src/spx_spark/app_settings.py"""
from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict, TomlConfigSettingsSource


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SPX_", frozen=True, extra="forbid",
        toml_file=("config/defaults.toml", "config/production.toml"),
    )

    data_root: Path = Path("/srv/data/spx-spark")
    log_level: str = "INFO"
    # 只允许添加本 PR 内有真实消费者的字段

    @classmethod
    def settings_customise_sources(cls, settings_cls, init_settings, env_settings, dotenv_settings, file_secret_settings):
        # 优先级：init > env > production.toml > defaults.toml > code defaults
        return (init_settings, env_settings, TomlConfigSettingsSource(settings_cls), file_secret_settings)


@lru_cache
def get_settings() -> AppSettings:
    return AppSettings()
```

施工步骤：

1. `uv add pydantic-settings`（会带入 pydantic，本卡唯一新依赖，占用 C4 预算）；
2. 写 `AppSettings`：**单个扁平类**，v1 字段只放 `data_root`、`log_level` 以及 P1-2/P1-3 实际要读的字段，每个字段在本 PR 内必须有消费者；
3. `config/defaults.toml` 只写已定义字段；`config/production.toml.example` 给一个覆盖示例；真实 `production.toml` 留给部署时人工创建（gitignore）；
4. `tests/test_app_settings.py` 只测优先级链（TOML < env < init）和 `extra="forbid"` 拒绝未知键，用 Hypothesis 或参数化都可，一个文件内解决。

- 删除：无（旧 loader 为存量模块继续服务，删除发生在 Phase 5）。
- 自检断言：`rg -c 'class .*Settings' src/spx_spark/app_settings.py` = 1；新文件总行数 ≤120。
- 禁止：按域拆多个 Settings 类或建 `settings_v2/` 目录；搬运 `runtime.yaml` 的任何键；写 TOML 解析辅助函数；给存量模块加“双读兼容层”。

#### P1-2 structlog 日志入口

新文件（封闭清单）：`src/spx_spark/logging_setup.py`（≤60 行）。

骨架（抽象上限）：

```python
import logging

import structlog


def configure_logging(service: str, level: str = "INFO") -> None:
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper())
        ),
        cache_logger_on_first_use=True,
    )
    structlog.contextvars.bind_contextvars(service=service)


get_logger = structlog.get_logger
```

施工步骤：`uv add structlog` → 写上述文件 → 在 P1-3 的 `spx` CLI 启动时调用一次。字段规范（blueprint §7.6 的 event/service/session_date/provider/... 键）通过调用点 `log.info("event_name", provider=..., source_at=...)` 传入，不做 schema 校验。

- 删除：无（存量 `print(json.dumps)` 随 Phase 3 宿主消亡）。
- 自检断言：文件 ≤60 行；`rg -c 'class ' src/spx_spark/logging_setup.py` = 0。
- 禁止：自定义 processor、按环境切换 renderer、包 Logger wrapper 类、加 journald handler（systemd 收 stdout 即可）、回头改造任何存量日志。

#### P1-3 `spx` Typer 命令树

新文件（封闭清单）：`src/spx_spark/cli.py`（≤100 行）。

骨架（抽象上限）：

```python
import typer

from spx_spark.app_settings import get_settings
from spx_spark.logging_setup import configure_logging

app = typer.Typer(no_args_is_help=True)


@app.callback()
def _init() -> None:
    configure_logging("spx-cli", get_settings().log_level)


@app.command()
def status(all_providers: bool = False) -> None:
    """复用 spx_spark.latest_state 的读取与渲染，只读。"""
    ...
```

施工步骤：

1. `uv add typer`；
2. `pyproject.toml [project.scripts]` 增加一行 `spx = "spx_spark.cli:app"`（43→44，唯一一次允许 +1，此后每卡只减不增）；
3. `spx status` 的实现是调用 `src/spx_spark/latest_state.py` 现有的 `run()`/`print_table`/`print_provider_states`，**不复制**其逻辑；需要的话给 `latest_state.run` 加可选参数，不新写读取器；
4. 在生产主机以只读方式验证 `uv run spx status` 输出与 `spx-spark-latest-state`（若存在对应旧入口）一致。

- 删除：无。
- 自检断言：`sed -n '/\[project.scripts\]/,/^\[/p' pyproject.toml | rg -c '='` = 44；`rg -c 'def ' src/spx_spark/cli.py` ≤ 4。
- 禁止：建 `cli/` 包或命令自动发现机制；预挂空壳子命令组（`spx replay`、`spx report` 等在对应 Phase 才出现，S8）；在 CLI 里写任何业务逻辑。

#### P1-4 Import Linter 替换架构测试

新文件：无（配置写进 `pyproject.toml`）。

配置（抽象上限，contract 数量 ≤3）：

```toml
[tool.importlinter]
root_package = "spx_spark"

[[tool.importlinter.contracts]]
name = "domain and analytics stay pure"
type = "forbidden"
source_modules = ["spx_spark.domain", "spx_spark.analytics"]
forbidden_modules = [
    "spx_spark.web", "spx_spark.notifier", "spx_spark.infrastructure",
    "spx_spark.ibkr", "spx_spark.schwab", "spx_spark.application",
]

[[tool.importlinter.contracts]]
name = "layers for packaged code"
type = "layers"
layers = [
    "spx_spark.web | spx_spark.cli",
    "spx_spark.application",
    "spx_spark.infrastructure",
    "spx_spark.analytics | spx_spark.domain",
]
exhaustive = false
```

施工步骤：

1. `uv add --dev import-linter`；
2. 写上述两个 contract；顶层约 40 个孤立模块**不入层**（它们随后续 Phase 迁移入层），layers contract 只约束既有的四个包目录 + `web`/`cli`；
3. 跑 `uv run lint-imports`；现存违规逐条加入 `ignore_imports`，每条带注释 `# TODO PN-M`（指向解除它的任务卡）；**不许为了减少豁免而移动业务文件**；
4. 删除 `tests/architecture/test_module_registry.py`、`tests/test_architecture.py` 及 `tests/architecture/` 目录（若空）；
5. 在 `AGENTS.md` §5 验证命令清单中把架构测试替换为 `uv run lint-imports`。

- 自检断言：`uv run lint-imports` 通过；`ls tests/architecture/ 2>&1` 报不存在；`rg -c 'LAYERS' tests/ src/` = 0。
- 禁止：写第 4 个 contract；为通过 lint 移动/重命名任何生产模块；放宽 forbidden contract 来清空豁免清单。

#### P1-5 Tenacity + Hypothesis 样板

新文件：无（原地替换）。

执行核实（2026-08-07）：本卡的两个文件前提已被后续重构改变。`collector.py`
唯一的 `time.sleep` 位于 `run_loop`，职责是 planner cadence，不是网络重试；把它改成
Tenacity 会改变常驻采集循环语义。当前真实的同步网络 retry owner 是
`schwab/gateway.py`，而本卡明确禁止触碰该文件。因此步骤 1–2 以“不适用、不得引入未使用
依赖”收口，gateway 的替换必须另开 Change Brief。`tests/test_trade_geometry.py` 也不存在
payoff 示例；S2 已在 `tests/test_strategy_payoff.py` 建立 Hypothesis payoff 性质测试，步骤
3–4 在该真实 owner 完成。以下原施工步骤保留为基线偏差记录，不再机械执行。

施工步骤：

1. `uv add tenacity`；
2. 在 `src/spx_spark/schwab/collector.py` 中定位手写 `time.sleep` 重试循环（施工时 `rg -n 'time.sleep' src/spx_spark/schwab/collector.py` 确认具体函数），替换为 `@retry(stop=stop_after_attempt(N), wait=wait_exponential(...), retry=retry_if_exception_type(...))`，**N、上限、异常类型逐一保持原语义**，在 Change Brief 里列出新旧对照；只换这一处，`gateway.py` 的留给后续卡；
3. `uv add --dev hypothesis`；
4. 在 `tests/test_trade_geometry.py` 中挑 payoff 示例断言改写为性质测试，不变量限于：到期 payoff 分段线性且在行权价外斜率恒定、vertical 的 `max_gain + max_loss == width`（同一乘数下）、breakeven 处 payoff 恰为 0、max loss 有界。被替换的示例断言同 PR 删除。

- 自检断言：`rg -n 'time.sleep' src/spx_spark/schwab/collector.py` 恰一处且位于 planner cadence；
  `rg -c 'from hypothesis' tests/test_strategy_payoff.py` ≥1；Vertical payoff 的分段斜率、
  `max_gain + max_loss == width`、breakeven 为 0、max loss 有界均有性质断言。
- 禁止：一次替换所有重试点；给 tenacity 包 wrapper（直接用装饰器）；用 Hypothesis 重写与本卡无关的测试文件。

### Phase 2：HTTP 统一（P2-1 → P2-2 → P2-3，严格按序）

三张卡共用施工模式（每张卡内按 1→5 执行）：

1. **先固化契约**：给现有 server 写“契约测试”——对下方列出的每条路由发请求，断言 status、关键 header（含 ETag/HEAD 行为、security headers）、body 关键字段。现有测试（`tests/test_surface_replay_service.py`、`tests/test_surface_live_session.py`、`tests/test_schwab_oauth_service.py`）已覆盖的不重写，只补缺口；
2. **建薄 route**：新建 web 模块提供 `create_app(...) -> FastAPI`；每个 route 函数体 ≤15 行：解析参数 → 调用现有 service 对象/函数 → 异常映射为 HTTP status。业务逻辑一行都不许复制进 route，缺函数就先在原 service 模块抽函数（原文件内，不建新层）；
3. **同套契约测试过 TestClient**；
4. **切换入口**：unit `ExecStart` 改为 uvicorn 启动（unit 修改需用户授权，平日合码周末切换，C11）；UDS/loopback 绑定与文件权限语义保持；
5. **同 PR 删除旧 server 代码**。

#### P2-1 surface replay → FastAPI

- 新文件（封闭清单）：`src/spx_spark/web/__init__.py`、`src/spx_spark/web/replay_api.py`（≤200 行）、`tests/web/test_replay_api.py`。
- 必须逐字保持的路由（来自现实现）：`/healthz`、`/api/v1/replay/sessions`、`/api/v1/replay/sessions/{date}/timeline`、`/api/v1/replay/sessions/{date}/trend`、`/api/v1/replay/sessions/{date}/session-surface`、`/api/v1/replay/sessions/{date}/frame`、`/api/v1/replay/frames/{ts}`（见 2.10：不改名）。
- 委托对象：`surface_replay_service.py` 的 `ReplayCatalog`/`ReplaySession`。施工核查确认旧 transport 没有 hmac token 认证；`hmac.compare_digest` 只服务于弱 ETag 比较，因此新 route 保留 ETag 语义，不虚构认证 dependency。
- 删除：`src/spx_spark/surface_replay_http.py` 全文件及其 console script。
- 自检断言：`rg -c 'BaseHTTPRequestHandler|ThreadingHTTPServer' src/spx_spark/surface_replay_http.py` 报文件不存在；契约测试全绿。

#### P2-2 surface live → 同一 Core app

- 新文件（封闭清单）：`src/spx_spark/web/live_api.py`（≤80 行）。
- 路由：`/healthz`（复用）、`/api/v1/live/session-surface`。live router 与 replay router 组合进同一个 `create_app`。
- 删除：`src/spx_spark/surface_live_session_http.py` 及其 console script。

#### P2-3 Schwab OAuth/gateway → Schwab FastAPI app

- 新文件（封闭清单）：`src/spx_spark/web/schwab_api.py`（≤150 行）。
- **保留不动**的业务代码：`OAuthCoordinator`、`PendingAuthorization`、`validate_oauth_settings`、`require_loopback`、`validate_oauth_paths`、`status_payload`、`probe_gateway_ready`；
- **只替换**传输层：`callback_handler_factory`、`gateway_handler_factory`、`RedactedHTTPServer`、`RedactedThreadingHTTPServer`、`OAuthServers`；
- 路由：现 callback server 的 `/healthz` + OAuth callback 路径；gateway server 的 `/livez`、`/healthz` + provider 数据路径（施工时从 handler factory 逐条抄录，Change Brief 列全）；
- redaction 语义保持：uvicorn access log 关闭（`access_log=False`），query string 带 code/token 的请求不得整条进日志；迁移后在测试环境跑一次完整 OAuth 流程并 `journalctl` 检索确认无 secret（C10）。
- 删除：`oauth_service.py` 中上述传输层类/函数（业务函数留在原文件）。
- 自检断言：`rg -c 'BaseHTTPRequestHandler|ThreadingHTTPServer' src/` = 0（Phase 2 完成标志）；OAuth 流程实测通过。

### Phase 3：Core 进程与内部直接调用

#### P3-1 `spx-core` 骨架与首批热任务

新文件（封闭清单）：`src/spx_spark/core_main.py`（≤150 行）、`systemd/spx-core.service`（需用户批准，计入 4 服务预算）。

骨架（抽象上限）——没有 task 框架，就是一个 TaskGroup：

```python
"""src/spx_spark/core_main.py"""
import asyncio

from spx_spark.app_settings import get_settings
from spx_spark.logging_setup import configure_logging


async def main() -> None:
    settings = get_settings()
    configure_logging("spx-core", settings.log_level)
    async with asyncio.TaskGroup() as tg:
        tg.create_task(_run_es_bar_sampler(settings))
        tg.create_task(_run_spx_minute_sampler(settings))
        tg.create_task(_serve_api(settings))  # uvicorn.Server(web.create_app(...))


# 每个 _run_* 是对现有模块主循环的最薄 async 包装；
# 现有同步循环用 asyncio.to_thread 包住整个循环，不重写内部逻辑。
```

施工步骤：

1. `spx core run` 挂入 Typer（`asyncio.run(main())`）；
2. 包装 `application/runtime/es_bar_sampler.py` 与 `spx_minute_sampler.py` 的现有主循环，内部逻辑零改动；
3. **staging 验证**：让新进程把输出写到隔离目录（复用现有 output-root 参数），与旧 unit 并行跑一个完整 session，逐文件 diff；这是读侧并行验证，不构成双写（输出不进生产路径）；
4. 周末 cutover：启用 `spx-core.service`，停用并删除 `spx-spark-es-bar-sampler.service`、`spx-spark-spx-minute-sampler.service`；
5. 崩溃恢复策略只有一条：任一任务异常 → TaskGroup 取消全部 → 进程退出 → systemd `Restart=on-failure` 拉起。**不写 per-task 重启、退避或健康仲裁逻辑。**

- 自检断言：`systemctl --user list-units 'spx-spark-*sampler*'` 为空；diff 报告在完成报告中附原始输出。
- 禁止：task registry、BaseTask、动态发现、进程内重启策略、把 sampler 内部逻辑“顺手 async 化”。

#### P3-2 hot worker 迁入

- 对象：`market_features_hot_worker.py`、`intraday_shock_hot_worker.py`、`market_regime_signal.py` 三个宿主 unit 的任务，逐个（每个一次 cutover）迁为 Core 内任务。
- feature 间通信改直接函数调用：调用点从“读对方 latest JSON”改为“调对方计算函数拿返回值”；对应 `latest/*.json` 若仍有 dashboard/export 消费者则降级为**单向导出**（写不读），无消费者则停写并在 P0-3 清单标记删除。
- 验收：3 个 unit 删除；shock 热路径延迟用 replay 对比不劣于现状（报告附数据）。
- 禁止：建进程内 pub/sub 或事件总线来替代 JSON 总线——直接调函数（blueprint §4.3）。

#### P3-3 service loop 退役

- `spx-spark-24h.service`（service_loop）中剩余任务按 1.3 归属分流：热任务 → Core TaskGroup；慢任务 → 记入 Phase 4 迁移清单（此时可暂留旧 unit 只跑慢任务，限一个发布周期，C13）。
- 全部分流完成后同 PR 删除：`src/spx_spark/service_loop.py`、`application/runtime/scheduler.py`、`runner.py`、`registry.py`、`tasks.py`、`supervisor.py`、仅供内部调度的 console scripts（名单以 P0-3 清单为准）、stdout JSON 解析逻辑。
- 自检断言：`rg -c 'ThreadPoolExecutor' src/spx_spark/application/runtime/` = 0；`spx-spark-24h.service` 从 `systemd/` 删除；console scripts 计数下降并记账。

### Phase 4：Job/通知统一

#### P4-1 `spx-worker` + Huey + 最小 Alembic 基线

新文件（封闭清单）：`src/spx_spark/infrastructure/jobs.py`（≤120 行）、`alembic.ini`、`alembic/`（init 生成 + 1 个 revision）、`systemd/spx-worker.service`（需用户批准）。

骨架（抽象上限）：

```python
"""src/spx_spark/infrastructure/jobs.py"""
from huey import SqliteHuey, crontab

from spx_spark.app_settings import get_settings

huey = SqliteHuey(filename=str(get_settings().data_root / "huey.sqlite"))


@huey.periodic_task(crontab(...))  # 从对应 .timer 的 OnCalendar 逐一翻译
def maintenance_daily() -> None:
    from spx_spark.maintenance import run_daily  # 调现有函数，不搬逻辑
    run_daily()
```

施工步骤：

1. `uv add huey`；worker unit `ExecStart=uv run huey_consumer spx_spark.infrastructure.jobs.huey -w 1 -k thread`（单 worker 单线程起步，不调优）；
2. `uv add sqlalchemy alembic`；`alembic init alembic`；initial revision `0001_notification_tables` 只建两张表：
   - `notification_events(id, idempotency_key UNIQUE, channel, payload_json, status CHECK(status IN ('pending','processing','delivered','failed','uncertain')), created_at, updated_at)`
   - `notification_attempts(id, event_id REFERENCES notification_events, attempt_no, started_at, finished_at, outcome, error_code, error_detail)`
   - 列可按实际 payload 微调，表数量不可加；
3. 首批只迁 3 个低风险 timer：`maintenance-daily`、`storage-pressure`、`schwab-reauth-reminder`；每个 task 函数体就是 import 现有入口函数并调用，crontab 表达式与原 OnCalendar 的对照表写进 Change Brief；
4. 周末 cutover：停用删除这 3 对 service+timer。

- 自检断言：`ls /srv/data/spx-spark/*.sqlite`（或实际 data_root）只多出 `huey.sqlite` 与 `spx.sqlite`；`uv run alembic history` 恰一条 revision；3 对 unit 从 `systemd/` 消失。
- 禁止：给 Huey 包任务基类或调度 wrapper；建第二个 queue；自建 job 状态表（Huey 自管执行状态，业务结果落 notification 表）。

#### P4-2 通知 delivery cutover

对象：1.2 中 notifier outbox 全家族 + `application/notifications/outbox_consumer.py` + `infrastructure/ledger/outbox.py`。

目标语义（blueprint §7.4，逐字执行）：producer 写一行 `notification_events(status='pending')` 并入队一个 Huey task；task 执行单条投递；显式可重试错误 → Huey retry（上限 3 次）；结果未知 → 标 `uncertain`，**不自动重发**；重复防护靠 `idempotency_key` 唯一约束，不靠 claim。

周末 cutover runbook（按序，写进 Change Brief）：

1. 切 producer：所有 `delivery_outbox.enqueue` 调用点改为写 `notification_events` + 入队 Huey task；
2. 排空旧 outbox：旧 delivery worker 继续跑直到 outbox 为空（`sqlite3` 只读查询确认）；
3. 停用 `spx-spark-notification-delivery.service`；
4. 删除：`notifier/delivery_outbox*.py`（4 个）、`delivery_worker.py`、`delivery_executor.py`、`receipts.py`、`receipt_mirror.py`、`outbox_consumer.py`、`infrastructure/ledger/outbox.py` 及对应测试中测“claim/receipt 实现形状”的部分（测投递行为的保留改写）；
5. 实测：`spx notify test`（本卡挂入 Typer）发一条测试通知，人工确认 target 收到，`notification_events` 状态为 `delivered`；
6. 回滚点：cutover 前打 tag；回滚 = revert + 重启旧 unit。

- 自检断言：`rg -c 'delivery_outbox|receipt_mirror' src/` = 0；测试通知投递实录贴报告。
- 禁止：保留旧 outbox“以防万一”；给 uncertain 状态写自动对账器（人工用 `spx status` 或 DB 查询处理）。

### Phase 5：Operational DB 统一

#### P5-1 表迁移

- 在 `0001` 基线上追加 revision，按 blueprint §5.4 建表：`sessions`、`events`、`decisions`、`decision_legs`、`outcomes`、`provider_incidents`、`compaction_manifests`（`schema_migrations` 由 Alembic 自管，不重复建）。
- 逐个 owner 迁移（每个 owner 一次 cutover）：现有各模块 SQLite/JSON 事实源 → `spx.sqlite`；JSON 一律降级为单向导出；历史数据迁移脚本一次性使用后删除，不留在生产代码里。
- SQLAlchemy 用法上限：`Table` + `insert/select/update` 语句 + `engine.begin()` 事务。**不建 ORM entity、relationship、session factory、repository 层。**

#### P5-2 旧机制清零

- 删除：所有运行时 `PRAGMA table_info` + `ALTER` 逻辑、各模块自带 DDL、双写 owner、`spx-spark-*` wrapper scripts、`config.py` env helper 家族、`settings/loader.py` YAML merge、`runtime.yaml`（存活键逐个迁入 TOML/AppSettings 或转研究 spec，迁移决定在 Change Brief 逐键列出）、PyYAML 生产依赖（见 2.9）。
- 自检断言：`rg -c 'PRAGMA table_info' src/` = 0；mutable DB 恰为 `spx.sqlite` + `huey.sqlite`；`sed -n '/\[project.scripts\]/,/^\[/p' pyproject.toml | rg -c '='` = 1。

### Phase 6：退出 Rust 控制面（按 blueprint §9.3 顺序，每步独立周末 cutover）

- P6-1 ledger：Python DB 接管（依赖 P5-1）；cutover 后用只读查询核对 Rust ledger 最后写入时间戳早于 cutover 时刻，证明无双写（C13）。
- P6-2 report：report schedule 迁为 Huey task；对比一个周期的 report 产出。
- P6-3 contract 清理：删除 `contracts/golden/`、`tests/golden/`、`tests/contracts/`、`tests/test_shared_golden_contracts.py`、`tests/test_rust_operator_notification_ingress.py` 及 Python 侧 bridge 写入代码。
- P6-4 退役：Rust 在 Oracle 上是 host system units，必须按依赖反序执行
  `sudo systemctl disable --now spx-rust-normalized-bridge.service spx-rust-report.service spx-rust-delivery.service spx-rust-frame-retention.timer spx-rust-core-shadow.service`
  （不得使用 `systemctl --user`）→ 观察一周 → 打 tag `pre-rust-removal` → 从 active tree 删除 `rust/` 与 `rust/systemd/`。
- 每步独立回滚点；报告附 Rust 侧时间戳核对原始输出。

### Phase 7：数据平台简化

- compaction 迁为 Huey task（复用 P4-1 骨架，不加新抽象）；manifest 写 `compaction_manifests` 表；raw deletion 默认禁用，清理只允许“verified + age”单一规则（一个函数，两个参数：manifest 校验通过 + 天数阈值），或干脆交给 systemd-tmpfiles；
- 删除：raw-delete 多阶段状态机、quarantine evidence、重复 audit 文件及对应 unit（data-compact 系列在 P4 已迁的除外）。
- 验收：一次完整周末 compaction 在新路径跑通，manifest 可用 SQL 查询；删除清单记账。

### S-track：策略信号引擎 v2（S1–S6，替代原 Phase 8）

实施合同是 `docs/strategy-signal-engine-v2.md`（算法、公式、阈值、验收案例全部以它为准）；本节任务卡只补充集成约束和自检断言，不复述算法。S1–S5 对应 v2 §24 的 PR 1–5。全程遵守协同裁决 11–16 与 v2 §23 硬约束。

**开工前置门（所有 S 卡）**：P0-2、P0-3 完成；P1-4（Import Linter）已落地。S 卡之间严格按序。

#### S1 统一事实与决策出口（= v2 PR 1）

- 范围：v2 §24 PR 1；新文件限 `strategy_facts.py`、`strategy_regime.py`、`strategy_select.py`（v2 §18.1 清单内），修改 `application/order_map/service.py` 一处接线。
- 第 0 步（先于一切代码）：核实 Rust bridge / `desk_map_projection.v1` 对 payload 新增键的行为，按裁决 14 处理，结论写进 Change Brief。
- bootstrap 阈值按裁决 12 做成 `policy_version` 冻结常量集。
- 自检断言：一个 Order Map 周期恰产生一个 `strategy_decision`；NO_TRADE 卡可完整生成；`uv run lint-imports` 通过；`uv run pytest -q tests/test_desk_map_projection_export.py tests/test_shared_golden_contracts.py` 通过（证明冻结契约未被污染）。
- 禁止：本卡实现 Butterfly、P/Q 模型或 LLM；为 StrategyDecision 建通用 lifecycle/状态机。

#### S2 Vertical Anti-Chase（= v2 PR 2）

- 范围：v2 §8–§10；新文件限 `strategy_payoff.py`（两腿部分）与 `strategy_decision_replay.py`。
- 回放对照按 v2 §19；通过 §19.5 门后，旧 Vertical 人工权限的切换在周末窗口执行（C11），同 PR 或紧邻 PR 降级旧 owner（v2 §18.4）。
- 自检断言：payoff/breakeven 的 Hypothesis 不变量测试在位；replay 报告含 legacy vs new 对照与四档滑点；Late Chase 案例输出 `direction_valid_but_entry_too_late`。

#### S3 Stable Pin Butterfly（= v2 PR 3）

- 范围：v2 §11；`analytics/options/density.py` 按 v2 §18.3 增量扩展（不破坏 percentile API）。
- 自检断言：8 月 5 日、8 月 6 日冻结案例按 v2 §20 通过并附回放原始输出；三腿 conservative BBO 拒绝 mid 定价的测试在位。

#### S4 P/Q 与 Bootstrap Utility（= v2 PR 4）

- 范围：v2 §13–§14；引入 `scipy` + `scikit-learn`（裁决 13/16 的已批准例外）。
- 自检断言：每个决策输出 `n_raw`/`n_effective`/`shrinkage_weight`/`historical_sessions`；相似样本不足时收缩向 Q 而不是拒绝输出；Utility 门（>0 且保守下界 >0）有测试。

#### S5 LLM Idea/Critic 与旧权限删除（= v2 PR 5）

- 范围：v2 §15 + §18.4 全部删除/降级项：radar 固定 lane 最终排名、GTH direct green-card、legacy candidate 直达 Trade Ready 的路径。
- LLM 用 provider 官方 SDK + 结构化输出（blueprint §10.2），按 v2 §15 的触发时点调用；LLM 不可用时确定性假设生成器兜底。
- 自检断言：`rg` 证明旧最终候选权限代码已删除/降级；LLM 输出的每条 supporting fact 有验证逻辑；完成报告按 v2 §25 逐条核对完成定义。

#### S6 决策持久化收尾（依赖 P5-1）

- P5-1 落地后，把 `StrategyDecision` 持久化 owner 从 order-map payload artifact 切到 `spx.sqlite` 的 `decisions`/`decision_legs` 表，replay 读取路径同步切换，JSON 降级为导出。一次周末 cutover 完成。

---

## 5. 验收与记账

- 每张任务卡的完成报告按 blueprint §15.2 模板记账，并附本卡自检断言的原始命令输出（S5）；本文档维护者在第 6 节累计登记。
- 全局完成标准按 blueprint §16.3；关键量化终点：console scripts 43+1→1、应用常驻服务 →4、`rg 'BaseHTTPRequestHandler' src/`→0、mutable DB→2 个文件、`rust/` 从 active tree 移除、`config/runtime.yaml` 退役。
- 每个 Phase 完成后跑一次全量 `uv run pytest -q` + `uv run ruff check src tests scripts` + `uv run lint-imports` 作为 Phase 闸门；单张卡按改动比例跑相关测试即可（blueprint §15 Test rules 第 5 条）。

## 6. 记账台账（随执行更新）

| 日期 | 任务卡 | files +/- | LOC +/- | deps +/- | units +/- | scripts +/- | DB tables +/- | 备注 |
|---|---|---|---|---|---|---|---|---|
| 2026-08-07 | P0-2 | +0 / -0 | production +3 / -0 | +0 / -0 | +0 / -0 | +0 / -0 | +0 / -0 | 三个冻结模块各只增加一行标记；39 个相关测试通过 |
| 2026-08-07 | P0-3 | +1 / -0 | production +0 / -0 | +0 / -0 | +0 / -0 | +0 / -0 | +0 / -0 | 120 行 owner 实体清单；已与 Oracle 只读事实交叉核对 |
| 2026-08-07 | P1-1 | +4 / -0 | production +45 / -0 | +1 / -0 | +0 / -0 | +0 / -0 | +0 / -0 | AppSettings + defaults/production TOML + 优先级测试 |
| 2026-08-07 | P1-2 | +1 / -0 | production +22 / -0 | +1 / -0 | +0 / -0 | +0 / -0 | +0 / -0 | structlog 单入口；实际 JSON 事件验证通过 |
| 2026-08-07 | P1-3 | +1 / -0 | production +21 / -0 | +1 / -0 | +0 / -0 | +1 / -0 | +0 / -0 | `spx status` 复用旧读取和渲染，scripts 43→44 |
| 2026-08-07 | P1-4 | +0 / -2 | production +0 / -0 | dev +1 / -0 | +0 / -0 | +0 / -0 | +0 / -0 | 两个 contracts kept；删除 309 行自研 registry/兼容测试 |
| 2026-08-07 | P1-5 | +0 / -0 | production +0 / -0 | +0 / -0 | +0 / -0 | +0 / -0 | +0 / -0 | collector 唯一 sleep 核实为 planner cadence，不误加 Tenacity；Hypothesis payoff 不变量在 S2 真实 owner 补齐 |
| 2026-08-07 | S1 | +3 / -0 | production +453 / -0 | +0 / -0 | +0 / -0 | +0 / -0 | +0 / -0 | 唯一 StrategyDecision/NO_TRADE；未来 frame 拒绝；Rust wire 未污染 |
| 2026-08-07 | S2 | +2 / -0 | production +794 / -268（S-track 当前 979） | dev +1 / -0 | +0 / -0 | +0 / -0 | +0 / -0 | Vertical BBO/payoff/Anti-Chase/replay；18 sessions、12 可比机会；gate collecting，未切 owner |
| 2026-08-07 | S3 | +0 / -0 | production +233 / -43（S-track 当前 1139） | +0 / -0 | +0 / -0 | +0 / -0 | +0 / -0 | Stable Pin/De-pin、Q mode/local mass、三腿 BBO；8/5 与 8/6 frozen cases PASS；未切 owner |
| 2026-08-07 | S4 | +0 / -0 | production +188 / -133（S-track 当前 1194） | +2 / -0 | +0 / -0 | +0 / -0 | +0 / -0 | Nearest-neighbor P、SciPy 区间、向 Q 收缩、Utility/保守下界；158 tests + import contracts PASS；仍无 fill/完整 path 标签 |
| 2026-08-07 | S5 | +0 / -2 | production +574 / -1472（S-track 当前 296） | +1 / -0 | +0 / -0 | +0 / -0 | +0 / -0 | 删除 fixed opportunity board；GTH/RTH 旧 READY 降为 selector evidence；统一决策接现有 outbox/Rust lane；官方 OpenAI-compatible SDK + 结构化事实校验；3045 tests + Ruff + import contracts PASS；待 Oracle 实际消息验收 |
| 2026-08-07 | P2-1 | +3 / -1 | production +286 / -675 | +1 / -0 | +0 / -0（1 unit 改入口） | +0 / -1 | +0 / -0 | Replay 七条路由迁 FastAPI/Uvicorn UDS；删除 623 行手写 transport；131 tests + Ruff + import contracts PASS；实际 UDS health/security headers PASS；周末才允许切 owner |
| 2026-08-07 | P2-2 | +1 / -1 | production +110 / -437 | +0 / -0 | +0 / -0（2 units 共用 app/UDS 入口） | +0 / -1 | +0 / -0 | Live accumulator 进 FastAPI lifespan 且保留单 owner lock；删除 402 行手写 transport；132 tests + Ruff + import contracts PASS；真实 UDS health/503/0660/clean shutdown PASS；周末才允许切 owner |
| 2026-08-07 | P2-3 | +1 / -0 | production +218 / -323 | +0 / -0 | +0 / -0（原 unit 内双 Uvicorn loopback） | +0 / -0 | +0 / -0 | Callback `/healthz`/动态回调与 gateway `/livez`/`healthz`/quotes/chains 全迁 FastAPI；`BaseHTTPRequestHandler` 全仓归零；54 card tests、3045 phase tests、Ruff、import contracts PASS；合成 callback→ready→quotes 双端口实测且 code/state 未入日志；周末才允许切 owner |
| 2026-08-07 | P3-1 implementation | +2 / -0 | production +178 / -15 | +0 / -0 | +1 / -0 | +0 / -0 | +0 / -0 | `spx-core` TaskGroup 首批 sampler 已实现；Oracle 隔离输出 staging 正在跑完整 session，未切 owner |
| 2026-08-07 | P3-2 implementation | +0 / -0 | production +369 / -85 | +0 / -0 | +0 / -0 | +0 / -0 | +0 / -0 | hot feature/shock/regime 迁入 Core 直接函数调用；ARM64 replay feature p95 4341.5ms、shock p95 273.2ms，5 份 schema 一致；未切 owner |
| 2026-08-07 | P4-1 implementation | +8 / -0 | production +185 / -0 | +3 / -0 | +1 / -0 | +0 / -0 | +2 / -0 | 单线程 Huey worker、唯一 Alembic revision、`spx.sqlite` 两张通知表；ARM64 staging NRestarts=0；3 对 timer 尚待周末切换 |
| 2026-08-07 | P4-2 implementation | +5 / -18 | production +2072 / -5882 | +0 / -0 | +0 / -1 | +0 / -0 | +0 / -0 | producer/READY/report 统一进 `notification_events`；删除旧 claim/receipt/worker/missed queue；`spx notify test` 已注册；2982 tests、Ruff、import contracts PASS，源码自检 0；旧队列排空与真实 Bark/飞书 receipt 待周末 cutover |
| 2026-08-07 | P3-3 implementation | +1 / -31 | production +142 / -2152 | +0 / -0 | +0 / -9 | +0 / -15 | +0 / -0 | 删除 service-loop scheduler/runner/registry/supervisor、旧实时 wrappers 与 9 个 unit；热任务由 Core TaskGroup 直接调用，低频上下文进入唯一 Huey worker，Live/Replay 共用 Core UDS；2945 tests、Ruff、import contracts PASS；完整 session diff 与周末 owner cutover 待完成 |
| 2026-08-07 | P4-1/P4-2 cutover preparation | +0 / -8 | production +39 / -150 | +0 / -0 | +0 / -6 | +0 / -2 | +0 / -0 | 删除已迁入 Huey 的 maintenance/storage-pressure/Schwab 提醒三对 unit 与两个专用 wrapper；installer 在 daemon-reload 前停旧通知和旧周期 owner，防止双投递/双执行；真实 cutover/receipt 仍待周末窗口 |
| 2026-08-07 | P5-1/S6 implementation | +2 / -2 | production +990 / -680（net +310） | +0 / -0 | +0 / -0 | +0 / -0 | +7 / -0 | Alembic `0002` 追加七张 operational 表；live StrategyDecision 与 legs 原子写 SQL 后才导出/入 READY；旧 research ledger 写入归并 `spx.sqlite`，删除两份手写 migration、runtime init 与独立 DB 默认路径；2948 tests、Ruff、import contracts、Alembic 单头链 PASS；Oracle 周末 cutover 待完成，未标记 P5/S6 完成 |
| 2026-08-07 | P5-2 CLI/TOML transition | +5 / -4 | production +216 / -68（net +148） | direct -1 / transitive unchanged | +0 / -0 | +0 / -0 | +0 / -0 | console registrations 27→1；旧 YAML 与 fixture 迁为等价紧凑 TOML，清除 14 个 YAML 说明误解析节点；全仓 +2026/-4235；2972 tests、Ruff、import contracts PASS；legacy loader/config helpers 尚未退役 |
| 2026-08-07 | P5-2 thin-wrapper cleanup | +0 / -19 | production +16 / -157 | +0 / -0 | +0 / -0 | +0 / -0 | +0 / -0 | 9 个现存 unit 改为绝对路径直接调用 `spx`（Core 已是直接入口），删除 19 个纯转发 wrapper；保留仍承担 flock/清理顺序的 3 个脚本；2973 tests、Ruff、import contracts、diff-check PASS；Oracle cutover 待周末窗口 |
| 2026-08-07 | P5-2 dead config/Grok removal | +0 / -0 | production +0 / -102 | +0 / -0 | +0 / -0 | +0 / -0 | +0 / -15 | 删除无调用 Grok CLI sink/兼容字段及 15 个无消费者配置叶；DeepSeek/Bark/飞书 owner 不变；全仓 +18/-173；2972 tests、Ruff、import contracts、diff-check PASS |
| 2026-08-07 | production safety: Rust frame retention | +0 / -0 | production +1 / -1 | +0 / -0 | +0 / -0（1 unit 策略值） | +0 / -0 | +0 / -0 | Oracle `/srv/data` 触发 20 GiB reserve 并导致 bridge start-limit；官方 manifest-gated prune 已恢复写入，日终 raw-frame 上限由 12 GiB 降至 8 GiB，归档验证和 7 天 completed-day 保护不变 |
| 2026-08-08 | P3/P4/P5 production cutover remediation | +0 / -0 | production +6 / -2 | +0 / -0 | +0 / -0（installer stop 语义修复） | +0 / -0 | +0 / -0 | Oracle 首次 cutover 暴露两项真实问题：已删除但仍 loaded 的 unit 使批量 stop 短路，以及旧 market-data root 生成第二个 `spx.sqlite`；改为逐 unit 独立 stop，并强制通知队列使用 AppSettings operational root；56 tests、Ruff、Import Linter PASS；重新部署后 Bark/飞书实投均一次 delivered，错误 DB 已移入 rollback 备份 |
| 2026-08-08 | deployment idempotence remediation | +0 / -0 | production +1 / -1 | +0 / -0 | +0 / -0（Core restart 语义） | +0 / -0 | +0 / -0 | `--now` 对首次 cutover 和后续部署统一执行 Core enable+restart；避免 editable checkout fast-forward 后旧进程继续运行旧模块；18 个 systemd 测试与 `bash -n` PASS，Oracle 本次已完成等价 restart，后续由 installer 自动执行 |
| 2026-08-08 | P5-2 duplicate runtime loader removal | +0 / -1 | production +57 / -208（net -151） | +0 / -0 | +0 / -0 | +0 / -0 | +0 / -0 | 删除重复 `runtime_config.py`，Schwab universe 与 human-focus 默认值迁入现有 typed settings owner；128 个相关测试、Ruff、Import Linter、tracked production-config smoke 与 diff-check PASS；Oracle 已 fast-forward 到 `1261e97`，Core/Worker/IBKR/Schwab units active 且 NRestarts=0，Python Bark/飞书测试均一次 delivered |
| 2026-08-08 | P5-2 obsolete runtime-value budget removal | +0 / -1 | production +0 / -0 | +0 / -0 | +0 / -0 | +0 / -0 | +0 / -0 | `runtime_value()` 已无定义和调用，删除 128 行专属 allowlist 测试；64 个 architecture tests、Ruff、Import Linter 与 diff-check PASS |
| 2026-08-08 | P5-2 hand-written dotenv removal | +0 / -0 | production +8 / -50（net -42） | +0 / -0 | +0 / -0 | +0 / -0 | +0 / -0 | 删除 `config.load_dotenv` 及全仓调用；Oracle 继续由现有 systemd `EnvironmentFile` 注入部署环境，不新增 dotenv 包或第二个配置 owner；117 tests、Ruff、Import Linter 与 diff-check PASS |
| 2026-08-08 | production safety: alert-candidate semantic dedupe | +0 / -0 | production +11 / -3 | +0 / -0 | +0 / -0 | +0 / -0 | +0 / -0 | 修复同一 cooldown bucket 内候选 payload 随行情变化触发幂等冲突并锁存 Core `BLOCKED`；仅 `ALERT_CANDIDATE` 保留首个快照并按 ID 去重，普通 Bark/飞书/operator 通知仍严格拒绝同 ID 异 payload；204 tests、Ruff、Import Linter 与 diff-check PASS |
