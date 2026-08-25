# SPX Spark 项目协作说明

本文件是 Agent 进入本仓库后的第一份项目说明。它记录项目目标、环境入口、工作边界和验收要求；实现细节仍以代码、测试和相应专题文档为准。

## 1. 项目基本信息

- 项目名称：`SPX Spark`
- GitHub 仓库：`git@github.com:hzy-hits/SpxOpDaily.git`
- 默认分支：`master`
- 生产主机：Oracle Ubuntu，`129.146.3.211`
- SSH 用户：`ubuntu`
- 服务器项目目录：`/home/ubuntu/spx-spark`
- 技术栈：Python 3.12、`uv`、DuckDB、pytest、Ruff；Rust 1.93、Cargo；systemd user/system services

本机 SSH 入口：

```bash
ssh -i "/Users/ivena/Downloads/ssh-key-2026-03-30 (1).key" ubuntu@129.146.3.211
```

这里使用的是 **SSH 私钥认证，不是 Ubuntu 密码**。私钥只存在于本机，不得读取其正文、复制到项目目录、写入文档/日志/聊天、提交 Git 或上传服务器。服务器上的 Git remote 使用了本机 SSH alias：

```text
git@github-spxopdaily:hzy-hits/SpxOpDaily.git
```

它与上面的 GitHub 仓库是同一个项目；没有明确原因时不要修改该 alias。

## 2. 我们的目标

建设一套近实时的 SPX/SPXW 0DTE 行情、研究、可视化和提醒系统，为人工决策提供可信的盘前地图、盘中结构、执行提示、风险边界和盘后复盘。

核心目标：

1. 以 Schwab 作为常规 RTH 主数据源，以 IBKR Paper 负责 SPXW GTH 和 Schwab 故障时的备用行情。
2. 根据 provider 的源时间戳明确区分 `live`、`delayed`、`frozen` 和 `missing`，不把陈旧数据伪装成实时数据。
3. 持续采集 SPX、ES、SPXW 期权链/NBBO，并生成 IV surface、Greeks、墙位、市场结构、策略观察和告警。
4. 同时支持 live dashboard 和因果、可审计的 replay/backtest；任何策略优势结论都必须由覆盖充分的前向数据证明。
5. 输出简洁、专业、可执行的 SPX desk 风格信息，并明确 `Desk View / Execution / Risk / Targets / Data Quality`。
6. 保持系统只读和失效关闭：当前项目不下真实订单；Paper 仓位、成交和模拟结果不得表述为用户真实账户风险敞口。
7. 将策略逐步统一到一份可版本化的 decision contract；当前生产策略与固定时点、10 点 vertical 的候选方案不得混作同一套回测证据。

## 3. 事实与安全边界

- `systemctl` 显示 `active` 只证明进程存活，不证明行情新鲜或策略可交易。分别检查服务状态、provider/token 状态、源时间戳/NBBO readiness、数据覆盖和通知投递。
- SPXW exact-leg 必须核对订阅需求、确认状态、`ready_at` 和 bid/ask 新鲜度，不能只看宽链或 latest-state 是否有行。
- GTH 与 RTH 的 provider 规则不同；不得用 frozen Schwab 行情静默替代 GTH 的 IBKR SPXW 两边 NBBO。
- IBKR 的 `100` 是所有 concurrent ticker lines 的总容量，不是 100 个 SPXW。当前 option lane 上限为 84：RTH validation 使用 44 hot + 20 rotation，GTH/fallback/prefetch 使用 46 hot + 38 rotation，其余容量留给 base、temporary exact-leg 和 reserve。
- 手机/TWS 占用共享 Live entitlement 时，IB Gateway 必须退让且 collector 进入 `10197` circuit/backoff；不得踢掉用户交易 session。RTH 继续使用 Schwab 主链，GTH 必须停止产生可执行建议，直到 IBKR fresh flush 恢复。
- IB Gateway API 端口（Paper 通常为 4002）只能由服务器 loopback 访问；OCI ingress 和主机 firewall 都不得向公网开放 4001/4002。
- OI/volume/exposure surface 是结构代理，不等同于真实做市商或参与者持仓；没有带方向和 open/close 标签的数据时必须如实标注限制。
- 非显然的业务阈值和策略规则写入 typed config、deployment overlay 或文档，不要散落硬编码。
- 配置优先级固定为 `defaults < deployment < environment`。机器专属值放在 gitignored 的 `config/runtime.local.yaml` 或本机环境中。
- 不读取、打印或提交 `.env`、token、broker 凭据、私钥、cookie、通知密钥以及 `/srv/data` 下的运行时 secrets。
- 不覆盖用户已有改动；开始和结束时都检查工作树，并把观察事实与推断分开说明。

## 4. 权威入口

- `README.md`：产品范围、单仓库布局、常用命令和运行方式。
- `docs/monorepo-layout.md`：Python/Rust 所有权、保留历史、CI 与部署边界。
- `rust/AGENTS.md`：Rust workspace 的严格状态机与安全边界。
- `rust/docs/ARCHITECTURE.md`：Rust core、ledger、report、delivery 架构。
- `module-architecture.md`：模块分层和依赖规则；新增生产模块时必须同步架构登记测试。
- `docs/headless-deployment.md`：Oracle、IB Gateway/IBC、VNC 和 systemd 部署说明。
- `docs/runtime-configuration.md`：typed settings 与部署配置规则。
- `docs/market-data-capability-matrix.md`：数据源能力和 readiness 语义。
- `docs/architecture-simplification-blueprint-v1.md`：架构简化与第三方能力替代总方案（上位约束）。
- `docs/architecture-simplification-execution-plan-v1.md`：简化重构的事实基线、偏差澄清与逐阶段任务卡（执行基线）。
- `docs/strategy-signal-engine-v2.md`：0DTE 统一策略信号引擎实施合同（S-track 基线；排期与边界裁决见执行方案第 2 节 11–16 条）。
- `docs/strategy-signal-engine-v4.md`：**已合入**的 reuse-first 事件结算观点扩展（`EVENT_SETTLEMENT_THRESHOLD`）；在高影响宏观事件发布前，把“昨收上方/下方结算”映射为 5 点 Debit Vertical，仍走 `build_strategy_decision`。宏观日历由 `macro_event_clock` + `macro_event_calendar` 在 Core 周期内按 TTL 自动刷新到 `data_root/runtime/macro_events.auto.json`（FF 本周 high-impact + Fed FOMC），种子 `config/macro_events.toml` 作回退。
- `docs/refactor-architecture-acceptance-plan.md`：架构目标与验收门槛；与简化方案冲突的章节以简化方案为准。
- `systemd/`：服务与 timer 定义。
- `scripts/install-spx-spark-services.sh`：正式部署入口及分支、工作树和 unit drift 防护。

主要代码目录：

```text
src/spx_spark/     Python 应用、provider、分析、策略、通知和运行编排
rust/              Rust typed core、bridge、ledger、report 和 delivery
contracts/golden/  Python/Rust 共用的版本化 wire contract fixtures
tests/             单元、架构、契约、状态机、集成和端到端测试
config/            默认配置与部署配置样例
systemd/           生产 user service/timer 定义
scripts/           校验、运维和部署脚本
docs/              设计、数据语义、部署与验收证据
site/              SPXW surface 与 strategy review 前端
```

## 5. Agent 工作流程

开始工作：

```bash
git status --short --branch
git remote -v
uv sync --frozen
```

先读与任务直接相关的权威文档和测试，再做最小范围修改。新增或移动模块前必须遵守 `module-architecture.md` 的分层方向和 Import Linter contracts，不得靠扩大豁免绕过架构检查。

本地验证按风险递进：

```bash
uv run pytest -q path/to/relevant_tests.py
uv run lint-imports
uv run ruff check src tests scripts
uv run pytest -q
(cd rust && cargo fmt --all --check)
(cd rust && cargo clippy --locked --workspace --all-targets --all-features -- -D warnings)
(cd rust && cargo test --locked --workspace --all-targets --all-features)
git diff --check
```

纯文档改动至少运行 `git diff --check` 并复核链接、命令、主机路径和敏感信息；代码、配置、systemd 或运行语义改动必须运行相关测试，重要改动应跑全量 pytest 和 Ruff。

## 6. 远端检查与部署

未得到明确部署授权时，只做远端只读检查，不执行 `pull`、restart、enable/disable、配置写入或数据迁移。

常用只读检查：

```bash
ssh -i "/Users/ivena/Downloads/ssh-key-2026-03-30 (1).key" ubuntu@129.146.3.211
cd /home/ubuntu/spx-spark
git status --short --branch
git rev-parse --short HEAD
systemctl --user list-units --type=service --state=running 'spx-spark*' 'ibc*'
journalctl --user -u <service-name> -n 100 --no-pager
```

部署前必须确认：

1. 本地变更范围正确，没有 secrets，测试和 `git diff --check` 通过。
2. 变更已经按用户要求 commit/push，服务器位于干净的 `master`，且目标提交等于 `origin/master`。
3. Python 使用仓库部署脚本和已有 systemd user units；Rust 按 `rust/docs/OPERATIONS.md` 使用 host system units，不临时手工复制生产代码。
4. 只重启受影响服务，并避免双 writer；合并仓库本身不得顺带转移 report/delivery owner。
5. 部署后分别验证 unit 状态、restart count、日志、health endpoint、数据源时间戳/NBBO readiness 和实际通知投递状态。

生产核心通常包括 `spx-spark-24h`、`spx-spark-ibkr-stream`、`spx-spark-schwab-marketdata`、`spx-spark-schwab-oauth`、`spx-spark-market-features-hot`、`spx-spark-intraday-shock-hot`、`spx-spark-notification-delivery`、`spx-spark-surface-dashboard`、`spx-spark-surface-live`、`spx-spark-surface-replay` 和 `ibc-gateway`。实际状态始终以远端 systemd、health endpoint 和数据新鲜度检查为准。

## 7. 完成交付标准

最终汇报必须说明：改了什么、验证了什么及结果、是否部署、远端实际状态，以及仍存在的风险或未完成项。不得仅以“代码已写完”或“服务 active”作为完成依据。

## 8. Architecture Simplification Contract

本节约束优先于任何“多写测试、全量验证、多做防御”的默认偏好。完整设计基线见 `docs/architecture-simplification-blueprint-v1.md`；事实核实、偏差澄清与逐阶段任务卡见 `docs/architecture-simplification-execution-plan-v1.md`。所有简化重构工作必须引用其中的 Phase 与任务卡编号。Rust workspace、`service_loop`/runtime scheduler 与自研 notification outbox 处于冻结状态：只准修复生产故障，不接受新功能。

### Before coding

For every non-trivial task, first output a Change Brief:

- User-visible goal
- Existing owner and files
- Existing dependency that can solve it
- New dependency, if any, and why stdlib/existing packages are insufficient
- Old files/services/config/tests that will be deleted
- Persistence, process and notification impact
- Minimal end-to-end acceptance path

Do not edit code until this brief is accepted when the user requested design-first work.

### Implementation rules

1. Prefer extending the existing owner. Do not create a new module unless no current module owns the responsibility.
2. A new abstraction needs at least two real callers in the current change. Hypothetical future callers do not count.
3. Do not create a service, timer, database, queue, state machine, schema version or Rust contract without explicit user approval.
4. Use the blessed dependency for generic infrastructure. Do not hand-write another config loader, HTTP server, scheduler, retry loop, logger, database migrator, CLI parser or architecture linter.
5. Do not add a second package for a responsibility already covered by the blessed stack.
6. Build the smallest working vertical slice first. Tests and refactoring follow after the user-visible path works.
7. Delete the replaced implementation in the same phase. Compatibility wrappers require an explicit deletion issue and may live for at most one release cycle.
8. Do not use feature flags to retain rejected architecture indefinitely.
9. Do not turn an internal Python call into a CLI subprocess or JSON-file IPC.
10. Do not use LLM frameworks for a single structured model call. Use the provider client and typed output directly.

### Test rules

1. Test public behavior, money/time/data invariants and external boundaries.
2. Do not test helper call order, private dictionaries, exact prose or transient weights.
3. Prefer Hypothesis for mathematical invariants and timestamps.
4. Use a small number of frozen replay sessions for strategy semantics.
5. Run tests proportional to the change. Full Python + Rust validation is a release/cutover gate, not the default for an unrelated small edit.
6. A test is not a reason to keep an unnecessary abstraction.

### Complexity budget

Every final report must show:

- production files added/deleted
- net production LOC
- dependencies added/removed
- config keys added/removed
- services/timers added/removed
- databases/tables added/removed
- legacy paths removed

A change that increases process count, active languages, mutable stores or ownership paths requires explicit user approval.

### Strategy engine (S-track) constraints

策略信号引擎基线合同是 `docs/strategy-signal-engine-v2.md`；事件结算观点扩展见
`docs/strategy-signal-engine-v4.md`（`EVENT_SETTLEMENT_THRESHOLD`）。排期与边界裁决见
`docs/architecture-simplification-execution-plan-v1.md` 第 2 节第 11–16 条与 S1–S6 任务卡。要点：

1. 人类可见交易候选只能来自 `payload["strategy_decision"]`（`build_strategy_decision` 唯一出口）；旧 candidates、GTH direct green-card、radar lane 排名在对应 S 卡落地后必须删除或降级，不得双写两套“最终候选”。
2. 策略候选第一版固定五类（NO_TRADE、Call/Put Debit Vertical、Call/Put Butterfly）；v4 仅新增 setup `EVENT_SETTLEMENT_THRESHOLD`，仍复用 Call/Put Debit Vertical，不新增 payoff 类型。v42 起 RTH 人读 Debit 为 `EVENT_SETTLEMENT_THRESHOLD`、`ES_VOLUME_MOMENTUM`、`PREAVERAGE15_PULLBACK` 与用户明确授权但仍标记 forward-unvalidated 的 `WALL_BREAKOUT_HAZARD`。后者只用因果 SPX 路径尺度与 Call/Put Wall、Zero Gamma、剩余 EM 的冻结四变量三分类模型；要求 OI-GEX、方向概率至少 0.17、15 秒内证据、exact BBO、通用几何/借记门和按目标结算价值计算的保守执行 EV > 0，不继承旧策略方向。v43 起 RTH `STABLE_PIN` 要求同一中轴 3 个决策快照且持续 10 分钟，10 点内挑战者须领先至少 0.05 才换轴；仅已确认的第一中轴可枚举 10/15/20/50 点 look-window 蝶，人读卡接受后在 15 分钟内锁定中轴/翼宽/期权方向。v44 起用户明确授权但仍标记 forward-unvalidated 的 `CLOSE_CONVERGENCE_60M` 在 15:00 ET 用严格因果 raw SPX/ES online-pool 收盘分布的 5 点 modal center 枚举 10/15/20 点 Call/Put 蝶；它不继承 STABLE_PIN、dealer/GEX/OI/wall/HMM/方向门，要求 Schwab exact BBO ≤15 秒、skew ≤2 秒、借记占翼宽 ≤0.45、风险 ≤$1,000，并固定持有到 15:55 ET 新鲜组合 bid，仍只授权人工卡且 `automatic_ordering=false`。`PREAVERAGE15_PULLBACK` 仍仅用因果 SPX 五秒路径触发，固定 Schwab 60Δ / 15 点价差，不继承 HMM/GEX 方向、旧 entry-quality 或历史方向 stick 门。失败突破/回踩/突破仍枚举供 funnel，硬门 `unevidenced_debit_not_human_authorized`。GTH 宽链/delta 扫描只进入 Desk Map 与拒绝漏斗，不再授权 `trade_ready`；GTH 人读 Debit 只保留确认水平 / 回踩收复（`gth_level_manual_candidate` / `gth_dip_reclaim_evidence`）。GTH 方向锁 30 分钟、同 setup/direction 冷却 15 分钟、每个 session mode 每方向最多 2 张；借记上限 0.45。RTH pin TRADE 仍可出蝶。v36 起铁鹰只挂 Desk Map（`iron_condor_map`），硬门 `iron_condor_not_human_authorized`，不套借记 20 分钟管理路径。v39 起 `POST_EVENT_DISCOVERY` 不再挡量比卡；借记管理去掉 v1 20 分钟 time stop，保留 50% premium stop / trail / 15:45 ET 硬退出。现金 HMM 反向 TREND、加仓要新冲动、翻向要 HMM TREND 仍在。扩大候选空间需用户批准。
3. bootstrap 阈值是带 `policy_version` 的冻结代码常量，不进 `runtime.yaml`、不进 AppSettings；改阈值 = 改代码 + 版本递增 + replay 对照。
4. `strategy_decision` 不得进入 `contracts/golden/` 或任何 Rust 消费的投影；候选卡走现有 Python `trade_ready` lane。
5. 不为策略新增 service、timer、数据库、队列、状态机或 Rust；v4 新增生产文件上限为 `event_settlement_vertical.py` 一个组合模块。
6. 所有判断在 SPX 坐标完成；conservative synthetic BBO 不得用 mid 代替；回放必须满足 `available_at <= decision_at`，冻结验收案例为 2026-08-05 与 2026-08-06。
7. LLM 只做 bounded idea/critic（结构化假设 + 反证），不计算价格/概率/payoff、不覆盖 hard gate、不直接创建 Trade Ready；不引入 LangChain/LangGraph。
8. `automatic_ordering=false` 不变；回放通过 v2 §19.5 门后直接进入人工候选卡，不得以“继续 Shadow N 天”代替工程接入。
9. v45 起曲面只拥有结构排序风险降权：对已过 hard gate 的候选按入场冻结 strike 计算 ATM、左右 skew、左右 curvature 的 1 vol bump-and-revalue，`surface_decision_modifier∈[-0.05,0]`。它不得产生或翻转方向、增加正向分数、绕过 hard gate 或改变 `automatic_ordering=false`；IV 缺失时记录 unavailable 且对旧决策零影响。v46 铁鹰人工授权来自用户明确的独立 RTH 合同，不是 surface 解锁。
10. v46 起 RTH 铁鹰开放当日第一个合格人工候选；v52 当前合同只在 10:00–11:00 ET 枚举，Put/Call 分别从 Schwab 最新 Greeks 中选择绝对 Delta 不超过且最接近 20Δ 的短腿，两侧固定 10 点翼，并锁定当天首个满足贷记、风险与几何门的行权价。exact 四腿 BBO age ≤15 秒/source skew ≤2 秒，Greeks age ≤15 秒，定义风险 ≤$1,000，保守贷记占翼宽 25%–55%。ATM IV、左右 25Δ skew 与联合曲面归因只作风险说明和非正向排序降权；高值或缺失不得拒绝铁鹰、锁死当日资格或绕过其他 hard gate。回购价 ≤0.5C 止盈、≥3C 止损（净亏 200%）、15:45 ET 硬退；每 RTH session 最多一张，GTH 继续 map-only，`automatic_ordering=false`。该合同仍为 `forward_unvalidated_user_override`，不得声明长期 alpha。
11. v48 起 GTH 已确认水平/回收证据可通过分钟级人工门禁，不再要求已推广 first-touch 模型：当前 1m 必须同向，5m 反向幅度不得超过 0.5×ATR5m；仍要求上游因果证据与有效期、exact BBO、借记/翼宽 ≤45%、定义风险 ≤$1,000，并保留方向锁、冷却、人工-only 与 `automatic_ordering=false`。`GTH_WIDTH_SCAN`、`GTH_DELTA_SCAN` 和纯趋势切换背景继续不可授权。
12. **v4 触发条件（运维）**：`config/macro_events.toml` 中存在尚未发布、`impact∈{high,critical}`、且 `release_at` 不晚于当日 front expiry 收盘的事件；同时 `day_move.prior_close`、front expiry、两腿 exact BBO 就绪。事件发布后停止生成。入口：`enumerate_event_settlement_candidates` → `rank_candidates` → `strategy_decision`；Codex/Agent 以本文件与 v4 文档为准，勿把 Draft PR 状态当作现行事实。
13. v51 起 RTH 使用唯一 `rth_environment` 作过滤和结构选择，不获得方向权限。事件风险继续挡普通入场；VIX1D/ATM/跨式扩张只允许已有价格触发的方向结构继续过门；波动收缩、breadth 35%–65% 且 BALANCED/PIN_STABLE 才允许 IC/STABLE_PIN；核心输入缺失或混合状态失效关闭。HYG/LQD、SHY/IEF/TLT、UUP、USO 仅为 ETF 价格确认代理，不得表述为真实 credit spread、2Y/10Y、DXY 或 WTI。`EVENT_SETTLEMENT_THRESHOLD` 与 `CLOSE_CONVERGENCE_60M` 保持各自独立合同，`automatic_ordering=false` 不变。
