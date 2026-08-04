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
- `docs/refactor-architecture-acceptance-plan.md`：架构目标与验收门槛。
- `docs/unified-strategy-runtime-refactor-2026-07-31.md`：本轮策略、采集 session、通知链路和 HMM 的统一重构决策。
- `systemd/`：服务与 timer 定义。
- `scripts/install-spx-spark-services.sh`：正式部署入口及分支、工作树和 unit drift 防护。

主要代码目录：

```text
src/spx_spark/     Python 应用、provider、分析、策略、通知和运行编排
rust/              Rust typed core、bridge、ledger、report 和 delivery
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

先读与任务直接相关的权威文档和测试，再做最小范围修改。新增或移动模块前必须遵守 `module-architecture.md`，不得靠扩大白名单绕过架构测试。

本地验证按风险递进：

```bash
uv run pytest -q path/to/relevant_tests.py
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
