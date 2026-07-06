# 设计缺陷 Review 与改进计划

日期：2026-07-06（同日第二轮更新：流式采集、文件锁、并发调度已落地）。范围：整个 spx-spark 项目，重点是 IBKR 数据链路的可用性（登录被抢占后的自动恢复能力）。

结论先行：项目的分层（provider adapter → 归一化 Quote → storage/latest state → features → alerts）是清晰的，测试面也不错；真正的短板集中在**运行时韧性**（会话恢复链条有断点）、**采集架构**（快照轮询而非常驻订阅）和**工程卫生**（无版本控制、重复样板、过宽的异常吞噬）。

## 一、会话恢复链条（本次已修复）

系统被手机/桌面登录抢占后的预期恢复链条是：

1. IBC 配置 `ExistingSessionDetectedAction=secondary`：礼让手动会话，Gateway 退出；
2. systemd `Restart=always` + `RestartSec=60`：每 60 秒重试登录；
3. 手动会话结束后，IBC 登录成功，API 端口恢复；
4. 采集侧 `IBKR_CONFLICT_PROBE_SECONDS=60`：collector 每 60 秒无侵入探测，恢复后自动回到 IBKR 主源。

实际上链条在第 2 步就断了，另外还有两个隐蔽断点：

### 1.1 systemd start limit 耗尽后永久放弃（严重，已修复）

`ibc-gateway.service` 原来写的是 `StartLimitIntervalSec=3600` + `StartLimitBurst=60`，而 `RestartSec=60` 意味着每小时恰好尝试 60 次——正好打满限额。只要手动会话保持超过约一小时，systemd 就把服务标记为 `failed` 并**永久停止重试**，之后即使手动会话退出也不会有人把 Gateway 登回去。这就是"没有 IBC 登录回去"的直接原因。

修复：`StartLimitIntervalSec=0`，彻底关闭 start limit，让 60 秒一次的礼让式重试无限进行。secondary 模式不会抢占手动会话，所以无限重试是安全的。

### 1.2 "进程活着但 API 死了"没有任何恢复手段（严重，已修复）

`Restart=always` 只在进程退出时生效。Gateway 卡在登录对话框、静默丢会话、API socket 起不来时，进程不退出，systemd 永远不会介入，collector 只会一直报 `connect failed`。

修复：新增 `scripts/ibc-watchdog.sh` + `systemd/ibc-watchdog.timer`，每 2 分钟检测 API 端口，连续 3 次（默认，`IBC_WATCHDOG_FAILURE_THRESHOLD`）失败才重启 `ibc-gateway.service`。安全约束：runtime mode 为 `protected` 时跳过；服务被手动 disable 时跳过；端口健康时立即清零计数，因此不可能重启一个正常工作的会话。

### 1.3 每日强制登出导致的重登/2FA 风险（中等，已修复）

Gateway 每天强制 logoff/restart。不配置 `AutoRestartTime` 时走完整重登，遇到 2FA 超时窗口就可能挂一夜。`configure-ibc-secrets.sh` 现在默认写入 `AutoRestartTime=03:55 AM`（服务器时间，可改可禁用）：Gateway 走"保持认证的每日自动重启"，一次 2FA 大约能撑一周。

### 1.4 user service 依赖 linger（中等，已缓解）

所有单元都是 `systemctl --user`，若未 `loginctl enable-linger`，SSH 登出/宿主机重启后服务全部不再运行——这也是"看起来不会一直保持最新状态"的可能来源之一。`install-ibc-service.sh` 现在会检测并给出警告。文档已同步说明。

### 1.5 恢复链条中仍然接受的行为（设计取舍，非缺陷）

- 抢占发生瞬间，正在进行的 collector 采样以 `competing session (10197)` 或 socket disconnect 失败，一个周期内数据降级到 fallback——这是 `STRICT_NO_SESSION_FIGHT=true` 的预期行为；
- probe 是 API 层连接，不会顶掉手机端的 broker 会话，保持 60 秒探测是安全的。

## 二、采集架构层面的缺陷（本轮已实现）

### 2.1 快照式轮询而不是常驻订阅（已实现：`ibkr/stream_collector.py`）

原缺陷：`ibkr/collector.py` 每个周期完整地建连 → 订阅 → `sleep(8s)` → 快照 → 断开，每分钟约 52 秒盲区，且 `sampling.py` 的轮换分组从未被使用。

已实现的流式采集器（`spx-spark-ibkr-stream`，`systemd/spx-spark-ibkr-stream.service`）：

- **常驻连接**：独立 `IBKR_STREAM_CLIENT_ID=172`（与快照 collector 的 171 分开，互不踢会话），read-only、market-data-only 连接方式与快照路径完全一致（同样通过 `connect_market_data_only`，受 data-only AST 测试约束）。
- **订阅结构**：基础合约（指数/ETF/期货/CFD）永久订阅；SPXW 期权按行数预算 `IBKR_STREAM_MAX_OPTION_LINES=60` 分两层——约 70%（`HOT_LANE_SHARE`）给 ATM 附近的热区常驻订阅，剩余预算在 sampling planner 的 rolling groups 上逐片轮换，每次 flush 换一片，完成全窗口扫描。
- **写入节奏**：每 `IBKR_STREAM_FLUSH_SECONDS=5` 秒把 ticker 状态快照写入 raw + latest state（走既有 `persist_provider_snapshot`），盲区从 ~52s/min 降到 ~0。
- **重规划**：SPX 相对当前 ATM 漂移超过 `IBKR_STREAM_REPLAN_DRIFT_POINTS=10` 点、或跨日换到期日时，重建期权订阅计划。
- **韧性**：断线走指数退避重连（5s→300s 封顶）；检测到 10197 竞争会话时退订断开、按 `IBKR_CONFLICT_PROBE_SECONDS` 静默等待再探测（不抢会话）；每次 flush 后复查 runtime mode，`protected` 随时生效。
- 快照 collector 保留，作为验证/一次性检查路径；24h loop 中的 ibkr 任务与 stream service 二选一，避免双份 IBKR 行情写入。

### 2.2 service_loop 串行调度（已实现：线程池并发）

`run_loop` 改为 `ThreadPoolExecutor`（`SPX_SERVICE_MAX_CONCURRENT_TASKS=4`）：到期任务提交线程池，同名任务最多一个 in-flight（不会自我堆积），完成后按既有 `next_delay_seconds`（含 IBKR 冲突退避）重排。一个卡住的任务不再拖偏其他任务节奏。真实任务本来就是子进程（带 subprocess 超时）；SIGALRM 超时只在主线程可用，worker 线程内的 in-process fn 任务不再挂硬超时（仅测试路径使用）。heartbeat 事件新增 `in_flight_tasks` 字段便于观察。

### 2.3 latest state 的多进程读改写竞态（已实现：flock 文件锁）

`LatestStateStore.update` 的整个 load → merge → write 现在包在 `state.json.lock` 上的 `fcntl.flock` 排它锁里，跨进程（24h loop、stream collector、手动 collector、iv_surface）串行化，不再丢报价。`load` 只读不加锁——tmp+rename 保证读到的总是完整文件。新增并发写测试验证 20 个线程并发 update 无丢失。

### 2.4 抢占后可以自动进入短 TTL 的 protected（低，未做）

collector 已经能识别 10197。检测到抢占时自动写一个如 30 分钟 TTL 的 `protected` override，可以减少手动交易期间无意义的重连尝试和日志噪声（probe 本身无害，此项是体验优化）。

## 三、工程卫生（建议）

### 3.1 版本控制（勘误：仓库存在）

第一轮 review 误报"项目不在 git 里"——那是 IDE 工作区元信息的误判。`.git` 存在，remote 为 `git@github-spxopdaily:hzy-hits/SpxOpDaily.git`（master 跟踪 origin/master）。真正的待办只剩：把本次改动 commit/push，保持工作区不长期悬空。

### 3.2 verifier.py 身兼 CLI 与核心库（中）

`collector.py`、`trading_hours_report.py` 都从 `verifier.py` 导入合约构建、连接、订阅、快照逻辑，而它同时又是一个 CLI。建议拆成 `ibkr/contracts.py`（build_*、parse_index_spec、estimate_atm_reference）和 `ibkr/session.py`（connect/subscribe/snapshot/cancel），verifier/collector/report 三个 CLI 只做编排。同理 `clean_float`、`first_present`、`midpoint` 在 verifier 与 marketdata 各有一份，应只留 marketdata 一份。

### 3.3 env 解析样板重复（低）

`config.py` 的 `_env_bool/_env_int` 与 `service_loop.py` 的 `env_bool/env_int` 是两套等价实现，且行为略有差异（config 版对非法布尔抛错，service_loop 版静默按 falsy 处理）。统一到 config.py。

### 3.4 过宽的异常吞噬（低-中）

大量 `except Exception  # noqa: BLE001`，其中 `cancel_subscriptions` 直接 `pass`。采集类代码需要容错，但至少应该把异常记进 `IbkrError` 列表或 stderr，否则排障时只能看到"没有数据"而不知道为什么。

### 3.5 阈值不一致（低）

`IBKR_STALE_AFTER_SECONDS=10` 与 `MARKET_DATA_LATEST_STALE_AFTER_SECONDS=15` 两套 stale 判定并存：同一条报价可能在采集侧标 stale、在 latest state 侧又算新鲜。不是错误，但建议统一或在文档中写明两层判定的语义差别（采集时刻 vs 读取时刻）。

### 3.6 无 CI（低）

有 pytest + ruff，但没有任何自动执行入口。有了 git 之后加一个最小 CI（或者 pre-commit 钩子）跑 `ruff check` + `pytest` 即可。

## 四、改动清单

### 第二轮（流式采集 / 并发 / 锁）

| 类别 | 文件 | 内容 |
| --- | --- | --- |
| 流式采集 | `ibkr/stream_collector.py`（新） | 常驻订阅采集器：热区+轮换分组、5s flush、退避重连、冲突探测、runtime mode 复查 |
| 流式采集 | `config.py` | 新增 `IbkrStreamSettings`（`IBKR_STREAM_*`） |
| 流式采集 | `pyproject.toml`、`scripts/run-ibkr-stream.sh`、`systemd/spx-spark-ibkr-stream.service` | CLI 入口、运行脚本、systemd 单元 |
| 并发调度 | `service_loop.py` | `run_loop` 线程池化；`submit_due_tasks`/`drain_finished_tasks`；worker 线程跳过 SIGALRM |
| 文件锁 | `storage.py` | `LatestStateStore.exclusive_lock()`，update 全程持锁 |
| 测试 | `tests/test_stream_collector.py`（新）、`tests/test_service_loop.py`、`tests/test_storage.py` | 订阅预算/轮换/重规划/退避、并发调度、并发写不丢数据 |
| 配置 | `.env.example` | `IBKR_STREAM_*`、`SPX_SERVICE_MAX_CONCURRENT_TASKS` |

### 第一轮（CFD / 会话恢复）

| 类别 | 文件 | 内容 |
| --- | --- | --- |
| CFD 标的 | `config.py` | 新增 `IBKR_VERIFY_CFDS`（默认 `IBUS500`） |
| CFD 标的 | `marketdata.py` | 新增 `InstrumentType.CFD`、`InstrumentId.cfd()`、`cfd:` 标签解析、CFD→现货指数映射 |
| CFD 标的 | `ibkr/verifier.py` | `build_base_contracts` 生成 CFD 合约；`estimate_atm_reference` 新增 IBUS500 回退（SPX → ES → IBUS500 → SPY×10） |
| CFD 标的 | `ibkr/trading_hours_report.py` | 新增 `cfd_proxies` 分组（非必需组，不影响整体判定） |
| 会话恢复 | `systemd/ibc-gateway.service` | `StartLimitIntervalSec=0`，重试永不放弃 |
| 会话恢复 | `scripts/ibc-watchdog.sh`、`systemd/ibc-watchdog.{service,timer}` | API 端口保活看门狗 |
| 会话恢复 | `scripts/configure-ibc-secrets.sh` | 默认写入 `AutoRestartTime=03:55 AM` |
| 会话恢复 | `scripts/install-ibc-service.sh` | 安装 watchdog timer；linger 检测警告 |
| 观测 | `scripts/show-ibc-status.sh` | safe_config 增加 `AutoRestartTime/AutoLogoffTime` |
| 测试 | `tests/test_ibkr_collector.py` | CFD 合约构建、归一化、ATM 回退用例 |
| 文档 | `README.md`、`docs/headless-deployment.md`、`.env.example` | 同步以上全部变更 |

## 五、策略/告警层问题（审阅结论，待修复）

对 `alert_engine`、`alert_profile`、`options_map`、`iv_surface`、`market_context`、`human_focus`、`strategy/micopedia` 的量化正确性审阅，按优先级归为四组。

### 5.1 会直接产生错误信号的（已修复）

1. ~~位移告警基准与去重失效~~ **已修复**：movement 告警改为阈值台阶边沿触发（`bucket = |move_bps| // threshold`，仅在首次穿越、bucket 升级或方向翻转时触发，回落后清状态），状态持久化在 `ALERT_MOVEMENT_STATE_PATH`（默认 `data/latest/movement_state.json`），推送失败不落盘、下轮重报。`notifier.alert_key` 改为 `kind|instrument|dedup_group`，不再含 title 浮点；gamma regime 与墙位告警也带上 `dedup_group`。
2. ~~Hyperliquid 基差锚定优先 ES~~ **已修复**：`TRADFI_ANCHOR_IDS` 优先 `index:SPX`；仅剩期货锚时用独立阈值 `HYPERLIQUID_PROXY_FUTURES_BASIS_WARN_BPS=80` / `BLOCK_BPS=150`，gate 输出 `anchor_is_future`。
3. ~~期权图 underlier 可退化到 ES/HL~~ **已修复**：underlier 来源非 `index:SPX` 时标记 `underlier_mismatch`，gamma_state 变为 `unknown_underlier_mismatch`、`nearest_wall(_distance)` 置 None（抑制墙位/gamma 告警），`call_wall`/`put_wall`/`zero_gamma` 保留供展示。
4. ~~DELAYED 报价混入 IV/GEX 聚合~~ **已修复**：`options_map.BAD_QUALITIES` 纳入 `DELAYED`/`DELAYED_FROZEN`；coverage 中 delayed 计数保留。
5. ~~IV 曲面 degraded 时仍发 jump/skew 告警~~ **已修复**：degraded expiry 追加 degraded 告警后 `continue`，跳过 jump/skew/shift 阈值告警。
6. ~~`zero_gamma_transition` 被映射成 pin~~ **已修复**：Micopedia 新增 `transition` gamma 状态，regime 归入 `negative_gamma_trend`，trigger 文案明确"零 gamma 交叉区突破会放大波动，勿按 pin 处理"；codex/agent prompt 同步加了 underlier_mismatch 与 transition 的解读守卫。

### 5.2 定义/命名与真实含义不符的

7. **`expected_move` 实为 ATM straddle 中价**，非 `S·IV·√T`。改名 `atm_straddle_implied_move` 或补标准公式。
8. **GEX 墙假设 dealer 净短、只用 OI**，0DTE 日内 OI 是前日快照。标注 `dealer_short_oi_gex` / `oi_as_of_prior_session`，可加 volume 加权墙。
9. **`put_skew_ratio` 是 wing/ATM 比值**，steepening 阈值 0.08 指比值变化 +8% 而非 vol points；wing 权重可 fallback 到 1.0。并行输出 vol-point 差值并加 OI/质量门槛。
10. **`atm_iv_jump_5m` 实为相邻快照差**，cadence 不保证 5 分钟。按时间差归一化或改名。

### 5.3 严重级别与推送边界

11. **开盘窗口 20 bps（相对昨收）即 critical**（severity 直接继承窗口 priority）：movement severity 应与 priority 解耦，ES/IV 交叉确认才升级。
12. **`broker_unavailable_proxy_watch` 用 crypto perp 数据推 `index:SPX` 非 research 告警**：固定低 severity + `research_only`，写明 "proxy only"。
13. **正 gamma pin 无告警**（只告警负 gamma 加速与 zero-gamma），如属有意需在文档写明。

### 5.4 死配置与文档漂移

14. `AlertWindow` 的 `cadence_seconds`/`spxw_sampling_mode`/`user_unattended`/`primary_sources` 从未被告警引擎消费——要么接线（如 unattended 提高阈值），要么从 schema 删除。
15. `human_focus.time_phase_from_window` 用字符串子串匹配，多个 RTH 窗口落到 `unknown`，Micopedia 的 `midday`/`late` 永远不会被赋值——给 `AlertWindow` 加显式 `time_phase` 字段。
16. Micopedia `inputs.vix` 读了但 regime 分类只用 VIX1D——加 term-structure 规则或删字段。
17. `market_context` 的 ratio 不检查 quote quality，STALE 也算 usable——与告警层口径对齐。
18. Micopedia guidance 与实现漂移（"max-payoff strikes" 未实现、墙来自 GEX 非 OI、sampling 枚举不一致；`TrendSpreadScore` 仅存在于文档）——改写 guidance 或标 backlog。

做得好的（保持）：freshness 失败时倾向抑制而非硬报、IBKR 会话中断/恢复的边沿触发告警、human-visible 与 research 的隔离边界。

## 六、建议的后续优先级

1. ~~策略层 5.1 组修复~~（已完成，见 5.1）。
2. 流式采集器实盘验收：交易时段跑 `scripts/run-ibkr-stream.sh --force --duration-seconds 300`，确认 flush 事件、期权轮换和 latest state 新鲜度。
3. 24h loop 切换：`SPX_SERVICE_ENABLE_IBKR` 保持 false，改用 `spx-spark-ibkr-stream.service`（避免两条 IBKR 写入路径并存）。
4. 策略层 5.2–5.4 组（命名勘误、severity 解耦、死配置接线或删除）。
5. verifier 拆分（contracts/session 与 CLI 解耦）+ 去重样板。
