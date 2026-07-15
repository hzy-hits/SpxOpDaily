# IBKR 会话抢占（Session Preemption）数据一致性加固设计

状态：设计已实现（2026-07-11，commit `51aefde`）；RTH 抢占演练验收仍待。实现要点：`freeze_quotes_on_connectivity_loss`、断线 purge/世代标记、`ibkr_recovery_observations` 防抖。下文保留调查与验收清单，行号可能随后续提交漂移。

## 0. 背景与问题定义

用户会不定期用同一 IBKR 账号在个人电脑登录 TWS / 移动端，抢占服务器上 IB Gateway 的登录会话，导致服务器端断线；稍后 IBC 自动重登。诉求：**抢占 → 断线 → 重登 → 恢复的全过程中，latest 状态与 raw JSONL 不得出现数据冲突或污染**（旧数据假新鲜、重连后旧缓存混入、farm 半恢复期坏数据、断线边界不可追溯）。

部署事实（调查确认）：

- IBC 配置 `/srv/data/spx-spark/runtime/ibc/config.ini`：`ExistingSessionDetectedAction=secondary`、`ReadOnlyApi=yes`、`ReloginAfterSecondFactorAuthenticationTimeout=yes`。即抢占发生时 Gateway 主动退让（自己下线），不与用户的 TWS 争抢；用户登出后 IBC 重登。
- stream collector：`spx-spark-ibkr-stream.service`，client id 172（`config/runtime.yaml:618-620`），flush 间隔 5s（`runtime.yaml:621-623`），重连退避 5→300s（`runtime.yaml:636-641`）。
- latest 状态文件：`{data_root}/latest/state.json`（`src/spx_spark/config.py:951-954`）；raw 落盘 `raw/provider=ibkr/date=…/hour=…/quotes.jsonl`（`src/spx_spark/storage.py:109-120`）。

## 1. 现状行为时间线（抢占 → 断线 → IBC 重登 → farm 恢复 → 订阅重建 → 数据恢复）

### T0 抢占发生

抢占对服务器侧呈现为两种形态（可能先后出现）：

- **A. 上游连接断（socket 仍在）**：IBKR 发 error 1100（"Connectivity between IB and TWS has been lost"）。`_on_error` 将 `tws_connectivity_lost=True` 并递增 `tws_connectivity_loss_sequence`（`src/spx_spark/ibkr/stream_collector.py:658-661`；错误码集合 `src/spx_spark/ibkr/farm_health.py:30-31`）。
- **B. Gateway 整体退出 / API socket 断**：`ExistingSessionDetectedAction=secondary` 下 Gateway 退让下线，`ib.isConnected()` 变 False。
- **C.（并存/交叉场景）竞争会话错误 10197**：另一会话占用行情数据。`has_competing_session_error` 专门识别（`src/spx_spark/ibkr/collector.py:70-71`）。

### T1 断线期间的 flush（现状最脆弱的一段）

session_loop 每 5s 醒来先 `flush()` 再判定动作（`stream_collector.py:2137-2145`）：

- `flush()` 用 `snapshot_rows` 从**冻结的 ticker 对象**读值（`src/spx_spark/ibkr/verifier.py:403-455`），此时值不再更新但仍会被写入。
- 时效标记依赖 `ticker.time`：超过 `ibkr.stale_after_seconds=10`（`runtime.yaml:601-603`）后 `row.stale=True`（`verifier.py:433-438`），adapter 把 LIVE 降为 STALE（`src/spx_spark/ibkr/adapter.py:125-126`）。**因此断线后前 ~10 秒内，冻结数据仍以 quality=live 写入 raw 与 latest。**
- 形态 A（1100）：`subscription_outage_reason` 产生原因文本、`error_count` 提升 → provider 状态 DEGRADED（`stream_collector.py:395-404, 1855-1861`；`src/spx_spark/provider_adapter.py:159-164`），订阅生命周期暂停（`_subscription_lifecycle_blocked`，`stream_collector.py:1599-1608`）。
- 形态 B（socket 断）：flush 携带 `connected=False` → provider 状态 UNAVAILABLE（`provider_adapter.py:156-158`），但**该次 flush 仍把冻结 quotes 写入了 raw 和 latest**（`stream_collector.py:1862-1873`）。随后 `decide_after_flush` 返回 RECONNECT（`stream_collector.py:373-388`）。
- **RECONNECT 路径不调用 `persist_state_only` / 不 purge latest 中的 IBKR quotes**（`stream_collector.py:2211-2213`），对比：CONFLICT_WAIT 会 persist unavailable（其中 UNAVAILABLE 状态触发 `purge_provider_quotes`，`stream_collector.py:443-449, 2184-2191`）、session 异常路径也会（`stream_collector.py:2070-2076`）。断线残留的 IBKR quotes 只能靠读取时 `degrade_stale_quote` 老化成 STALE（`src/spx_spark/storage.py:171-183, 375-407`）。
- 形态 C（10197）：CONFLICT_WAIT → persist unavailable + purge + `defer_market_data_after_conflict(15s)`。defer 期间 `market_data_allowed()` 为 False；若 `account_read_enabled=false`，`connection_required()` 也为 False。外层循环保留 `competing live session owns shared market data (IBKR 10197)` 真实原因，不再覆盖成泛化的 runtime-policy blocked。IBKR 规则决定了 Live/Paper 共享行情不能被两个会话同时消费，因此 15 秒探测用于 Live 退出后快速恢复，不会绕过该限制。

### T2 teardown 与重连退避

- 每个 session 结束都会走 `teardown()`（`stream_collector.py:2077-2078`）。teardown 重置订阅字典、option_plan、slow_cache、qualified 缓存、错误状态等（`stream_collector.py:1935-1978`），并按连接状态选择 `discard_subscriptions`（仅本地清理）或 `cancel_subscriptions`（`stream_collector.py:1936-1942`）。
- **teardown 不清 `self.option_cache`**（对照 `stream_collector.py:1962-1968`：清了 `slow_cache`、`qualified_option_contracts`，唯独没有 `option_cache`）。该缓存 TTL 900s（`stream_collector.py:531`），设计目的是让轮换期权行在非订阅窗口仍进入每次 flush（`stream_collector.py:639-642`）。
- 重连退避：`ReconnectPolicy` 指数退避 min 5s → max 300s（`stream_collector.py:113-125`）；`sleep_until_reconnect` 若端口本来就开着则老实等完退避，端口从关到开会提前唤醒（`stream_collector.py:457-479`）。只有出现过健康 flush 才 `reconnect.reset()`（`stream_collector.py:2079-2080, 2176-2177`）。IBC 重登耗时内 `open_session()` connect 失败 → persist unavailable（"connect failed: …"）+ 退避（`stream_collector.py:2022-2043`）。
- client id 冲突（IBKR 326，例如另一进程占用 172）：无专门识别，走同一 connect_failed 退避路径。10182（重连后需重新请求行情）也未显式处理，但因每次 session 都全量重建订阅，实际影响被掩盖。

### T3 IBC 重登成功、farm 恢复

- 连上后 persist `connected_state()`：DEGRADED + reason "connected; awaiting first flush"（`stream_collector.py:407-416, 2049-2050`）。alert engine 把 DEGRADED 视作**过渡态**、不覆盖已持久化的 session 状态（`src/spx_spark/alert_engine.py:147-152, 820-829`），保证 interrupted → available 的边不被吞掉。
- `probe_data_plane` 只测 `reqCurrentTime` + qualify ES（sec-def farm）（`farm_health.py:140-187`），**不验证行情 farm（usfarm 等）是否全绿**。probe 失败仅标记 farm broken 并继续（`stream_collector.py:2051-2058`）。
- 重登后 IBKR 会推 2119（farm connecting）→ 2104/2106/2158（farm ok），`FarmHealthTracker.observe` 跟踪状态转移（`farm_health.py:100-114, 202-246`）；farm 持续 broken ≥180s 且允许时自动重启 Gateway（`stream_collector.py:2216-2252`；`runtime.yaml:645-653`）。
- **farm 半恢复窗口的数据面**：订阅成功但行情 farm 未全绿时，ticker 常只有 `close`（mdt=1、`ticker.time=None`）。此时 `row.stale=None`（`verifier.py:433-438` 不进分支）→ `classify_quote_quality` 因 mdt=1 **短路直接判 LIVE**（`src/spx_spark/marketdata.py:662-664`）、`row_stale is True` 的降级也不触发（`adapter.py:125-126`）；`snapshot_rows` 首次见到非空指纹就赋 `last_update_at=now`（`verifier.py:449-453`）；`has_price` 把 `close` 算进 `effective_price`（`marketdata.py:298-303`）→ 该行可用（`is_usable`）、且 `quote_use_decision` 的 transport_time 取 `last_update_at` → 判 FRESH（`marketdata.py:411-446`）。**即 close-only 行在半恢复窗口内呈现为"新鲜 live"**。ATM 参考路径对此有专门防护（close_only 判定，`stream_collector.py:322-324`），但通用 best-quote / failover 路径没有。

### T4 订阅重建

- `subscribe_base` 每次全新构建合约与 VerifyRow（`stream_collector.py:781-856`；`verifier.py:262-299`），并用 connectivity-sequence 检测重建中途再次断线（`stream_collector.py:858-875`）——**幂等且有中断保护**。
- SPXW 期权：teardown 已重置 `option_plan` / `OptionReplanController`，重连后 `ensure_option_plan` 依据 ATM 控制器（状态持久化在磁盘，`stream_collector.py:608-612`）重新规划；批量订阅有 rejection/connectivity 双重校验与回滚（`stream_collector.py:1319-1401, 1462-1496`）。SPY plan（`spy_plan_key=None`）同样重建。**幂等。**
- 1101（"data lost"）→ `subscriptions_lost=True` + `subscription_health_failed=True`（`stream_collector.py:663-668`）→ session_loop persist unavailable + purge 并重连（`stream_collector.py:2151-2165`）；1102（"data maintained"）仅清除 lost 标记。**订阅丢失场景已覆盖。**

### T5 数据恢复与首个 flush

- 首个 flush 把 `option_cache` 中**断线前**未过 TTL 的期权行合并进 rows（`stream_collector.py:1834-1854`；`merge_cached_option_rows`，`stream_collector.py:562-568`），以 `received_at=now` 重新写入 raw 与 latest（`provider_adapter.py:181-201`）。
- 健康 flush 后 `session_had_healthy_flush=True` → 退避复位（`stream_collector.py:2176-2177`）。

### T6 告警与 failover 衔接

- alert engine 从 provider state 推导会话状态：reason 含 "competing session"/"10197" → `competing_session`；UNAVAILABLE → `unavailable`；两者构成 `IBKR_INTERRUPTED_SESSION_STATUSES`（`alert_engine.py:147, 772-786`）。
- **抢占走的边**：形态 B/C 产生 `unavailable` 或 `competing_session` → 若 `ibkr_session_is_position_critical()`（live 执行或有 SPXW 持仓，`alert_engine.py:1023-1030`）发 `ibkr_session_interrupted`；否则静默（`alert_engine.py:1057-1075`）。恢复后首个健康 flush → AVAILABLE → position-critical 时发 `ibkr_session_restored`，否则发 `ibkr_session_login` ops 通知（`alert_engine.py:862-935`）。形态 A（1100，DEGRADED）是过渡态：**不发 interrupted**，只有升级为 UNAVAILABLE（掉 socket / 订阅生命周期失败）才发。
- provider failover：Schwab 是 primary（`runtime.yaml:15-17`），IBKR 断线在 SCHWAB_PRIMARY 模式下无模式切换，Schwab 天然"平滑接管"。若正处 IBKR_FALLBACK 被抢占：`ibkr_unhealthy_observations=2` 连续观察后 → BOTH_UNAVAILABLE（`src/spx_spark/provider_failover.py:167-178`；`runtime.yaml:59-61`）。Schwab 恢复回切有 `schwab_recovery_observations=3` 防抖（`provider_failover.py:167-172`）；**IBKR 恢复方向（BOTH_UNAVAILABLE/RECOVERY_PENDING → IBKR_FALLBACK）只需单次 healthy 观察、无确认防抖**（`provider_failover.py:152-155, 185-187`）。
- `quote_max_age_seconds=30` 只作用于 `required_instruments=[index:SPX, future:ES]` 锚（`runtime.yaml:41-49`；`src/spx_spark/provider_failover_controller.py:183-214`）——**不覆盖期权行/greeks/OI**。期权时效由 alert engine 的 `alerts.max_option_quote_age_ms=20000` + `min_option_live_ratio=0.5` 把关（`runtime.yaml:1085-1090`）；greeks 附着在 quote 上随 quote 一起降级；OI 是日频数据、风险低。但 options_map 的 structure 通道允许 stale-but-recent（≤900s）通过（`src/spx_spark/options_map.py:357-375`）。
- `new_entries_allowed` 写入 failover 控制文件并有 fail-closed 读取函数（`provider_failover_controller.py:145-148`；`provider_failover.py:239-260`），**但 src/ 下无生产消费方**（仅测试引用）——抢占期间它不会实际拦截任何入场逻辑。

## 2. 风险清单（按严重度排序）

| # | 场景 | 严重度 | 现状判定 |
|---|------|--------|----------|
| R1 | **重连后旧 option_cache 行混入首个 flush**：teardown 不清 `option_cache`（`stream_collector.py:1935-1978` 无此项），断线前最长 15 分钟的期权行（含 greeks/IV/OI）以 `received_at=now` 重新写入 raw+latest；缓存 VerifyRow 的 `stale` 字段冻结在缓存时刻的 False，`classify_quote_quality` 又因 mdt=1 短路（`marketdata.py:662-664`）→ **raw 记录 quality 永久标 live**。latest 侧靠 `degrade_stale_quote` 按 quote_time 老化兜底（`storage.py:375-407`），但 options_map structure 通道容忍 ≤900s 的 stale 行（`options_map.py:357-375`）→ 断线前的 greeks/walls 可继续影响 GEX/DEX/structure 特征 | 高 | **真缺口** |
| R2 | **raw JSONL 无连接世代标记**：断线-重连边界不可区分（quote 序列化字段见 `marketdata.py:317-347`，无任何 session/generation 字段）。同一 quote_time 的缓存行每 5s 重复落盘属设计行为，但重连边界处无法事后剔除"跨代"记录；received_at 单调（取自 `datetime.now`）故无时间戳回退，但缓存行的 quote_time 相对邻近记录"倒退"，研究回放无法判别 | 高 | **真缺口** |
| R3 | **断线检测前的脏 flush 窗口**：socket 已断但当轮 flush 仍写入冻结 ticker 数据（`stream_collector.py:2137-2145, 1862-1873`）；且断线后前 `stale_after_seconds=10` 秒内冻结值仍标 live。RECONNECT 路径不 purge latest（`stream_collector.py:2211-2213`），残留 IBKR 行仅靠读取端老化 | 中 | **真缺口** |
| R4 | **farm 半恢复期 close-only 假新鲜**：重登后行情 farm 未全绿时，close-only 行呈现 LIVE+FRESH（证据链见 T3），可进入 best_quotes；failover 的 `provider_health` source_at 回退链含 `last_update_at`（`provider_failover_controller.py:190-191`）→ 可能把 IBKR 判 healthy，叠加 IBKR 恢复方向无确认防抖（`provider_failover.py:185-187`）→ 半恢复即切入 IBKR_FALLBACK | 中 | **真缺口** |
| R5 | **quote_max_age_seconds 覆盖面**：只覆盖 SPX/ES 锚。期权行由 alerts 20s 门槛覆盖（已覆盖）；greeks 随 quote 降级（已覆盖）；OI 日频（低风险）。剩余敞口 = options_map structure 通道 900s 容忍（与 R1 叠加时最痛） | 中 | 部分覆盖，敞口与 R1 合并处理 |
| R6 | **1100 半断线期持续写入**：形态 A 下每 5s 仍写冻结行，10s 后正确标 STALE、provider 转 DEGRADED——标记机制可用，但 raw 里会积累大量重复 stale 记录且 provider 未及 UNAVAILABLE、latest 不 purge | 低-中 | 大体已覆盖（标记正确），仅可观测性/存储噪声 |
| R7 | **client id 冲突（326）/ 10182 无专门处理**：走通用退避，行为安全但日志无法区分"IBC 未起来"vs"id 被占" | 低 | 可接受，建议仅加日志分类 |
| R8 | **`new_entries_allowed` 无消费方**：抢占期间失效保护仅停留在控制文件层 | 信息 | 记录，不在本批次范围 |

**已覆盖、无需改动**（调查确认）：重连退避与端口唤醒；订阅重建幂等 + connectivity-sequence 中断保护；1101/10197 处理；account standby purge（`stream_collector.py:452-454`）；alert 的 interrupted/restored/login 三条边与过渡态保护；Schwab 平滑接管与回切防抖。

## 3. 补强设计（最小改动）

### P1 连接世代（connection generation）写入 raw 与 latest —— 对应 R2

- `src/spx_spark/ibkr/stream_collector.py`：`StreamCollector.__init__` 增加 `self.connection_generation: int = 0`；`open_session()` 成功后自增并 `log_event({"event": "session_generation", "generation": n})`。
- `src/spx_spark/marketdata.py`：`Quote` 增加可选字段 `source_session: str | None = None`（默认 None；`to_dict` 仅在非 None 时输出，向后兼容旧 raw/旧 latest 解析），`quote_from_dict` 容忍缺失。
- `src/spx_spark/ibkr/adapter.py`：`quote_from_ibkr_row` / `quotes_from_rows` / `snapshot_from_rows` 增加透传参数 `source_session`；`stream_collector.flush()` 传入 `f"ibkr-stream:{connection_generation}"`。
- 无新 runtime 键（纯附加字段，不需要开关）。
- compaction/研究侧的去重指引（文档性约定，不改代码）：按 `(instrument_id, provider, quote_time, source_session)` 去重可剔除跨代重复缓存行。

### P2 teardown 强制丢弃断线前 ticker 缓存 —— 对应 R1

- `src/spx_spark/ibkr/stream_collector.py` `teardown()`：在重置 `slow_cache` 的同一段加 `self.option_cache = {}`。
- 由于每个 session（正常断线、CONFLICT_WAIT、GATEWAY_RESTART、异常）结束都必经 `run()` 的 `finally: teardown()`（`stream_collector.py:2077-2078`），一处即可保证"重连后首个 flush 前丢弃断线前缓存"。
- 可选加固（推荐一并做）：`update_option_cache` 的缓存值里带上 generation，`merge_cached_option_rows` 只合并当前 generation 的行——防未来出现绕过 teardown 的路径。若嫌重，仅做 teardown 清空即可。

### P3 断线即冻结 IBKR 派生字段并统一 purge —— 对应 R3 / R6

- `src/spx_spark/ibkr/stream_collector.py` `flush()`：开头判断 `not self.ib.isConnected()` 时，跳过 quotes 落盘，改为 `persist_state_only(unavailable_state("IBKR disconnected mid-session", connected=False), …)`（自动触发 latest purge），并直接返回 flush 事件（`quotes=0`）。
- `flush()` 中 `tws_connectivity_lost=True`（1100 半断线）时，把所有行强制 `stale=True` 后再交给 `snapshot_from_rows`（新增辅助函数 `mark_rows_stale(rows)`），消除 10 秒 live 窗口；raw 仍落盘（保留观测证据）但 quality 正确为 stale。
- `session_loop` RECONNECT 分支（`stream_collector.py:2211-2213`）补 `persist_state_only(unavailable_state(...))`，与 CONFLICT_WAIT/异常路径对齐。
- 新 runtime 键：`ibkr_stream.freeze_quotes_on_connectivity_loss`，默认 `true`（description：断线或上游连接丢失时冻结 IBKR 派生字段为 stale 并停止 quote 落盘）。

### P4 farm 未全绿前不解除 stale + close-only 降级 —— 对应 R4

- `src/spx_spark/ibkr/farm_health.py`：`FarmHealthTracker` 增加 `market_data_ready() -> bool`（`self.farms` 中无 BROKEN/CONNECTING 的行情类 farm；无观测记录时视为 ready 以免误伤冷启动）。
- `src/spx_spark/ibkr/adapter.py` `quote_from_ibkr_row`：当 `market_data_type==1` 且 `last/bid/ask` 全空、仅 `close` 有值、`quote_time is None` 时，quality 判 `MarketDataQuality.UNKNOWN`（close-only 不得伪装 live）。这同时封堵 `quote_use_decision` 的 last_update_at 回退链（UNKNOWN 属 bad_feed，failover 直接 reject，`provider_failover_controller.py:193-201`）。
- `src/spx_spark/ibkr/stream_collector.py` `flush()`：`farm_health.market_data_ready()` 为 False 时并入 `outage_reason`（provider 保持 DEGRADED），阻止 alert engine 过早把 session 判成 available。
- `src/spx_spark/provider_failover.py` `advance_failover`：进入 IBKR_FALLBACK 的两条边（RECOVERY_PENDING→、BOTH_UNAVAILABLE→）要求连续 `ibkr_recovery_observations` 次 healthy（新增 `FailoverThresholds.ibkr_recovery_observations` 与状态字段 `ibkr_recovery_streak`）。
- 新 runtime 键：`provider_failover.ibkr_recovery_observations`，默认 `2`（description：连续健康 IBKR 观察次数，达到后才允许切入 IBKR fallback，防 farm 半恢复期过早切换）。

### P5 日志可观测性（可选小项）—— 对应 R7

- `stream_collector.run()` 的 connect_failed 分支：识别异常文本中的 "client id"/"326" 与 "10182" 并在 log_event 加 `error_class` 字段。无行为变化、无新配置。

改动范围汇总：`stream_collector.py`（P1/P2/P3/P5）、`adapter.py`（P1/P4）、`marketdata.py`（P1）、`farm_health.py`（P4）、`provider_failover.py` + `provider_failover_controller.py`（P4）、`config/runtime.yaml`（2 个新键）。不改 alert_engine（其边逻辑依赖 provider 状态正确性，自动受益）。

## 4. 测试用例清单

现有测试基座：`tests/test_stream_collector.py` 广泛使用 `object.__new__(StreamCollector)` + FakeIB 模式（如 :59, :852, :927），错误注入直接调用 `collector._on_error(req_id, code, msg, contract)`。

| 测试函数（建议） | 构造方式 | 覆盖 |
|---|---|---|
| `test_teardown_clears_option_cache` | `object.__new__(StreamCollector)`，预填 `option_cache` 两行旧 VerifyRow，跑 `teardown()`，断言 cache 为空 | P2 |
| `test_first_flush_after_reconnect_excludes_pre_disconnect_option_rows` | 预填 option_cache（quote_time=10 分钟前），模拟 teardown → 重建 base_subs → `flush()`（monkeypatch `persist_provider_snapshot` 捕获 snapshot），断言旧 label 不在 quotes 中 | P2 端到端 |
| `test_flush_skips_quote_persistence_when_socket_disconnected` | FakeIB.isConnected→False，monkeypatch `persist_state_only`/`persist_provider_snapshot`，断言只写状态不写 quotes 且状态为 UNAVAILABLE | P3 |
| `test_flush_marks_all_rows_stale_during_tws_connectivity_loss` | 注入 `_on_error(-1, 1100, …)` 后 `flush()`，捕获 snapshot，断言所有 IBKR quotes quality==stale | P3 |
| `test_session_loop_reconnect_path_persists_unavailable` | 构造 StreamRuntime + FakeCollector（isConnected False、无 competing），跑一轮 `session_loop`，断言 `persist_state_only` 被调用且 reason 含 disconnected | P3 |
| `test_quote_to_dict_round_trips_source_session` | 纯 `marketdata` 单测：带/不带 `source_session` 的 to_dict/from_dict | P1 |
| `test_flush_stamps_connection_generation_on_quotes` | flush 后捕获 snapshot，断言每条 quote 的 `source_session == "ibkr-stream:N"`；`open_session` 两次后 N 递增 | P1 |
| `test_close_only_live_row_downgrades_to_unknown_quality` | `adapter` 单测：VerifyRow(mdt=1, close=6900, 其余 None, ticker_time=None) → quality UNKNOWN | P4 |
| `test_provider_health_rejects_close_only_anchor`（放 `tests/test_provider_failover_controller.py`） | 构造 latest 里 SPX 只有 close-only UNKNOWN 行，断言 IBKR unhealthy | P4 |
| `test_failover_requires_consecutive_ibkr_recovery_observations`（放 `tests/test_provider_failover.py`） | BOTH_UNAVAILABLE 状态下第一次 ibkr_healthy 观察不切换、第二次才进 IBKR_FALLBACK | P4 |
| `test_farm_tracker_market_data_ready_transitions`（放 `tests/test_ibkr_farm_health.py`） | observe(2119) 后 not ready；observe(2104) 后 ready；1100 后 not ready | P4 |
| `test_flush_reports_outage_while_farm_not_ready` | farm_health 注入 2119，flush 后断言 provider 状态 DEGRADED、reason 提及 farm | P4 |
| `test_preemption_session_alert_sequence`（放 `tests/test_alert_engine.py`） | 依次喂 UNAVAILABLE → DEGRADED(过渡) → AVAILABLE 的 provider state 序列 + system_event_state 临时目录，断言恰好产生 interrupted（position-critical 时）与 login/restored 各一次 | 告警衔接验证 |

## 5. 验收标准（可勾选）

### 自动化

- [ ] 上表新测试全部通过；`uv run pytest tests/test_stream_collector.py tests/test_provider_failover.py tests/test_provider_failover_controller.py tests/test_ibkr_farm_health.py tests/test_alert_engine.py` 全绿。
- [ ] `uv run pytest` 全量无回归；`uv run ruff check` 通过。
- [ ] 旧 raw JSONL（无 `source_session` 字段）与旧 latest state 仍能被 `quote_from_dict` / compaction 正常解析。

### 手动演练（周末，非交易时段）

前置：`spx-spark-ibkr-stream.service` 运行中，`journalctl --user -fu spx-spark-ibkr-stream` 观察日志，另开窗口 `watch -n2 jq '.provider_states' /srv/data/spx-spark/data/latest/state.json`。

1. **抢占**：在个人电脑用同一账号登录 TWS（Gateway 应因 `ExistingSessionDetectedAction=secondary` 退让下线）。
   - [ ] 日志出现 `disconnected` 或 `session_error`（或先出现 1100 的 farm_status broken）。
   - [ ] latest 中 IBKR provider 状态 ≤10s 内变为 unavailable（改造后 RECONNECT 路径也 persist）；IBKR quotes 被 purge 或全部 quality=stale，**不存在 quote_time 早于断线时刻但 quality=live 的 IBKR 行**。
   - [ ] 告警：有 SPXW 持仓/live 模式时收到 `ibkr_session_interrupted`（competing 场景文案含 "another IBKR session"）；standby 时静默。
   - [ ] 日志出现 `session_reconnect_backoff`，间隔按 5→10→20→…→300s 递增。
2. **持续抢占期**（保持 TWS 在线 ≥5 分钟）：
   - [ ] raw `provider=ibkr` 当小时文件停止新增 live 记录；Schwab 侧采集不受影响（failover 保持 SCHWAB_PRIMARY，无异常切换）。
3. **释放**：退出个人 TWS，等 IBC 自动重登（或等下一次退避重连）。
   - [ ] 日志顺序：`connected` → `data_plane_probe` → farm_status（connecting→ok）→ `subscribe_base_done` → `session_generation`（generation 递增）→ 首个 `flush`。
   - [ ] 首个 flush 的 quotes 中所有 IBKR 行 `source_session` 为新 generation，且不含断线前 quote_time 的期权行（P2 生效）。
   - [ ] farm 全绿前 latest 中 IBKR 状态保持 DEGRADED（P4），全绿后首个健康 flush 转 AVAILABLE。
   - [ ] 告警：收到 `ibkr_session_login`（standby）或 `ibkr_session_restored`（position-critical），且只有一次。
4. **模拟替代**（无需 TWS 时）：`systemctl --user kill --signal=SIGKILL ibc-gateway.service` 可复现形态 B 的 socket 断线（注意：演练完确认服务被自动拉起）。验收点同 1/3。
5. **raw 世代校验**：`jq -r 'select(.provider=="ibkr") | .source_session' raw/provider=ibkr/date=…/hour=…/quotes.jsonl | sort | uniq -c` 应显示断线前后两个 generation，且同一 generation 内 received_at 单调。

## 6. 结论概览

- 真缺口 4 项：R1（高：重连后旧期权缓存混入）、R2（高：raw 无世代标记）、R3（中：断线脏 flush 窗口 + RECONNECT 不 purge）、R4（中：farm 半恢复 close-only 假新鲜 + IBKR 恢复无防抖）。R5 的敞口并入 R1，R6/R7 低危仅做可观测性，R8 记录不动。
- 现状已覆盖且无需改动：重连退避、订阅重建幂等、1101/10197 处理、alert 三条边、Schwab 平滑接管与回切防抖。
- 实现工作量估计：P2 约 0.5h；P3 约 3h；P1 约 4h；P4 约 6h；P5 约 0.5h；测试与演练约 4h。合计 **约 2 个工作日**（可拆两个 PR：PR1 = P2+P3+P5（收敛污染），PR2 = P1+P4（世代标记与恢复防抖））。
