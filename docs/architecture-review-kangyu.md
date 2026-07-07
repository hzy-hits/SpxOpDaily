# SPX Spark 架构逻辑梳理与问题指正

- 日期:2026-07-05
- 作者:kangyu(基于 master 分支代码通读)
- 范围:`src/spx_spark/` 全部模块、`scripts/`、`systemd/`、README

---

## 一、这个系统是什么

SPX Spark 是一个 **SPX / SPXW 0DTE 期权的准实时行情监控与告警研究系统**,跑在一台
headless Ubuntu 服务器上做 24 小时值守。它刻意收窄为"只读行情"(market-data only):
不下单、不查账户、不存交易凭证,IBKR 侧保持 Read-Only API。

最终的人机交互产物只有两个:

1. **盘中推送**:经过多层过滤后,通过 OpenClaw 微信推给人的"需要看盘"提醒(只允许提 SPX / SPXW / ES)。
2. **盘后复盘**:`post_close_review.py` 生成的每日 SPX 期权复盘 Markdown/JSON。

## 二、分层架构

代码实际形成了清晰的六层管线,层与层之间用**文件**(JSONL + JSON)解耦,而不是进程内消息:

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. 采集层 (Providers)                                            │
│    ibkr/collector  schwab/verifier  hyperliquid/collector        │
│    polymarket/collector  mock_collector                          │
└──────────────────────────┬──────────────────────────────────────┘
                           │ 各自的 adapter 归一化
┌──────────────────────────▼──────────────────────────────────────┐
│ 2. 归一化模型层 (marketdata.py / provider_adapter.py)            │
│    Quote / InstrumentId / OptionGreeks / ProviderState           │
│    ProviderSnapshot → canonical_id + MarketDataQuality           │
└──────────────────────────┬──────────────────────────────────────┘
                           │ persist_provider_snapshot()
┌──────────────────────────▼──────────────────────────────────────┐
│ 3. 存储层 (storage.py)                                           │
│    raw:  data/raw/provider=X/date=Y/hour=H/quotes.jsonl (追加)   │
│    state: data/latest/state.json (原子替换, 含 best_quotes)      │
└──────────────────────────┬──────────────────────────────────────┘
                           │ LatestStateStore.load()
┌──────────────────────────▼──────────────────────────────────────┐
│ 4. 特征层                                                        │
│    options_map.py   ATM/straddle/expected move/GEX/墙位          │
│    iv_surface.py    5 分钟 IV 曲面快照 + 1 小时历史摘要           │
│    market_context.py Hyperliquid proxy 质量门等                  │
│    strategy/micopedia.py 观察性 regime/检查清单                  │
└──────────────────────────┬──────────────────────────────────────┘
┌──────────────────────────▼──────────────────────────────────────┐
│ 5. 告警层                                                        │
│    alert_profile.py  按 NY/北京时间划分监控窗口→优先级/节奏      │
│    alert_engine.py   数据健康 + 价格异动 + gamma/IV/墙位告警     │
│    human_focus.py    只保留 SPX/SPXW/ES 的人类可见上下文         │
└──────────────────────────┬──────────────────────────────────────┘
┌──────────────────────────▼──────────────────────────────────────┐
│ 6. 通知/运维层                                                   │
│    notifier.py      Codex 判断门 + 范围门 + OpenClaw 微信         │
│    service_loop.py  24h 单进程调度器 (subprocess 派发)           │
│    runtime_mode.py  带 TTL 的本地开关 (ibkr-on/protected)        │
│    maintenance.py / systemd timers / post_close_review           │
└─────────────────────────────────────────────────────────────────┘
```

### 关键设计决策(读代码得出的"为什么")

**1. 一切皆文件,单机自包含。**
采集器、特征计算、告警引擎都是独立 CLI(`pyproject.toml` 注册 console scripts),
通过 `data/latest/*.json` 交换状态。好处是每一环都能单独跑、单独测(23 个测试文件
基本每个模块一个);代价是没有锁、没有事务(见问题 §4.6)。

**2. 统一 Quote 模型 + 质量标签,而不是 if-provider 分支。**
`marketdata.py` 把 IBKR/Schwab/Hyperliquid 的行情统一成 `Quote`,附带
`MarketDataQuality`(live/frozen/delayed/stale/…)。下游选价用
`choose_best_quote`:质量分 → provider 优先级 → 新鲜度 → 有无 mid
(marketdata.py:493-504)。这是整个 repo 最核心的抽象,README 也明确要求下游
只比较 quality/priority。

**3. 数据不好时宁可"承认不知道",不硬算。**
`options_map.py` 没有 OI 时输出 `unknown_no_open_interest` 而不是假墙位;
`option_coverage_is_fresh` 失败时直接压制 wall/gamma 告警,改发 freshness 告警
(alert_engine.py:244-277)。这是全 repo 一以贯之的原则,值得保持。

**4. 人类可见面收窄成"白名单 + 黑名单"双保险。**
推送链路有四道闸:
- `is_human_visible_alert`:instrument 前缀白名单(notifier.py:143-152);
- Codex 判断门:输出必须以 `需要看盘:` 开头才外发(notifier.py:333-338);
- `codex_message_respects_human_scope`:VIX/SPY/hyperliquid 等词黑名单(notifier.py:341-349);
- 冷却 + 最低严重度过滤(notifier.py:185-213)。

**5. 时间即配置。**
`alert_profile.py` 把一天切成十几个窗口(盘前/开盘/午盘/收盘/亚洲时段…),每个窗口
决定优先级、采样模式、告警节奏。`MOVE_THRESHOLDS_BPS` 反向映射:窗口优先级越高,
异动阈值越低(越敏感)。

**6. 运行状态与永久配置分离。**
`runtime_mode.py` 用带 TTL 的 `runtime/mode.json` 让 agent 临时允许/禁止 IBKR
连接("手机上正在交易,别抢会话"),过期自动失效,不动 `.env`。

## 三、值得肯定的地方

- 测试覆盖面广(`tests/` 23 个文件,和 `src` 模块几乎一一对应)。
- 到处都是 frozen dataclass,状态写入统一走 tmp 文件 + `replace()` 原子替换。
- 质量门思想贯穿始终:freshness gate、proxy gate、scope gate、delivery cue gate。
- 安全边界清楚:read-only API、scan-secrets 脚本、VNC 只绑 localhost、
  subprocess 全部走 list 形式(无 shell 注入面)。

## 四、问题指正

按严重度排序;"P1"是会产生错误行为的正确性问题,"P2"是设计/健壮性隐患,"P3"是小问题。

### P1-1 告警冷却对"数值型标题"完全失效 → 推送风暴风险

冷却去重的 key 是 `kind|instrument_id|title`(notifier.py:133-140),但大量告警的
title 里嵌了实时数值:

- `movement_alert`:`f"{id} up {move_bps:.1f} bps from close"`(alert_engine.py:190)
- 墙位接近:`f"SPX near SPXW wall {wall:.0f} ({dist:+.1f} pts)"`(alert_engine.py:310-313)
- ATM IV jump / skew steepening / surface shift 同理(alert_engine.py:646/661/679)

价格每 30 秒都在变,`move_bps` 每轮都不同 → key 每轮都是新的 →
`cooldown_seconds=300` 形同虚设。只要一根持续的趋势行情,就会每个告警周期都推一次。
目前没炸只是因为默认 `min_severity=high` + Codex 门挡着。

**建议**:key 只用 `kind|instrument_id`(必要时加方向或分桶后的阈值档位),
数值放 detail,不进 key。

### P1-2 没有时间戳的报价永远不会降级为 STALE

`degrade_stale_quote` 依赖 `quote_age_ms`,而后者只看 `quote_time or trade_time`
(marketdata.py:282-287);两者都缺时返回 `None`,函数原样返回
(storage.py:239-247)。IBKR 行如果 `ticker_time` 为空但 `market_data_type=1`,
会被判为 LIVE(marketdata.py:788-794),然后**在 latest state 里以 LIVE 身份永生**。

后果:采集器停了之后,昨天的价格仍以 LIVE 参与 `choose_best_quote` 和
`movement_alert`,可能基于隔夜旧价发"价格异动"告警。

**建议**:`quote_age_ms` 无源时间戳时退回用 `received_at` 和 `as_of` 之差;
或者在 `degrade_stale_quote` 里对 `quote_time is None` 的 quote 用
`received_at` 计龄。

### P1-3 latest state 只增不减:过期 SPXW 期权永久残留

`LatestStateStore.update` 合并新旧 quotes,只有 `replace_providers` 指定的
provider 会被整体替换(storage.py:157-198),而全仓库只有 **Polymarket** 设置了
`replace_provider_quotes: True`(polymarket/collector.py:540,经由
provider_adapter.py:186-188)。IBKR/Schwab/Hyperliquid/mock 都是纯合并。

对 0DTE 系统这是结构性问题:**SPXW 的 expiry 每天换**,昨天的几十上百条期权
quote 会永远留在 `state.json` 里。连锁反应:

1. `state.json` 逐日膨胀(每天净增一个到期日的期权行);
2. `build_options_map` 按 expiry 分组时会给每个历史到期日都建一张图
   (options_map.py:406-424),STALE 行情过不了 freshness gate →
   **每个旧 expiry 每轮都产生一条 `option_quote_freshness_degraded` 告警**
   (alert_engine.py:285-287),而它的 instrument_id 前缀 `option_map:SPXW`
   恰好在人类可见白名单里;
3. `maintenance.py` 只清 raw/logs,不清 latest state,没有兜底。

**建议**:IBKR/Schwab 期权采集也走 replace 语义;或在 `update`/`load` 时按
`instrument.expiry < today(NY)` 淘汰期权行;至少 `build_options_map` 应过滤
已到期的 expiry。

### P1-4 IV 归一化的 `>3.0 → /100` 启发式会掐坏 0DTE 翼部

`normalize_implied_vol`:大于 3.0 一律当成百分数除以 100(marketdata.py:830-836)。
但这个系统的主战场是 **0DTE**——临近收盘的深 OTM 翼常见 300%+ 甚至 500% 的年化 IV,
3.5 的真实 IV 会被悄悄改成 0.035,直接污染 `put_wing_iv`/`call_skew_ratio` 和
IV surface 的 smile 曲率,而且无声无警告。

**建议**:按 provider 约定处理(Schwab 的 `volatility` 字段本来就是百分数,IBKR
model IV 本来就是小数),而不是按数值猜;至少对 0DTE 放宽阈值并在触发换算时打 warning。

### P1-5 `StrikeGex` 的 OI 字段可能为 None,违反自身类型契约

`build_gex_by_strike` 里 `call_open_interest=finite_float(call.open_interest) if call else 0.0`
(options_map.py:245-246):`finite_float` 可返回 `None`,而字段声明是 `float`。
当前恰好只被 `asdict` 序列化所以没炸,但任何下游做算术就会 `TypeError`。
应为 `finite_float(...) or 0.0`。

### P2-1 latest state 并发写没有锁

`update` 是读-改-写(storage.py:157-198),`replace()` 只保证单次写原子,不保证
读改写序列原子。24h service loop 内部是串行的,但手工跑
`scripts/run-ibkr-collector.sh` / systemd timer / service loop 同时在跑时,
两个进程交错更新会**丢掉一方的 quotes**(后写者以自己 load 到的旧底板覆盖)。
README 的验收流程恰恰鼓励 service 跑着的时候手工 `--force` 采一把。

**建议**:`fcntl.flock` 一个 `state.json.lock`,或约定单写者(所有写都经 service loop)。

### P2-2 service loop 单线程串行,一个慢任务饿死其它任务

`run_loop` 在同一线程里依次执行到期任务(service_loop.py:347-368)。任务超时上限
120s(`SPX_SERVICE_TASK_TIMEOUT_SECONDS`),即 IBKR 连接挂起时,alert engine
(30s 周期)最多被拖 2 分钟——对一个"盘中告警"系统,这正好发生在最需要告警的
网络异常时刻。另外 `build_tasks` 给所有任务都配了 `command`(subprocess 路径),
`ServiceTask.fn` + SIGALRM 的进程内路径实际是死代码,徒增一套超时语义。

**建议**:至少把 alert_engine 和采集任务分池(线程/进程各一);删掉 fn 路径或真正用它。

### P2-3 env 解析逻辑三处重复、语义互相矛盾

- `config._env_bool`:非法值**抛异常**(config.py:33-42)
- `service_loop.env_bool`:非法值**静默 False**(service_loop.py:77-81)
- `alert_engine.env_bool`:同上但集合又不含 `"y"`(alert_engine.py:127-131)

同一个字符串 `"On"` 在三处会得到 True / True / False 三种…两种结果。
**建议**:全部收敛到 `config.py` 一处。

### P2-4 IBKR 路径根本采不到 open interest → GEX 长期废在半残状态

`quote_from_ibkr_row` 没有映射 open_interest(marketdata.py:554-573 无此字段),
所以 IBKR 采集的 SPXW 期权永远 `gex_quality=no_open_interest_gex`,墙位/零 gamma
全靠 Schwab 链兜底。IBKR 其实有 generic tick 101(OptionOpenInterest)可请求。
README 把"没 OI 就不出墙位"写成设计原则没问题,但主数据源天然采不到 OI 应当在
文档里写明,或补上 tick 101。

### P2-5 通知关闭时,IBKR 会话中断告警会无限重复

`alert_engine.run`:只有"没有 pending 系统事件"或"本轮真的发出去了"才持久化
系统事件状态(alert_engine.py:785-794)。当 `ALERT_NOTIFY_ENABLED=false`(默认!)
且 IBKR 会话被抢占时,`ibkr_session_interrupted` 每 30 秒重新生成一次,
`previous_status` 永远不更新——日志里全是重复的 high 告警。设计意图大概是
"没送达就重试",但应该区分"通知未启用"和"通知失败"。

### P2-6 ES/SPY 直接顶替 SPX 现货价,忽略基差

`UNDERLIER_CANDIDATES` 把 ES(×1.0)、SPY(×10)当作 SPX 等价参考
(options_map.py:16-22)。ES 对现货常有 10-30 点基差,SPY×10 还有分红/费用偏移;
用它们算 ATM strike、`zero_gamma_distance_points`、墙位距离,在夜盘/数据降级时
会系统性偏移一到两个 strike(step=5)。当前 `underlier.source` 有记录但下游
没有任何基差修正或"参考价降级"告警。

**建议**:非 `index:SPX` 来源时在 options_map 的 warnings 里注明,并对
`zero_gamma_transition`(阈值 0.5%,≈37 点)这类精细判定降权。

### P3(小问题清单)

1. **`Quote.mid` 拒绝 `bid==0`**(marketdata.py:243-249):深 OTM 0DTE bid=0 很常见,
   这些行没有 mid → `effective_price` 落到 last/close,wing IV 覆盖率被压低。
   0 bid 是有信息量的合法报价,建议区分"无报价"和"bid=0"。
2. **`is_time_in_window` 中 `start == stop` 返回 True(永远开)**(config.py:114-119),
   而 IBKR 调度默认恰好是 `00:00-00:00` → 默认"永远在窗口内"。语义容易被误读成
   "零长度窗口=永不",至少该加注释/文档。
3. **`QUALITY_RANK` 里 FROZEN(85) > DELAYED(75)**(marketdata.py:56-66):
   盘中"冻结的收盘价"排在"延迟 15 分钟的真实行情"前面,选价时可能选到更旧的价。
4. **`load_dotenv` 相对 CWD**(config.py:55-65):所有 CLI 都要求从 repo 根目录跑,
   systemd unit 必须记得配 `WorkingDirectory`,这是隐性契约。
5. **`codex_message_requests_delivery` 正/负 cue 判定不对称**(notifier.py:333-338):
   正向 cue 只看第一行,负向 cue 匹配全文——一条"需要看盘:…(末尾提到'其余不需要推送')"
   会被整体拦下。按设计"负向优先"合理,但全文匹配容易误伤。
6. **`_env_csv` 把所有符号强制大写**(config.py:45-47):对指数符号没问题,
   但和 `_env_csv_preserve` 的分工全靠调用方记忆,踩错一次就是"符号找不到"。
7. **`parse_timestamp` 的秒/毫秒启发式**(marketdata.py:844-850)与
   `normalize_implied_vol` 同属"按数值猜单位",建议集中管理并打日志。
8. **`run_codex_exec` 把完整 prompt 作为 argv 传给 codex**(notifier.py:394-413):
   `ps` 里可见完整市场上下文;已经开了临时文件读输出,不如 prompt 也走文件/stdin。

## 五、如果只修三件事

1. **修告警冷却 key**(P1-1)——这是唯一直接面向"半夜把人吵醒"的 bug,改动量最小。
2. **给期权行加过期淘汰 + 无时间戳降级**(P1-2、P1-3 一起)——两者都在 storage 层,
   一次改造能同时解决 state 膨胀、幽灵 LIVE、旧 expiry 告警噪音三个症状。
3. **收敛 env 解析 + 加 state 文件锁**(P2-1、P2-3)——为后面把 IBKR 真正接入
   24h service 扫清运维雷区。

---

# 第二章:架构与策略深度评估(vs 业界一般实践)

- 日期:2026-07-06
- 范围:第一章是代码正确性层面;本章从三个更高的层面评估——架构模式缺陷、
  策略/金融逻辑缺陷、与业界一般实践的对照。

**TLDR:架构上最大的偏离是"快照轮询 + 单 JSON 文件总线",业界同类系统都是
流式订阅 + 时序存储;策略上最实质的问题是 GEX 用前日 OI 对 0DTE 结构性失真、
expected move 公式偏大约 18%、告警阈值全是静态常数不随波动率环境归一;
最缺的一环是没有"信号→结果"的验证闭环——这是个研究系统,却无法回答
"我的告警有没有信息量"。**

## 六、架构层缺陷

### A1. 快照轮询模型,而非流式订阅 —— 与 0DTE 的时间尺度不匹配

`ibkr/collector.py` 每轮的生命周期是:连接 → qualify → 订阅 →
`ib.sleep(quote_wait_seconds)` → 取一帧 → 取消订阅 → 断开。后果:

- 看到的市场是**每 N 秒一帧的抽样**,帧间的所有变动(0DTE gamma squeeze
  恰恰发生在秒级)全部丢失;
- 每轮对几百个期权合约反复 subscribe/cancel,容易撞 IBKR pacing limit,
  还平白增加每帧延迟;
- 5 分钟一次的 IV surface 快照做 "5m 差分",分辨率对尾盘 0DTE 太粗。

**业界一般实践**:长连接流式订阅(`ib_async` 本身就是事件驱动设计),tick 进内存,
由 bar builder 聚合成 1s/1m bar 再落盘。收益不只是延迟——有了分钟 bar 才有
VWAP、opening range、realized vol,而这些正是策略文本(micopedia 的 map focus)
声称要看、系统却根本算不出来的东西(见 S7)。

### A2. 单 JSON 文件当消息总线,承载了超出其能力的职责

`data/latest/state.json` 是全量读-改-写的共享状态,同时充当采集器的 sink、
特征层和告警引擎的 source、跨进程通信媒介。具体病症第一章已列
(无锁 P2-1、只增不减 P1-3、幽灵 LIVE P1-2);架构层结论是:**这个文件被要求
同时做"消息队列 + 数据库 + 缓存"三件事,而它三件都做不好**。

**业界一般实践**:不需要上 Kafka/kdb+ 那么重,单机自包含哲学下的标准答案是
**SQLite(WAL 模式)**——一个文件就换来事务、并发读写、按 expiry 删除过期行、
schema 管理,`scripts/` 生态完全不用变。raw JSONL append-only 这部分是对的,保留。

### A3. 自制单线程调度器,却已经有 systemd

`service_loop.py` 在一个线程里串行 subprocess 派发所有任务(慢任务饿死告警任务
的问题见 P2-2)。更值得指出的是这个自制调度器**和已有的 systemd 基础设施是
重复建设**:repo 里已经有 7 个 unit 文件。业界对单机多任务的标准做法就是每个
collector/engine 一个常驻 systemd service(带 `Restart=always`、`WatchdogSec`),
互不阻塞,journald 天然分流日志。自己写调度器承担了进程管理的全部复杂度,
却没拿到任何 systemd 给不了的东西。

### A4. 系统健康告警与市场信号共用一条通道

`required_data_missing`、`ibkr_session_interrupted` 和 `price_move_from_close`、
`option_gamma_regime` 都是同一个 `Alert` 类型,走同一条微信推送链。业界惯例是
严格分流:**ops 信号**(数据断了、进程挂了)走运维通道(Grafana/Uptime/邮件),
**市场信号**走交易通道。混在一起的实际后果:数据抖动的日子里,微信里"系统坏了"
和"市场动了"交替出现,真正的市场信号被淹没——而且 Codex 门是按"要不要看盘"
的措辞裁决的,让它裁决"IBKR 会话被抢占要不要通知"是错位的。

### A5. LLM 作为推送链路的强依赖门

Codex 判断门(输出必须以 `需要看盘:` 开头才外发,notifier.py:333-338)是个
有意思的设计,但把一个**非确定性组件放在告警链的关键路径上**:模型换版本、
输出格式漂移、API 超时,都表现为**静默漏报**——最危险的失败模式。业界如果用
LLM,一般放在"摘要/润色"位置(信号已决定要发,LLM 只组织语言),门本身用
确定性规则。至少应给 Codex 门加 fail-open 保底:critical 级别信号在 Codex
超时/异常时直接裸发。

### A6. 没有 replay/回测路径 —— 研究系统缺了研究闭环

raw JSONL 都存了,但没有任何工具把历史 raw 重放给特征层和告警引擎。后果是
所有阈值(`ATM_IV_JUMP=0.03`、`SKEW_STEEPENING=0.08`、move 20-85bps)都是
拍脑袋常数,**无法用自己积累的数据校准自己**。业界 record→replay→calibrate
是标配,而且这个架构(文件解耦、纯函数特征层)其实非常适合做 replay——
只差一个"从 raw 目录重建 LatestState 序列"的驱动器。这是投入产出比最高的缺失件。

## 七、策略层缺陷

### S1. GEX 对 0DTE 结构性失真:OI 是昨天的

`signed_gex`(options_map.py:252-257)用 open interest 加权。但 **OI 由 OCC
每日结算后更新,盘中不变**——而 0DTE 成交量里当天开仓占绝对大头(这正是 0DTE
的定义性特征)。也就是说:即使 OI 数据齐全,算的也是"昨天收盘时留下的仓位"的
gamma 地图,当天真正主导对冲流的新开仓完全不在里面。README 诚实地写了
"没 OI 不出墙位",但没承认更深的这层:**有 OI 也是滞后一天的 OI**。

**业界修正**:对 0DTE 用**当日成交量**替代/补充 OI 做加权(volume-based GEX),
更进一步是 trade-level 的 aggressor 分类估计客户方向(SpotGamma/MenthorQ 这类
商业服务的做法)。最低成本的改进:`gex_quality` 加一档 `stale_oi_gex`,
并在 0DTE expiry 上默认用 volume 加权。

### S2. 零 gamma 的求法在概念上就不对

`nearest_zero`(options_map.py:260-275)在 **strike 维度**上找相邻 strike 间
net_gex 的符号翻转点。但"零 gamma/gamma flip"的标准定义是:**把整条链在不同
假想 spot 价位下重估,总 dealer gamma 作为 spot 的函数过零的位置**。strike
剖面上的符号翻转和"spot 移到哪里对冲方向反转"不是同一个量——前者只是后者的
粗糙代理,在 gamma 分布不对称时会差出几十个点。`zero_gamma_transition` 判定
(距离 <0.5%)又建立在这个量之上,等于用一个错的量做精细判断。

### S3. Expected move 直接等于 straddle,系统性高估约 18%

`expected_move_points = straddle`(options_map.py:382)。ATM straddle 价格
≈ 1.25σ,业界通行近似是 **EM(1σ) ≈ 0.85 × straddle**(或直接 S·IV·√T)。
所有下游消费(human focus、Codex prompt、复盘)都在用一个偏大 18% 的
expected move。另外没有日内时间衰减处理:上午 10 点的 straddle 定价的是
"余下 6 小时",下午 3 点定价的是"余下 1 小时",两者作为 "expected move"
给人看时语义完全不同。

### S4. Skew 度量用固定 moneyness 带 + 比值,跨波动率环境不可比

- Wing 定义是固定 moneyness(put 0.97-0.995,call 1.005-1.03,
  options_map.py:334-337)。VIX1D=25 的日子,3% moneyness 大约在 25-delta 附近;
  VIX1D=10 的日子,同一个 3% 已经是 5-delta 的深翼。**同一个 "skew" 数字在
  不同 vol 环境测的是曲线上完全不同的位置**。
- `skew_ratio = wing_iv / atm_iv` 用比值,低 IV 时天然放大。
- 告警阈值 `SKEW_STEEPENING_5M = 0.08` 是常数 → 告警的实际敏感度随 vol regime
  漂移,低波日狂响、高波日失聪。

**业界惯例**:delta 标准化(25Δ risk reversal / butterfly),skew 用 vol point
差值不用比值。这不是学术洁癖——它直接决定 5 分钟差分告警是否可比。

### S5. "ATM IV 5m 跳变"告警会被 ATM 换档污染

`atm_iv` 取"离 spot 最近的 strike"的 call/put IV 均值。spot 每穿过两个 strike
的中点,ATM 就换到下一个 strike——**ATM IV 随之不连续跳变**,与真实 vol 变化
无关(sticky-strike 伪影)。尾盘 0DTE 相邻 strike 的 IV 差完全可以超过
`ATM_IV_JUMP_THRESHOLD=0.03`,即 spot 的每次换档都可能触发一条假 "IV jump"
告警。业界做法:先把曲线插值到固定 moneyness/delta 网格,再做时序差分。
同理 `smile_curvature`(3 个点:两翼均值减 ATM)也继承这个问题。

### S6. 5m 差分不校验时间间隔

`build_expiry_surface` 拿 `previous = load_latest_snapshot(...)` 做差分,
**从不检查 previous 的 as_of 距今多久**。服务停 2 小时后重启,第一帧的
"iv_surface_shift_5m" 实际是 2 小时的变化量,几乎必然越过 0.03 阈值 →
每次重启都白送一波假告警。差分前应校验 `as_of` 间隔在 [4, 7] 分钟窗口内,
否则输出 None。

### S7. 策略文本与数据能力脱节;micopedia 与特征层没打通

1. micopedia 的 map focus 说要看 "opening range, VWAP, realized-vs-implied
   range"——但系统只存快照,**没有任何分钟 bar,VWAP/opening range 根本算不出来**。
   checklist 让人看一个系统提供不了的东西。
2. 更直接的集成缺陷:options_map 里明明算出了 `gamma_state`,而
   `inputs_from_latest_state`(micopedia.py:367-411)的 `gamma_state` 要靠人工
   命令行传 `--gamma-state`,默认永远 `unknown`。同理 `has_option_chain` 也是
   人工 flag,而 latest state 里有没有链是可以直接查的。**自动算出的量退化成了
   人工输入**,micopedia 实际上是个与实时数据断连的文本模板引擎。

### S8. movement 告警只有一个视角:距昨收 bps,且阈值不随 vol 归一

- 唯一基准是 `quote.close`(昨收)。0DTE 交易的实际参照系是开盘价、当日高低、
  关键位——距昨收 +30bps 在跳空 +100bps 开盘的日子里是"回落",告警却说
  "up 30bps"。
- `MOVE_THRESHOLDS_BPS` 是静态常数(20-85bps)。VIX=12 和 VIX=30 的日子,
  30bps 的罕见程度差一个数量级。**讽刺的是系统自己算出了 expected_move,
  告警却不用它**。业界表达方式是"移动超过当日 expected move 的 X%"或 ATR
  分数——自适应且跨环境语义一致。

### S9. 没有信号验证闭环(策略层视角)

micopedia 的 `next_checks` 里写着 "record MFE/MAE validation fields"、
post_close_review 做叙述性复盘,但**没有任何代码把"发过的告警"和"之后的价格
路径"关联起来**。业界哪怕最简陋的做法:每条告警落库,T+5m/30m/收盘时回填价格,
周度统计各 kind 的命中率/噪声率,退化的 kind 自动升阈值或停用。没有这个闭环,
阈值调优永远是玄学,而且无法回答系统存在的根本问题:"这些告警值不值得吵醒我"。

## 八、业界对照速览

| 维度 | 本 repo | 业界一般实践 |
|---|---|---|
| 数据获取 | 连接→快照→断开轮询 | 长连接流式订阅 + bar 聚合 |
| 状态存储 | 全量 JSON 读改写 | SQLite/时序库 + append log |
| 任务调度 | 自制单线程 subprocess 循环 | 每任务独立常驻进程(systemd 管) |
| GEX | 前日 OI × gamma,call+/put− | 0DTE 用当日 volume 修正,flow-based 方向估计 |
| 零 gamma | strike 剖面符号翻转 | spot 扫描重估总 gamma 求根 |
| Expected move | = straddle | ≈ 0.85×straddle 或 S·IV·√T |
| Skew | 固定 moneyness、比值 | 25Δ RR/BF、vol point 差值 |
| 告警阈值 | 静态常数 | 按 expected move/realized vol 归一 |
| 阈值校准 | 无 | record→replay→回测 |
| 信号质量追踪 | 无 | 告警 outcome 落库 + 命中率统计 |
| Ops vs 信号 | 同一通道 | 严格分流 |

## 九、按投入产出排的改进优先级

1. **建 replay 驱动器 + 告警 outcome 落库**(A6+S9)——一次投入,之后所有阈值
   问题(S4/S5/S8)都从"拍脑袋"变成"可测量",这是把系统从"告警脚本集合"
   升级为"研究系统"的分水岭。
2. **0DTE GEX 改 volume 加权 + 承认 OI 滞后**(S1)——墙位/gamma state 是这个
   系统的核心卖点,现在的版本在它最想服务的 0DTE 场景恰恰最失真。
3. **IV 特征先插值到固定 moneyness 再差分 + 差分校验时间间隔**(S5+S6)——
   消掉两类必然发生的假告警,改动局部。
4. **movement 阈值改用 expected move 归一**(S8)——数据已经在手,只是没接上。
5. **采集改流式长连接**(A1)——收益最大但工程量也最大,可以放最后;做了之后
   VWAP/opening range/realized vol 才有原料,S7 的脱节才可能弥合。
