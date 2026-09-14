# 资金流漏识别恢复（S1/S3、生产故障修复）

## 用户目标与变更边界

7625 附近反弹受阻没有被观察到，必须先恢复行情观察，再补齐资金跟随减弱的表达。本次复用现有 Core、intraday_shock_hot_worker、ProcessLock、run_worker_loop 和流状态文件，不新增服务、timer、数据库、队列、依赖或交易 setup。Bark 不读取、不修改；订单权限仍关闭。

## 根因与运行修复

2026-09-14 10:08:31 ET，原始 Schwab SPX last 到达 7624.07。同期市场采样持续，但 flow minute 状态只有09:35、09:58、10:22，之后10:24开始连续。Core 原先在 market_features 的 `on_analytical_snapshot` 回调中运行 shock；因此策略数据库查询长时间阻塞后，下一次 shock 也无法运行。

`c528ab3f` 已修复索引选错。此次继续将现有 shock runner 作为独立线程任务挂在同一 Core TaskGroup，各自持有已有锁。删除 embedded runner、analytical callback 和 feature 代持 shock 锁的路径，避免双 writer。没有新进程。保留已有循环失败阈值/异常传播，未吞掉关键故障。

## captured_option_flow.v6 的观察表达

旧熊/牛背离离场、冷却、覆盖5%、unknown占比50%、成交滞后5秒、连续20分钟等合同保持。

新增 snapshot.exhaustion，仅 observation_only，不创建 Alert、候选或离场权限。最近5分钟价格较此前15分钟创新高后回落，并且最近5分钟方向净流仍正、较前一个5分钟下降，归一化不平衡至少减少现有0.05尺度时，显示“冲高回落、看涨资金跟随减弱（观察）”；下行镜像同理。两个流窗口都必须通过原质量门。每分钟重算，缺口/质量不足明确 unavailable。

这是窗口资金不确认，不是完整的双波峰模型，不要求或声称已证明7625是Gamma墙，不预测全天顶部。压力位依旧由现有结构上下文解释。新提示复用 Desk Map 的资金流摘要；不改变推送 cadence 或 Bark。

质量不足摘要同时显示覆盖与未分类成交占比，避免把unknown过多误解释成只差覆盖率。

## 原始行情对照

工件位于 `/srv/data/spx-spark/research/flow-exhaustion-recovery-2026-09-14/`。

执行 `uv run python /srv/data/spx-spark/research/flow-exhaustion-recovery-2026-09-14/replay.py YYYY-MM-DD`，基线固定 `c528ab3f`，新版应在本修复提交运行。每个日期冻结09:40–10:21 ET，按received_at <= decision_at重建5秒最新报价，先取最新再检验质量，调用真实 SPX/ES 同步采样与 advance_captured_option_flow。不是历史卡筛选，不读Bark，不写生产状态。

| 日期 | 重建快照行 | 合格周期 | 原离场提醒（旧/新） | 新观察 |
| --- | ---: | ---: | ---: | ---: |
| 2026-09-10 | 77493 | 492 | 2/2 | 0 |
| 2026-09-11 | 76540 | 492 | 1/1 | 2 |
| 2026-09-14 | 74313 | 492 | 1/1 | 0 |

9月14日反事实连续运行时，旧规则在10:04 ET、SPX mid 7616.28 已生成熊背离条件离场提醒（signal_minute 10:03）。价格随后仍升至约7624；因此它是提前风险提示，不能称为精确抓顶。新版本没有人为补出当天的新观察，也没有改变既有事件序列。

三段41分钟窗口不能证明盈利或评价新观察的总体误报率；没有选腿、成交、持仓和净损益回放，也不声称生产当时已经发送提醒。新观察是用户授权的未验证上下文。

## 验收与复杂度

慢策略注入验证真实 Core 编排下 shock 能继续运行；上下行观察、历史缺口、低覆盖、unknown过多不产生错误权限；原有锁冲突和运行循环测试保留。针对性68项，全量Python3449项（两个已有弃用警告）；最终诊断字段/摘要修改后48项通过。Rust fmt/clippy/workspace测试、Ruff、Import Linter、模块预算与diff检查通过。

生产文件新增/删除0/0，修改5个，净减少80行；依赖、配置键、进程、服务/timer、数据库/表均0增减；Core新增一个线程任务，复用已有shock循环。删除embedded runner及其callback/双锁路径和仅断言旧内部调用顺序的测试。
