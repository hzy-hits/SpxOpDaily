# 2026-09-08 宏观日历路径错配

Phase 8 / S1（现有 Worker 与策略事实出口的生产修复）。

生产 operational root 为 `/srv/data/spx-spark`，行情 root 为
`/srv/data/spx-spark/data`。Worker 曾以 AppSettings.data_root 更新日历，
Core、订单地图与其他行情消费者则以 StorageSettings.data_root 读取日历。
因此刷新成功并不意味着决策端日历有效。

09:30–11:34 ET 的 939 条策略决策均记录 macro unavailable；546 个周期有候选，
首要阻断为 macro_entry_not_authorized。这是生产链路诊断，不是策略收益回测，
候选评估次数不代表独立交易次数，也不能据此断言移除宏观门后应成交或获利。

修复使 Worker 使用与读端相同的 StorageSettings.data_root，不修改 operational
数据库、Huey 队列位置或交易阈值。桌图根据已有事件状态区分日历不可用与
事件前窗口；无上下文时仅表示宏观未授权，不虚构事件。NO_TRADE 文案不再断言空仓。
旧 operational root 中的日历不再由此任务更新，保留旧文件供事故核查。

回归验收使用不同的 operational/market root，通过真实 Worker 任务写入、真实
macro_event_state 读取：刷新前未知禁止，刷新后覆盖有效允许，进入事件前窗口
再次禁止。网络来源使用显式替身，不声称模拟行情或实盘收益。

部署沿用 scripts/install-spx-spark-services.sh，重启受影响 Core/Worker。
应立即调用修复后的刷新任务，并检查真实最终决策的 event.state、entry_allowed、
runtime_git_sha 和剩余候选门控；服务 active 本身不构成验收。
Bark 配置和投递逻辑不变，不通过测试推送验收。
