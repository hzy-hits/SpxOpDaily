# GTH 因果路径 Rank

GTH 方向观察使用 ES 实时报价的 5 秒确定性采样桶。它是近 tick 路径，不是交易所逐笔成交；SPXW NBBO、wall 和 skew 仍是独立的期权结构与执行证据。

## 窗口和 readiness

- 15 分钟和 60 分钟窗口分别满窗后启用，不再共用 1 小时全局暖机。
- 同一个 5 秒桶只保留一个样本，较新的报价替换桶内旧值，不能用更快轮询制造样本量。
- 小幅内部缺口不重置整个窗口；报告同时披露覆盖率、最大样本间隔和采样质量。
- 极稀疏窗口仍可读，但不参与方向分或正式触发。方向用途至少要求平均每 5 分钟一个样本、任一内部 gap 不超过 10 分钟；恢复后无需重新等待完整 1 小时。
- 行情 provider 必须连续改变 30 秒才切换坐标并重置窗口；短暂 Schwab/IBKR 抖动被忽略，避免单个 fallback tick 制造新的 15m/60m 黑窗。

## Rank 定义

每个满窗周期输出：

- `position_percentile`：当前 ES 在该窗口全部 5 秒样本中的经验 CDF 位置；
- `drawdown/recovery`：先跌后收复路径及其同日历史 rank；
- `rally/pullback`：先涨后回撤路径及其同日历史 rank；
- `effective_reference_windows`：只计算当前窗口开始前已经结束的同日、同 provider、非重叠完整窗口。

历史 rank 没有参考窗时为 unavailable，少于 5 个参考窗时标为 small sample。相同值使用 mid-rank，所以全程平盘为 50 而不是 100。所有 rank 都是 0–100 的因果经验排序，不是上涨/下跌概率，也不做 50% 收缩或跨日补样。

## 信号与执行边界

达到上述最低路径质量后，路径 rank 是 Call/Put 双边机会榜的连续 modifier：

- 低位置与 dip/recovery rank 增强 Call 观察；
- 高位置与 rally/pullback rank 增强 Put 观察；
- 窗内位置 rank 可在首个满窗使用；历史 shape rank 至少需要 5 个已结束参考窗才计入方向分，小样本仍只展示；
- wall、skew 和期权链缺失不会删除 ES 方向观察，只影响排序与可执行性。

固定点数/expected-move 阈值仍负责正式 dip-reclaim 触发。路径 rank 不单独生成订单，不放宽 SPXW 双边 NBBO、parity、报价新鲜度、13:00 ET 退出或人工确认要求；`action_authority=none`、`automatic_ordering=false`。

## 持久化

- `latest/gth_path_ranks.json`：供状态页和 15 分钟报告读取的有界投影，不含原始样本；
- `features/gth_detector_health/date=YYYY-MM-DD/samples.jsonl`：每个热循环保留 ES 样本；readiness/rank 投影每分钟抽样一次，均可由原始样本重放；
- intraday shock state：最多保留最长窗口加 60 秒的原始尾部，以及每周期最多 1,000 个紧凑历史窗口。
