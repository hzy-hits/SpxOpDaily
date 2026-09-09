# GTH 已确认水平优先级修复与铁鹰长持有复核

## 范围与代码修复

执行方案 Phase 8 / S3，复用 gth_trend_entry_source.py。原来源选择顺序是Asia-range、advance、transition、confirmed level；新鲜但未获现行人工授权的趋势背景可能先占用唯一来源，令有效水平信号无法进入下游。修改为Asia-range、confirmed level、advance、transition。水平必须同时满足formal_signal=true、confirmed、quality_ok=true、expires_at>now；不会复活过期水平。Asia-range既有优先级保留。欧洲transition与水平同时出现时也以已确认水平优先，这是本次明确的选源政策。

删除旧优先级路径，同步source_priority到既有候选policy hash；全局策略版本仍v68，源码提交和来源policy hash共同区分本补丁。没有新增setup、放宽价格/报价门或改变人工-only权限。合成测试验证新鲜advance/transition均不能吞掉合格水平；旧测试强制趋势压过水平的断言已替换。没有从这些合成输入推断今日生产确实漏发过一笔盈利交易。

## 铁鹰补充检验

延续v68的原始IBKR样本，11个日期中9组20Delta/10点翼首次平缓收缩候选。将原四腿报价检查延长到12:30 ET。全程仅按received_at<=检查时点重建，五秒采样、源年龄<=30秒、四腿偏斜<=10秒；GTH仅IBKR，RTH可用完整同provider的Schwab四腿。长腿零bid允许按零回收价值估算，ask必须有效；不把缺行情当成零价格。保留两版研究输出用于核查，修正零bid后退出覆盖数量未改善。

“只在12:30退出”与“0.5C止盈/3C止损/12:30退出”是两个不同假设。前者只报告端点保守BBO净值；后者只要阈值观察前存在报价缺口就标QUOTE_GAP，不作为完成收益。全部扣四份合约双边$10.56费用；没有假设mid成交，没有用推送或历史卡筛选。

|日期|12:30固定退出净标记($)|首次观察到的退出条件|路径完整性|
|---|---:|---|---|
|2026-08-05|99.44|hard_exit|QUOTE_GAP|
|2026-08-06|44.44|take_profit|QUOTE_GAP|
|2026-08-26|209.44|take_profit|QUOTE_GAP|
|2026-08-27|缺报价|take_profit|QUOTE_GAP|
|2026-08-28|134.44|take_profit|QUOTE_GAP|
|2026-08-31|缺报价|take_profit|QUOTE_GAP|
|2026-09-01|缺报价|stop_loss|QUOTE_GAP|
|2026-09-02|-195.56|hard_exit|QUOTE_GAP|
|2026-09-03|缺报价|stop_loss|QUOTE_GAP|

9组中只有5组有合格12:30报价，4正1负，覆盖子集均值+$58.44。这个均值存在缺失样本选择问题，不是策略edge。9月1日与3日分别观察到约-$640.56、-$510.56的止损阈值报价，但此前存在缺口，不能判定是否曾先止盈、真实止损时间或实际成交损益。不得将两笔直接归为已完成止损，也不得将缺口样本删掉后宣称80%胜率。

一小时净值为负不能否定较长持有：8月5/6日固定12:30退出端点为+$99.44/+$44.44。但现有止盈止损合同完整路径、成交概率、Greeks独立时间与全部信号门尚未重建。因此本轮没有将平缓收缩接成新生产入口；当前证据不足以授权新规则，不代表数学上已证明它无效。

## 验收、复杂度与部署

相关256项测试通过；全量Python3404项通过（2 warnings）；Ruff、Import Linter、Rust fmt/Clippy/workspace测试通过。生产文件新增/删除0/0，修改1个，净生产LOC +3；依赖、配置键、服务/timer、数据库/表增删均0。旧来源优先级已替换，没有新增架构路径。

沿用此前合并部署授权，通过正式脚本部署，仅重启Core；实际提交、运行状态、行情时间写入研究目录deployment-verification.json。Bark代码、配置、投递行为保持不动，未读取其记录或发送测试通知。

复现：/srv/data/spx-spark/research/gth-source-priority-2026-09-09/hold_replay.py；结果hold_results.json，保留positive-bid-only旧口径结果。入场样本来源与限制见docs/gth-entry-recovery-v68.md。
