# Desk Map 模型身份失败回退与空ATM崩溃修复

## 生产证据

2026-09-10 14:00北京时间Python已生成GTH desk_map_projection，observed_through为06:00:03 UTC，源质量ready；Python任务记录rust_report_owner是既有单owner设计。Rust report health处于backoff，last_error_code=unexpected_model，last_response_model=deepseek-flash；最后成功持久化时间为2026-09-09T19:32:17Z。故地图生成失败不能解释成没有策略机会。没有读取Bark记录，尚未核验终端收件。

Core在9月9日13:26至23:54 UTC出现133次NoneType格式化异常，自动重启计数17。按小时分布13时4次、20时33次、21时35次、22时35次、23时26次，主要发生在开盘前/闭市；不能据此声称整个RTH都被该异常阻断。

## 修改与验收

S1：现有options.py同合约衰减比较在front存在但atm_strike缺失时仍执行:g格式化；改为atm_strike_unavailable且不生成衰减。覆盖有历史而当前ATM缺失的输入（含straddles空映射与直接调用两条路径）。不修改经济阈值。

Phase 6冻结Rust生产故障例外：复用spx-report已有projection fallback。HTTP成功但返回未知模型时，模型内容仍被拒绝，改用当前有效、已通过validate的Python DeskMessage原文；不推断模型别名，不使用未知模型内容。原有投影到期检查、slot幂等、owner lease和持久化流程继续生效。针对未知模型返回不可信内容，集成测试要求只持久化原始地图一次。其他transport/HTTP等错误不在本次新增回退范围。

这是已有源消息回退的故障修复，不改变报告owner、Bark代码/配置/投递行为、交易候选范围或自动下单权限。报告client仍拒绝未知model，health明确记录unexpected_model回退原因；不把回退报告描述成模型生成成功。

## 发布边界

Python按现有安装脚本发布并仅重启Core。Rust按rust/docs/OPERATIONS.md使用host system service；报告生产旧release的spx-report/domain/ledger源码与本次修改前仓库一致。创建新不可变release，其他三个binary保持旧文件校验和，仅替换spx-report并重启报告service。不重启Rust core/bridge/delivery，不转移owner。

源文件新增/删除0/0，生产修改3个，净生产LOC +8；依赖、配置键、service/timer、数据库/表增删均0。旧空ATM格式化路径和未知模型导致整张原始地图被阻断的分支被替换。全量测试、部署提交和健康验收结果存于/srv/data/spx-spark/research/desk-map-recovery-2026-09-10/。

## RTH候选预算异常

额外核查9月9日RTH日志：3570个特征周期成功，1次FrozenInstanceError（不能赋值passed）。strategy_select在三个候选路径被否决、仍有第四个候选时向frozen RankResult.passed赋值；改为清空其原有可变列表，保持现有预算及未评估项审计。新增公共build_strategy_decision的四候选反例，验证NO_TRADE和第四候选path_not_evaluated_budget均正常输出。这里修复的是异常，不提高预算、不跳过任何候选hard gate。该故障不能证明全天没有交易机会。

发布验收：全量Python3407项通过（2 warnings），相关206项测试通过；Rust fmt、Clippy、workspace tests及release build通过；新报告binary的生产check-config通过；Ruff、Import Linter与git diff --check通过。
