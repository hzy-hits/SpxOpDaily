# Frozen runtime defaults for unit/architecture tests

<!-- documentation-status: 2026-10-05 -->
> **文档定位：现行运行与参考。** 现行说明；历史段落保留原适用日期。运行状态以实际服务和源字段时钟为准。
> [全仓文档、当前运行状态与合同优先级](../../docs/README.md)（目录核对：2026-10-05）。

`runtime.toml` is copied from `config/runtime.toml` at the Phase 0 baseline
freeze. Tests load it through `SPX_SPARK_RUNTIME_CONFIG` (see
[conftest.py](../conftest.py)), preventing deployment overrides or a local
`.env` from changing assertions. When intentionally changing product defaults,
update both the runtime configuration and this fixture in the same change.
The documentation refresh itself does not alter either configuration.
