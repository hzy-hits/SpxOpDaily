# Shared wire-contract registry

<!-- documentation-status: 2026-10-05 -->
> **文档定位：现行运行与参考。** 现行说明；历史段落保留原适用日期。运行状态以实际服务和源字段时钟为准。
> [全仓文档、当前运行状态与合同优先级](../docs/README.md)（目录核对：2026-10-05）。

## Existing recovery projection fields

The production fault repair reuses `desk_map_projection.v1` with optional
`recovery_seen` and `recovery_of`, validated by the existing Python/Rust readers.
It adds no wire version or strategy authority. Full `strategy_decision` and
per-side `data_diagnostics` remain Python objects; Rust receives the bounded
rendered message. See [acceptance](../docs/desk-data-recovery-2026-10-05.md).

> **FROZEN（2026-08-07）：本 contract 集已冻结，不得新增 contract、version 或 fixture。**
> Phase 6 退出已延期，当前继续保留和验证这些 fixtures；只处理授权的生产故障。
> 见 [执行方案](../docs/architecture-simplification-execution-plan-v1.md) 的收口决定。

`contracts/golden/` contains sanitized, versioned JSON examples used at runtime
boundaries. A file being stored here means its shape is auditable across the
monorepo; it does not mean both languages produce that shape.

| Contract | Version | Producer | Validator / consumer | Authority |
|---|---|---|---|---|
| Desk Map projection | `desk_map_projection.v1` | Python | Python producer acceptance; Rust bridge/domain/report | Advisory facts only; `action_authority=none` |
| Research context | `research_context.v2` | Python | Python producer acceptance; Rust bridge/domain/report | Causal research only; `automatic_ordering=false` |
| Legacy research signals | `experimental_research_signals.v1` | Historical Python compatibility lane | Rust bridge/domain | Advisory compatibility only |
| Quote batch | `quote_batch.v1` | Rust bridge after mapping Python normalized state | Rust domain/core | Typed ingress; not the Python latest-state wire shape |
| Provider state | `provider_state.v1` | Rust bridge mapping | Rust domain/core | Readiness input; `10197` remains fail-closed |
| Strategy decision | `strategy_decision.v1` | Rust core | Rust ledger | `NO_TRADE` or `MANUAL_CANDIDATE` only |
| Notification intent | `notification_intent.v1` | Rust core | Rust ledger/delivery | Manual advisory contract |
| Delivery receipt | `delivery_receipt.v1` | Rust delivery | Rust ledger/operator audit | Transport outcome evidence |

The `invalid/` fixtures are required negative cases. Unknown enums, provider
mismatches, unknown fields, and invalid spread widths must fail closed.

Rules for changes:

1. Never put raw broker payloads, account identifiers, credentials, endpoints,
   private keys, or notification content secrets in a fixture.
2. A breaking field or enum change requires a new schema version; do not mutate
   an old fixture until it silently means something else.
3. Changes to `research_context.v2` or `desk_map_projection.v1` must pass both
   Python and Rust tests in the root CI.
4. Rust ingress fixtures such as `quote_batch.v1` must not be described as the
   Python normalized source format. A future normalized-mirror golden belongs
   under `contracts/golden/bridge/` and must be consumed on both sides.
5. Production releases are built from the complete monorepo checkout because
   Rust compile-time tests reference this root registry. Installed binaries do
   not require fixture files at runtime.
