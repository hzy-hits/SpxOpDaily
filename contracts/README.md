# Shared wire-contract registry

> **FROZEN（2026-08-07）：本 contract 集已冻结，不得新增 contract、version 或 fixture。**
> 跨语言 contract 计划随 Rust 控制面退出一并删除，见
> `docs/architecture-simplification-execution-plan-v1.md` Phase 6（P6-3）。

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
