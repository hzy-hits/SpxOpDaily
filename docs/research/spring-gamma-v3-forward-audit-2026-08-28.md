# Spring Gamma v3 forward audit · 2026-08-28

## Decision

Spring Gamma v3 does not receive confirmation or veto authority. Its persisted
ES direction is decoupled from option/Wall expression readiness, but it is
removed from human trade cards and retained only as causal research context.

## Causal sample

- Source: persisted `features/spring_gamma_v3/date=*/predictions.jsonl` records.
- Window: the ten RTH dates from 2026-08-14 through 2026-08-27; 2026-08-27 is
  partial.
- Deduplication: last persisted record per RTH minute; a test origin is the
  first minute of a new UP/DOWN run, with a 30-minute refresh ceiling.
- Price label: frozen SPX `level_gate.spot` versus the same field available at
  exactly +5m, +15m and +30m. Positive aligned points mean the subsequent SPX
  move agreed with the model direction.
- No future observation was used to form a direction or origin.

## Direction result

| Shadow interpretation | Horizon | N | Win rate | Mean aligned points | Median aligned points | Positive dates |
|---|---:|---:|---:|---:|---:|---:|
| Persisted post-Wall direction | 5m | 273 | 47.3% | -0.301 | -0.185 | 3/10 |
| Persisted post-Wall direction | 15m | 273 | 49.5% | -0.325 | -0.100 | 3/10 |
| Persisted post-Wall direction | 30m | 257 | 41.6% | -1.636 | -0.965 | 0/10 |
| Reconstructed ES direction before Wall-only suppression | 5m | 292 | 47.6% | -0.398 | -0.172 | 3/10 |
| Reconstructed ES direction before Wall-only suppression | 15m | 286 | 48.6% | -0.354 | -0.113 | 3/10 |
| Reconstructed ES direction before Wall-only suppression | 30m | 268 | 40.7% | -1.751 | -1.197 | 0/10 |

The reconstruction retains a formerly suppressed direction only when the
persisted record has no core direction failure, has a Wall-only failure and
has absolute confidence at least 0.20. It is a diagnostic counterfactual, not
a claim about the newly emitted v3 records.

## Existing price-trigger candidates

Spring context was present on only eight unique selector candidates in the
available recent files: four same-direction, one conflict and three abstain.
Same-direction candidates won 2/4 at each horizon; their mean aligned moves
were +1.126 points at 5m, -0.270 at 15m and -1.859 at 30m. The sole conflict
candidate moved with the existing price trigger rather than Spring at all
three horizons. This sample is too small to estimate an incremental effect,
and it gives no basis for a Spring veto.

## Spread-PnL boundary

The recent matched candidate set does not contain enough independent,
execution-consistent debit-spread outcomes to estimate net PnL by
Spring-confirmed versus Spring-conflicted bucket. Some records are single-leg
legacy candidates, and stored marks are explicitly not fills. Because the
direction prerequisite already failed and net spread PnL is not estimable,
the promotion gate fails without fitting another threshold.

## Production consequence

1. ES direction status no longer depends on option-frame or Wall-probability
   readiness.
2. Option and Wall failures remain visible in their nested expression payloads
   and continue to fail closed for any contract expression.
3. Spring remains `direction_authority=none`, `action_authority=none` and
   `automatic_ordering=false`.
4. Manual candidate and pre-arm trade cards no longer render READY, conflict,
   abstain or unavailable Spring prose. The bounded research status projection
   remains available for audit. A future promotion requires independent forward
   direction and exact spread-PnL evidence.
