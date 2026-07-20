# Call / Put Skew Spread Shadow

Date: 2026-07-20
Status: production shadow, automatic ordering disabled

## Purpose

The 15-minute SPX status report now evaluates up to two same-expiry `1x/-1x`
debit verticals independently:

- Call: buy a lower-strike Call in the executable core and sell a higher-strike
  Call in a confirmed rich upside wing;
- Put: buy a higher-strike Put in the executable core and sell a lower-strike Put
  in a confirmed rich downside wing;
- sell a wing option only when its observed IV and executable bid are rich versus
  the local fit on the liquid-core side;
- expose the package economics, defined risk, net Greeks, and rejection reason;
- never create an order or promote the result into `plan_candidates`.

This is a forward shadow selector, not a validated strategy. A missing or one-sided
quote is not evidence of mispricing. The sell leg must remain executable.

## 15-minute report contract

`build_order_payload()` always attaches `call_skew_spread_shadow` and
`put_skew_spread_shadow`.

- RTH candidate: the compact status line and the deterministic Feishu section show
  the two legs, conservative package debit, fitted-value edge, risk, and net Greeks.
- RTH no candidate: the report shows quote coverage and the stable rejection reason.
- Outside RTH: the report shows `unavailable` because a frozen or partial chain cannot
  support executable spread pricing.

The deterministic section is named `Call / Put Skew Spread Shadow`. LLM output may
summarize it, but it cannot call either side a plan or an order. A candidate appearance
or leg change becomes a material status change; ordinary quote noise does not.

## Price and edge definitions

For either side, `Klong` is the bought option and `Kshort` is the sold option:

```text
executable_debit = ask(long) - bid(short)
fair_debit       = mid(long) - fitted_fair_mid(short)
edge             = fair_debit - executable_debit
max_loss         = executable_debit * 100
max_profit       = (abs(Kshort - Klong) - executable_debit) * 100
call_breakeven   = Klong + executable_debit
put_breakeven    = Klong - executable_debit
```

The selector never uses `mid(K1) - mid(K2)` as an executable package price.

The Call sell-leg fair IV is a robust Theil-Sen local fit over three to five executable
lower-strike Calls. The Put sell-leg fit mirrors it over three to five executable
higher-strike Puts. The fair short premium is the observed short mid scaled by the ratio
of r=0 Black-Scholes values at fitted IV and observed IV. This keeps the estimate anchored
to the actual premium level while using the fit only for relative skew.

The minimum short-IV deviation is the greater of 0.50 vol points and three times the
local fit MAD. The next farther-wing executable option—higher Call or lower Put—must
confirm at least half that deviation, with a 0.25-vol-point floor. The executable edge
must remain at least 0.10 SPX option points after paying the long ask and receiving the
short bid.

## Fail-closed gates

A candidate requires all of the following:

- official RTH and the current 0DTE expiry;
- an available SPX pricing reference;
- fresh, two-sided, pricing-allowed SPXW quotes for the evaluated right;
- positive displayed bid and ask sizes on both legs;
- existing point, bps, percentile, source-age, transport-age, provider-divergence, and
  model-underlier gates;
- one provider for both selected legs and at most five seconds of source/transport
  timestamp skew;
- absolute long delta between 0.25 and 0.65, absolute short delta between 0.05 and 0.40;
- width from one to six observed strike steps;
- positive debit below spread width, positive short-bid richness, adjacent skew
  confirmation, and positive executable edge.

Any failure produces `no_candidate` or `unavailable` with diagnostics. There is no
fallback to stale quotes, frozen history, indicative mids, single-leg execution, or
GTH partial chains.

## Output boundary

Each side's candidate includes:

- exact contract IDs, provider, NBBO, sizes, timestamps, IV, Delta, and Gamma;
- local fit anchors, residual error, required/observed richness, and adjacent
  confirmation;
- executable debit, fair debit, edge, maximum loss/profit, and breakeven;
- net Delta, Gamma, Theta, Vega, and Charm per calendar minute;
- `order_style=combo_net_debit_limit_shadow`, `leg_orders_prohibited=true`, and
  `automatic_ordering=false`.

Production promotion requires forward evidence from exact two-leg snapshots and
realistic combo fills. This release only records and reports shadow candidates.
