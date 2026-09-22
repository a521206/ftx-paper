# FTX Paper Task List

Open parity and coexistence-symmetry work is tracked below. Completed foundation,
feature, location, score, sizing, and risk-gate work is intentionally omitted.

## Bit-for-bit canonical FTX parity

### Track P9 — Decision-replay comparator

- [ ] Compare event identity, cell, direction, score, factors, stop, quantity, reason, and timing

The comparator reads the requested range from the canonical decision-artifact
manifest (rather than widening or narrowing it to the trade audit's range) and
compares one normalized causal decision record per candidate when
`--canonical-decisions` is supplied. It normalizes Paper's one-based sequence
to the canonical zero-based bar and treats missing, extra, reordered, or
changed candidate decisions as symmetry failures.
Use the canonical runner's `--emit-decision-trace` flag only for parity
fixtures and holdout runs; ordinary replays omit the trace artifacts.

Fresh validation used canonical run
`research_20260922T013200649202Z_research-replay-v2-ftx-pipeline-research_810d8dba`
for 2026-09-01..2026-09-21. Paper initially had no market bars on
2026-09-03; the normal Paper runtime was then populated through the existing
runtime market-bar API with September 2 prior-day context and September 3
bars. The Pipeline's direct ATM lookup also returns the September 3 contract
rows (752 CE/PE bars for expiry `260915`, strike `23950`); the earlier zero
result came from a different weekly-expiry helper. Paper now matches both
canonical trades exactly:
cell, short direction, eight lots, 13:33 entry, 15:10 exit, futures net
Rs 14,200, and synthetic net Rs 13,377.90. Synthetic remains a derived
reporting record and does not create a second decision or mutate futures state.
The ordered decision stream still reports one modified field (`entry_price`,
absent from the canonical decision trace), `stop_bp` unavailable, and 373
diagnostic Paper events outside the one canonical candidate scope. The saved
comparison predates the synthetic fix and should be regenerated before claiming
zero unexplained decision differences.
Paper's default research profile and cell manifest have since been aligned to
the Pipeline research run: 3% per-trade risk, 8 lots, 12 directional lots,
no drawdown scaling, the 09:15–11:45 morning window, and the same seven active
session cells with their direction, exit mode, hypothesis, and stability.
Paper remains an independent implementation. The saved comparison predates
this alignment and remains historical evidence only; regenerate an isolated
Paper replay over dates shared with canonical before claiming ordered parity.
Stop distance is also unavailable in the canonical decision-trace row.

### Track P10 — Ledger and vehicle comparator

- [ ] Complete futures exit, fill, cost, and final-ledger parity comparison
- [ ] Complete synthetic premium entry/exit, sizing, cost, and settlement comparison
- [ ] Compare daily PnL, drawdown, and rejection counts in the Phase 2 report

### Track P11 — Coexistence validation

- [ ] Run the complete holdout replay and achieve zero unexplained differences in required shared decision fields
- [ ] Obtain explicit approval for any remaining intentional differences
- [ ] Record the coexistence-symmetry result, configuration hashes, fixtures, and holdout artifacts
