# FTX Paper Task List

Open parity and coexistence-symmetry work is tracked below. P0's behavioral
baseline is complete for its defined scope; documented P0 differences are
later-phase work. Completed foundation, feature, location, score, sizing, and
risk-gate work is intentionally omitted.

## Bit-for-bit canonical FTX parity

### Track P9 — Decision-replay comparator

- [x] Compare event identity, cell, direction, score, factors, stop, quantity, reason, and timing

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
Stop distance is also unavailable in the canonical decision-trace row. P9 is
complete for its intended accepted-decision scope; it is not a claim that all
downstream execution and settlement fields are complete.

### Track P10 — Ledger and vehicle comparator

- [x] Implement separate execution, exit, synthetic, and ledger normalization/comparison
- [x] Compare published futures fill/exit fields and synthetic settlement fields
- [x] Resolve remaining Paper-only execution artifacts and unavailable ledger fields
- [x] Close the minimal futures exit, fill, cost, final-ledger, daily PnL, drawdown, and rejection-counter parity contract

Optional raw implementation IDs, broker-style order/fill statuses, synthetic
quote provenance, equity, and directional exposure remain diagnostic fields;
they are not required by the minimal P10 gate.

The verified Sep 3 replay used the canonical Sep 2–Sep 3 fixture and the
consolidated runtime at `data/runtime/ftx-paper`; rejected decisions were not
emitted. The minimal P10 report currently shows 4 matched rows, 0 modified
rows, 0 canonical-only rows, 0 Paper-only rows, 0 unavailable fields, 0
synthetic-only differences, and 0 ledger-only differences.

### Track P11 — Coexistence validation

- [ ] Run the complete holdout replay and achieve zero unexplained differences in required shared execution/settlement fields
- [ ] Obtain explicit approval for any remaining intentional differences
- [ ] Record the coexistence-symmetry result, configuration hashes, fixtures, and holdout artifacts
