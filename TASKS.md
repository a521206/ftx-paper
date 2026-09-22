# FTX Paper Task List

Open parity and cutover work is tracked below. Completed foundation,
feature, location, score, sizing, and risk-gate work is intentionally omitted.

## Bit-for-bit canonical FTX parity

### Track P0 — Parity contract

- [x] Approve [PARITY_CONTRACT.md](PARITY_CONTRACT.md) as the working specification

### Track P5 — Session-policy parity

- [x] Resolve composite-cell acceptance differences against the canonical policy
- [x] Match transition-policy eligibility and rejection reasons for the policy matrix
- [x] Add regression coverage for the corrected policy matrix and entry-state lifecycle

The remaining fixture differences are downstream exit-state/decision-order
effects and are tracked under P8. For example, canonical rejects the
2026-09-15 13:51 `or_low` candidate because the 13:45 hard-stop result has
already locked the thesis; Paper evaluates that candidate while its position
is still open and later exits it as `counter_move`.

### Track P8 — Exit parity

- [x] Resolve the remaining exit-state divergences (counter-move versus canonical hard-stop)
- [x] Confirm exit timestamps, held bars, and exit reasons across the configured signal/trail modes
- [x] Add deterministic regression fixtures for each discovered exit mismatch

Fresh causal replays now match the known 2026-09-15 10:30 and 13:45 exits,
including hard-stop timestamps, held bars, reasons, and fills. Remaining
2026-09-07 and 2026-09-15 Paper-only acceptances are downstream
canonical-batch lookahead differences; the decision is documented in
[`PARITY_CONTRACT.md`](PARITY_CONTRACT.md) and remains open under P9.

### Track P9 — Decision-replay comparator

- [x] Match the current signed OHLCV delta-divergence proxy and disable score-based quantity scaling by default
- [x] Keep risk-gate state continuous within each session and use configured 15-bar entry / 30-bar post-exit cooldowns
- [x] Add persistent scoped risk buckets keyed by date, session, cell, and direction
- [x] Regenerate canonical and Paper replay artifacts after the core pipeline alignment changes
- [ ] Compare event identity, cell, direction, score, factors, stop, quantity, reason, and timing
- [x] Connect the ordered replay comparator to both independent implementations
- [x] Add regression tests for every discovered decision-stream mismatch

The comparator reads the requested range from the canonical decision-artifact
manifest (rather than widening or narrowing it to the trade audit's range) and
compares Paper replay terminal decisions directly when `--canonical-decisions`
is supplied. It normalizes Paper's one-based sequence to the canonical
zero-based bar, scopes by candidate date/sequence/cell, and reports unscoped
Paper decisions rather than hiding them.

Fresh validation used canonical run
`research_20260922T013200649202Z_research-replay-v2-ftx-pipeline-research_810d8dba`
for 2026-09-01..2026-09-21. Paper initially had no market bars on
2026-09-03; an isolated replay copy was then seeded through the DuckDB
futures/index repository APIs, including September 2 prior-day context. The
Pipeline's direct ATM lookup also returns the September 3 contract rows (752
CE/PE bars for expiry `260915`, strike `23950`); the earlier zero result came
from a different weekly-expiry helper. Those rows were copied into the
isolated Paper runtime only. Paper now matches both canonical trades exactly:
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

### Track P11 — Cutover

- [ ] Run the complete holdout replay and achieve zero unexplained decision differences
- [ ] Obtain explicit approval for any remaining exceptions
- [ ] Switch runtime execution to the parity-proven decision path
- [ ] Remove the canonical live implementation from NiftyZoning only after the parity gate passes
