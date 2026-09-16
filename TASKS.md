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

- [ ] Compare event identity, cell, direction, score, factors, stop, quantity, reason, and timing
- [x] Connect the ordered replay comparator to both independent implementations
- [x] Add regression tests for every discovered decision-stream mismatch

The comparator is now wired to the canonical run decision artifact and Paper's
replay events through `--canonical-decisions`. It normalizes terminal decisions
onto a shared semantic stream, converts Paper's one-based sequence to the
canonical zero-based bar, scopes Paper events to canonical candidate identity,
and preserves out-of-scope event counts in `decision_replay.json`.

The event-field comparison remains open. Fresh runs for 2026-09-07,
2026-09-08, and 2026-09-15 are still mismatched; the streams now compare
`score_factors` with no unavailable-field warning. The first differences remain
quantity/candidate-scope differences (for example, canonical requested quantity
1 versus Paper 3 on 2026-09-07). P9 remains open until factor parity and
candidate-scope alignment are both evidenced by fresh comparator results.

### Track P10 — Ledger and vehicle comparator

- [ ] Complete futures exit, fill, cost, and final-ledger parity comparison
- [ ] Complete synthetic premium entry/exit, sizing, cost, and settlement comparison
- [ ] Compare daily PnL, drawdown, and rejection counts in the Phase 2 report

### Track P11 — Cutover

- [ ] Run the complete holdout replay and achieve zero unexplained decision differences
- [ ] Obtain explicit approval for any remaining exceptions
- [ ] Switch runtime execution to the parity-proven decision path
- [ ] Remove the canonical live implementation from NiftyZoning only after the parity gate passes
