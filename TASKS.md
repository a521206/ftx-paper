# FTX Paper Task List

The canonical FTX and `ftx-paper` implementations continue to coexist
independently. Parity claims must be revalidated against the current repository
state and fixed replay inputs before being marked complete.

## Open Parity And Coexistence Work

### Incremental Canonical/Paper Replay Parity Harness

- [x] Layer 1: normalized ordered stage-trace contract, comparator, deterministic harness, and focused validation
- [x] Layer 2: fixed replay inputs and session-context parity
- [x] Layer 3: feature, location, transition, and signal-context parity
- [x] Layer 4: candidate populations and identity parity for active-policy candidates
- [ ] Layer 5: risk and sizing parity for candidates reaching those stages
- [ ] Layer 6: order, fill, exit, and settlement parity for candidates reaching execution
- [ ] Layer 7: portfolio and ledger parity
- [ ] Layer 8: fixed-input end-to-end replay report

Work stops at the first unexplained mismatch or missing source input. Never
change canonical behavior, frozen baselines, runtime stores, or research
artifacts to make Paper appear aligned.

Layer 4 scope and normalization rules are defined in `PARITY_CONTRACT.md`.
The fixed-date report matched active-policy populations at 3/3, 2/2, and 1/1
for Sep 3, Sep 15, and Sep 29. Layers 5-8 remain `NOT_APPLICABLE` because no
active-policy candidate reached sizing.

### Track P11: Coexistence Validation

- [ ] Run the complete holdout replay and achieve zero unexplained differences in required shared execution and settlement fields
- [ ] Obtain explicit approval for any remaining intentional differences
- [ ] Record the coexistence-symmetry result, configuration hashes, fixtures, and holdout artifacts
