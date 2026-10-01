# FTX Paper Task List

P0, P9, and the minimal P10 contract are complete for their documented scope.
Completed work is removed from this active task list; detailed contracts remain
in `PARITY_CONTRACT.md`. The canonical FTX and `ftx-paper` implementations
continue to coexist independently.

## Open Parity And Coexistence Work

### Incremental Canonical/Paper Replay Parity Harness

- [x] Layer 1: normalized ordered stage trace contract, comparator, deterministic harness, and focused validation
- [x] Layer 2: input and session-context parity
- [x] Layer 3: feature, location, and transition parity
- [x] Layer 4: candidate-stream parity
- [x] Layer 5: risk and sizing parity for candidates reaching the sizing stage
- [ ] Layer 6: order, fill, exit, and settlement parity
- [ ] Layer 7: portfolio and ledger parity
- [ ] Layer 8: fixed-input end-to-end replay report

Work stops at the first unresolved mismatch or missing source input. Never
change canonical behavior or frozen baselines to make Paper appear aligned.

### Layer 4 Status

Layer 4 candidate parity is complete for the three available full replay dates:
Sep 3, Sep 15, and Sep 29, 2026. Read-only comparisons report matching input,
feature, location, transition, candidate identity, count, and shared-field
results:

- Sep 3: 354 candidates on each side
- Sep 15: 354 candidates on each side
- Sep 29: 267 candidates on each side

The Paper expiry calendar was synchronized with the configured canonical
calendar (417 dates), and the input diagnostic now reports matching PCR values
and normalized fingerprints on all three dates.

Decision-outcome parity is outside the current diagnostic scope because the
canonical raw capture does not expose equivalent policy outcomes or rejection
reasons. This is an explicit limitation, not an unexplained mismatch. No
canonical behavior, frozen baseline, replay state, or research artifact was
changed by the parity validation.

### Layer 4 Validation

- `.venv\Scripts\python.exe -m pytest ftx-paper\tests\test_store.py ftx-paper\tests\test_core_boundaries.py ftx-paper\tests\test_parity_trace.py ftx-paper\tests\test_session.py tests\test_compare_candidate_replay.py tests\test_scoring_parity.py -q` — 106 passed
- `.venv\Scripts\python.exe scripts/ftx/compare_replay_inputs.py` — read-only; inputs matched on all three dates
- `.venv\Scripts\python.exe scripts/ftx/compare_candidate_replay.py` — read-only; candidate stage matched on all three dates
- `.venv\Scripts\ruff.exe check ftx-paper\src\ftx_paper\runtime\store.py ftx-paper\src\ftx_paper\core\features.py ftx-paper\src\ftx_paper\core\adaptive_stop.py ftx-paper\src\ftx_paper\core\live_decision.py ftx-paper\src\ftx_paper\core\risk.py ftx-paper\tests\test_store.py ftx-paper\tests\test_core_boundaries.py scripts\ftx\compare_candidate_replay.py tests\test_compare_candidate_replay.py` — passed
- `git diff --check` — passed

### Layer 5 Status

Layer 5 is complete for the documented sizing-stage scope. Candidates rejected
before the sizing engine are `capital_risk_not_applicable`; they are not counted
as missing risk diagnostics. The three available replay dates each contain one
candidate that reached sizing, and risk ceiling, strategy request, effective
quantity, and common sizing stages matched on all three. No capital-risk
rejections occurred in the compared sizing-stage population.

Layer 5 does not claim parity for pre-sizing thesis or policy gates, or for
capital-risk rejection diagnostics that were not exercised by the fixed dates.

Validation:

- `.venv\Scripts\python.exe -m pytest tests\test_compare_candidate_replay.py ftx-paper\tests\test_core_boundaries.py ftx-paper\tests\test_session.py -q` — 90 passed
- `.venv\Scripts\ruff.exe check scripts\ftx\compare_candidate_replay.py tests\test_compare_candidate_replay.py ftx-paper\src\ftx_paper\core\live_decision.py` — passed
- `.venv\Scripts\python.exe scripts\ftx\compare_candidate_replay.py` — read-only; sizing-stage parity matched on all three dates

### Track P11: Coexistence Validation

- [ ] Run the complete holdout replay and achieve zero unexplained differences in required shared execution and settlement fields
- [ ] Obtain explicit approval for any remaining intentional differences
- [ ] Record the coexistence-symmetry result, configuration hashes, fixtures, and holdout artifacts
