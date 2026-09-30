# FTX Paper Task List

P0, P9, and the minimal P10 contract are complete for their documented scope.
Completed work is removed from this active task list; the detailed contracts
remain in `PARITY_CONTRACT.md`. The canonical FTX and `ftx-paper`
implementations continue to coexist independently.

## Open parity and coexistence-symmetry work

### Incremental canonical/Paper replay parity harness

- [x] Layer 1 — normalized ordered stage trace contract, comparator, deterministic harness, focused validation
- [x] Layer 2 — input and session-context parity; validate before continuing
- [x] Layer 3 — feature, location, and transition parity; validate before continuing
- [ ] Layer 4 — complete candidate and decision parity; validate before continuing
- [ ] Layer 5 — risk and sizing parity; validate before continuing
- [ ] Layer 6 — order, fill, exit, and settlement parity; validate before continuing
- [ ] Layer 7 — portfolio and ledger parity; validate before continuing
- [ ] Layer 8 — fixed-input end-to-end replay report

Layer 2 fixed-date comparison passes for Sep 3, Sep 15, and Sep 29 after
normalizing to the canonical 09:15–15:10 window and replay's effective expiry
context. Layer 3 feature/location/transition comparison is complete; its
fixed-date diagnostic is implemented in `scripts/ftx/compare_replay_inputs.py`
and compares the same minute-level features, locations, cells, VWAP states, and
transitions. Its prior report should be retained with the parity evidence.

Work stops at the first unresolved mismatch or missing source input. Never
change canonical behavior or frozen baselines to make Paper appear aligned.

### Current Layer 4 Status

The only full replay dates available in the Paper SQLite store are Sep 3,
Sep 15, and Sep 29, 2026. The post-score-fix comparison was run read-only using
`scripts/ftx/compare_replay_inputs.py` and
`scripts/ftx/compare_candidate_replay.py`.

- Input, supporting-bar, PCR, prior-day-level, and feature/location/transition
  comparisons matched on all three dates. Feature/location/transition row
  counts were 354, 354, and 319.
- Paper's session-dependent `time_window` score factor was removed because the
  canonical score is time-neutral. Focused regression tests passed after this
  change.
- Candidate counts remain mismatched: 353 canonical vs 354 Paper on Sep 3;
  353 vs 354 on Sep 15; 266 vs 267 on Sep 29.
- First reported difference: Sep 3 and Sep 29 have sequence 2 canonical vs 3
  Paper at 09:17. Sep 15 has direction `short` canonical vs `none` Paper for
  `vwap_zone+prior_day_high` at 09:17.

The candidate comparator currently compares raw canonical capture projections
with Paper candidate events. Layer 4 remains incomplete.

The comparator now compares `(date, time, cell)` identity sets first, reports
canonical-only/Paper-only identities, compares the first field difference for
shared identities, and retains ordered comparison after identity alignment.
Paper's `sequence=len(_futures)` is the one-based count of observed futures
bars; canonical `k` is the zero-based index in the session bar array. The
diagnostic projects Paper sequence to `sequence - 1` only in the adapter via
`observed_bar_count_to_index`; Paper still passes its original sequence to
thesis/cooldown/risk gates. A focused regression test covers the projection.
Canonical capture `session`/`direction` values are policy labels attached by
the runner's raw-capture projection, not fields assigned by raw event
extraction. The diagnostic resolves the active configured policy by event
minute and uses time-derived session/`none` labels when no cell policy is
active. Candidate identity, score, score factors, and stop basis remain
compared as causal fields. The canonical raw capture's `reason` is an
extraction/capture filter annotation; it is not a decision rejection reason.
The comparator now reports it separately and marks the canonical decision
outcome/rejection reason unavailable instead of comparing unlike fields.

Read-only candidate identity comparison still mismatches on all three dates:

- Sep 3: 353 canonical / 354 Paper; Paper-only `(2026-09-03, 15:10,
  vwap_zone+or_low)`. First shared difference: `stop_basis` at 09:17,
  `vwap_zone` (19.795734579455072 canonical vs 19.772840324987865 Paper).
- Sep 15: 353 / 354; Paper-only `(2026-09-15, 15:10, or_low)`. At 09:17,
  `vwap_zone+prior_day_high`, canonical capture has score 0 and annotates
  `setup_score_skip`; Paper emits a candidate with score 1 then rejects it as
  `cell_not_configured`. These annotations come from different flow stages and
  are not equivalent rejection reasons. Score, score factors, and stop basis
  also differ.
- Sep 29: 266 / 267; Paper-only `(2026-09-29, 14:37, vwap_zone)`. First shared
  difference: `stop_basis` at 09:17, `vwap_zone` (31.863392307014266 canonical
  vs 47.7782666080056 Paper).

Flow trace for Sep 15: canonical `run_sessions` calls
`collect_qualified_events` with policy cells and the session score policy;
`signal_extraction.py` converts scores below the weak threshold (2) to
`Skip` and omits them before session-window filtering and policy decision
creation. The separate parity-only capture calls that same extractor over all
cells and records `setup_score_skip` as an extraction annotation. Paper's live
decision path detects the location, emits `CANDIDATEDECISION`, then checks its
morning cell configuration before its score gate, producing
`cell_not_configured`. Thus the previous message incorrectly described the
capture annotation as a canonical decision rejection. The canonical source path
does have a score qualification filter; no claim is made that it emits a
decision-level score rejection for this identity.

Input diagnostics still match on all three dates: bars, supporting bars, PCR,
prior-day levels, and feature/location/transition traces (counts 354, 354,
319). The one-extra-candidate identities are not yet explained. The remaining
shared differences are causal and have not been ignored or attributed to a
confirmed source behavior mismatch. No Paper strategy or canonical behavior
was changed for Layer 4. Layers 5–8 remain blocked.

Validation performed:

- `.venv\Scripts\python.exe -m pytest ftx-paper\tests\test_parity_trace.py ftx-paper\tests\test_session.py ftx-paper\tests\test_core_boundaries.py ftx-paper\tests\test_location_engine.py tests\test_scoring_parity.py -q` — 98 passed.
- `.venv\Scripts\python.exe scripts/ftx/compare_replay_inputs.py` — read-only; input/features matched on all three available dates.
- `.venv\Scripts\python.exe scripts/ftx/compare_candidate_replay.py` — read-only; identity/field differences above.
- `.venv\Scripts\ruff.exe check ftx-paper/src/ftx_paper/core/features.py ftx-paper/src/ftx_paper/core/live_decision.py ftx-paper/src/ftx_paper/core/location_engine.py ftx-paper/src/ftx_paper/runtime/replay_worker.py ftx-paper/src/ftx_paper/contracts/parity_trace.py ftx-paper/tests/test_core_boundaries.py ftx-paper/tests/test_location_engine.py ftx-paper/tests/test_session.py ftx-paper/tests/test_parity_trace.py tests/test_scoring_parity.py scripts/ftx/compare_candidate_replay.py scripts/ftx/compare_replay_inputs.py` — passed.
- `git diff --check` — passed.

Focused validation after the score-factor fix:

- Canonical scoring/comparator/location/runner suite: 76 passed.
- Paper core/session/location/parity-trace suite: 93 passed.
- Ruff passed for the changed score implementation and test; `git diff --check`
  passed.

### Score parity recheck (2026-09-30)

The cross-implementation fixture test passes for both score calculations and
score tiers (4 tests passed). Both active implementations use the same
time-neutral factor rules, and both expose `setup_score_skip_filter=True` by
default. This validates the scoring contract on fixtures; it does not validate
fixed-date replay outputs.

Static tracing found a diagnostic context bug that can explain the earlier
Sep 15 score difference: `_canonical_trace` loaded the preceding trading
date's bars into `arrays` but called `run_sessions` with
`days=[session_date]`. `collect_qualified_events` previously derived prior-day
high/low only from the preceding entry in `days`, so this invocation supplied
`None` prior levels to canonical score construction. Paper had its prior-day
levels. The collector now treats `days` as the extraction target and the
earlier entries in `arrays` as warmup context; it extracts only the target
session while using the latest earlier array for prior-day levels. The
diagnostic remains routed through `run_sessions` and its canonical configured
session path. Its comparison is explicitly limited to candidate-stream
identity and shared candidate fields; Paper decision outcomes are not compared
because the canonical parity capture does not expose equivalent outcomes.

The corrected fixed-date diagnostic has **not** been run. The configured
`data/db/nifty_slim.duckdb` is absent, and the user instructed us to stop
database access after an unverified copy was discovered. The attempted
read-only copy run also reached a canonical expiry-calendar gap; no database
was modified. Earlier fixed-date candidate score/stop differences are not
considered verified until rerun with the user's designated canonical data
source. Layer 4 remains unchecked and Layers 5–8 remain blocked.

### Track P11 — Coexistence validation

- [ ] Run the complete holdout replay and achieve zero unexplained differences in required shared execution/settlement fields
- [ ] Obtain explicit approval for any remaining intentional differences
- [ ] Record the coexistence-symmetry result, configuration hashes, fixtures, and holdout artifacts
