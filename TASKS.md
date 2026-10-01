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
default.

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

After the user fixed the missing Canonical NIFTY index bars, both configured-
path diagnostics were rerun read-only. The input comparison now matches on all
three dates, including Sep 3's 356 NIFTY index bars and normalized fingerprint.
Candidate numeric scores match at 09:17 on all three dates (score 1 on both
sides). Canonical capture does not publish score factors for score-filtered
candidates, so those factor maps are explicitly treated as unavailable.

Candidate traces still mismatch: Paper-only identities are Sep 3 15:10
`vwap_zone+or_low`, Sep 15 15:10 `or_low`, and Sep 29 14:37 `vwap_zone`. The
first shared difference is `stop_basis` at 09:17 on each date: Sep 3
19.795734579455072 vs 19.772840324987865; Sep 15 64.07014352562167 vs
64.04809448952713; Sep 29 47.7950884605214 vs 47.7782666080056 (canonical vs
Paper). Decision outcomes remain outside this comparator because canonical
parity capture does not expose equivalent outcomes. The input report still
notes that Paper's effective expiry list lacks the expected 2026-09-08 expiry
for Sep 3, even though compared PCR values and the overall input trace match.
No replay state or research artifacts were written. Layers 4–8 remain blocked.

Latest focused validation: `.venv\Scripts\python.exe -m pytest
tests\test_scoring_parity.py ftx-paper\tests\test_parity_trace.py -q` — 10
passed; focused Ruff and `git diff --check` passed. The configured-path
diagnostic commands were `.venv\Scripts\python.exe
scripts/ftx/compare_candidate_replay.py` and `.venv\Scripts\python.exe
scripts/ftx/compare_replay_inputs.py`.

### Layer 4 continuation (2026-09-30)

Event-time tracing confirmed the first shared stop discrepancy was caused by
Paper dividing the pre-event ATR by the previous bar close; canonical divides
by the current event bar close (`safe_atr(day_arr, k)`). Paper now accepts the
event close for normalization, and both the candidate stop calculation and
risk sizing path supply the entry/event price. A focused regression covers the
prior-range/current-close distinction. The configured-path comparison now
passes the 09:17 stop values and advances to later causal differences; no stop
field was ignored.

The fixed-date input diagnostic still reports `MATCHED` on Sep 3, Sep 15, and
Sep 29, with matching normalized fingerprints and feature/location/transition
counts of 354, 354, and 319. Sep 3 still lacks the expected Sep 8 expiry in
Paper's effective expiry list, but it is not an expiry day on either side and
PCR values still match at every compared minute.

Candidate identities remain 353 canonical / 354 Paper on Sep 3, 353 / 354 on
Sep 15, and 266 / 267 on Sep 29. The Paper-only identities remain Sep 3
15:10 `vwap_zone+or_low`, Sep 15 15:10 `or_low`, and Sep 29 14:37 `vwap_zone`;
each Paper event is rejected as `outside_session_window`, while canonical raw
extraction emits no candidate at that exact identity. The mismatch remains
unresolved; capture annotations are not substituted for decision rejection
reasons.

With stop parity corrected and Paper `buy`/`sell` labels normalized to canonical
`long`/`short` at the comparator boundary, the first remaining shared-field
mismatches are score: Sep 3 12:36 `session_low+or_low` (1 canonical / 0 Paper),
Sep 15 13:21 `session_low+or_low` (4 / 3), and Sep 29 09:36
`vwap_zone+session_low+or_low` (2 / 1). The canonical raw capture does not
publish score-factor maps, so these differences are not attributed to a
specific factor or changed speculatively. The comparator's identity-aligned
single-record comparison now reindexes ordinals before constructing a trace;
a focused regression covers both ordinal normalization and direction-label
normalization.

Layer 4 remains incomplete and Layers 5–8 remain blocked. No canonical behavior,
frozen baseline, replay state, or research artifact was changed or written.
Latest validation:

- `.venv\Scripts\python.exe -m pytest ftx-paper\tests\test_core_boundaries.py ftx-paper\tests\test_parity_trace.py ftx-paper\tests\test_session.py tests\test_compare_candidate_replay.py -q` — 93 passed.
- `.venv\Scripts\ruff.exe check ftx-paper/src/ftx_paper/core/adaptive_stop.py ftx-paper/src/ftx_paper/core/live_decision.py ftx-paper/src/ftx_paper/core/risk.py ftx-paper/tests/test_core_boundaries.py scripts/ftx/compare_candidate_replay.py tests/test_compare_candidate_replay.py` — passed.
- `.venv\Scripts\python.exe scripts/ftx/compare_candidate_replay.py` — read-only; reports the candidate counts, identities, and score differences above.
- `.venv\Scripts\python.exe scripts/ftx/compare_replay_inputs.py` — read-only; inputs and feature/location/transition traces matched on all three dates.
- `git diff --check` — passed in final validation.

### Layer 4 VIX and terminal-bar follow-up (2026-09-30)

The score mismatches above were traced to Paper's opening-VIX lookup, not to
the scoring formula. Replay represents VIX as an `INDEX` instrument named
`INDIAVIX` (or `INDIA VIX`), while `vix_open_and_event` previously accepted
only instrument type `VIX`. Paper therefore left `vix_open` unset and its
`self._vix_open or vix_bar.close` fallback substituted event VIX for opening
VIX. At each previously reported score mismatch, canonical's only positive
factor beyond Paper's factors was `vix_spike`; canonical VIX open/event values
were 10.89/11.44 on Sep 3, 12.31/12.93 on Sep 15, and 13.83/14.63 on Sep 29.
Paper now recognizes the replay's VIX index symbols, has no event-value
fallback, and explicitly rejects missing opening VIX with `missing_vix`.
Focused tests cover the replay instrument representation and missing-open
rejection.

After this fix, the candidate comparator reports no shared identity field or
ordered differences on any of the three dates. Counts remain 353 canonical /
354 Paper on Sep 3, 353 / 354 on Sep 15, and 266 / 267 on Sep 29. The sole
Paper-only identity is the last available futures minute on each date: Sep 3
15:10 `vwap_zone+or_low`, Sep 15 15:10 `or_low`, and Sep 29 14:37 `vwap_zone`.
Direct inspection confirms each is index `k=n-1`; canonical
`_extract_events_retargeted` intentionally skips it via `if k + 1 >= n`, while
Paper emits the completed-bar candidate and rejects it as
`outside_session_window`. This is a verified terminal-bar stream difference,
not an unexplained shared-field mismatch; it remains a candidate-count parity
failure and Layer 4 stays incomplete. No causal field is excluded to obtain
this result; canonical score-factor maps remain unavailable from the raw
capture projection as noted above.

The read-only input diagnostic still matches all three dates and fingerprints.
The expected Sep 8 expiry is still absent from Paper's effective expiry list
for Sep 3, but Sep 3 is not an expiry day and per-minute PCR remains matched.
No database, canonical implementation, frozen baseline, replay state, or
research artifact was changed.

Latest follow-up validation:

- `.venv\Scripts\python.exe -m pytest ftx-paper\tests\test_core_boundaries.py ftx-paper\tests\test_parity_trace.py ftx-paper\tests\test_session.py tests\test_compare_candidate_replay.py tests\test_scoring_parity.py -q` — 99 passed.
- `.venv\Scripts\ruff.exe check ftx-paper/src/ftx_paper/core/features.py ftx-paper/src/ftx_paper/core/adaptive_stop.py ftx-paper/src/ftx_paper/core/live_decision.py ftx-paper/src/ftx_paper/core/risk.py ftx-paper/tests/test_core_boundaries.py scripts/ftx/compare_candidate_replay.py tests/test_compare_candidate_replay.py` — passed.
- `.venv\Scripts\python.exe scripts/ftx/compare_candidate_replay.py` — read-only; zero shared-field differences, terminal-bar count differences above.
- `.venv\Scripts\python.exe scripts/ftx/compare_replay_inputs.py` — read-only; matched on all three dates.

### Layer 4 terminal-event parity recheck (2026-09-30)

Canonical terminal-bar extraction now retains the causal event; forward
research horizons are explicitly `None` when unavailable, and outcome means,
sample counts, and fitted-prior minimum-N checks use only events with an
available 5-minute label. Focused tests verify terminal extraction and
unavailable outcomes. This preserves event identity without fabricating a
forward label.

The read-only candidate comparison now reports `MATCHED` on all three dates,
with no missing, extra, duplicate, or shared-field differences: 354 canonical /
354 Paper candidates on Sep 3; 354 / 354 on Sep 15; 267 / 267 on Sep 29. The
input comparison also remains `MATCHED` on all three dates, including bars,
supporting inputs, PCR, prior-day levels, feature/location/transition traces,
and normalized fingerprints. The Sep 8 expected expiry remains absent from
Paper's Sep 3 effective expiry calendar, but Sep 3 is not an expiry day and no
per-minute PCR differences were reported.

This validates candidate stream parity, not full Layer 4 decision parity: the
candidate diagnostic explicitly sets `decision_outcomes_compared=false` because
the current canonical raw capture does not expose equivalent policy outcomes
or decision rejection reasons. Capture-filter annotations remain separate
from those unavailable outcomes. Layer 4 therefore remains incomplete; Layers
5–8 remain blocked.

Latest validation:

- `.venv\Scripts\python.exe -m pytest tests\test_location_engine.py tests\test_location_research.py -q` — 12 passed.
- `.venv\Scripts\python.exe -m pytest ftx-paper\tests\test_core_boundaries.py ftx-paper\tests\test_parity_trace.py ftx-paper\tests\test_session.py tests\test_compare_candidate_replay.py tests\test_scoring_parity.py -q` — 99 passed.
- `.venv\Scripts\ruff.exe check src/ftx/location_engine.py src/ftx/location_research.py tests/test_location_engine.py tests/test_location_research.py ftx-paper/src/ftx_paper/core/features.py ftx-paper/src/ftx_paper/core/adaptive_stop.py ftx-paper/src/ftx_paper/core/live_decision.py ftx-paper/src/ftx_paper/core/risk.py ftx-paper/tests/test_core_boundaries.py scripts/ftx/compare_candidate_replay.py tests/test_compare_candidate_replay.py` — passed.
- `.venv\Scripts\python.exe scripts/ftx/compare_candidate_replay.py` — read-only; candidate stage matched on all three dates.
- `.venv\Scripts\python.exe scripts/ftx/compare_replay_inputs.py` — read-only; input stage matched on all three dates.
- `git diff --check` — passed.

### Layer 4 expiry-calendar repair (2026-09-30)

The Paper runtime `expiry_dates` reference table was empty (0 rows), not just
missing Sep 8. The configured canonical `weekly_expiry_dates` repository
returned 417 dates. Added `RuntimeStore.replace_expiry_dates`, an idempotent
Paper-owned API that replaces only the expiry reference table, validates and
normalizes ISO dates, and refuses writes through a read-only store. The focused
store test verifies calendar replacement/idempotence and that an existing
replay run is untouched. The Paper runtime table was synchronized from the
configured canonical calendar: it now contains all 417 dates, including
2026-09-08. No replay state or research artifact was written.

After sync, `compare_replay_inputs.py` reports Sep 3's expected expiry present
in Paper's effective calendar, PCR difference count 0, matching normalized
fingerprints, and `MATCHED` input status on all three dates. The candidate
diagnostic remains `MATCHED` with counts 354, 354, and 267 on both sides.
Decision outcomes remain outside its scope; Layer 4 remains incomplete until
equivalent canonical outcome/rejection capture is available.

Latest validation:

- `.venv\Scripts\python.exe -m pytest ftx-paper\tests\test_store.py ftx-paper\tests\test_core_boundaries.py ftx-paper\tests\test_parity_trace.py ftx-paper\tests\test_session.py tests\test_compare_candidate_replay.py tests\test_scoring_parity.py -q` — 106 passed.
- `.venv\Scripts\ruff.exe check ftx-paper/src/ftx_paper/runtime/store.py ftx-paper/src/ftx_paper/core/features.py ftx-paper/src/ftx_paper/core/adaptive_stop.py ftx-paper/src/ftx_paper/core/live_decision.py ftx-paper/src/ftx_paper/core/risk.py ftx-paper/tests/test_store.py ftx-paper/tests/test_core_boundaries.py scripts/ftx/compare_candidate_replay.py tests/test_compare_candidate_replay.py` — passed.
- `.venv\Scripts\python.exe scripts/ftx/compare_replay_inputs.py` — read-only; expected expiry present, PCR matched, inputs matched.
- `.venv\Scripts\python.exe scripts/ftx/compare_candidate_replay.py` — read-only; candidate stage matched on all three dates.
- `git diff --check` — passed.

### Track P11 — Coexistence validation

- [ ] Run the complete holdout replay and achieve zero unexplained differences in required shared execution/settlement fields
- [ ] Obtain explicit approval for any remaining intentional differences
- [ ] Record the coexistence-symmetry result, configuration hashes, fixtures, and holdout artifacts
