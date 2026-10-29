# FTX Paper Orderflow Implementation Plan

This document is the implementation roadmap for the native FTX Paper orderflow
strategy. It defines the order in which the system is built, the contracts each
stage must provide, and the tests required before the next stage starts.

Behavioral research from `C:\Users\agraw\Python\NiftyZoning\src\orderflow`
may inform design decisions, but FTX Paper owns its implementation, fixtures,
runtime state, and tests. It must not import NiftyZoning, read its runtime state,
or modify its files.

Producer-neutral parity artifacts, normalization, and mismatch reporting remain
separate deliverables. Their contract and roadmap are defined in
`docs/PRODUCER_NEUTRAL_PARITY.md` and
`docs/PRODUCER_NEUTRAL_PARITY_IMPLEMENTATION_PLAN.md`. The paired replay
validation required by Phase 9 is part of this plan: it compares Paper's
diagnostic decisions with its opt-in executable projections and validates the
resulting simulated lifecycle. Phase 9 paired replay is Paper-internal
validation; it is not the cross-producer parity verdict. Cross-producer verdicts
use only the four-layer artifacts and comparator defined by the parity documents.

## Goals And Guardrails

The implementation must:

1. Make every decision from completed evidence available at that decision time.
2. Preserve source lineage, availability, and immutable reference identity.
3. Advance typed state deterministically on one decision clock.
4. Keep observation, interaction, response, opportunity, admission, and
   execution state distinct.
5. Preserve the causal per-bar ordering defined below.
6. Keep diagnostic mode non-executable until the complete decision pipeline is
   validated.

### Reference-Fidelity Protocol

This plan is a porting specification, not authorization to invent new Paper
behavior. Before changing code for any mapped behavior, implementation must
record a Paper-owned mapping containing every field below:

```text
Canonical behavior:
Canonical source symbol or test:
Paper ownership boundary:
Existing Paper contract:
Minimal adaptation:
Production-path acceptance test:
Undefined-behavior result:
```

The mapping is incomplete if any field is missing. In particular:

- No new public field, class, property, enum value, lifecycle transition,
  authorization state, or fallback value may be introduced without an explicit
  canonical source or an explicit requirement elsewhere in this plan.
- Phrases such as "enable", "authorize", "eligible", or "executable" do not
  define a new state or contract by themselves. Use the existing Paper
  execution owner and existing types unless the canonical reference defines a
  distinct contract that must be reproduced.
- Admission and diagnostic projections must not be promoted into executable
  intents, authorization, reservations, fills, settlement, or portfolio state.
  Those transitions remain owned by the existing Paper execution boundary.
- If the canonical reference does not define a policy, transition, fill rule,
  accounting result, or serialization field, the Paper result must remain
  explicitly unavailable with a machine-readable reason. Do not infer neutral,
  passed, authorized, zero, stale, or equivalent fallback behavior.
- A test that only invokes a generic helper, substitutes callbacks, or asserts
  a static phase list cannot establish live/replay integration behavior. Claims
  about runtime ordering or execution must invoke the relevant production path
  (`OrderflowPolicy`, `PaperEngine`, `RuntimeSession`, or replay orchestration)
  and assert observable Paper state or persisted events.
- Reference code and tests are read-only behavioral evidence. Copying names or
  structure is not evidence of a contract, and Paper-specific adaptations must
  preserve the ownership boundaries defined in this plan.

Implementation must stop rather than extend a contract when the mapping cannot
be completed. Record the gap as unavailable and defer it for an explicit scope
decision; do not resolve it with a speculative helper or convenience property.

### Canonical Strategy And Ownership Boundary

`C:\Users\agraw\Python\NiftyZoning\src\orderflow` is the behavioral authority
for this strategy. Paper is an independent implementation of that behavior and
an execution/accounting host; it is not a second strategy design. Every
location, response, score, trigger, expiry, conflict group, permission, and
exit rule must map to a canonical NiftyZoning symbol, test, or an existing Paper
contract explicitly required by this plan. Paper must not add a setup family,
score interpretation, fallback, lifecycle transition, or admission rule merely
because it is convenient to implement.

The ownership split is non-negotiable:

- NiftyZoning behavior defines market observations, causal interactions,
  responses, opportunity templates, score semantics, trigger rules, and lifecycle
  transitions. Paper's strategy layer ports these contracts without inventing
  policy.
- `OrderflowReplayInput` and the existing `MarketObservation`/observation
  contracts are the source boundary. Extend those existing contracts only when
  a corresponding canonical field is proven; do not add a parallel source
  snapshot type. Prior-session rows, source fingerprint, gaps, and ATR must be
  passed through the existing replay/observation inputs with explicit
  availability.
- Port NiftyZoning's `build_context_scores()`/`_score_for_aggregate()` causal
  recursion into the existing Paper scoring boundary only after mapping its
  inputs to existing Paper aggregate/observation contracts. It must consume
  exact causal raw rows, completed aggregates, and the already existing frozen
  ATR input; it must not average producer-emitted score fields or introduce a
  new score state type without a canonical counterpart.
- The producer publishes normalized bars, aggregates, and lineage only. It does
  not publish final decision scores or decide score permission.
- Candidate formation may create a proposal and evidence, but cannot reserve
  portfolio capacity or consume an execution group. Portfolio gates, risk
  sizing, pending-entry re-admission, fill-price calculation, group claim, and
  position creation are one atomic responsibility of the existing Paper
  execution owner, matching NiftyZoning's `handle_execution()` and lifecycle
  fill path.
- A group identifier is metadata before fill, not ownership. The claim is
  consumed only after a fill succeeds; unfilled siblings remain diagnosable and
  are invalidated only after that claim is consumed.
- The proposed generic labels `interaction`, `candidate_formed`,
  `admission_evaluated`, `intent_armed`, `trigger_observed`, `entry_pending`,
  and `filled` are reporting boundaries, not permission to add a new strategy
  state machine. Paper must map them onto the canonical lifecycle stages and
  intent/execution statuses already defined by NiftyZoning and existing Paper
  contracts. If a one-to-one mapping is unavailable, retain the label only as
  an evidence/event projection and leave the typed state unavailable.

If the canonical source is silent, Paper records `unavailable` with a reason
and does not choose a default strategy. This rule overrides any generic phase
wording below.

### Minimal Delivery Scope

The implementation is intentionally minimal. A requirement is included in the
current delivery only when it protects causal correctness, Paper execution
safety, deterministic replay, or the existing audit/accounting contract.

#### Required Now

- One live path through `RuntimeSession`, `PaperEngine`, `OrderflowPolicy`, and
  the existing execution owner.
- One replay path through the existing replay worker and `ReplayMachine`.
- Completed-evidence availability, source identity, deterministic decisions, and
  no future-data leakage.
- Diagnostic mode with zero execution or portfolio mutation.
- Explicit opt-in executable mode using existing Paper orders, reservations,
  fills, exits, settlement, and ledger state.
- Production-path tests for next-available-bar timing, existing-position
  management, duplicate/idempotent fills, and diagnostic immutability.

#### Required Before Executable Release

- Paired replay checks for decisions, accepted orders, fills, exits, settlement,
  and ledger results.
- Reference replacement and permission changes visible before same-timestamp
  execution.
- Undefined canonical behavior retained as unavailable rather than inferred.

#### Deferred Unless Required By A Failing Contract

- New generic strategy abstractions or parallel lifecycle owners.
- Exhaustive intermediate-object IDs or provenance fields that are not persisted,
  compared, or required for a decision.
- Presentation-only projections beyond the persisted live-event and replay
  reporting contracts defined in Phase 10.
- Cross-producer parity artifacts and comparator enhancements. Those remain
  separate deliverables and must not block the Paper live/replay path.
- Full-capture detail beyond what changes calculations, availability, decisions,
  execution, or accounting.

Deferred work must not be implemented speculatively. It becomes active only when
an existing contract, a production failure, or an explicit scope decision
requires it. When requirements conflict, this minimal scope takes precedence
over completeness or generality.

`OrderflowConfig.execution_mode` defaults to diagnostic, and diagnostic
processing must create zero executable entries, stops, exits, fills,
reservations, or portfolio mutations. The explicit executable mode is already
available for Paper-owned replay and runtime tests, but it is not enabled by
default and is not Phase 9 release-complete until the remaining execution,
paired-replay, and lifecycle acceptance criteria pass. No environment or
implicit eligibility path may switch it on.

## Phase 9 Scope Decisions

These decisions resolve the remaining P9 scope while aligning VWAP stop behavior
with the causal NiftyZoning reference pipeline.

### Authoritative Executable Scope

The authoritative scope is the executable behavior already represented by
NiftyZoning's causal orderflow pipeline. Paper may read those modules and tests as a behavioral reference only;
it must not import, execute, or modify NiftyZoning. The scope includes only
remaining canonical location/response/opportunity templates and their defined
admission, trigger, management, exit, duplicate, and settlement rules. It
includes VWAP location and response evaluation alongside the structural
templates. Structural acceptance templates may use the causal `OR_VWAP`
protective-stop policy.

If the reference does not define a policy, transition, fill rule, or accounting
result, Paper records an explicit unavailable reason and does not infer a
replacement rule.

### Validated NiftyZoning Behavior

The read-only validation against `C:\Users\agraw\Python\NiftyZoning` found the
following executable contracts to reproduce:

- `src/orderflow/state_machine.py` publishes session-extrema replacements,
  manages existing execution state, updates permissions, advances interactions,
  expires opportunities before formation, admits eligible opportunities,
  evaluates management, evaluates new entries, and selects proposals in that
  causal order.
- `src/orderflow/execution.py` ranks same-bar proposals by setup availability,
  structural rank, lifecycle identity, and intent identity; suppresses duplicate
  causal identities; applies active-position/regime/portfolio gates; and records
  denied proposals rather than silently dropping them.
- The same execution module uses next-available-bar timing for selected entries,
  discretionary exits, partial 3R exits, and protective-stop exits, while
  preserving observed and available timestamps separately.
- `src/orderflow/lifecycle.py` owns reservations, entry fills, partial-exit
  fills, exit fills, idempotent lifecycle transitions, and realized points from
  the authoritative fill sequence. Paper maps these responsibilities to its
  existing `PaperExecutionCoordinator` and portfolio/ledger boundaries rather
  than copying NiftyZoning's owner.
- `src/orderflow/templates.py` defines the remaining response, trigger,
  conflict-group, exit-rule, and expiry identities. Paper reproduces the
  `OR_VWAP` stop policy for the corresponding structural acceptance templates,
  while retaining VWAP location observations and responses.

### Paper Execution Opt-In

Executable orderflow is enabled only by constructing the existing
`OrderflowConfig` with `execution_mode=ExecutionMode.EXECUTABLE` and passing
that config through the existing strategy factory/runtime composition. The
default remains `ExecutionMode.DIAGNOSTIC`; no environment or implicit
eligibility path may switch it on. Enabling the mode authorizes proposals to
the existing Paper execution owner, but does not authorize broker/live
execution. Executable authorization is available through the strategy/runtime
path. The standalone `evaluate_admission()` helper currently evaluates the
same strategy gates for both modes and always returns a detached diagnostic
projection; it does not accept a `diagnostic` argument and does not create
execution state. Pending-entry portfolio, sizing, risk, and execution-group
authorization must remain owned by the execution boundary before Phase 9 is
marked complete.

### Parity Coverage

Paired replay validation covers the complete defined Paper projection, not just
decision labels: canonical decisions and predicates, diagnostic eligibility
projections, executable authorization, risk/sizing, trigger and entry timing,
position management, exits, duplicate handling, fills/partial fills,
reservations, settlement, and ledger results. The diagnostic side remains
mutation-free and supplies the causal decision projection; the executable side
may additionally produce orders and lifecycle/accounting facts by mutating only
the existing Paper portfolio, capital, and execution state. Comparisons must
distinguish shared decision/provenance fields from executable-only lifecycle
facts and compare the latter against the corresponding diagnostic absence or
unavailable result rather than requiring diagnostic mutations. Comparisons are
made at each causal boundary using canonical IDs, evidence, timestamps,
availability, generations, source IDs, and unavailable reasons. VWAP location
evaluation is covered by paired fixtures; `OR_VWAP` protective-stop behavior is
covered by executable acceptance fixtures.

Scope and decision-clock equivalence is checked before population comparison.
Paired replay first aligns source/session bounds, cutoff inclusivity, and
selected/completed/gap/processed counts, then compares causal phases from source
through execution. This Paper-internal validation does not replace the
producer-neutral four-layer artifact comparison.

## Runtime Shape

The strategy is built as a causal live decision engine around these layers:

```text
source rows
  -> OrderflowReplayInput + MarketObservation (rows, history, gaps, ATR input)
  -> completed 1m/5m aggregates
  -> existing causal score/provenance fields
  -> session context, values, and permissions
  -> as-of location registry
  -> location observations and interactions
  -> response hypotheses and outcomes
  -> opportunities and structural setups
  -> candidate evidence and canonical admission predicates
   -> execution and accounting (opt-in only)
    -> persisted runtime events / replay facts
      -> diagnostic and reporting projections
```

The layers are dependency boundaries, not mandatory modules. A later layer may
consume only the completed, available contracts published by earlier layers.

### Live Ownership Boundary

This plan applies to the live Paper runtime. `RuntimeSession` remains the one
in-process application/runtime owner, and `OrderflowPolicy` remains the live
orderflow lifecycle owner within the strategy engine. The policy's existing
state is authoritative for interactions, response state, opportunities,
strategy-derived context history, permission projections, admission
projections, and live policy snapshots. Market input and completed observations
remain producer-owned; account, recovery, authorization, reservations, and
execution state remain owned by the shared coordinator and `RuntimeSession`.
Do not introduce a second `LiveOrderflowRuntime`, coordinator, or parallel
interaction store.

The implementation should refactor `OrderflowPolicy` in place: its existing
state and lifecycle transitions remain the live path, while admission evaluation
uses the existing `orderflow_admission.py` contract and its decisions are
published through the shared `PolicyDecision` boundary. Any runtime event
projections emitted by the live policy must reflect those same transitions, not
a second lifecycle implementation. Policy snapshot and restore remain the
single live snapshot boundary.

The project already has replay functionality. `ReplayMachine` in
`orderflow_replay.py` remains a replay-only consumer/orchestrator and must not be
inserted into the live strategy callback path or made the owner of live policy
state. Reuse the existing replay path for replay tests and replay capture;
share only pure contracts and helpers where appropriate. Mutable state,
orchestration, and snapshot ownership remain distinct between live policy
execution and replay.

The live orderflow policy boundary is `OrderflowPolicy.evaluate()` and its
existing strategy/runtime call path after the producer has published completed
market evidence. The complete causal order spans the existing runtime,
producer, shared coordinator, and policy calls; the policy must preserve its
orderflow lifecycle sub-order without adopting replay-only input loading,
replay clocks, or replay ownership.

### Refactor Ownership Boundaries

Refactor `src/ftx_paper/strategy/orderflow.py` in place. The ownership rules in
the preceding sections and the causal order below are authoritative; the
canonical NiftyZoning split must not become a second Paper state machine or
execution owner.

Safe extractions are limited to configuration, snapshot codecs, pure causal
scoring/evidence/ID helpers, and pure event-payload builders. Extracted helpers
must receive causal inputs explicitly and return typed decisions or projections;
they must not load replay rows, advance a second clock, persist events, or
mutate portfolio or execution state. `ReplayMachine` remains replay-only.

Refactor acceptance must continue to exercise the production path and this
causal order:

```text
producer evidence
  -> context and permission projections
  -> existing-position management
  -> interaction and response advancement
  -> opportunity expiry and formation
  -> strategy admission and trigger projection
  -> shared execution authorization and accounting
  -> runtime persistence/reporting
```

Do not add public lifecycle state, fallback values, or compatibility fields
without completing the reference-fidelity protocol. If an extracted boundary
cannot prove its evidence or ownership, retain an explicit `unavailable`
result and defer the change.

## Causal Per-Bar Contract

Every completed live bar follows this causal order across the existing runtime
components. `OrderflowPolicy.evaluate()` owns only the policy lifecycle steps;
it does not re-ingest source rows or manage portfolio execution:

1. The existing input/producer path normalizes the bar, handles session
   boundaries, detects gaps, and publishes only completed available observations.
2. The shared runtime/coordinator updates account, execution, recovery, and any
   externally owned authorization inputs. `OrderflowPolicy` then publishes its
   as-of context and strategy permission projections before new proposals.
3. `OrderflowPolicy` consumes prepared 1m/5m observations in causal order,
   advances interactions and response state, and expires completed lifecycles.
4. `OrderflowPolicy` forms opportunities and evaluates admission using current
   context and gate inputs; it does not manage positions, orders, fills,
   reservations, or portfolio mutations.
5. The shared coordinator arbitrates policy proposals and applies the existing
   diagnostic execution boundary.
6. `RuntimeSession` persists the existing policy decision and runtime event
   outputs after the bar's decisions complete. No additional live diagnostic
   record is created; replay capture is performed only by the existing replay
   path.

The ordering is part of the contract. In particular, existing execution state
is managed before new proposals, reference replacements precede execution at
the same timestamp, interactions precede opportunity formation, and recording
cannot feed information back into decisions. Live diagnostics are projected
from the persisted runtime events produced after decisions; they are not an
additional per-bar replay-record or capture owner.

## Implementation Phases

### Phase 0: Foundation And Diagnostic Boundary

Establish the native package boundaries, configuration, deterministic IDs, typed
records, and the diagnostic execution guard.

Required contracts:

- Explicit session date, timezone, market session, vehicle, execution mode, and
  capture mode, validated at the public entry point.
- Stable deterministic IDs for source, session, aggregate, observation,
  location, interaction, response episode, opportunity, decision, intent, and
  runtime event. IDs must include enough scope to avoid collisions across
  sessions, timeframes, and source generations.
- Typed unavailable values with a machine-readable reason and availability
  boundary; never substitute `NEUTRAL`, zero, an older value, or a display
  string for missing evidence.
- `OrderflowPolicy` is the sole live orderflow lifecycle owner. Its existing
  interaction and processed-observation state is retained and normalized in
  place; no parallel live owner or duplicate lifecycle store is introduced.
- The existing `orderflow_admission.py` contract is the live admission boundary;
  the policy must not emit an incompatible legacy admission decision shape.
- The existing project replay path, including `ReplayMachine`, remains
  replay-only and is reused rather than replaced or embedded in live execution.
- Keep policy diagnostic decision projections separate from executable order
  intents. Diagnostic mode may record eligibility, predicates, and a
  non-executable projection, but must not create executable intents or mutate
  accounting state.
- Policy snapshots and the existing runtime event persistence boundary must not
  expose mutable policy state. `ReplayRecord` detachment remains exclusively a
  replay-path concern; no live `ReplayRecord` projection is required.

Exit criteria:

- The scaffold can process an empty and a one-row session deterministically.
- Diagnostic tests prove zero executable entries, stops, exits, fills,
  reservations, and portfolio mutations across empty, one-row, gap, and
  eligible-decision fixtures.
- Processing the same completed-bar fixture twice produces identical IDs, event
  order, state transitions, and availability results.
- Unavailable-state serialization preserves status and reason without
  presentation fallbacks; detached records remain unchanged after later state
  updates.
- Domain, contracts, strategy, market, execution, and infrastructure imports
  follow the project architecture boundaries.

Phase 0 conformance tests:

- `test_empty_and_one_row_live_session_are_deterministic`
- `test_orderflow_policy_is_the_single_live_lifecycle_owner`
- `test_orderflow_policy_has_no_parallel_lifecycle_store`
- `test_orderflow_policy_snapshot_restore_round_trips_live_state`
- `test_replay_machine_is_not_used_by_live_strategy_runtime`
- `test_diagnostic_eligibility_creates_no_executable_intent_or_mutation`
- `test_identifiers_are_stable_and_scope_safe`
- `test_unavailable_values_preserve_reason_and_availability`
- `test_published_records_are_detached_from_runtime_mutation`

### Phase 1: Market Input And Source Identity

Implement deterministic source resolution before any strategy policy. Replay and
live normalization may use different transports, but both must publish the same
Paper-owned source contract.

Required behavior:

- Validate every ingested row before it enters aggregation. Reject duplicate or
  out-of-order `(session_date, time)` keys; do not silently sort, overwrite, or
  drop a row to make the stream usable.
- Normalize timestamps using the configured timezone and market-session clock,
  while retaining the original source timestamp for provenance.
- Model source identity as a typed tuple containing at least instrument/symbol,
  expiry or continuous-series identity, contract type, exchange/source, and
  session date. The aggregation key must include every identity component that
  can distinguish two instruments with the same symbol.
- Select exactly one deterministic futures source per session. Define the
  continuous-series rule separately from the nearest-dated-contract rule, and
  record selected, eligible, and rejected candidates rather than choosing the
  earliest expiry implicitly.
- Preserve source-row count, ordered-unique-minute status, missing-minute count,
  source fingerprint, identity metadata, and selection diagnostics.
- Compute the source fingerprint from canonicalized source-row content, not from
  derived aggregate IDs or child observation IDs.
- Keep requested dates, selected sessions, rejected sessions, and actually
  replayed sessions as separate fields. `replayed` must not be inferred only
  from whether a non-empty row list happens to exist.
- Resolve the existing `OrderflowReplayInput` and `MarketObservation` before
  replay. They must carry the selected continuous-series identity, normalized
  rows, prior-session history required by the canonical feature builder, source
  fingerprint, and coverage/gap report. Do not introduce a parallel snapshot
  object.
- Materialize Paper's own ATR artifact using the canonical daily-feature
  semantics (prior completed sessions only, continuous front-series history)
  from Paper-owned stored market bars. Persist it through the existing
  `FeatureSnapshot`/`FeatureRepository` input using feature set
  `orderflow_daily_atr_v1`, daily timeframe, continuous-series parameters, and
  a source fingerprint. Preserve its value and provenance in existing
  observation/location fields. A replay worker must never depend on a
  NiftyZoning database, import, or generated artifact.
- Reject or mark the snapshot unavailable when required history, ATR, source
  identity, or coverage cannot be proven. Do not require an arbitrary fixed
  count such as 375 contiguous bars unless that is a canonical source rule.

Exit criteria:

- Invalid ordering, duplicate rows, identity changes, unsupported session rows,
  and ambiguous source selection fail with typed diagnostics.
- A same-symbol stream containing different expiry, exchange, or contract-type
  rows cannot share one continuous replay state.
- Valid input produces a stable market-input contract consumed by Phase 2, with
  deterministic source metadata and fingerprint values.
- A replay cannot start without resolved existing source/observation inputs or an explicit unavailable
  source decision; no downstream score or admission code may silently repair it.
- Paper tests cover out-of-order ingestion, duplicate ingestion, mixed source
  identity, continuous versus dated selection, missing-minute metadata, and
  requested-versus-replayed date accounting.

### Phase 2: Causal Market Observations

Build the completed-evidence producer for the live decision clock.

Required behavior:

- Filter rows to the configured market session before aggregation.
- Use session-aligned windows, not groups of five observed rows. For the
  configured `09:15` session start, 5m windows are `09:15-09:19`,
  `09:20-09:24`, and so on, regardless of whether the first observed row is
  delayed or a minute is missing.
- Publish a 1m aggregate only when its source row is complete. Publish a 5m
  aggregate only when the aligned parent has all required child minutes and the
  final child is complete and available.
- Never shift a parent window to the first observed row, fill a missing child,
  or fabricate an incomplete parent. Keep the incomplete window and its reason
  in diagnostics.
- Attach aggregate ID, timeframe, aligned window boundaries, observed time,
  available time, child source IDs, child lineage, and the original source-row
  fingerprint to every aggregate.
- Propagate source gaps, missing-minute status, boundary gaps, and incomplete
  parent status into the published observation/provenance contract. A separate
  gap detector is insufficient if the observation still reports `gap_status:
  none`.
- Filter session rows before aggregation and ensure out-of-session rows cannot
  alter aggregate windows or fingerprints.
- Expose only aggregates available at the current replay timestamp. Future 1m
  rows, incomplete 5m parents, and post-gap shifted windows must remain
  unavailable.
- Ensure VWAP and every prefix feature consumes the same completed aggregate
  prefix used by replay decisions.

Exit criteria:

- 1m and session-aligned 5m publication is independently testable.
- A non-session-aligned first row does not shift the 5m window origin.
- Missing or delayed children produce explicit gap/incomplete-parent
  diagnostics and never produce a shifted or fabricated 5m aggregate.
- Every aggregate and observation has source-row fingerprint and lineage that
  can be traced back to the exact input rows.
- Availability and gap tests demonstrate that future or incomplete evidence is
  never visible and that gap status is preserved into observations.
- Out-of-session rows are excluded before aggregation and cannot affect counts,
  windows, or fingerprints.
- VWAP and other prefix features read only completed aggregate prefixes.
- Capture-mode tests prove summary, diagnostic, and full capture produce the
  same aggregates, availability decisions, gaps, and calculated observations;
  capture mode may change persistence detail only.

### Phase 2A: Canonical Causal Scores

Implement scores as a pure decision-layer service after the source snapshot and
aggregate graph exist. This phase ports NiftyZoning's
`src/features/contextual.py` recursion; it does not create a Paper scoring
policy.

Required behavior:

- The existing Paper score boundary consumes an aggregate, causal raw rows,
  completed aggregates, and the existing frozen ATR input, returning the
  existing score representation or an explicit unavailable result. Do not add
  `ScoreProvider` or `ScoreSnapshot` types without canonical counterparts.
- Compute the aggregate's own contextual support from its prior aggregate and
  prior volume window, then recursively compute child scores from the exact raw
  rows or completed child aggregates visible at that aggregate's availability
  boundary. Never average score fields previously emitted by a producer.
- For 5m scores, retain exact 1m child row/aggregate identities and the causal
  cutoff in existing observation/provenance fields.
- The producer publishes bars, aggregates, and lineage only. `OrderflowPolicy`
  requests scores at the canonical formation/decision point and applies the
  canonical score permission rule; it does not trust copied producer fields.
- Re-read or recompute the score at pending-entry admission when the canonical
  execution path requires current decision context. Formation-time scores must
  not be treated as fill-time scores.

Exit criteria:

- Complete-history fixtures match canonical recursive 1m and 5m scores;
  changed ATR, missing children, and changed causal cutoffs produce distinct
  snapshots or explicit unavailable results.
- Missing ATR or source history is the first upstream divergence and cannot
  become a neutral, zero, stale, or permitted score.
- Producer state cannot make a score appear or disappear independently of the
  immutable source snapshot and causal graph.

Phase 2A conformance tests:

- `test_score_provider_reproduces_canonical_recursive_one_minute_score`
- `test_score_provider_reproduces_canonical_recursive_five_minute_score`
- `test_five_minute_score_records_exact_child_lineage_and_cutoff`
- `test_score_provenance_uses_existing_observation_and_atr_inputs`
- `test_missing_atr_is_unavailable_not_defaulted`
- `test_producer_does_not_publish_final_decision_scores`

### P1-P2 Conformance Test Set

These tests are implementation acceptance tests, not parity-artifact tests.
They may use equivalent Paper-owned fixtures and probes, but each must assert
the causal contract explicitly:

- `test_rejects_out_of_order_rows`
- `test_rejects_duplicate_rows`
- `test_rejects_mixed_expiry_exchange_or_contract_identity`
- `test_selects_continuous_or_dated_source_by_explicit_rule`
- `test_records_missing_minute_and_ordered_unique_minute_metadata`
- `test_separates_requested_selected_and_replayed_dates`
- `test_aligns_five_minute_windows_to_session_start`
- `test_does_not_shift_parent_after_missing_child`
- `test_does_not_publish_incomplete_five_minute_parent`
- `test_propagates_gap_status_into_observation_provenance`
- `test_preserves_source_row_fingerprint_through_aggregates_and_observations`
- `test_filters_out_of_session_rows_before_aggregation`
- `test_availability_hides_future_one_and_five_minute_evidence`
- `test_capture_modes_preserve_calculation_and_availability_semantics`

P1 and P2 cannot be marked complete while any of these contracts is untested
or failing. Matching Paper aggregate values on complete happy-path fixtures is
not sufficient evidence for completion.

### Phase 3: Session Context, Values, And Permissions

Build the state needed to interpret observations without allowing future context
to leak into earlier decisions.

Required behavior:

- Derive opening range and prior-session levels from completed aggregates and
  prior-session inputs with explicit availability.
- Maintain ordinary market context and an as-of context history.
- Publish value references and session-extrema references before consumers use
  them; replacements create a new generation rather than mutating history.
- Classify opening-gap state at session start and update it from completed 1m
  evidence.
- Suspend admission after a source gap and resume only after the configured
  completed-5m recovery condition.
- Update active-position and directional permission state before new
  interactions or setups.

Exit criteria:

- Context projections are immutable and unavailable when their source is not
  complete.
- Permission decisions include the context version and evidence that produced
  them.
- Opening-gap classification and every state transition are available only from
  completed 1m evidence and retain the opening/value reference IDs used.
- A source gap suspends admission immediately, and admission resumes only after
  the configured completed-5m recovery evidence; no setup or intent can bypass
  the suspension.
- Session-extrema replacement is visible to same-timestamp existing execution
  but cannot rewrite earlier context, interaction, or decision records.

Phase 3 conformance tests:

- `test_context_is_as_of_and_immutable`
- `test_value_reference_requires_complete_prior_profile`
- `test_opening_gap_state_evolves_only_on_completed_one_minute_evidence`
- `test_gap_suspends_admission_until_completed_five_minute_recovery`
- `test_permission_contains_context_version_and_evidence`
- `test_session_extreme_replacement_creates_new_generation`

### Phase 4: Location Registry And As-Of Resolution

Implement immutable location records consumed by both observation timeframes.

Required behavior:

- Register opening-range, prior-day, session-high, and session-low locations
  with IDs, aliases, bands, generation, availability, and source identity.
- Resolve a location as of each observation timestamp.
- Preserve canonical priority and ownership when bands overlap.
- Create a new generation for session boundaries and replacements.
- Apply the configured carry/termination policy to interactions across those
  boundaries.

Exit criteria:

- A later reference replacement cannot alter earlier observations.
- Location identity, geometry, and source lineage survive serialization.
- As-of resolution returns the record available at the observation timestamp,
  never the latest record unconditionally.
- Overlapping bands resolve to the canonical owner and priority deterministically
  across both timeframes.
- Session boundaries create the next generation and apply carry or termination
  exactly once; ended interactions cannot be revived by a later lookup.

Phase 4 conformance tests:

- `test_location_registry_publishes_required_identity_and_lineage`
- `test_as_of_resolution_hides_later_reference_replacement`
- `test_overlapping_locations_use_deterministic_priority_and_owner`
- `test_session_boundary_creates_generation_and_applies_carry_policy`
- `test_location_geometry_and_lineage_survive_detached_serialization`

### Phase 5: Observations And Interaction Episodes

Build independent 1m and completed-5m interaction detectors.

Required behavior:

- Create interactions only from completed candles and available locations.
- Freeze level, band, generation, location, contact candles, and provenance on
  interaction creation.
- Advance interaction status independently from response status.
- Record projections and score snapshots where the selected policy requires
  them.
- Close interactions for cross-band departure, same-side departure,
  outside-band departure, session end, data gap, and reference replacement.

Exit criteria:

- 1m and 5m detectors cannot consume one another's incomplete evidence.
- Interaction lifecycle tests cover creation, advancement, and every close
  reason.
- Interaction snapshots freeze reference level, band, generation, source
  aggregate, observation, and contact candles at creation.
- Interaction status and response status remain independently observable;
  ending an interaction does not implicitly confirm or invalidate its response.
- Duplicate source observations are idempotent and cannot create duplicate
  interaction episodes.

Phase 5 conformance tests:

- `test_creates_interaction_only_from_completed_available_evidence`
- `test_freezes_reference_generation_band_and_contact_lineage`
- `test_one_minute_and_five_minute_detectors_have_independent_clocks`
- `test_interaction_and_response_statuses_are_independent`
- `test_closes_for_cross_band_same_side_outside_band_session_gap_and_replacement`
- `test_duplicate_observation_does_not_duplicate_interaction`

Remaining implementation work:

- Add live rollover regressions for carried and terminated generations,
  including interactions that are active at the boundary. The detector already
  uses completed-candle high/low band intersection, and the policy already owns
  generation advancement and interaction closure.

### Phase 6: Response Policies

Implement response hypotheses as policy-owned evaluation over completed candles.

Required behavior:

- Select location-specific policies and all applicable hypotheses.
- Evaluate response candles in order using causal OHLC evidence.
- Track arming, direction, evidence IDs, confirmation candle, response status,
  invalidation, and expiry separately from interaction status.
- Support standard and prior-day response rules, including rejection,
  acceptance, cross-band confirmation, invalidation, and expiry.
- Use pre-event context when a rule requires it; never use the event bar close or
  later context accidentally.

Exit criteria:

- Response evaluations expose explicit unavailable outcomes.
- Delayed acceptance and confirmation tests prove that entries cannot use
  evidence before it is complete.
- Every resolved response names its policy version, direction, evidence IDs,
  confirmation availability, and invalidation/expiry reason where applicable.
- Response rules use pre-event context and reject event-bar or future context
  when the selected policy requires prior evidence.
- Interaction lifecycle transitions do not overwrite response outcomes, and
  response outcomes do not change interaction close reasons.

Phase 6 conformance tests:

- `test_selects_all_applicable_location_response_hypotheses`
- `test_evaluates_response_candles_in_causal_order`
- `test_response_tracks_arming_direction_evidence_and_confirmation_availability`
- `test_rejection_acceptance_cross_band_invalidation_and_expiry_are_distinct`
- `test_pre_event_context_is_required_when_policy_disallows_event_close`
- `test_response_unavailable_is_not_reported_as_neutral_or_confirmed`

Remaining implementation work:

- Persist and publish the complete per-hypothesis state, including arming,
  direction, evidence IDs, confirmation availability, and expected availability
  boundaries. The live policy already retains this state in memory.
- Complete location-specific prior-day policies and pre-event-context wiring;
  the generic response adapter is not a substitute for those policies.
- Add conformance coverage proving every newly available pending or unavailable
  outcome is retained at the runtime persistence boundary.

### Phase 7: Opportunities And Structural Setups

Turn eligible response outcomes into deterministic, bounded lifecycles.

Required behavior:

- Resolve context relation, directional scores, location template, and opening
  interaction policy before formation.
- Create candidate, lifecycle, response-episode, execution-group, and setup
  identities deterministically.
- Attach direction, entry zone, failure boundary, expiry, confirmations, regime
  version, and evidence lineage.
- Advance opportunities only on their configured evidence timeframe.
- Implement formation, invalidation, expiry, supersession, retest waiting, and
  confirmation exactly as defined by the canonical templates. Carry the
  canonical execution-group identity as proposal metadata only; do not claim,
  reserve, or consume it during formation.
- Count timeout evidence before structure formation on each bar.

Exit criteria:

- Opportunity tests cover the complete lifecycle and bounded timeout behavior.
- No setup can outlive its source interaction, response episode, or context
  version without an explicit transition.
- Formation, invalidation, expiry, supersession, retest waiting, confirmation,
  and denial each emit one deterministic lifecycle transition with its evidence
  and availability timestamp.
- Timeout counting is idempotent, counts only new evidence on the configured
  timeframe, and occurs before formation can inspect a trigger on the same bar.
- A previously filled execution group denies later candidates, while multiple
  unfilled candidates retain diagnostic identity until the execution owner
  atomically consumes the group at fill.

Phase 7 conformance tests:

- `test_forms_candidate_only_after_context_relation_and_opening_policy`
- `test_opportunity_identity_and_evidence_lineage_are_deterministic`
- `test_structural_setup_contains_direction_zone_failure_boundary_expiry_and_regime`
- `test_opportunity_lifecycle_covers_formation_invalidation_expiry_supersession_and_retest`
- `test_timeout_counts_only_new_configured_timeframe_evidence`
- `test_timeout_expires_before_same_bar_structure_formation`
- `test_filled_execution_group_denies_later_candidate_but_unfilled_candidates_remain_diagnostic`
- `test_setup_cannot_outlive_source_interaction_or_context_without_transition`

Remaining implementation work:

- Complete live orchestration for retest waiting, supersession, and replacement
  selection. The opportunity model already supports these transitions and live
  source invalidation.
- Persist denied candidates and execution-group claims as detached diagnostic
  facts at the runtime event boundary. Canonical template conflict groups and
  template/policy identity are already part of opportunity construction.

### Phase 8: Admission, Intent, And Trigger Eligibility

Implement decision gates without enabling execution mutations.

Required behavior:

- Apply strategy-owned gates in canonical order: context relation, regime
  permission, opening-gap permission, and setup/trigger validity. Preserve
  unavailable predicates without allowing them. Active-position, exposure,
  allocation, sizing, risk, and execution-group checks are evidence requested
  by the candidate but are not policy admission gates; the execution owner
  evaluates them atomically at pending-entry and fill time.
- Persist every passed, failed, and unavailable predicate with predicate ID,
  value, threshold, units, reference window, evidence IDs, context version, and
  reason. Preserve denied and unavailable decisions, not only winners.
- Construct a non-executable diagnostic candidate intent only for a current,
  valid setup with matching direction, regime version, confirmations, and
  evidence lineage. The execution owner may construct an executable order
  intent only in explicit opt-in mode after its own current-state gates pass.
- Validate trigger type, effective time, expiry, setup version, permission
  version, displacement, pullback/retest touch, retest confirmation, and
  next-bar timing before eligibility is reported.
- Apply requalification rules only for explicitly permitted cancellation causes;
  do not silently recreate an intent after a terminal denial or invalidation.
- In diagnostic mode, report eligibility but create no executable order intent
  and mutate no execution/accounting state.

Exit criteria:

- Every denied and unavailable admission is explainable from recorded evidence.
- Strategy-owned predicates and requalification tests pass; portfolio and group
  decisions are tested at the execution boundary, not in policy admission.
- Gate order is observable and stable; unavailable predicates do not become
  false, neutral, or allowed through fallback values.
- Diagnostic decision projections and opt-in executable intents are distinct
  types and cannot be confused by the execution owner.
- Trigger observations retain observed/available timestamps and cannot fill from
  an expired, superseded, mismatched, or future setup.

Phase 8 conformance tests:

- `test_admission_gates_run_in_canonical_order`
- `test_denied_and_unavailable_predicates_are_persisted_with_evidence`
- `test_diagnostic_decision_intent_is_not_executable`
- `test_intent_requires_current_matching_setup_regime_and_confirmations`
- `test_trigger_validates_type_effective_time_expiry_and_setup_version`
- `test_retest_requires_touch_and_confirmation_in_causal_order`
- `test_requalification_is_limited_to_allowed_cancellation_reasons`
- `test_unavailable_gate_does_not_fallback_to_neutral_or_allowed`

### Phase 9: Opt-In Execution And Accounting

The opt-in execution path is implemented through the existing Paper execution
owner. Diagnostic mode remains the default and remains mutation-free. The shared
turn contract and exit lifecycle adapters are implemented, while remaining work
is to make one coordinator invocation own every phase, close the admission-
boundary inconsistency, and finish paired replay/lifecycle conformance before
treating the phase as complete.

Required behavior:

- Manage existing positions before evaluating new entries on every bar, including
  same-timestamp reference replacement and permission changes.
- Apply session deadlines, exit priority, stop/target rules, protective-stop
  updates, and next-bar-open timing without same-bar hindsight.
- Reproduce the reference's management sequencing: execute already-available
  pending exits, evaluate/update the position exit plan, update protective
  stops and trailing state, then queue newly observed exits for the next
  available bar. A same-bar bar touching multiple levels follows the reference
  exit-priority rule.
- Preserve the reference template-owned trigger and exit semantics for
  recovery/rejection and acceptance responses, including breakout versus
  pullback triggers and conditional target interactions. Do not substitute
  generic VWAP stops for templates whose stop policy is unavailable in Paper.
- Record attempts, fills, partial exits, reservations, realized points, exit
  reasons, and settlement through one execution owner with explicit timestamps.
- Atomically evaluate current active-position, exposure, allocation, sizing,
  risk, quantity, and portfolio constraints at pending-entry and fill-price
  calculation. Consume an execution-group claim only after the fill succeeds;
  then invalidate unfilled siblings and record the claim. Policy formation and
  diagnostic admission may carry the group ID as metadata but must not claim it.
- Make fill, partial-fill, cancellation, and settlement transitions explicit and
  idempotent; no event may be applied twice after replay or retry.
- Keep execution vehicle, side, quantity, fill price, fill timestamp, exit
  timestamp, costs, and ledger state typed and source-linked.

Exit criteria:

- Execution tests run only with explicit opt-in mode.
- Diagnostic mode remains mutation-free after this phase is added.
- Existing-position management is proven to precede new entry proposals.
- Exit priority, session deadline, stop/target, partial-fill, reservation, and
  settlement behavior are covered without future-bar inputs.
- Duplicate-entry and duplicate-fill probes leave one authoritative lifecycle
  and ledger result.
- Paired diagnostic/executable replay fixtures agree on every defined decision,
  intent, fill, exit, settlement, and ledger projection; unavailable fields are
  compared as unavailable with their reasons rather than coerced to values.

Phase 9 conformance tests:

- `test_existing_position_management_precedes_new_entry_evaluation`
- `test_reference_replacement_is_visible_before_same_timestamp_execution`
- `test_session_deadline_and_exit_priority_are_causal`
- `test_stop_target_and_next_bar_open_timing_do_not_use_hindsight`
- `test_partial_fills_reservations_and_settlement_are_idempotent`
- `test_execution_group_and_active_position_constraints_are_enforced`
- `test_execution_owner_records_typed_fill_exit_and_ledger_lineage`
- `test_diagnostic_mode_remains_mutation_free_after_execution_is_enabled`
- `test_paired_replay_matches_diagnostic_decisions_and_executable_projections`
- `test_paired_replay_matches_fills_exits_settlement_and_ledger`
- `test_paired_replay_marks_reference_undefined_behavior_unavailable`

### Phase 10: Runtime Event And Replay Reporting

Complete the existing live runtime-event projection and replay reports after the
decision pipeline is stable. Live diagnostics consume persisted runtime events;
replay reporting consumes detached replay facts. Neither path recomputes
decisions from future rows.

Required behavior:

- Keep live diagnostics on the existing path: `OrderflowPolicy` returns
  `PolicyDecision` objects, `PaperEngine` converts them to runtime events, and
  `RuntimeSession` persists the existing strategy and bundle events. Do not add
  a live `ReplayRecorder`, `ReplayRecord` stream, or live capture adapter.
- Reuse `contracts/decision_view.py` as the canonical strategy-neutral persisted
  projection for every `PolicyDecision`. Require a dedicated stable
  `decision_id`; do not fall back to strategy-specific `candidate_id` values.
  Version this projection explicitly, reject unsupported reader versions, and
  document migrations for persisted records. The generic projection must support
  strategy/policy/config version, decision and availability timestamps,
  source/evidence IDs, lifecycle state, predicate results, unavailable reasons,
  and optional diagnostic-intent metadata.
  Orderflow may place interaction, response, opportunity, and admission details
  in generic evidence/provenance fields; the shared contract must not depend on
  those concepts.
- Add `unavailable` to the shared decision outcome vocabulary and validate it as
  distinct from `rejected`, `cancelled`, and `recorded`. An unavailable decision
  must retain its machine-readable reason, expected availability boundary, and
  status without being serialized as a rejection, false predicate, zero, or
  successful decision.
- Enhance the existing `RuntimeSession` persistence boundary to deep-detach
  nested decision payloads before storage and event-bus publication, preserve
  deterministic event ordering, and use a collision-safe idempotent event key for
  retries. The key must include strategy identity, session/source identity,
  event type, bundle identity, and decision identity.
- Make decision-event classification strategy-neutral rather than dependent on a
  fixed orderflow event name. Retain accepted, rejected, and unavailable
  strategy decisions for diagnostics through the existing generic
  `emit_rejected_decisions`-style policy, rather than relying on event absence.
  Rejected decisions must retain structured reason codes and zero effective
  quantity where quantity applies; unavailable decisions must remain distinct.
- Define a versioned generic event envelope containing event identity, sequence,
  session/date, observed/available/decision timestamps, strategy/policy version,
  provenance, and canonical timezone-aware timestamps. Preserve
  `observed_at <= available_at <= decision_at` when all three apply.
- Add explicit monotonic event/decision ordering metadata. Order by session/date,
  causal sequence, decision/availability timestamp, decision identity, event
  type, and canonical payload tie-breaker. SQLite insertion order may back this
  contract but must not be the only observable ordering guarantee.
- Version the live decision/event envelope and replay artifacts independently;
  readers must reject unsupported versions deterministically. This does not
  require live before/after replay snapshots, aggregate collections, or a live
  `ReplayRecord` stream.
- Keep the existing `ReplayRecord` fields and capture modes unchanged for the
  replay path. Summary, diagnostic, and full replay capture may differ in
  persisted detail only, never in aggregate availability, decisions, event
  order, or state transitions.
- Build reports from the facts already persisted by the relevant path: runtime
  events for live diagnostics and existing replay records for replay reports.
  Reports must not query future source rows or recalculate decisions.
- Persist the exact context/version references consulted by each decision in
  canonical provenance fields; do not retain only copied scores or ambiguous
  legacy aliases.
- Keep unavailable fields unavailable in records and reports, including the
  reason and relevant availability boundary.
- Preserve schema/version metadata and deterministic ordering for multi-session
  aggregation.

Exit criteria:

- Existing replay summary versus diagnostic capture produces the same decisions.
- Unsupported live-event and replay-artifact schema versions are rejected
  deterministically.
- Live diagnostics and replay reports can be generated from their respective
  persisted facts without consulting rows after the decision boundary.
- Full replay capture is a superset of diagnostic/summary detail and does not
  change calculations.
- Existing runtime events remain sufficient for live diagnostic inspection, and
  their payloads are detached, ordered, retry-safe, and sufficiently explained.
  Existing replay records remain detached and complete for replay capture modes.
- Report projections preserve unavailable status and do not invent neutral or
  stale values.

Phase 10 conformance tests:

- `test_replay_capture_modes_preserve_decisions_events_and_availability`
- `test_replay_records_remain_detached_in_existing_replay_path`
- `test_policy_decision_contains_generic_live_diagnostic_lineage`
- `test_generic_decision_view_requires_stable_decision_id`
- `test_generic_decision_view_preserves_unavailable_outcome_and_boundary`
- `test_unsupported_decision_and_replay_schema_versions_are_rejected`
- `test_unavailable_and_rejected_decisions_serialize_differently`
- `test_delayed_event_timestamps_preserve_causal_order`
- `test_generic_event_ordering_has_deterministic_tie_breakers`
- `test_runtime_event_persistence_deep_detaches_generic_policy_payload`
- `test_runtime_event_persistence_retains_rejected_and_unavailable_decisions`
- `test_runtime_event_persistence_is_strategy_neutral_idempotent_and_ordered`
- `test_generic_decision_event_classification_is_not_strategy_specific`
- `test_reports_use_recorded_facts_without_future_rows`
- `test_reports_preserve_unavailable_fields_and_reasons`
- `test_multi_session_reports_have_deterministic_schema_and_order`

## Canonical Reference Map

The following are absolute read-only references in the current project. Paper
must reproduce the contracts and behavior independently; it must not import,
execute, or modify these files.

### Generic Decision, Event, And Artifact References

These references inform the strategy-neutral runtime event and decision
contracts added in Phase 10. They are design references only; they do not make
NiftyZoning a dependency and do not authorize copying its replay owner into the
Paper live path.

- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\state_engine.py` —
  `PredicateResult` status semantics, explicit `UNAVAILABLE` handling,
  detached `ReplayRecord` serialization, and capture-mode behavior.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\state_machine_reporting.py` —
  versioned diagnostic and replay artifact envelopes, canonical reporting
  projections, and recorded-fact reporting boundaries.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\state_machine_evidence.py` —
  evidence extraction and transition capture without recomputing decisions from
  future inputs.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\market_context_history.py` —
  immutable context projections and `decision_input_context_refs` lineage.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\state_machine.py` —
  causal sequence ordering, availability boundaries, and unavailable predicate
  examples at the decision clock.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\parity_artifacts.py` —
  explicit artifact type and schema-version envelope conventions only. Parity
  artifacts remain outside this implementation plan's acceptance criteria.
- `C:\Users\agraw\Python\NiftyZoning\tests\test_orderflow_state_machine_reporting.py` —
  detached reporting, schema-version, and recorded-fact assertions.
- `C:\Users\agraw\Python\NiftyZoning\tests\test_ftx_decision_stream.py` —
  strategy-neutral decision stream and decision identity expectations.
- `C:\Users\agraw\Python\NiftyZoning\tests\test_event_types.py` —
  generic event classification and event-envelope validation expectations.
- `C:\Users\agraw\Python\NiftyZoning\tests\test_orderflow_replay_regressions.py` —
  delayed availability, causal timestamp ordering, rejected/unavailable
  outcomes, and replay regression coverage.

### Phase 0: Foundation

- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\config.py` — replay,
  execution, and capture configuration.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\state_engine.py` —
  `CaptureMode`, `PredicateResult`, `ReplayRecord`, `ReplayRecorder`, detached
  capture, and unavailable predicate status.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\state_machine_types.py` —
  typed replay context, identity, evidence, and lifecycle contracts.
- `C:\Users\agraw\Python\NiftyZoning\tests\test_orderflow_scaffold.py` —
  scaffold and diagnostic boundary expectations.
- `C:\Users\agraw\Python\NiftyZoning\tests\test_orderflow_state_machine_types.py` —
  typed invariants and serialization expectations.

### Phase 1: Replay Input And Source Identity

- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\state_machine_source.py` —
  `source_fingerprint()` and `source_metadata()`.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\research_replay.py` —
  session input loading and replay-session selection.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\replay_common.py` —
  replay mode and availability helpers.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\runner.py` — replay
  entry-point and session orchestration contracts.
- `C:\Users\agraw\Python\NiftyZoning\tests\test_orderflow_replay_common.py` —
  replay mode, availability, and common timing tests.
- `C:\Users\agraw\Python\NiftyZoning\tests\test_orderflow_replay_regressions.py` —
  real-session input and causal replay regressions.

### Phase 2: Causal Market Observations

- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\market_observation.py` —
  `rows_available_for_observation()`, `observe_market()`,
  `derive_session_levels()`, `MarketObservation`, and `ObservationGap`.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\orderflow_common.py` —
  `Aggregate`, `build_aggregate_result()`, timestamps, child lineage, and
  aggregate fingerprints.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\vwap_location.py` —
  causal VWAP prefix construction.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\state_machine.py` —
  `ReplayMachine.process_bar()` availability and completed-aggregate ordering.
- `C:\Users\agraw\Python\NiftyZoning\tests\test_orderflow_market_observation.py` —
  aggregate completion, gaps, session filtering, and ordering.
- `C:\Users\agraw\Python\NiftyZoning\tests\test_orderflow_vwap.py` —
  independent timeframe prefixes and non-executable observation behavior.

### Phase 3: Context, Values, And Permissions

- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\market_context_history.py` —
  as-of context intervals and immutable projections.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\value_publication.py` —
  prior-profile completeness and value-reference publication.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\opening_gap_policy.py` —
  `OpeningGapPolicy` and opening-gap state transitions.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\regime_classification.py` —
  causal regime and directional permission classification.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\hierarchy.py` —
  `MarketContext`, `ContextVersionRef`, `RegimePermission`, and relations.
- `C:\Users\agraw\Python\NiftyZoning\tests\test_orderflow_value_publication.py` —
  value availability and source completeness.
- `C:\Users\agraw\Python\NiftyZoning\tests\test_orderflow_market_context_history.py` —
  as-of context and dependency isolation.
- `C:\Users\agraw\Python\NiftyZoning\tests\test_orderflow_hierarchy.py` —
  context lineage, permissions, and trigger observations.

### Phase 4: Location Registry And As-Of Resolution

- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\state_machine_types.py` —
  `LocationRecord`, `LocationRegistry`, `LocationStatus`, and generations.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\location_geometry.py` —
  `LocationBand` and overlap geometry.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\location_observation.py` —
  `LocationObservation` provenance contract.
- `C:\Users\agraw\Python\NiftyZoning\tests\test_orderflow_state_machine_types.py` —
  registry, generation, and typed location invariants.

### Phase 5: Observations And Interaction Episodes

- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\interaction_detector.py` —
  `InteractionDetector` and `InteractionTransition`.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\interactions.py` —
  `LocationInteraction`, `InteractionSnapshot`, lifecycle statuses, closure
  reasons, projections, and score snapshots.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\vwap_location.py` —
  VWAP location publication.
- `C:\Users\agraw\Python\NiftyZoning\tests\test_orderflow_interactions.py` —
  interaction creation, advancement, closure, and projections.
- `C:\Users\agraw\Python\NiftyZoning\tests\test_orderflow_vwap_acceptance_delay.py` —
  direction-aware delayed acceptance evidence.

### Phase 6: Response Policies

- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\response.py` —
  response status, windows, hypotheses, and causal evaluation.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\policies` — location
  response policies, including prior-day behavior.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\templates.py` — response
  types, templates, trigger types, and exit policies.
- `C:\Users\agraw\Python\NiftyZoning\tests\test_orderflow_pd_policy.py` —
  prior-day acceptance, rejection, invalidation, and expiry.
- `C:\Users\agraw\Python\NiftyZoning\tests\test_orderflow_templates.py` —
  template and response contract tests.

### Phase 7: Opportunities And Structural Setups

- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\opportunity_coordinator.py` —
  structure, opening interaction, and response-generation coordination.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\opportunity.py` —
  opportunity policy and candidate formation.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\opportunity_timeout.py` —
  evidence-timeframe timeout advancement.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\lifecycle.py` — setup,
  execution-group, lifecycle, and evidence identities.
- `C:\Users\agraw\Python\NiftyZoning\tests\test_orderflow_opportunity.py` —
  formation and lifecycle behavior.
- `C:\Users\agraw\Python\NiftyZoning\tests\test_orderflow_opportunity_timeout.py` —
  bounded timeout and expiry ordering.
- `C:\Users\agraw\Python\NiftyZoning\tests\test_orderflow_entry_features.py` —
  causal entry-time feature lineage.

### Phase 8: Admission, Intent, And Trigger Eligibility

- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\intent_policy.py` —
  `ExecutionPolicy.evaluate_entry()`, causal context, and decision evidence.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\execution.py` —
  admission, intent, trigger, and proposal coordination.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\active_position_gate.py` —
  active-position and session-extreme gates.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\allocation.py` —
  exposure, reservation, and sizing constraints.
- `C:\Users\agraw\Python\NiftyZoning\tests\test_orderflow_active_position_gate.py` —
  active-position and requalification behavior.
- `C:\Users\agraw\Python\NiftyZoning\tests\test_orderflow_execution.py` —
  admission, triggers, intents, management, and execution ownership.

### Phase 9: Execution And Accounting

- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\execution.py` —
  `ExecutionCoordinator.handle_management()`, `handle_execution()`,
  `select_execution_proposals()`, exits, fills, and settlement.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\lifecycle.py` —
  `ExecutionState`, `ExecutionEngine`, `Fill`, and `ExitExecution`.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\hierarchy.py` —
  position management, exit rules, and trigger contracts.
- `C:\Users\agraw\Python\NiftyZoning\tests\test_orderflow_execution.py` —
  execution and accounting contract tests.
- `C:\Users\agraw\Python\NiftyZoning\tests\test_orderflow_replay_regressions.py` —
  real-session management and confirmation regressions.

### Phase 10: Capture And Reporting

- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\state_engine.py` —
  detached replay records and capture modes.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\state_machine_reporting.py` —
  causal actions, summaries, reports, and recorded-fact projections.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\state_machine_evidence.py` —
  evidence extraction and transition capture.
- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\reporting.py` —
  terminal reporting and presentation boundaries.
- `C:\Users\agraw\Python\NiftyZoning\tests\test_orderflow_state_machine_reporting.py` —
  detached records and reporting behavior.

The separate parity implementation and comparison references remain outside
this implementation roadmap:

- `C:\Users\agraw\Python\NiftyZoning\src\orderflow\parity_artifacts.py`
- `C:\Users\agraw\Python\NiftyZoning\tests\test_orderflow_parity_artifacts.py`
- `C:\Users\agraw\Python\NiftyZoning\tests\test_orderflow_parity_comparison.py`

## NiftyZoning To FTX Paper File Mapping

This is a responsibility mapping, not an import or code-copying map. The
NiftyZoning paths are read-only behavioral references. The Paper paths are the
implementation owners and may split one reference responsibility across
strategy, contracts, runtime, and execution modules. A listed Paper file is
not evidence that the corresponding behavior is complete; the phase exit
criteria and verified gaps above remain authoritative.
Paper source paths below are relative to `src/`; Paper documentation paths are
relative to the repository root.

### Foundation, Input, And Observations

- `NiftyZoning/src/orderflow/config.py`, `state_machine_types.py` ->
  `ftx_paper/strategy/config.py`, `strategy/orderflow.py`, and
  `strategy/orderflow_contract.py`: validated orderflow configuration, stable
  identities, typed unavailable values, and policy contracts.
- `NiftyZoning/src/orderflow/state_machine_source.py`, `research_replay.py`,
  `replay_common.py`, `runner.py` -> `ftx_paper/strategy/orderflow_source.py`,
  `strategy/orderflow_replay.py`, `application/run_replay.py`, and
  `contracts/replay.py`: source identity, input selection, replay scope, and
  replay orchestration.
- `NiftyZoning/src/orderflow/market_observation.py`, `orderflow_common.py` ->
  `ftx_paper/strategy/orderflow_producer.py`,
  `strategy/orderflow_observation.py`, and `contracts/market.py`: completed
  market evidence, aggregate availability, gaps, lineage, and source-row
  fingerprints.
- Canonical NiftyZoning `daily_atr`/`build_context_scores` semantics -> Paper's
  independent replay/observation and score boundaries: Paper materializes its
  own prior-session ATR and recursively calculates scores. These values are not
  producer-owned fields and no NiftyZoning runtime artifact is consumed.
- `NiftyZoning/src/orderflow/vwap_location.py` ->
  `ftx_paper/strategy/orderflow_producer.py` and
  `strategy/orderflow_observation.py`: causal VWAP and prefix evidence;
  VWAP participates in opportunity formation, while structural acceptance
  templates may use the executable `OR_VWAP` protective stop.

### Context, Locations, And Interactions

- `NiftyZoning/src/orderflow/market_context_history.py`,
  `value_publication.py`, `opening_gap_policy.py`, `regime_classification.py`,
  `hierarchy.py` -> `ftx_paper/strategy/orderflow_context.py` and
  `strategy/orderflow_contract.py`: as-of context, values, opening-gap state,
  permissions, generations, and unavailable context evidence.
- `NiftyZoning/src/orderflow/state_machine_types.py`,
  `location_geometry.py`, `location_observation.py` ->
  `ftx_paper/strategy/orderflow_location.py`: location records, bands,
  generations, as-of resolution, ownership, and observation provenance.
- `NiftyZoning/src/orderflow/interaction_detector.py`, `interactions.py` ->
  `ftx_paper/strategy/orderflow_interaction.py`: independent 1m/5m
  interaction clocks, frozen contact lineage, lifecycle transitions, and
  close reasons.

### Responses, Opportunities, And Admission

- `NiftyZoning/src/orderflow/response.py`, `policies/`, `templates.py` ->
  `ftx_paper/strategy/orderflow_response.py` and `strategy/templates.py`:
  response hypotheses, location-specific policies, response state, template
  identity, confirmation, invalidation, and expiry.
- `NiftyZoning/src/orderflow/opportunity_coordinator.py`, `opportunity.py`,
  `opportunity_timeout.py`, `lifecycle.py` ->
  `ftx_paper/strategy/orderflow_opportunity.py`,
  `strategy/opportunity_timeout.py`, and `strategy/opportunity.py`:
  structural setups, bounded opportunity lifecycles, timeout ordering,
  execution-group identities, and setup identities. Group claims remain owned
  by the execution boundary and occur only after fill.
- `NiftyZoning/src/orderflow/intent_policy.py`, `execution.py`,
  `active_position_gate.py`, `allocation.py` ->
  `ftx_paper/strategy/orderflow_admission.py`,
  `strategy/orderflow_trigger.py`, and the existing execution owner:
  strategy-owned predicate ordering and trigger evidence are ported in the
  strategy layer; active-position, allocation, sizing, risk, reservations,
  and proposal arbitration are evaluated by execution/account ownership.

### Execution, Persistence, And Reporting

- `NiftyZoning/src/orderflow/execution.py`, `lifecycle.py`, `hierarchy.py` ->
  `ftx_paper/execution/coordinator.py`, `execution/service.py`,
  `execution/settlement.py`, `contracts/orders.py`, and
  `runtime/engine.py`: Paper execution ownership, order lifecycle,
  management, fills, exits, reservations, and settlement. Diagnostic mode
  must remain mutation-free.
- `NiftyZoning/src/orderflow/state_engine.py` ->
  `ftx_paper/strategy/orderflow_capture.py` and `contracts/replay.py`:
  replay capture modes and detached replay records. This mapping does not
  introduce a live replay recorder.
- `NiftyZoning/src/orderflow/state_machine_reporting.py`,
  `state_machine_evidence.py`, `reporting.py` ->
  `ftx_paper/strategy/orderflow_reporting.py`, `contracts/decision_view.py`,
  `contracts/decision_artifacts.py`, `runtime/events.py`, and
  `runtime/session.py`: recorded-fact projections, generic decision views,
  runtime event envelopes, persistence, and reports.
- `NiftyZoning/src/orderflow/state_machine.py` ->
  `ftx_paper/strategy/orderflow.py`, `strategy/orderflow_replay.py`,
  `runtime/engine.py`, and `runtime/session.py`: causal sequencing is split
  between the live `OrderflowPolicy`, replay-only `ReplayMachine`, shared
  runtime coordination, and persistence; no single Paper file replaces the
  reference state machine.

### Reference-Only Or Intentionally Unmapped Areas

- `NiftyZoning/src/orderflow/parity_artifacts.py` and its parity tests map to
  the separate Paper parity deliverables in
  `ftx_paper/runtime/parity_artifacts.py`, `docs/PRODUCER_NEUTRAL_PARITY.md`,
  and `docs/PRODUCER_NEUTRAL_PARITY_IMPLEMENTATION_PLAN.md`; they are not part
  of the live orderflow owner.
- VWAP locations, `reversal_at_vwap`, and VWAP cell policies map to the Paper
  orderflow location and response contracts. Structural acceptance templates
  also map the `OR_VWAP` protective-stop policy.
- NiftyZoning's single replay/state-machine owner has no direct live Paper
  equivalent. Paper preserves separate ownership between `OrderflowPolicy`,
  `ReplayMachine`, `RuntimeSession`, and `PaperExecutionCoordinator`.

## Cross-Project Omissions And Paper-Only Responsibilities

The mapping above can hide important asymmetries. These are the omissions and
additional responsibilities that must remain visible when assessing parity.

### Paper Omissions Relative To NiftyZoning

- There is no Paper equivalent of NiftyZoning's dedicated
  `entry_features.py`. Entry-time feature construction and lineage are not yet
  a separate Paper contract; they must not be inferred from the generic market
  feature cache or from a later decision payload.
- There is no Paper equivalent of `liquidity_sweep.py` or
  `vwap_acceptance_delay.py`. Liquidity-sweep evidence and delayed,
  direction-aware VWAP acceptance are therefore unavailable unless explicitly
  implemented as Paper-owned diagnostic contracts. They must not be replaced
  with a generic response or VWAP rule.
- NiftyZoning's `release_sequence.py` and `canonical_projection.py` have no
  one-to-one Paper module. Release ordering and canonical producer-neutral
  projection remain cross-cutting concerns covered by runtime/event and parity
  contracts, not by `OrderflowPolicy` alone.
- Paper currently has a generic response adapter rather than NiftyZoning's
  complete `policies/` package. Prior-day behavior, pre-event-context rules,
  persistence of complete per-hypothesis evidence, and the full response
  template registry remain implementation gaps identified in Phase 6. Repeated
  pending/unavailable evaluation is implemented in the live policy but still
  needs persistence-boundary coverage.
- Paper's live producer now retains incomplete and duplicate source rows as
  explicit source issues, including identity, fingerprint, and availability
  bounds, while keeping them unavailable to aggregation. Source-selection,
  incomplete-parent, and candidate-rejection diagnostics remain incomplete on
  the live path. A skipped or unavailable result is not equivalent to the
  detailed NiftyZoning diagnostic unless its reason, boundary, lineage, and
  identity are persisted.
- Paper does not have a direct equivalent of NiftyZoning's single
  `ExecutionEngine` lifecycle owner. This is intentional, but the split across
  `PaperExecutionCoordinator`, `contracts/orders.py`, portfolio state, and
  settlement must still provide one authoritative idempotent lifecycle before
  Phase 9 can be accepted.

### Paper Responsibilities Without A NiftyZoning Counterpart

- `ftx_paper/runtime/session.py`, `runtime/engine.py`, `runtime/store.py`, and
  `application/` own the loopback runtime lifecycle, recovery, leases, and
  persistence boundary. NiftyZoning's research replay runner is not a
  substitute for these operational responsibilities.
- `ftx_paper/broker/` and `runtime/futures_source.py` own broker market-data
  connectivity, authentication, feed normalization, and source selection for
  Paper's supported providers. NiftyZoning's local replay inputs do not cover
  this deployment boundary.
- `ftx_paper/domain/`, `execution/`, `contracts/orders.py`, and
  `execution/settlement.py` own Paper's simulated account, portfolio, costs,
  order lifecycle, and settlement. These are paper-trading concerns and must
  not be interpreted as broker/live execution parity.
- `ftx_paper/contracts/decision_view.py`, `contracts/events.py`,
  `runtime/events.py`, and `runtime/parity_artifacts.py` provide strategy-neutral
  persisted decision/event and producer-neutral parity boundaries. They extend
  beyond the NiftyZoning orderflow module and must remain separate from its
  replay implementation.
- `ftx_paper/contracts/futures_rollover.py` and the broker adapters provide
  explicit futures contract identity and rollover handling. This is a Paper
  deployment concern even where the NiftyZoning reference assumes a prepared
  replay series.

## Test And Delivery Workflow

For each phase:

1. Add a focused Paper-owned test that expresses the causal contract.
2. Prove it fails for the missing behavior.
3. Implement the smallest change that satisfies it without weakening the
   diagnostic boundary or orchestration order.
4. Run the focused tests, all previously completed orderflow tests, Pyright on
   changed Python files, and Ruff.
5. Add an ordering regression for every new per-bar boundary or helper.

Fixtures must be owned by FTX Paper and use explicit timestamps, aggregate
boundaries, availability, source IDs, and expected event ordering. Tests should
assert project behavior and causal contracts, not implementation structure.

The implementation is complete only when all phases through Phase 10 meet their
exit criteria. A separate parity effort may later compare independently
produced producer-neutral artifacts, but those artifact comparisons must not
change the implementation contracts or be used as a substitute for the paired
replay tests defined in Phase 9.

## Verified Implementation Gaps

The roadmap is not complete through Phase 10. The following gaps are verified
against the current Paper source and must be resolved before the corresponding
phase can be marked complete:

- The causal policy now emits trigger observations for formed opportunities and
  advances opportunity timeout evidence before same-bar response formation. It
  still emits lifecycle/status transitions rather than the full recorded
  hypothesis stream; repeated pending and unavailable outcomes remain
  selectively published.
- Incomplete in-session source rows are retained as explicit observation issues
  while remaining unavailable to aggregation. Recovery VWAP state is advanced
  only when the current completed bar closes an aligned five-minute window; it
  no longer scans future rows to decide an earlier observation.
- Response hypotheses now retain their existing per-hypothesis state while
  publishing newly available pending and unavailable outcomes instead of
  suppressing unchanged causal evidence.
- Active opportunities sourced from an interaction are explicitly invalidated
  when that interaction ends, with the interaction close reason and source
  observation retained in the transition.
- Runtime order handling now passes executable proposals through the existing
  `PaperExecutionCoordinator` before authorization. Denied proposals are
  persisted as explicit `ORDER_DENIED` events instead of disappearing between
  policy output and execution. The public
  `evaluate_admission()` helper now evaluates the same gates without creating
  execution state; remaining work is predicate metadata and trigger-persistence
  coverage.
- The current live policy still claims an execution group before constructing
  the executable order proposal. NiftyZoning consumes the group claim only
  after a successful fill and then invalidates unfilled siblings. Paper must
  move that claim and sibling invalidation into the execution owner; until then
  the live path does not satisfy the authoritative group-ownership contract.
- Interaction, response, opportunity, admission, and execution-group state is
  not yet fully independent at the persistence boundary. Hypothesis status is
  serialized, but hypothesis evidence and arming state are not.
- Opening-gap state is currently reduced to `UNAVAILABLE`/`NONE` in the policy
  context update. The planned candidate, accepted, reclaimed, return-to-value,
  and expiry transitions are not implemented in the live path.
- Location generations and carry policy are integrated with the live session
  rollover path; remaining work is focused regression coverage for carried and
  terminated interactions at the boundary.
- Phase 9 is no longer wholly deferred: executable proposals, fills, exits, and
  settlement are available only through explicit opt-in mode. Diagnostic mode
  must continue to create no executable intents, reservations, fills, exits, or
  portfolio mutations. Paired replay and full lifecycle conformance remain
  required before the phase is complete.

### Resolved Gaps

- Phase 10 runtime decision views now reject missing outcomes instead of mapping
  them to `recorded`, preserve unavailable expected-availability boundaries, and
  recursively detach nested evidence before persistence and publication.
- Runtime persistence now validates the versioned event envelope, uses a
  collision-safe retry key containing strategy/session/source/event/bundle/
  decision identity, and exposes deterministic causal ordering with explicit
  tie-breakers rather than SQLite insertion order alone.
- Replay reporting now sorts records and same-timestamp facts deterministically
  and keeps multi-session aggregation ordered by session date. Focused
  regression tests cover status distinction, detachment, retry idempotency,
  schema rejection, causal ties, and report ordering.
- Normalized parity fingerprints now reject malformed timestamps and incomplete
  OHLC data and sort normalized rows by canonical content, while preserving
  optional volume handling used by existing Paper bars.
- Legacy source fingerprints and replay fields are no longer adapted into
  canonical parity/replay artifacts; canonical boundaries reject them instead.
- Live producer diagnostics now retain incomplete and duplicate source rows in
  the immutable `OrderflowContext` while excluding them from published
  aggregates. Incomplete-parent and source-candidate diagnostics remain open.

- Trigger eligibility is now projected through `orderflow_trigger.py` from the
  live policy path; executable authorization requires that projection while
  diagnostic decisions retain the same causal result without creating orders.
- Opportunity timeout advancement now runs once per completed observation before
  response and opportunity formation, preserving the required same-bar order.
- Replay no longer constructs the legacy `market.location.LocationDetector` or
  emits per-bar `trace` records. The legacy trace and events endpoints are
  removed from the API and OpenAPI contract.
- Replay artifacts no longer expose `candidate_trace`; decision projections are
  derived only from the canonical causal decision stream.
- Phase 10 is still not complete: the generic event repository and API-facing
  persistence path need the same persisted-fact envelope guarantees as the
  runtime store, and the remaining Phase 10 conformance tests are not all
  implemented. The API projection and replay reporting paths already exist.

The current implementation therefore provides a substantial Phase 0-8
foundation, an opt-in Phase 9 execution path, and substantial Phase 10 runtime
event/reporting behavior. It must not be described as a complete Phase 0-10
implementation until the remaining admission, live-producer diagnostics,
persistence, paired-replay, and Paper-owned conformance gaps are verified.
