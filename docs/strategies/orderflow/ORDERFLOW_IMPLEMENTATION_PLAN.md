# Paper Orderflow Implementation Plan

## Purpose

`C:\Users\agraw\Python\NiftyZoning` is the canonical orderflow implementation.
`C:\Users\agraw\Python\ftx-paper` is not currently at behavioral parity. The
existing Paper-native orderflow implementation is therefore not an incremental
foundation and must not be extended.

This plan replaces the existing implementation rather than adding another
compatibility layer. The delivery rule is:

> Remove the Paper orderflow implementation, port the canonical implementation,
> prove parity one layer at a time, and only then add the smallest Paper host
> adapter required by the runtime.

No phase may begin until the preceding phase's exit gate passes. A failing
comparison is a blocker, not permission to add a fallback, translation helper,
or Paper-specific behavior.

Generic strategy identity, isolation, extension, and replay guidance is maintained
in [Strategy boundaries](../shared/STRATEGY_BOUNDARIES.md) and
[Strategy development](../shared/STRATEGY_DEVELOPMENT.md). This plan owns only
the orderflow porting sequence and its canonical parity gates.

## Canonical Strategy Fidelity - Paper Trading

Canonical reference:
`C:\Users\agraw\Python\NiftyZoning\src\orderflow`

- Paper trading must faithfully adopt the canonical strategy, not independently
  implement or extend strategy rules.
- Trace strategy behavior to canonical code or an approved design; compare the
  corresponding execution paths when investigating discrepancies.
- Paper infrastructure may differ only at host boundaries and must preserve
  canonical semantics, point-in-time availability, and causal/replay correctness.
- Resolve divergence by fixing the paper implementation. Add fallbacks or
  compatibility layers only when canonical behavior or an explicit approval
  requires them; surface conflicts between canonical code and documented design.

## Non-Negotiable Rules

- NiftyZoning owns orderflow behavior, state transitions, execution semantics,
  lifecycle rules, replay ordering, and canonical reporting.
- Paper owns only deployment/runtime composition, broker market-data transport,
  paper-account persistence, API boundaries, and the paper execution host.
- Do not preserve the current Paper orderflow classes, enums, lifecycle states,
  feature names, or event meanings for compatibility. Reintroducing them would
  create a second strategy implementation.
- Do not import NiftyZoning at runtime. The ported implementation must be
  self-contained in the Paper source tree.
- Do not copy generated reports, databases, runtime state, credentials, or
  environment files.
- Port behavior, not just names or structure. Copy-paste from the canonical
  source is preferred, but is not required when a Paper boundary needs an
  adaptation that preserves the contract.
- Diagnostic or paper execution must not be described as live broker execution.
- Missing canonical evidence remains unavailable with its reason. It must not be
  converted to false, neutral, zero, observed, or accepted.
- Existing Paper code outside orderflow must not be changed merely to make the
  old implementation fit the new plan.

## Current Mismatch

Paper currently contains a strategy-specific implementation under
`src/ftx_paper/strategies/orderflow/`, plus orderflow runtime/API integration and
an extensive Paper-specific test suite. Its `OrderflowPolicy` owns a different
set of context, interaction, opportunity, admission, trigger, and execution
objects than canonical NiftyZoning.

Canonical NiftyZoning is organized around these modules and owners:

- source and replay: `state_machine_source.py`, `market_observation.py`,
  `replay_common.py`, `orderflow_common.py`, `state_machine_types.py`,
  `state_machine.py`;
- domain types and lifecycle: `state_machine_types.py`, `hierarchy.py`,
  `interactions.py`, `lifecycle.py`, `templates.py`;
- context and locations: `value_publication.py`,
  `opening_gap_policy.py`, `location_observation.py`, `location_geometry.py`,
  `vwap_location.py`; artifact projection: `market_context_history.py`;
- responses and setup formation: `response.py`, `opportunity.py`,
  `opportunity_timeout.py`, `opportunity_coordinator.py`, `interaction_detector.py`;
- intent and execution: `intent_policy.py`, `execution.py`, `active_position_gate.py`,
  `allocation.py`;
- replay capture and orchestration: `state_engine.py`, `state_machine.py`;
- reporting and parity: `state_machine_reporting.py`,
  `canonical_projection.py`, `parity_artifacts.py`;
- optional presentation: `reporting.py`.

These are dependency owners, not isolated implementation layers. In particular,
`state_machine.py` currently coordinates observation, context, interaction,
opportunity, execution, and capture in one replay engine. The port must therefore
carry the orchestration shell as a dependency closure, while enabling and proving
the behavioral layers in order. It must not pretend that copying one module at a
time creates independent runtimes.

The plan treats these as canonical behavior to port and validate, with Paper
limited to the host boundaries described below.

## Target Boundary

The final Paper orderflow boundary is:

```text
Paper broker/feed and stored bars
    -> Paper input adapter
    -> ported canonical orderflow behavior
    -> canonical decisions, lifecycle, replay facts, and reports
    -> Paper execution/accounting adapter (paper mode only)
    -> Paper runtime events, persistence, and API projections
```

The canonical package remains the only orderflow owner. The Paper adapter may
translate market bars into canonical input and canonical execution proposals into
existing Paper paper-trading records, but it may not reimplement policy or
maintain parallel orderflow state.

## Live And Replay Porting Contract

The port must support both Paper operating modes. Replay is not the production
implementation and live is not a separate strategy implementation.

- Live and replay use the same ported canonical domain types, observation logic,
  context logic, interaction/response policies, opportunity lifecycle, intent
  rules, execution calculations, and reporting projections.
- Live input is incremental: the broker/feed adapter publishes one completed
  available bar or aggregate at a time to the canonical decision boundary.
- Replay input is bounded: the replay adapter feeds the same canonical decision
  boundary in timestamp order from stored bars. It may control the clock and
  capture mode, but it may not change policy calculations.
- `ReplayMachine`-style orchestration, file loading, batch iteration, and replay
  capture must not be inserted into the live callback path.
- Live and replay have separate mutable owners for transport, clock, persistence,
  and recovery, but never separate orderflow state machines. A canonical state
  snapshot must be sufficient to continue a live session and to reproduce the
  equivalent replay prefix.
- Live restart recovery restores the canonical state snapshot and Paper runtime
  state according to the existing recovery contract. It must not reconstruct
  state by rereading future bars or by replaying an unbounded history implicitly.
- Replay restart/retry is idempotent and starts from an explicit input scope and
  snapshot. It must not read live runtime state, broker state, or a Paper account
  database unless the replay scope explicitly supplies those inputs.
- The execution adapter is selected by mode: replay can produce diagnostic or
  explicit paper-execution facts, while live can produce only Paper execution
  facts. Neither mode submits broker orders.

The parity comparison is implementation and contract parity against the pinned
canonical commit. Input data comparison is out of scope. Live and replay must
share the ported canonical behavior, while their input adapters are validated
only against the canonical input contract.

## Phase 0: Pin The Canonical Baseline

Pin the canonical commit.

Status: Complete for the Phase 2 runtime-boundary contract; behavioral parity
remains blocked until later canonical layers are ported.

Required work:

- Record the canonical repository commit ID used for the baseline.

Baseline record:

- Canonical repository: `C:\Users\agraw\Python\NiftyZoning`.
- Canonical branch at pin time: `main`.
- Canonical commit: `8114f553392ad01dd032aecfed0031a3bc08e2f8`.
- Commit subject: `docs: replace orderflow parity contract`.
- Canonical worktree was clean when this baseline was recorded on 2026-10-30.

Exit gate:

- The ported implementation is traced to the recorded commit ID.

## Phase 1: Remove The Existing Paper Orderflow

Remove the old behavior while keeping a minimal strategy structure for the
canonical port.

Status: Complete.

Required work:

- Keep `src/ftx_paper/strategies/orderflow/` and its basic strategy/package
  boundary in place, including canonical-support modules retained for the
  staged port (`templates.py`, `response.py`, and `opportunity.py`).
- Remove the existing Paper orderflow policy and implementation internals.
- Keep only minimal live/replay strategy entry points, configuration wiring, and
  registration stubs needed for Phase 2 porting.
- Remove old orderflow-specific tests and behavior-specific runtime/API wiring.
- Delete superseded Paper orderflow documents that describe the removed design or
  an unimplemented Paper parity artifact:
  `FTX_PAPER_ORDERFLOW_STRATEGY_DESIGN_PLAN.md`,
  `ORDERFLOW_ALIGNMENT_IMPLEMENTATION_PLAN.md`,
  `PRODUCER_NEUTRAL_PARITY.md`, and
  `PRODUCER_NEUTRAL_PARITY_IMPLEMENTATION_PLAN.md`.
- Preserve useful host-boundary constraints in this plan before deleting those
  documents. The canonical NiftyZoning documents remain read-only references.
- Leave unrelated Paper runtime, broker, portfolio, and generic execution code
  unchanged.

Exit gate:

- The orderflow package imports as a basic strategy scaffold plus retained
  canonical-support modules.
- No old orderflow behavior is reachable from the scaffold.
- Superseded orderflow design and unimplemented parity documents are removed from
  the active Paper documentation set.

Phase 1 verification record:

- The old Paper orderflow policy, lifecycle, producer, replay, reporting, and
  behavior-specific test modules were removed. The package retains only the
  no-op strategy/configuration scaffold, registration/API boundaries, and the
  explicitly retained canonical-support modules listed above.
- Live and replay runtime paths no longer invoke the deleted producer, pipeline,
  context provider, replay machine, or old trader-facing reporting projection.
- The retained API exposes canonical strategy decisions only; it does not
  recompute orderflow policy, lifecycle, or execution state.
- Validation at commit `12d957f`: 6 targeted tests passed; Ruff and Pyright
  passed with zero errors.

## Phase 2: Define The Parity Contract And Runtime Boundary

Before porting implementation code, define the checks applied to every layer and
establish the host-neutral transition boundary used by Paper live and replay.
Use `C:\Users\agraw\Python\NiftyZoning\docs\orderflow\PARITY_CONTRACT.md` as
the behavioral and artifact contract. Phase 2 does not port the whole package at
once or add policy behavior.

Status: Complete.

Phase 2 checklist and runtime-boundary record:

- `docs/strategies/orderflow/PHASE_2_PARITY_CHECKLIST.md` records the canonical
  modules, symbols, dependency closure, public contract facts, transitions,
  timestamps, availability, lineage, unavailable semantics, Paper ownership,
  live/replay behavior, and canonical/scaffold test gates for every required
  layer and reporting stream.
- `src/ftx_paper/strategies/orderflow/runtime_boundary.py` defines the
  host-neutral `transition_one` boundary, `CompletedObservation`, boundary event
  envelope, versioned state snapshot, and separate incremental live and
  bounded replay adapters. It also emits the versioned runtime-boundary artifact
  for replay, incremental, and recovery executions. It contains no orderflow
  policy.
- The pinned canonical source exposes no explicit full state restore API;
  `BoundaryState.from_snapshot` is therefore an explicit Paper runtime
  snapshot/recovery contract and is not represented as canonical behavior.

For each layer, record:

- canonical modules and symbols included in the layer;
- direct dependencies that must be ported with it;
- public types, fields, enums, serialized forms, and function signatures;
- state transitions, ordering rules, timestamps, availability, and error results;
- ownership boundary between canonical orderflow and Paper host adapters;
- live entry-point behavior and replay entry-point behavior;
- canonical tests that prove the layer;
- Paper scaffold tests proving the layer is reachable without duplicating it.

The parity contract defines the runtime-boundary artifact, phase substreams,
common record shape, final four layers, comparison statuses, and live/replay/
recovery rules. The Phase 2 checklist supplements it with the canonical module,
symbol, dependency, and test mapping needed to implement each stream.

Every ported layer must be checked for:

1. **Structure:** module names, public symbols, signatures, types, and schemas.
2. **Behavior:** decisions, transitions, ordering, idempotency, and failure rules.
3. **Causality:** observed/available timestamps, lineage, parent identity, and
   unavailable reasons.
4. **Ownership:** no Paper policy, lifecycle, or execution duplicate is added.
5. **Modes:** live and replay call the same ported layer with separate adapters.
6. **Tests:** canonical tests pass and Paper integration tests cover only the
   host boundary.

Contract gate:

- The layer checklist is recorded for the source, context, interaction, opportunity,
  and execution phases, plus the four reporting layers.
- The canonical commit ID is the source of truth for every ported symbol.
- The dependency map identifies the orchestration shell and every direct dependency
  required by each layer.
- A host-neutral canonical transition boundary is defined for one completed
  observation. Paper replay and Paper live adapters are allowed to call this
  boundary, but no Paper policy or second state machine exists.

### Runtime Boundary Work

The current canonical engine is replay-centered and is coordinated by
`ReplayMachine`. Paper already has separate live and replay modes; this work
connects both modes to one ported canonical transition boundary.

Required work:

- Port the canonical state container, event emission, causal timestamp handling,
  snapshot shape, and transition invocation needed by the later layers.
- Keep replay file loading, bounded iteration, and capture orchestration in the
  existing Paper replay worker.
- Define a live input adapter that submits one completed available observation at a
  time. It may own feed callbacks, buffering, recovery, and clock translation, but
  it may not evaluate orderflow policy.
- Define the same canonical transition call for live and replay. Differences are
  limited to input, clock, persistence, capture, and selected Paper execution
  adapters.
- Port dependency-closure modules required by the shell, but keep later-layer
  outputs disabled or unexposed until their parity gates pass.

Runtime boundary gate:

- A Paper replay prefix and a Paper live-style incremental driver invoke the same
  canonical transition boundary.
- Replaying a fixed prefix and feeding that prefix incrementally produce the same
  canonical state and event prefix.
- No live callback calls `ReplayMachine` batch/file orchestration, and no replay
  path calls the live feed adapter.
- Snapshot and recovery contracts are defined before policy-layer porting begins.

Phase 2 verification record:

- `tests/strategies/orderflow/test_runtime_boundary.py`: 6 passed. The live
  incremental and replay-prefix adapters produce identical state/event prefixes;
  recovery from a snapshot and duplicate input preserve the same prefix. The
  runtime-boundary artifact reports matching replay, incremental, and recovery
  hashes.
- `.venv\Scripts\ruff.exe check .`: passed.
- `.venv\Scripts\pyright.exe src/ftx_paper/strategies/orderflow/runtime_boundary.py src/ftx_paper/strategies/orderflow/__init__.py tests/strategies/orderflow/test_runtime_boundary.py`: passed with zero errors.
- `git diff --check`: passed.
- Research parity/replay regression test files are currently absent from the
  worktree because of unrelated pending deletions; they were not counted as
  Phase 2 evidence.
- The Paper adapters do not invoke each other, and no policy or Paper-native
  orderflow state machine was added. Later behavioral parity remains blocked
  until the pinned `ReplayMachine` layers are ported and compared.

This gate proves that the port is implementable; it does not prove behavioral
parity. Behavioral parity remains blocked until the layer gates below pass against
the pinned canonical commit.

## Phase 3: Port And Verify Layer 1 - Source And Market Observation

Status: Complete.

Port and validate the canonical source and observation behavior before adding
context or policy integration. Do not compare market input datasets between
repositories.

Canonical scope:

- source identity and fingerprinting;
- row ordering and duplicate rejection;
- session filtering and session boundaries;
- completed 1m bars and aligned 5m aggregates;
- gaps, incomplete parents, observed timestamps, available timestamps, and
  lineage;
- the source and observation metadata required to expose those facts.

Direct dependencies that must be carried with this layer are
`orderflow_common.py` and the source/aggregate portions of
`state_machine_types.py`. ATR, VWAP, context scores, locations, and policy
decisions are not Layer 1 behavior.

Port only the canonical modules needed for this scope and their direct
dependencies. Do not port context, interaction, opportunity, execution, or
reporting modules yet.

Paper work is limited to an input adapter that produces the canonical input
shape. It must not calculate a second aggregate, score, gap state, or VWAP.

Parity gate:

- Ported source and observation modules pass the canonical tests unchanged or
  with only package-root import changes.
- Paper live and replay adapters each satisfy the ported canonical input
  contract, including ordering, identity, completion, availability, and lineage.
- No later layer is enabled until the ported layer's canonical tests pass.

Phase 3 parity verification record:

- Ported the pinned canonical source fingerprint/metadata, aggregate builder,
  session filtering, strict ordering, duplicate rejection, gap and incomplete
  aggregate diagnostics, observed/available timestamps, and aggregate lineage.
- Added canonical source/observation tests with package-root import changes and
  Paper scaffold tests for identity, ordering, completion, availability, and
  live/replay lineage through the Phase 2 transition boundary.
- Paper adapters project canonical completed aggregates only; they do not
  calculate a second aggregate, score, gap state, or VWAP. Live publishes one
  completed observation at a time and replay remains bounded and ordered.
- Canonical and Paper DB-backed observation paths produced identical payloads
  for `2026-09-29`, including source fingerprints, aggregates, OHLCV values,
  timestamps, gaps, issues, and session boundaries.
- Post-repair parity samples matched for `2026-09-01`, `2026-09-15`,
  `2026-09-18`, and `2026-10-01`.
- Paper validates its selected dated futures source before projecting continuous
  futures and fails closed as `INCOMPLETE` when source data is missing or
  unverified. Paper 5m observations retain all contributing 1m lineage IDs.
- Both repositories normalize `HH:MM` source times consistently. Manifest
  hashes are compared before detailed records and report the first mismatch.
- `tests/strategies/orderflow`: 16 passed, with focused parity tests also
  passing in both repositories. Ruff and Pyright passed in both repositories.
- `.venv\Scripts\ruff.exe check .`: passed.
- `.venv\Scripts\pyright.exe` on all changed Phase 3 Python files: passed with
  zero errors, warnings, or informations.
- `git diff --check`: passed.

Extended multi-date edge-case coverage, duplicate/order-violation sessions, and
a commit containing the repair are not part of this verification record and
remain follow-up work.

## Phase 4: Port And Verify Layer 2 - Context, Values, Permissions, And Locations

Status: Contract and pure-behavior port complete. Causal runtime parity is
INCOMPLETE and requires the Phase 4 harness extension and Phase 4.5 below;
host persistence remains deferred to Phase 9.

Port and validate the canonical context and location contracts using exact
Layer 1-shaped inputs. This phase does not claim Paper runtime or DB-backed
context/location parity; Paper's orderflow runtime is still a scaffold until
the host adapter phase.

Port only the canonical context, value, permission, and location modules and
their newly required direct dependencies. Do not port response, opportunity, or
execution behavior ahead of this gate.

Canonical scope:

- prior-session value publication and opening-gap classification;
- context history and immutable generations;
- session high/low and reference replacement;
- location geometry, aliases, ownership, priority, and as-of resolution;
- directional/regime permissions and gap recovery;
- causal ATR inputs, VWAP prefix measurements, and their source-observation
  lineage where required by location bands or context.

The canonical runtime context store is the `MarketContextStore`/`ReplayContext`
path in `state_machine_types.py`. The ported store and deterministic contracts
are tested here, but runtime ownership and persistence integration are deferred
to Phase 9. `market_context_history.py` is an artifact-only projection and must
not be treated as the runtime context owner.

Paper may provide stored prior-session references through an adapter, but it may
not define replacement rules, fallback values, permissions, or location priority.

Phase 4 gate:

- Canonical context/location/value/permission tests pass against the ported
  contracts and deterministic projections.
- Immutable generations, replacement lineage, as-of resolution, causal
  timestamps, source-observation lineage, and explicit unavailable reasons are
  covered without reconstructing facts from presentation output.
- No Paper runtime parity claim is made while the host adapter is absent.

Phase 4 parity verification record:

- The ported context, value, permission, location, geometry, VWAP, opening-gap,
  immutable-history, and explicit-unavailable contracts are covered by 45
  focused Paper tests.
- Canonical context/value/history parity tests pass in the canonical repository.
- Source-observation parity remains `MATCHED` for `2026-09-15` and
  `2026-10-01`; `2026-10-29` is excluded because its source is mismatched.
- Paper does not yet emit persisted runtime `context_location` facts. The Phase
  4 context/location behavioral parity stream therefore remains `UNAVAILABLE`,
  not `MATCHED`, until Phase 9 wires the canonical runtime owner to the Paper
  store and replay boundary.

Phase 4 pending work:

- Extend the current direct-domain parity harness to drive context and location
  resolution from chronological completed observations rather than final
  `location_interactions` snapshots.
- Capture point-in-time location generations, source-observation IDs, causal
  parents, availability timestamps, and explicit unavailable outcomes at each
  replay clock step.
- Keep final context/location snapshots as assertions only; they must not drive
  the causal input stream.

## Phase 4.5: Add The Causal Interaction Replay Boundary

Phase 4.5 is the runtime-neutral layer between the pure context/location
contracts and Phase 5 event-stream parity. It adds no interaction or response
policy; it supplies the chronological orchestration required to exercise the
already ported domain contracts.

Status: Pending. Deterministic interaction/response primitives are ported, but
canonical policy dispatch and chronological event parity are blocked until this
layer exists.

Required work:

- Advance one shared replay clock over completed `1m` and `5m` aggregates.
- Resolve context and locations as of each aggregate's available timestamp.
- Publish causal location observations with timeframe, source IDs, parent
  lineage, reference generation, bands, and unavailable reasons.
- Feed observations into independent Paper `1m` and `5m` interaction episodes.
- Incrementally advance each applicable response hypothesis only with newly
  available response-timeframe evidence.
- Invoke the canonical policy owners for hypothesis eligibility, direction,
  role transitions, invalidation, and arbitration. For PD interactions this is
  the `policies/pd.py` path and its `evaluate_response_hypotheses` contract;
  the boundary must not select every location-matching template by itself.
- Preserve canonical response-window boundaries, including carry-session
  expiry, contact-candle selection, terminal-candle cutoff, instrument
  filtering, unavailable evidence, and the canonical as-of clock.
- Emit interaction creation, observation, departure, closure, response, and
  expiry facts at the transition where they occur.
- Make duplicate/retry replay idempotent while preserving event order.

Phase 4.5 parity gate:

- Paper and canonical streams match in chronological interaction/response
  identity, causal parent, timestamp, evidence lineage, status, and reason.
- Paper and canonical hypothesis selection, dynamic direction/role transitions,
  invalidation, winner selection, and loser arbitration match exactly.
- Final interaction snapshots are assertions only and do not drive Paper events.
- Phase 5 comparison reports the first causal mismatch from this stream.
- No Paper fallback, template selection rule, or response outcome exists unless
  the corresponding canonical implementation has the same behavior.

## Phase 5: Port And Verify Layer 3 - Interactions And Responses

Validate canonical observations, interaction episodes, response hypotheses, and
response outcomes before opportunity formation.

Status: Deterministic primitives and data contracts are ported. Canonical
policy-dispatch parity and chronological event parity are pending Phase 4.5;
runtime and database parity remain deferred to Phase 9.

Phase 5 pending strategy-port repairs:

- Align Paper response evaluation with canonical `opportunity.py` before using
  it in the Phase 4.5 boundary. This is a Phase 5 policy defect, not a Paper
  input-adapter fallback.
- Preserve canonical `carry_sessions` expiry and the
  `response_window_expired` outcome.
- Filter response evidence by the canonical instrument/symbol and causal
  availability boundary.
- Stop evaluation at the canonical terminal interaction candle.
- Locate and require the canonical contact candle before evaluating the response
  window; emit `contact_not_available` when it cannot be found.
- Preserve canonical as-of filtering, completed-bar filtering, response-window
  expiry, and invalidation-before-confirmation ordering.
- Do not replace any missing rule with a Paper-specific fallback or inferred
  neutral outcome.

Port only the canonical interaction and response modules and their newly required
direct dependencies. Do not preserve the deleted Paper interaction or response
implementation as an adapter.

Canonical scope:

- 1m and 5m interaction detectors with independent per-timeframe episode state;
  both are advanced by the same chronological replay clock;
- frozen contact/reference lineage;
- departure and close reasons;
- response policy selection, arming, rejection, acceptance, confirmation,
  invalidation, and expiry;
- canonical policy dispatch, including PD role transitions, hypothesis
  eligibility, dynamic direction, response-window boundaries, and arbitration;
- pre-event context and completed-evidence rules.

Paper must not retain the deleted `InteractionDetector`, response policy, or
location classes as wrappers. The adapter only supplies canonical market/context
inputs and consumes canonical outputs.

Parity gate:

- Canonical and ported event streams match in order, identity, causal
  parent, evidence, status, timestamps, and close/expiry reason.
- Canonical and ported policy dispatch match in hypothesis selection, role and
  direction changes, response invalidation, evidence boundaries, expiry, and
  winner/loser arbitration.
- Duplicate observations are idempotent in both runs.
- No response outcome is inferred from a terminal interaction state.
- Paper does not add a location-to-template fallback or evaluate all matching
  templates unless the canonical policy does so for that interaction.
- Live and replay invoke the same ported interaction and response implementation;
  replay-only batch boundaries cannot create a second implementation path.

Phase 5 verification record:

- Canonical `tests/test_orderflow_interactions.py` passes (3 tests) at the pinned
  baseline.
- Paper Phase 5 interaction/response and existing orderflow tests pass (51 tests).
- Independent 1m/5m episode state, causal observation validation, frozen
  reference/source lineage, duplicate idempotence, response arming and terminal
  outcomes, explicit incomplete evidence, and deterministic first-field mismatch
  diagnostics are covered by focused tests.
- Paper runtime/database context-location and interaction/response parity remain
  `INCOMPLETE` until Phase 9 wires the canonical runtime owner and persistence
  boundary; no DB-backed parity claim is made.
- The direct Phase 5 harness compares explicitly supplied Paper boundary facts
  and exposes causal mismatches, but it does not infer policy outcomes or claim
  policy/event-stream parity until Phase 4.5 supplies the canonical
  policy-dispatch and chronological input boundary, and the response evaluator
  matches the canonical opportunity contract above.

## Phase 6: Port And Verify Opportunities And Setup Lifecycle

Validate opportunity formation and setup lifecycle only after Layers 1-3 pass.

Status: Complete for the deterministic canonical contract gate. Paper runtime and
database parity remains incomplete and is deferred to Phase 9.

Port opportunity and setup-lifecycle behavior only after the first three layer
gates pass. Paper accounting and execution remain outside this phase.

Canonical scope:

- opportunity formation, timeout, invalidation, supersession, and retest;
- canonical templates, trigger types, setup identity, and execution group;
- candidate identity, parent context, response lineage, and unavailable outcomes;
- setup lifecycle stages, expiry evidence, invalidation, supersession, and
  execution-group eligibility.

Parity gate:

- Opportunity identities, lifecycle transitions, timeout counts, evidence, and
  close reasons match canonical output.
- Duplicate/retry replay is idempotent and does not create duplicate candidates.

Current Phase 6 verification record:

- Canonical opportunity and timeout tests pass (30 tests) at the pinned baseline.
- Paper Phase 6 opportunity, setup, timeout, lineage, future-evidence, and
  deterministic identity tests pass together with the Phase 4/5 gates (18 tests).
- Opportunity/setup contracts currently cover causal identity, parent context,
  response evidence lineage, retest stages, explicit expiry/invalidation/
  supersession, execution-group eligibility, and duplicate timeout observation
  idempotence.
- Ruff, Pyright, and `git diff --check` pass for the changed Paper scope.
- The canonical-vs-Paper replay harness currently reports mismatches for both
  source-matched dates; no Phase 6 parity gate is claimed until those are
  resolved. No runtime/database parity claim is made; Phase 9 remains the host
  integration and persistence gate.

## Phase 7: Port And Verify Intents, Admission, Allocation, And Execution

Validate the complete canonical execution lifecycle only after the opportunity
and setup-lifecycle gate passes.

Canonical scope:

- admission predicates and unavailable outcomes;
- active-position, allocation, and risk gates;
- reservations, intent transitions, entry/exit timing, fills, partial fills,
  protective stops, settlement, and realized points;
- canonical orderflow ledger mutations derived from reservations, authorizations,
  and fills.

The orderflow layer owns lifecycle settlement facts and ledger mutations. The
actual capital/equity account ledger remains a separate Paper/NiftyZoning host
boundary and must not be duplicated inside the ported orderflow state machine.

The ported canonical execution owner is authoritative for orderflow lifecycle
state. The Paper adapter maps successful canonical fills to the existing Paper
paper-accounting boundary. It must not create a second orderflow execution
coordinator or claim an execution group before canonical fill.

Parity gate:

- Canonical and ported decisions match before any Paper accounting call.
- Entry/exit observed and effective times, prices, direction, quantity, reasons,
  reservation transitions, and fills match canonical output.
- Duplicate/retry replay is idempotent.
- Diagnostic runs mutate no Paper account state; explicit paper execution runs
  mutate only the existing Paper portfolio and ledger owner.
- Live and replay invoke the same ported proposal and lifecycle implementation;
  only the selected Paper execution/capture adapter may differ.

## Phase 8: Canonical Reporting And Four-Layer Parity Artifacts

Port and validate canonical reporting before exposing orderflow through Paper API
or runtime events.

Required layers:

1. source/run scope;
2. decision lifecycle;
3. execution and settlement;
4. portfolio ledger.

Required work:

- Use canonical projection, identity, causal-parent, hashing, pagination, and
  unavailable semantics.
- Project the canonical replay payload fields produced by
  `state_machine_reporting.py`, including `execution_fills`,
  `execution_transitions`, and `ledger_mutations`.
- Compare manifests before records and stop at the first causal mismatch.
- Keep Paper transport, broker, database, and runtime metadata outside canonical
  values unless the canonical contract explicitly includes them.

Parity gate:

- The ported canonical reporting and artifact tests pass.
- Any contract mismatch identifies the first layer, symbol, field, or transition
  that differs from the pinned canonical commit.
- Missing Paper runtime evidence is tracked as `INCOMPLETE` and cannot be called
  parity.

## Phase 9: Add The Paper Host Adapter

Only after the ported canonical layers and artifact checks pass, integrate them
with Paper.

Required work:

- Connect the existing broker/feed and stored-bar inputs to one canonical input
  adapter contract. The live adapter publishes completed evidence incrementally;
  the replay adapter publishes the same contract from bounded stored input.
- Add a live `PaperOrderflowRuntime` composition boundary only if the existing
  `RuntimeSession` cannot host the canonical state directly. It may own feed
  subscriptions, session lifecycle, snapshots, and notifications, but not policy
  decisions or a second lifecycle state machine.
- Route canonical execution proposals through the existing Paper paper-execution
  owner. Do not route them through the deleted orderflow coordinator.
- Persist canonical decisions and lifecycle facts through existing Paper runtime
  event and store boundaries using an explicit versioned adapter schema.
- Wire the persisted `context_location` phase stream at this boundary. It must
  be produced from the canonical runtime context owner, include source
  observation IDs and causal parents, and remain `INCOMPLETE` or `UNAVAILABLE`
  when ATR, prior-session value, context, or location evidence is absent.
- Run the DB-backed context/location parity matrix for source-matched dates
  only, beginning with `2026-09-15` and `2026-10-01`. Compare manifests before
  records and stop at the first mismatch. Do not include `2026-10-29`.
- Add API and CLI projections only as views of canonical facts and Paper accounting
  facts; they must not recompute decisions.
- Define startup recovery so interrupted Paper sessions become `STOPPED` with an
  audit event, without reconstructing a second canonical engine.

Exit gate:

- Live and replay use the same ported canonical policy, lifecycle, and execution
  calculation code, with only input clock/orchestration and persistence adapters
  differing.
- Live and replay use the same ported canonical state and transition code,
  including recovery from a canonical snapshot.
- Live does not call replay batch/file orchestration, and replay does not call live
  feed callbacks.
- Capture mode and persistence do not change canonical decisions.
- Paper remains loopback by default and paper-only; no broker order submission is
  introduced.

## Validation Commands

Run from each repository's root with its own virtual environment:

```text
.venv\Scripts\python.exe -m pytest tests/test_orderflow_*.py
.venv\Scripts\python.exe -m pytest tests/strategies/orderflow
.venv\Scripts\ruff.exe check .
.venv\Scripts\pyright.exe <changed Python files>
```

Use the tests from the pinned canonical commit. Paper tests must be added only
after the corresponding canonical layer is ported and passes; tests for deleted
Paper contracts are not acceptance tests.

## Completion Criteria

Orderflow replacement is complete only when all of the following are true:

- The old Paper-native orderflow behavior is removed while the minimal strategy
  scaffold remains.
- The ported canonical behavior is self-contained and revision-traceable.
- The exact canonical commit used for the port is recorded, and every copied or
  adapted symbol is traceable to that commit.
- Every layer passes its structure, behavior, causality, ownership, modes, and
  test checks before the next layer is ported.
- A fixed replay prefix fed through Paper incrementally produces the same
  canonical state, event prefix, decisions, lifecycle facts, and execution
  proposals as the canonical replay.
- Paper live publishes only completed causally available observations and invokes
  the same transition code as replay; it does not invoke replay batch/file
  orchestration.
- Canonical execution proposals are handed to Paper accounting only after the
  canonical admission, reservation, trigger, and fill transitions succeed.
- Canonical reporting and Paper runtime contracts are verified for source,
  decision, execution/settlement, and portfolio ledger layers without comparing
  input datasets.
- Paper-specific code is limited to input, runtime, persistence, API, and paper
  accounting boundaries.
- No canonical behavior is replaced by a Paper fallback or inferred state.
