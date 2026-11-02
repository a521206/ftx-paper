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

### Current Integration Status

Paper has partial host wiring, but orderflow is not yet integrated as a complete
production live/replay machine:

- `runtime_boundary.py` defines the host-neutral one-observation contract and
  snapshot shape.
- `transition.py` provides the shared chronological owner used by the current
  live strategy boundary and bounded transition tests.
- `strategies/orderflow/boundary/orderflow.py` can advance that owner from a
  completed live-style bundle, and the generic `RuntimeSession`/`PaperEngine`
  dispatch path can host the strategy.
- `runtime/replay_worker.py` provides the generic bounded replay lifecycle, but
  `scripts/orderflow/orderflow_replay.py` still owns the orderflow parity-harness
  loading and evidence replay path.

This wiring is scaffolding and a partial transition boundary, not a claim that
Paper live and replay already share the complete canonical orderflow state,
execution, recovery, persistence, and reporting path. Phase 7 remains incomplete
until canonical exits, fills, settlement, and all execution streams are covered.
The production integration point is Phase 9, after the Phase 7 and Phase 8 exit
gates pass.

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
- Temporary phase/parity harnesses may load bounded Paper inputs and emit
  intermediate artifacts while the port is being verified, but they are not
  production replay engines or strategy owners. They must not be called by the
  live callback path and must be retired once the shared canonical transition
  boundary is wired for Paper live and replay.
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
- Added `scripts/orderflow/orderflow_replay.py` as a temporary executable
  Phase 3.5 evidence harness. It loads bounded raw session rows through the
  Paper source resolver, prepares the Phase 3 observation/evidence prefixes,
  and emits one evidence-only artifact record per source row. It is not the
  production replay engine, does not own strategy state or policy, and must be
  retired when Paper live/replay uses the shared canonical transition boundary.
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

Status: **Complete for the runtime-neutral Phase 4 contract.** Phase 4A evidence
publication, chronological Phase 4B replay, and the combined Phase 4C harness
are implemented. Source-observation, context/location, relation, method,
identity, and provenance parity match canonical behavior. The only remaining
reported status is accepted `INPUT_VARIANCE` for ATR-dependent band geometry;
the ATR difference is bounded by the parity tolerance. Persisted host runtime
parity remains deferred to Phase 9. Phase 4 consumes the Phase 3.5 evidence
publication and assembles its replay inputs.

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

### Canonical Implementation References

The following files and symbols in the pinned NiftyZoning repository are the
source of truth for this phase. Paths are relative to
`C:\Users\agraw\Python\NiftyZoning` and must be traced to canonical commit
`8114f553392ad01dd032aecfed0031a3bc08e2f8`:

- `src/orderflow/hierarchy.py`: `Permission`, `RegimePermission`,
  `ContextVersionRef`, `MarketContext`, `ContextObservation`, and
  `classify_context_relation`. These define immutable context identity,
  causal parent/child evidence, and directional permission semantics.
- `src/orderflow/state_machine_types.py`: `SessionLevels`, `LocationRecord`,
  `LocationRegistry`, `MarketContextStore`, `ObservationState`, and
  `ReplayContext`. In particular, trace `LocationRegistry.publish_session_levels`,
  `publish_session_extrema`, `publish_vwap`, `resolve_as_of`, and
  `available_as_of`, together with `MarketContextStore.publish`, `resolve`, and
  `latest_as_of`. `ReplayContext.publish_context` is the canonical runtime
  publication boundary; `market_context_history.py` is not the runtime owner.
- `src/orderflow/location_geometry.py`: `BAND_ATR_FRACTION` and
  `LocationBand`, including immutable band construction and intersection rules.
- `src/orderflow/location_observation.py`: `LocationObservation`, including
  timeframe, source-observation, child-observation, generation, and causal
  timestamp validation.
- `src/orderflow/value_publication.py`: `prior_profile_is_complete` and
  `publish_value_references`. The direct dependency closure is
  `src/ict/bars.py`, `src/ict/features.py`,
  `src/ict/replay/reference_streams.py`, `src/ict/value_profile.py`, and
  `src/replay/value_references.py`; these determine profile completeness,
  prior-session references, and their session-open availability.
- `src/orderflow/opening_gap_policy.py`: `OpeningGapState`,
  `OpeningGapPolicy.classify`, `transition`, and `regime_for`. These own gap
  classification, acceptance/reclaim/return/expiry transitions, and long/short
  permissions; Paper must not reproduce these rules.
- `src/orderflow/vwap_location.py`: `VwapCalculationConfig`,
  `VwapMeasurement`, and `causal_vwap_prefix`. VWAP is a completed-prefix
  measurement with source aggregate identity, availability, lineage, and a
  versioned calculation method.
- `src/orderflow/state_machine.py`: canonical composition and chronological
  ownership. Trace `ReplayMachine.__init__`, opening-reference resolution,
  `OpeningGapPolicy.transition`/`regime_for` calls, VWAP publication through
  `causal_vwap_prefix`, context publication through `ReplayContext.publish_context`,
  `publish_session_extrema`, and location resolution through
  `LocationRegistry.resolve_as_of`. This is the orchestration reference for
  later causal replay wiring, not a request to copy replay-only file loading
  into Paper live callbacks.
- `src/orderflow/market_context_history.py`: `project_history` and
  `project_saved_session` are artifact-only projections. They may validate and
  present captured context/location facts, but must not be used to reconstruct
  runtime context or infer unavailable evidence.

Canonical tests to port or run unchanged, apart from package-root import
changes, are `tests/test_orderflow_hierarchy.py`,
`tests/test_orderflow_state_machine_types.py`,
`tests/test_orderflow_value_publication.py`, `tests/test_orderflow_vwap.py`,
and `tests/test_orderflow_market_context_history.py`. The relevant contract
assertions include immutable context versions, causal availability, frozen ATR
geometry, generation replacement and as-of resolution, prior-session profile
publication, completed-prefix VWAP, and artifact-only projection behavior.

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

Phase 4 composition:

- **Phase 3.5 - Evidence publication:** consume the Phase 3 aggregate artifact
  and publish causal ATR, VWAP, opening/session levels, prior-session rows and
  levels, prior-session value references, and context generations. Evidence is
  either an explicit available publication or an explicit unavailable reason;
  Phase 4 must not infer missing values from presentation snapshots.
- **Phase 4B - Causal context/location replay:** advance one shared completed
  1m/5m clock, resolve every publication as-of its availability, and emit
  `source_observation` and `context_location` facts with generations, source
  IDs, causal parents, timestamps, lineage, and unavailable reasons.
- **Phase 4C - Combined parity harness:** run the Phase 3 source exporter,
  feed its artifact into Phase 3.5/4B, then compare source and context/location
  streams. It must not require a manually prepared Paper facts artifact.

## Phase 3.5 - Port The Canonical Evidence Publication Sequence

Status: Complete. Implemented as an artifact-driven Paper boundary with
canonical evidence parity established on valid replay sessions. The pinned canonical repository is
`C:\Users\agraw\Python\NiftyZoning` at commit
`8114f553392ad01dd032aecfed0031a3bc08e2f8`. NiftyZoning does not expose a
separate `EvidencePublication` or `AtrPublication` producer API. Its
`ReplayMachine` is the orchestration owner, and its published evidence is the
source contract to port. Paper Phase 3.5 must therefore implement one reusable
Paper-side boundary that reproduces the canonical publication sequence; it must
not invent a second canonical producer API or claim that both repositories call
the same runtime function.

### Canonical Owners And Call Order

Trace every ported symbol to the pinned canonical commit and preserve this
ordering:

1. `ReplayMachine.from_session_inputs` calls `observe_market`, derives
   `SessionLevels`, creates a `LocationRegistry`, and publishes opening-range
   and prior-day levels with `daily_atr`.
2. `value_publication.publish_value_references` supplies prior-session value
   references. Complete prior-session profiles are made available at the
   session open; missing or incomplete profiles produce no reference.
3. `ReplayContext.publish_context` publishes immutable `MarketContext` versions
   with parent context and source provenance. Context facts are recorded through
   the canonical `context_location` parity stream.
4. `LocationRegistry.publish_session_extrema` publishes only completed and
   causally available 5m extrema, replacing generations with parent lineage and
   frozen ATR bands.
5. `causal_vwap_prefix` publishes completed 1m and 5m VWAP prefixes. Each
   measurement retains source aggregate identity, source observation identity,
   observed/available timestamps, and `vwap_typical_price_volume_v1`.
6. `LocationRegistry.publish_vwap` projects each VWAP measurement into a
   location with matching timeframe, source IDs, frozen ATR, and generation.
7. `ReplayContext.record_parity_fact` and
   `state_machine_reporting.aggregate_payload` expose the resulting causal
   facts. Presentation snapshots and final state are not evidence sources.

### Required Work

- Add one reusable Paper producer boundary in the orderflow package. The parity
  harness and later runtime adapter must call it rather than deriving evidence
  in a script.
- Consume the validated Phase 3 aggregate artifact. Do not re-aggregate raw
  bars, reread presentation snapshots, or infer evidence from terminal state.
- Port canonical ATR lookup semantics using the canonical `daily_atr` evidence
  contract. When ATR is unavailable, preserve the canonical unavailable event
  and do not create ATR-dependent bands or VWAP locations.
- Port `SessionLevels` publication through `LocationRegistry`, including
  opening-range and prior-day levels, their source sessions, availability, and
  frozen ATR. Prior-day levels must remain available at session open.
- Port developing session-extreme publication exactly through
  `publish_session_extrema`: completed 5m inputs only, generation replacement,
  parent extreme generation, source aggregate identity, and causal availability.
- Port prior-session value publication through
  `publish_value_references`, preserving profile completeness, reference
  identity/fingerprint, scope, lineage, and session-open availability. Missing
  value evidence remains unavailable.
- Port immutable context publication through `MarketContext` and
  `ReplayContext.publish_context`, preserving context ID, version, parent
  context ID, source timeframe, source provenance, and availability. Do not
  reconstruct context from scores or presentation output.
- Port `causal_vwap_prefix` and `LocationRegistry.publish_vwap` without changing
  timeframe filtering, completed-prefix ordering, source observation IDs,
  calculation version, or ATR-band construction.
- Project comparable records using the parity contract's common record shape:
  `kind`, `occurred_at`, `observed_at`, `available_at`, `sequence`, `identity`,
  `causal_parent_id`, `values`, and `unavailable`.
- Serialize the Paper evidence publication deterministically for comparison.
  This is a parity artifact, not a new canonical domain model.

### Phase 3.5 Exit Gate

- Paper reproduces the canonical evidence publication sequence from the same
  Phase 3 aggregate shape for the eligible parity dates `2026-10-01` and
  `2026-10-05`. Both sessions have matching Phase 3 manifests; Phase 3.5
  reports accepted `INPUT_VARIANCE` only where ATR source histories differ.
- Canonical reference output is captured from `ReplayMachine` and
  `state_machine_reporting`; Paper output is produced by the ported boundary.
  Parity is established against canonical output, not by importing NiftyZoning or
  requiring a shared runtime producer implementation.
- Focused tests cover ATR availability, opening/prior-day publication,
  session-open value availability, extrema generation replacement, source
  lineage, frozen bands, VWAP calculation version, immutable context versions,
  unavailable evidence, and deterministic evidence serialization.
- The Phase 3.5 harness runs the Phase 3 exporter first, invokes the Paper
  producer on its aggregate artifact, obtains the canonical comparison payload
  through the pinned canonical replay/reporting path, compares manifests before
  records, and reports the first stream/field mismatch.
- A missing stream is `INCOMPLETE` or `UNAVAILABLE` according to the parity
  contract; it is never treated as an empty successful publication.
- No Phase 4B chronological replay, Phase 4C combined policy parity, runtime,
  database, API, or Paper-specific policy derivation is included in this phase.

### Phase 3.5 Implementation Record

- `src/ftx_paper/strategies/orderflow/evidence/` now owns the reusable
  artifact-driven producer and deterministic evidence serialization.
- The producer validates the Phase 3 aggregate envelope, preserves explicit
  unavailable evidence, and invokes the ported ATR, session-level, extrema,
  VWAP, value, and context contracts through `EvidenceAdapter`.
- `scripts/orderflow/orderflow_replay.py` runs an `OrderflowReplayEngine`
  against raw session rows selected through the Paper futures source resolver.
  It prepares prior-session inputs, derives observations, and emits one
  canonical-shaped replay record per source row with Phase 3.5 evidence as of
  that row's decision clock.
- The runner accepts `--date`, `--runtime-dir`, and an optional `--output`. It
  emits only the bounded Phase 3.5 evidence replay artifact; Phase 4 facts,
  execution, and comparison/reporting remain out of scope. Missing evidence
  stays `UNAVAILABLE` and is never treated as parity.
- Focused producer tests and the complete `tests/strategies/orderflow` suite
  pass.
- `scripts/orderflow/compare_canonical_replay.py` runs the canonical
  NiftyZoning exporter/replay and the Paper exporter/replay in isolated
  subprocesses. It compares Phase 3 source/aggregate sections first, then
  compares the canonical Phase 3.5 projection with Paper evidence using
  manifest-first diagnostics.
- The Paper replay injects computed prior-session value publications into the
  evidence adapter and projects them into the canonical value-reference shape.
- Validation command:
  `python scripts/orderflow/compare_canonical_replay.py --date 2026-09-15 --date 2026-09-29`
  exits successfully; focused orderflow tests pass (`13 passed`), and the
  changed replay scripts pass Pyright and Ruff.

### Temporary Harness Retirement

`scripts/orderflow/orderflow_replay.py` exists only to generate bounded
Phase 3.5 evidence artifacts during the staged port. Its raw-row loading,
prefix iteration, and artifact projection are harness orchestration, not a
second orderflow implementation. Production Paper live and replay must invoke
the same canonical transition boundary with separate input/clock adapters.
Retire this script only during Phase 9, after the production Paper replay adapter
and live input adapter both invoke the shared canonical transition boundary and
can produce the required evidence, replay facts, execution facts, and recovery
snapshot without this harness. Do not promote it into the live callback path or
retain it as a parallel replay engine. Until that gate passes, it remains a
temporary parity tool and must not be treated as the production replay entry
point.

Phase 4 input contract:

```text
Phase 3 market observation artifact
    -> Phase 3.5 evidence publication
    -> Phase 4 causal replay
    -> Phase 4 parity artifact
```

The Phase 4 evidence adapter owns the translation of stored or canonical
publications into this contract. It does not define replacement rules,
fallback values, permissions, or location priority. Those remain canonical
domain behavior.

Phase 4 parity verification record:

- The ported context, value, permission, location, geometry, VWAP, opening-gap,
  immutable-history, causal replay, and explicit-unavailable contracts are
  covered by focused Paper tests.
- Canonical context/value/history parity tests pass in the canonical repository.
- Source-observation parity is `MATCHED` for `2026-10-01` and `2026-10-05`;
  dates before `2026-09-21` remain prohibited by the comparison harness.
- The runtime-neutral replay boundary can emit explicit causal
  `source_observation` and `context_location` streams when Phase 4 evidence
  publications are supplied. The canonical replay now publishes the same
  streams through its explicit parity recorder and Phase 4 projection.
- The combined `compare_canonical_replay.py` entry point runs Phase 3, Phase
  3.5, and Phase 4B without a manually prepared Paper facts artifact.
- Phase 4B uses one shared completed-observation clock. Aggregates whose
  `available_at` is after the final source clock are excluded; this resolved
  the prior `448` versus `450` source-observation discrepancy.
- For `2026-09-15`, `source_observation` identities and counts match. The
  `context_location` stream reaches comparison and reports the expected ATR
  source-history variance: canonical ATR-dependent locations are available,
  while Paper preserves explicit ATR-unavailable evidence.
- Cross-repository comparison accepts `INPUT_VARIANCE` only when the ATR
  difference is bounded by `max(2.5, 1% of canonical ATR)`. ATR-dependent
  location geometry (`band_low`/`band_high`) must satisfy that same numeric
  tolerance; nonnumeric or oversized differences remain `MISMATCHED`.
- The `2026-10-01` and `2026-10-05` parity runs reached the accepted Phase 4
  gate. `close_relation`, location `method`, observation IDs, and related
  source/reference provenance match canonical records; only bounded ATR band
  geometry remains classified as `INPUT_VARIANCE`.
- Prior-session value input variance is handled by the same bound for `POC`,
  `VAH`, and `VAL`. Those tolerant values and producer fingerprints derived
  from them are excluded from the comparison fingerprint; reference identity,
  scope, source sessions, availability, and other non-tolerant fields must
  still match. For `2026-10-05`, the prior profile differences are bounded and
  report `INPUT_VARIANCE`, not an independent mismatch.
- Focused Paper Phase 4 replay tests pass (`4 passed`), Ruff and Pyright pass
  for the changed Paper replay files, and canonical Phase 4 projection lint
  passes. Two pre-existing canonical `InteractionDetector` test failures are
  unrelated to this phase.

Phase 4 implementation record:

- `context_location_replay.py` advances the shared completed-observation clock,
  resolves context and locations as-of availability, and emits point-in-time
  generations, source IDs, causal parents, timestamps, lineage, and explicit
  unavailable reasons.
- `replay_context_location_artifact()` consumes the completed aggregate sections
  emitted by Phase 3 without re-aggregating raw bars.
- The combined Phase 3 harness is the intended Phase 4C entry point; it must
  supply Phase 4 evidence through the evidence adapter before claiming parity.
- `phase5_parity_harness.py` compares explicit source/context streams and keeps
  missing streams unavailable; it no longer reconstructs facts from snapshots.
- `compare_canonical_replay.py` now includes the Phase 6 opportunity-lifecycle
  stream and compares explicit Paper lifecycle facts without reconstructing
  them from snapshots.

## Phase 4.5: Add The Causal Interaction Replay Boundary

Phase 4.5 is the runtime-neutral layer between the pure context/location
contracts and Phase 5 event-stream parity. It adds no interaction or response
policy; it supplies the chronological orchestration required to exercise the
already ported domain contracts.

Status: Boundary implemented. Deterministic interaction/response primitives,
canonical policy dispatch, and chronological first-mismatch comparison are now
available. Cross-repository Phase 4.5 parity is established for the validated
2026-10-01 replay; Phase 3.5/4 ATR-dependent context/location variance remains
accepted input variance. Phase 9 runtime/database integration is deferred.

Implemented boundary scope:

- Add one runtime-neutral interaction replay boundary consuming the explicit
  Phase 4B streams, with independent `1m` and `5m` detector state.
- Advance only newly available response-timeframe evidence and emit explicit
  interaction transition facts before wiring opportunity lifecycle behavior.
- Port the canonical PD policy dispatch at that boundary; do not select
  templates from location matches in the orchestration shell.

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

Phase 4.5 verification record:

- The 2026-10-01 Canonical/Paper replay comparison reports `MATCHED` for the
  `interaction_response` stream. Phase 3 is `MATCHED`; Phase 3.5 and Phase 4
  report only bounded ATR-dependent `INPUT_VARIANCE`.
- The remaining Phase 4.5 mismatch was a Paper replay bug. Canonical evaluates
  standard interactions only when a new 5-minute confirmation context is
  available, preserves repeated evaluations for active interactions, and keeps
  their original causal timestamps. Paper previously dispatched on every
  availability clock and deduplicated repeated facts by `(kind, identity)`.
- Paper now dispatches on the canonical 5-minute boundary, preserves later
  publication order separately from causal timestamps, and retains frozen
  interaction provenance for VWAP responses. Ended interactions remain gated
  and cannot produce later policy facts.
- The VWAP ordering/provenance regression and delayed repeated-fact publication
  are covered by focused tests. The changed Paper files pass Pyright and Ruff;
  the focused orderflow suite passes 25 tests.

## Phase 5: Port And Verify Layer 3 - Interactions And Responses

Validate canonical observations, interaction episodes, response hypotheses, and
response outcomes before opportunity formation.

Status: Complete for the deterministic Canonical contract and Phase 4.5 replay
parity gate. Runtime and database parity remain deferred to Phase 9.

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
- Paper Phase 5 interaction/response and existing orderflow tests pass (50 tests).
- Independent 1m/5m episode state, causal observation validation, frozen
  reference/source lineage, duplicate idempotence, response arming and terminal
  outcomes, explicit incomplete evidence, and deterministic first-field mismatch
  diagnostics are covered by focused tests.
- Response evaluation matches Canonical for carry-session expiry,
  `response_window_expired`, instrument and availability filtering, contact and
  terminal candle boundaries, as-of filtering, unavailable evidence, and
  invalidation-before-confirmation ordering.
- Standard arbitration reasons match Canonical for single latest responses,
  same-time rejection precedence, and later causal responses superseding prior
  responses.
- Regenerated cross-repository replays for `2026-10-01` and `2026-10-05` report
  Phase 3 `MATCHED`, accepted ATR/prior-value `INPUT_VARIANCE` for Phases 3.5/4,
  and Phase 4.5 `MATCHED`.
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

Phase 6 verification record:

- Canonical opportunity and timeout tests pass (30 tests) at the pinned baseline.
- Paper Phase 6 opportunity, setup, timeout, lineage, future-evidence, and
  deterministic identity tests pass together with the Phase 4/5 gates (18 tests).
- Opportunity/setup contracts currently cover causal identity, parent context,
  response evidence lineage, retest stages, explicit expiry/invalidation/
  supersession, execution-group eligibility, and duplicate timeout observation
  idempotence.
- Ruff, Pyright, and `git diff --check` pass for the changed Paper scope.
- The canonical-vs-Paper interaction/response replay now reports Phase 4.5
  `MATCHED` for both source-matched dates. The integrated Phase 6 lifecycle
  replay gate reports accepted `INPUT_VARIANCE` for `2026-09-29` and
  `2026-10-01`; the remaining differences are bounded ATR-dependent lifecycle
  geometry, not control-flow or candidate-numbering mismatches.
- Opening seeds are evaluated exactly once on the first completed 5m candle;
  later 5m candles cannot become invented opening seeds. Admission margin is
  coordinator-configured, normal lifecycle scoring uses the latest causally
  available 5m context, formation uses the structure confirmation clock, and
  opening score overrides use the configured margin.
- Canonical lifecycle projection includes explicitly recorded opening seed
  lifecycles and orders records by candidate number, keeping the canonical and
  Paper candidate streams aligned. Regression coverage includes this projection
  and the opening lifecycle behavior; the focused orderflow suite passes.
- No runtime/database parity claim is made; Phase 9 remains the host
  integration and persistence gate.

## Phase 7: Port And Verify Intents, Admission, Allocation, And Execution

Validate the complete canonical execution lifecycle only after the opportunity
and setup-lifecycle gate passes.

Status: **Complete for the Phase 7 replay parity gate.** The shared execution
owner now covers admission, ownership transfer, entry/exit execution facts, and
artifact parity for the validated source-matched dates. Remaining host
integration work belongs to Phase 8/9 and is not a Phase 7 blocker.

### Phase 7 Ownership And Non-Goals

NiftyZoning remains the sole owner of orderflow intent, admission, allocation,
execution, lifecycle settlement, and orderflow-ledger semantics. Paper may own
only input/clock adaptation, diagnostic capture, Paper execution-account
mapping, and persistence of canonical facts. The Paper capital/equity ledger is
a separate host owner.

Do not resolve this phase by publishing explicit empty streams when execution
was not evaluated, deriving facts from trades or terminal snapshots, adding a
Paper-specific coordinator/risk rule/template selector/settlement rule, or
mapping a lifecycle directly to an order without canonical admission,
allocation, reservation, trigger, and fill transitions.

### Phase 7A - Port The Canonical Execution Dependency Closure

Port the pinned canonical implementation and direct dependencies, preserving
symbols, transitions, causal timestamps, unavailable outcomes, and ownership:

- `src/orderflow/hierarchy.py`: `TradeIntent`, `ExitIntent`,
  `PositionManagement`, intent statuses, entry/exit transitions, and setup
  execution state;
- `src/orderflow/intent_policy.py`: entry/exit admission and decision-context
  rules;
- `src/orderflow/active_position_gate.py`: active-position and execution-group
  gates;
- `src/orderflow/allocation.py`: portfolio, session, direction, cooldown, and
  sizing gates;
- `src/orderflow/execution.py`: `ExecutionCoordinator`, entry/exit timing,
  trigger evaluation, reservations, partial fills, protective stops, settlement,
  and realized points;
- execution portions of `state_machine_types.py`: `ReplayContext`,
  `ExecutionConstraints`, `ExecutionProposal`, execution state, and
  `record_parity_fact`.

The current Paper `execution.py` is only an entry-state scaffold. It must not
be extended into a second strategy implementation; replace or retire it as
required by the canonical port and port the corresponding canonical tests.

### Phase 7B - Add The Shared Execution Transition Boundary

Extend the ported state-machine orchestration so one completed available
observation advances the same execution owner in live and replay, in canonical
order: lifecycle/setup registration; execution-group eligibility; admission and
unavailable/denied outcomes; decision-context and allocation/risk gates; intent
arming/effective time/cancellation; trigger/reservation/next-bar fill; position
management, protective stops, exits, partial fills, settlement, and idempotent
retry handling.

Replay may bound input and capture facts. Live may adapt feed callbacks and
Paper execution notifications. Neither path may own a separate strategy state
machine or alter canonical policy calculations.

### Phase 7C - Publish Canonical Execution Facts

Add the smallest Paper equivalent of canonical
`state_machine_reporting.build_replay_payload()` as a projection over the
ported execution owner. It must publish producer-owned facts under
`parity_phase_facts`, never reconstruct them from presentation output.

The streams are compared in this order:

1. `intent_admission`
2. `trigger_exit`
3. `execution_fills`
4. `execution_transitions`
5. `ledger_mutations`

Each record preserves `kind`, `identity`, `causal_parent_id`,
`occurred_at`, `observed_at`, `available_at`, `values`, and `unavailable`.
Ledger mutations derive from canonical reservation/admission history,
authorizations, fills, cancellations, exits, and settlement. Missing streams
remain `UNAVAILABLE`/`INCOMPLETE`; they are never treated as successful empty
parity.

### Phase 7D - Paper Host Adapter

Only after canonical proposal/fill facts exist, map successful canonical fills
to the existing Paper `OrderIntent`/`ExecutionService` boundary. Diagnostic mode
must not mutate Paper account state. Explicit Paper execution may mutate only
the existing Paper portfolio/accounting owner and must never submit broker
orders.

### Phase 7 Exit Gates

- Canonical intent, admission, allocation, execution, and reporting tests pass
  against the pinned commit, with only package-root import adaptations.
- Live and replay invoke the same ported execution transition boundary.
- Admission, intent, reservation, trigger, fill, partial-fill, stop, exit,
  cancellation, settlement, and realized-point records match in causal order.
- All five Phase 7 streams are explicit producer facts and manifests match
  before records are compared.
- Duplicate/retry replay does not duplicate reservations, fills, exits, or
  ledger mutations.
- Diagnostic replay makes no Paper account mutation; explicit Paper execution
  uses only the existing accounting adapter.
- Cross-repository parity passes on a no-trade date and a date with canonical
  entries, exits, and settlement. Any missing stream blocks the phase.

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

### Phase 7 Initial Slice Verification Record

- `transition.py` now owns the shared chronological boundary for live and
  bounded replay, including one observation coordinator, one execution owner,
  a monotonic completed-bar clock, and the producer parity stream collection.
  Replay lifecycle execution is advanced through this owner; the live strategy
  constructs the same owner instead of discarding execution state.
- `hierarchy.py` now carries the canonical intent statuses, entry policy, intent
  transitions, and causal trigger/fill guards from pinned commit
  `8114f553392ad01dd032aecfed0031a3bc08e2f8`.
- `execution.py` owns the Paper-side canonical execution state for admission,
  intent arming, trigger selection, reservations, and entry fills. It does not
  call Paper accounting or submit broker orders.
- `allocation.py` preserves canonical first-match portfolio, session, direction,
  and cooldown gates and keeps unavailable inputs as explicit reasons.
- Focused tests cover unresolved-regime admission, allowed intent arming,
  deterministic reservation release, duplicate entry selection idempotence, and
  rejected duplicate fills. The Phase 7 replay gate now passes for the validated
  source-matched dates; bounded ATR-dependent differences are reported as
  accepted `INPUT_VARIANCE`.

### Phase 7 Stream Contract And Current Evidence

The canonical owners are `src/orderflow/hierarchy.py` (`TradeIntent`,
`ExitIntent`, `PositionManagement`, and lifecycle transitions),
`src/orderflow/execution.py` (`ExecutionCoordinator` and execution calculations),
`src/orderflow/state_machine_types.py` (`ReplayContext.record_parity_fact`), and
`src/orderflow/state_machine_reporting.py` (`build_replay_payload`). The shared
projection is `phase7_projection.py` in each repository.

The stream order and common record shape are defined in Phase 7C above.
Manifests are compared before records; records are then compared in stream and
sequence order, reporting the first stream, record, or field mismatch.
Canonical replay payloads publish fills, execution transitions, reservations,
authorizations, and settlement mutations through `parity_phase_facts` without
reconstructing them from trades or terminal snapshots. Paper invokes the shared
execution owner from the chronological interaction clock, transfers same-
direction position ownership to successors, and emits the corresponding
execution diagnostics and parity facts. Parity artifact construction also
normalizes embedded execution-group timestamps so local canonical labels and
UTC Paper timestamps compare as the same causal instant.

Verification evidence for the requested source-matched dates (`2026-10-01`,
`2026-10-05`) is:

- Phase 3: `MATCHED`.
- Phase 3.5: `INPUT_VARIANCE`, with bounded ATR/prior-value variance.
- Phase 4: accepted bounded ATR-dependent `INPUT_VARIANCE`.
- Phase 4.5: `MATCHED`.
- Phase 6: accepted bounded ATR-dependent `INPUT_VARIANCE`.
- Phase 7: accepted `INPUT_VARIANCE`; the comparison gate passes for both
  `2026-10-01` and `2026-10-05`.

`execution_adapter.py` now defines the Paper host boundary: it maps an already
triggered canonical `TradeIntent` to the existing `OrderIntent` contract and
delegates explicit execution to `ExecutionService`. Diagnostic mode returns the
proposal without calling the service or mutating account state; the strategy
boundary accepts execution notifications without owning a second coordinator.

The bounded ATR tolerance remains 1% relative / 2.5 absolute, and prior-value
variance remains accepted only through the existing Phase 3.5 input-variance
rule. Phase 7 is complete for the bounded replay parity contract. Remaining
work is the reporting/artifact contract and host integration described in Phase
8 and Phase 9; no new Paper execution policy should be added.

## Phase 8: Canonical Reporting And Four-Layer Parity Artifacts

Status: **In progress (8D-8F implementation complete; cross-repo replay evidence pending).**

Port the canonical producer-reporting contract before exposing orderflow through
Paper API or runtime events. This is an artifact and validation phase, not a
human-readable reporting or host-integration phase. The Phase 7 projection and
manifest helpers are groundwork only: they do not satisfy the canonical
four-layer artifact contract.

### Canonical Scope And Current Gap

Trace the implementation to the pinned NiftyZoning commit:

- `src/orderflow/state_machine_reporting.py`: `build_replay_payload` and the
  causal source/decision/execution projections. Port only fields that are
  producer-owned parity facts; do not port presentation summaries, waterfalls,
  trade tables, or shadow-outcome reporting unless a later approved contract
  requires them.
- `src/orderflow/parity_artifacts.py`: `PHASE_STREAMS`, `LAYERS`, record
  validation, coverage/status rules, canonical hashing, artifact construction,
  artifact validation, JSON/JSONL persistence, and bounded `read_parity_page`.
- `src/orderflow/phase7_projection.py`: retain the five execution streams and
  use them as explicit inputs to the final artifact. Do not infer them from
  trades, terminal snapshots, counters, or presentation output.

Paper currently has separate `build_phase4_artifact` and `build_phase7_artifact`
helpers. Phase 8 must replace that split acceptance path with one canonical
artifact contract. It must not silently treat a missing stream as an observed
empty stream, and it must not claim parity from the top-level execution fields
that are only projections.

### Contract Shape

The artifact must declare all six decision substreams in canonical order:

1. `source_observation`
2. `context_location`
3. `interaction_response`
4. `opportunity_lifecycle`
5. `intent_admission`
6. `trigger_exit`

It must also declare all four final layers:

1. `run_scope`: source identity, selected/filtered/completed rows, gaps, and
   processed decision bars;
2. `decision`: interaction, candidate, admission, trigger, and exit decisions;
3. `execution_settlement`: reservations, authorizations, submissions, order
   intents, fills, pending exits, exits, cancellations, and settlement;
4. `portfolio_ledger`: reservations, releases, fills, partial fills,
   cancellations, exit settlement, daily reset, and closeout.

Every phase and layer record must preserve `kind`, causal timestamps,
contiguous `sequence`, stable unique `identity`, `causal_parent_id`, `values`,
and explicit `unavailable`. Normalize timestamps to UTC, reject naive or
invalid timestamps, reject duplicate identities, and reject invalid layer kinds.
Final layers may be built only when every required phase stream has a valid
`MATCHED` coverage entry. `UNAVAILABLE`, `INCOMPLETE`, and observed-empty are
distinct states and must remain distinct in the artifact and manifest.

### Required Work

- Add one Paper `build_parity_artifact` boundary matching the canonical
  envelope: artifact type/schema, contract identity, producer, run scope,
  coverage, decision substreams, final layers, and manifest.
- Make `build_replay_payload` expose the explicit facts and coverage needed by
  that boundary. It may project canonical facts, but it may not manufacture
  missing streams or derive a decision from a report or terminal state.
- Replace the current split Phase 4/Phase 7 comparison path with one
  manifest-first comparator that checks phase streams in order, then final
  layers, and reports the first stream, record, or field mismatch.
- Implement canonical SHA-256 hashing, excluding only the explicitly allowed
  producer-extension fields. Hashes must be deterministic across process runs
  and must not include Paper broker, transport, database, or runtime metadata.
- Persist the validated envelope, each decision/layer stream as JSONL, and the
  manifest using the canonical filenames and schema. Add bounded offset/limit
  pagination that reads persisted facts without re-evaluating strategy state.
- Keep runtime-boundary snapshot/recovery comparison on the existing Phase 2
  contract. Do not duplicate runtime recovery in the Phase 8 artifact builder;
  live/replay persistence and API wiring remain Phase 9.

### Phase 8 Delivery Sequence

Phase 8 will replace the split Paper acceptance path with one canonical artifact
contract. The existing Phase 4 and Phase 7 projections may remain temporarily as
producer-side extraction helpers, but neither is a parity artifact or an
independent acceptance gate.

#### 8A - Freeze The Canonical Contract Surface

- Port the pinned canonical constants and schemas: `PHASE_STREAMS`, `LAYERS`,
  decision stages, execution kinds, ledger kinds, artifact identity, and schema
  version.
- Record the exact Paper-to-canonical symbol mapping and remove any acceptance
  logic that treats a missing stream as an observed empty stream.
- Define the explicit payload inputs: six phase streams, four final layers, and
  phase/layer coverage. Missing inputs remain `UNAVAILABLE` or `INCOMPLETE`.

Exit gate: a contract-focused test rejects missing stream/layer declarations and
accepts only the canonical envelope shape.

Phase 8A verification record:

- `parity_artifacts.py` now exposes the pinned canonical artifact identity,
  schema version, six decision streams, four layers, decision stages, execution
  kinds, ledger kinds, and explicit payload keys.
- `validate_contract_declarations` rejects missing or extra stream/layer
  declarations.
- Missing phase facts remain `UNAVAILABLE`; a declared empty stream is distinct
  `MATCHED` `observed_empty` coverage.
- `tests/strategies/test_orderflow_phase8_contract.py`: 4 passed.
- Ruff, Pyright, and `git diff --check`: passed.

#### 8B - Port Producer Fact Projection

- Change `build_replay_payload` to expose only producer-emitted facts and
  coverage needed by the unified builder.
- Stop deriving ledger or execution facts from terminal `ExecutionState`, fills,
  trades, counters, or presentation summaries. If the canonical producer did not
  emit a fact, preserve it as unavailable.
- Keep the existing Phase 4 and Phase 7 helpers as explicit extraction steps
  only where they preserve canonical stream identity and causal fields.

Exit gate: no-trade and execution-bearing fixtures expose all required stream
keys without manufacturing absent evidence.

#### 8C - Implement Unified Artifact Construction And Validation

- Add `build_parity_artifact` and `build_parity_artifact_from_payload` with the
  canonical envelope, `decision_substreams`, `layers`, `coverage`, and manifest.
- Validate UTC timestamps, availability ordering, contiguous sequences, unique
  identities, causal parent types, required run scope counts, decision stages,
  execution kinds, and ledger kinds.
- Require every phase stream to be `MATCHED` before accepting final layers.
  Preserve `MATCHED`, `MISMATCHED`, `INCOMPLETE`, and `UNAVAILABLE` distinctly.
- Match canonical record hashing, excluding only `producer_extensions`, and
  validate manifest count/hash/coverage consistency.

Exit gate: tampered records, invalid layer kinds, duplicate identities, invalid
timestamps, coverage mismatches, and non-contiguous sequences are rejected.

#### 8D - Replace Comparison With Manifest-First Parity

Status: **Complete.**

- Add canonical-order comparison for all six phase streams followed by all four
  layers.
- Compare status, coverage, count, and hash before loading records.
- On a hash/count mismatch, report the first stream, record, and field mismatch
  in deterministic causal order.
- Remove Phase 4/Phase 7 split comparison as an acceptance path; retain only
  diagnostics needed to explain a unified-artifact mismatch.

Exit gate: comparison passes for no-trade and entry/exit/settlement sessions and
stops at the first deterministic mismatch.

#### 8E - Persist And Read Canonical Artifacts

Status: **Complete.**

- Persist the validated envelope, six decision-stream JSONL files, four layer
  JSONL files, and the canonical manifest filenames.
- Add bounded `offset`/`limit` pagination that reads persisted records only and
  never re-evaluates strategy state.
- Round-trip persisted artifacts through validation and preserve stable ordering.

Exit gate: JSON and JSONL round-trip tests pass, pagination is bounded, and
tampering is detected before persistence.

#### 8F - Run The Phase 8 Parity Gate

Status: **Complete for the artifact contract gate; cross-repo replay gate remains unavailable.**

- Port and adapt the canonical parity-artifact tests with package-root imports
  only; do not copy presentation/reporting tests that are outside the contract.
- Run focused Paper tests, Ruff, Pyright, and `git diff --check`.
- Record the verification evidence and exact remaining `UNAVAILABLE` inputs in
  this plan.

Exit gate: all Phase 8 artifact checks pass. Phase 9 remains blocked until this
gate is recorded; no API, runtime-event, or database parity claim is made from
the temporary Phase 8 harness.

### Phase 8 Exit Gate

- Canonical reporting and parity-artifact tests pass at the pinned commit, with
  only package-root import adaptations.
- Paper emits all six decision substreams and all four final layers with
  explicit coverage; missing or invalid evidence is `INCOMPLETE` or
  `UNAVAILABLE`, never successful empty parity.
- Artifact validation rejects missing stream declarations, invalid timestamps,
  non-contiguous sequences, duplicate identities, invalid layer kinds, coverage
  mismatches, and tampered record hashes.
- Manifest comparison checks status, count, and hash before loading records, then
  reports the first causal mismatch in deterministic stream/sequence/field
  order. Contract fixtures cover no-trade and execution-bearing record shapes;
  a cross-repo session comparison is not claimed without a complete canonical
  artifact.
- Persisted JSONL pages round-trip through the reader with stable ordering and
  bounded `offset`/`limit`; pagination does not mutate or recompute facts.
- The artifact contains no Paper transport, broker, database, or runtime
  metadata in canonical values, and no human-readable summary is used as a
  parity source.
- Phase 9 remains blocked until this gate passes; no API/runtime event or DB
  parity claim is made from the Phase 8 harness alone.

### Phase 8 Verification Record

- `tests/strategies/test_orderflow_phase8_contract.py`: 10 passed, including
  manifest-first comparison, deterministic first `stream`/`sequence`/`field`
  reporting, JSON/JSONL round-trip, and bounded pagination.
- `tests/strategies/test_orderflow_phase_comparison.py`: 17 passed; retained
  Phase 4/7 helpers remain diagnostic-only.
- Ruff, Pyright, and `git diff --check`: passed for changed implementation,
  harness, and tests.
- Remaining `UNAVAILABLE` inputs in contract fixtures: none. The latest
  cross-repo replay rerun intentionally excludes `2026-10-29`, whose retained
  canonical source history is incomplete and produces an out-of-bound ATR
  variance.
- For the eligible `2026-10-01` and `2026-10-05` replays, Phase 3.5, Phase 4,
  Phase 4.5, and Phase 6 establish matching causal structure with bounded ATR
  input variance. Their Phase 7 first differences are downstream execution
  stream consequences of that accepted ATR-dependent evidence variance, not a
  separate Paper lifecycle or execution-policy defect. The diagnostic records
  remain retained rather than being treated as independent parity failures.
- Unified parity still reports a separate comparison-layer timestamp
  normalization mismatch: canonical embedded `available_at` values use session
  labels while Paper values are serialized as UTC timestamps. The Phase 8
  cross-repo exit gate therefore remains open pending timestamp normalization
  and a rerun of the eligible dates.

## Phase 9: Add The Paper Host Adapter

Only after the ported canonical layers and artifact checks pass, integrate them
with Paper.

Status: **In progress; host persistence and execution seams are implemented, but
the Phase 8 cross-repository parity gate and full replay-owner migration remain
open.** The live strategy boundary,
shared transition owner, generic `RuntimeSession` dispatch, and generic
`ReplayWorker` provide host seams, but the complete canonical orderflow state and
execution path is not yet wired through Paper live/replay. The temporary
`scripts/orderflow/orderflow_replay.py` harness remains the parity entry point
until this phase's retirement gate passes.

### Phase 9 Lessons From Phases 3-8

The implementation and replay failures so far add these constraints to the host
adapter:

- A host coordinator must not become a second orderflow state machine. The
  canonical transition and execution owners are created once per runtime and
  shared by live and replay adapters.
- Canonical timestamps and Paper UTC timestamps are normalized at the boundary.
  Session labels remain metadata and are never compared as raw strings.
- Missing ATR, prior-session value, context, location, or execution evidence
  remains `UNAVAILABLE` or `INCOMPLETE`; it is never converted to an empty
  successful stream.
- Execution notifications return to the canonical transition owner. They do not
  justify a second Paper coordinator, and conflict/ownership/fill ordering stays
  canonical.
- Replay receives a fresh owner and explicit bounded inputs. It cannot read live
  runtime state, broker state, or the Paper account database implicitly.
- Persistence and reporting observe canonical facts. They do not reconstruct
  decisions from trades, counters, terminal snapshots, or summaries.
- Recovery is a snapshot contract. Startup may mark an unrecoverable interrupted
  session `STOPPED` with one audit event, but must not rebuild state from future
  bars or create a second engine.

### Phase 9 Delivery Sequence

#### 9A - Close The Phase 8 Prerequisite

- Normalize embedded `available_at`, `observed_at`, and parent timestamps in the
  unified artifact comparison to UTC.
- Rerun source-matched dates beginning with `2026-09-15` and `2026-10-01`; keep
  `2026-10-29` excluded while its canonical source history is incomplete.
- Record manifest-first comparison evidence before changing host wiring.

Exit gate: the cross-repository Phase 8 comparison is `MATCHED` or an explicitly
accepted bounded `INPUT_VARIANCE`; unit fixtures alone do not unblock Phase 9.

Phase 9A verification record:

- `scripts/orderflow/compare_canonical_replay.py` now normalizes top-level and
  embedded causal timestamps in unified phase/layer records to UTC before
  `build_parity_artifact_from_payload`; prior-session embedded dates use their
  own session date. The comparison date floor is `2026-09-15`.
- `tests/strategies/test_orderflow_phase8_contract.py`: 11 passed, including
  canonical local-label versus Paper UTC normalization.
- Replay report:
  `data/output/orderflow/canonical-replay-comparison-20261102T115443Z-84918a42.json`.
- `2026-09-15`: Phase 3 `MATCHED`; Phase 3.5/4 accepted bounded
  `INPUT_VARIANCE`; Phase 4.5 `MATCHED`; Phase 6 remains `MISMATCHED` due a
  lifecycle evidence difference; Phase 9A remains blocked.
- `2026-10-01`: Phase 3 `MATCHED`; Phase 3.5/4/6 accepted bounded
  `INPUT_VARIANCE`; Phase 4.5 `MATCHED`; Phase 7 and unified parity remain
  downstream `MISMATCHED` execution consequences. Phase 9A remains blocked.
- The rerun produced no remaining comparison-layer timestamp mismatch; the
  remaining failures are causal/lifecycle and ATR-dependent evidence, not UTC
  serialization differences. `2026-10-29` was not included.

#### 9B - Define The Host Adapter Contracts

Status: **Implemented for the versioned contract boundary.** Runtime composition
and persistence wiring remain in later Phase 9 subphases.

- Define one versioned input adapter from completed broker/stored-bar evidence to
  the canonical observation contract. Live emits one available observation at a
  time; replay emits the same shape from bounded ordered input.
- Define one versioned output adapter for events, proposals, snapshots, and
  artifacts. Paper transport/runtime metadata stays outside canonical values.
- Make UTC conversion, source identity, availability, lineage, and unavailable
  reasons explicit in the adapter schema.

Exit gate: live-style and bounded-replay fixtures produce identical canonical
input envelopes and no adapter performs policy or gap repair.

Phase 9B verification record:

- `strategies/orderflow/host_adapter.py` defines the versioned
  `orderflow_paper_host_adapter` input/output contract.
- `MarketEvidence` requires explicit source identity, UTC observed/available
  timestamps, lineage, values, and unavailable reasons; it does not infer gaps,
  completion, or policy facts.
- `LiveHostInputAdapter` and `ReplayHostInputAdapter` share
  `CanonicalInputAdapter` and the existing canonical transition boundary.
- `CanonicalOutputAdapter` projects supplied events, proposals, snapshots, and
  artifacts without reconstructing decisions.
- Focused Phase 9B tests pass: live and bounded replay produce identical
  canonical state, unavailable evidence is preserved, invalid timestamps/order
  are rejected, and the output envelope is versioned.

#### 9C - Compose One Canonical Runtime Owner

Status: **Implemented for retained runtime composition; recovery of the full
canonical transition graph remains open.**

- Extend `RuntimeSession` to own the orderflow transition state directly, or add
  a narrowly scoped `PaperOrderflowRuntime` only if direct composition is not
  possible.
- Construct observation, context, opportunity, and execution owners once per
  live session and route them through the existing transition boundary.
- Keep subscriptions, lifecycle, clock, notifications, and snapshots in Paper;
  keep all policy and lifecycle decisions canonical.

Exit gate: a fixed prefix produces the same state, event, decision, and execution
prefix through live and bounded replay.

#### 9D - Replace The Temporary Replay Path

Status: **Partially implemented.** Bounded cancellation-aware replay and fresh
`PaperOrderflowRuntime` composition are available through the shared strategy
boundary. The reusable canonical input/formation providers now publish bounded
causal evidence through that boundary for live-style and replay construction.
The existing `ReplayWorker` still contains generic Paper execution orchestration,
and the temporary harness is intentionally retained until the production replay
path owns all canonical capture responsibilities.

- Make `ReplayWorker` own only bounded loading, scope, cancellation, clock,
  capture, and fresh runtime composition.
- Route every replay observation through the live transition owner; do not call
  live feed callbacks from replay.
- Move artifact persistence behind the production runtime/store boundary.

Exit gate: replay retry is idempotent and isolated from live/account state.

Phase 9 provider verification record:

- `CanonicalInputProvider` converts bounded source rows into the shared
  `OrderflowClockInput`, preserving aggregate, ATR, session-level, prior-value,
  VWAP, location-observation, lineage, and unavailable publications at each
  causal availability boundary.
- `CanonicalFormationProvider` consumes owner-published lifecycle formations,
  preserves identity and availability ordering, and suppresses duplicate retries.
- `PaperOrderflowRuntime.from_rows` constructs a fresh bounded owner with the
  same strategy transition boundary used by live hosting.
- `scripts/orderflow/orderflow_replay.py` remains in place. The remaining
  retirement gate is production replay capture of all producer-owned evidence,
  interaction, execution, artifact, and recovery streams without the harness.

#### 9E - Connect Paper Execution Without Policy Duplication

Status: **Host execution binding implemented; canonical execution parity remains
open.** `RuntimeSession` binds the existing Paper `ExecutionService` to the
orderflow adapter without adding a second execution owner. Full execution-bearing
replay parity is not yet claimed.

- Map only admitted/triggered canonical proposals through
  `OrderflowExecutionAdapter` to the existing Paper execution owner.
- Preserve canonical reservation, authorization, conflict, ownership-transfer,
  entry-fill, exit-fill, cancellation, and settlement ordering.
- Keep Paper account/equity mutations separate from the orderflow ledger.

Exit gate: diagnostic mode has no account mutation, live remains paper-only, and
execution-bearing replay matches canonical lifecycle ordering.

#### 9F - Persist, Recover, And Serve Facts

Status: **Partially implemented.** Versioned strategy snapshots are persisted in
SQLite, canonical orderflow facts are stored idempotently through runtime events,
and a read-only facts API is available. Full canonical snapshot restoration is
not claimed while the transition graph lacks a complete restore contract.

- Persist versioned canonical events, snapshots, notifications, and artifacts
  through existing runtime/store boundaries.
- Restore the canonical snapshot on recoverable restart; otherwise mark the
  interrupted session `STOPPED` with one audit event.
- Make duplicate observations, notifications, and recovery attempts idempotent.
- Add API/CLI projections as read-only views; they must not recompute decisions.

Exit gate: recovery preserves the canonical prefix and live/replay projections
are equivalent for the same facts.

#### 9G - Wire Context/Location Persistence And Run The Matrix

Status: **Persistence seam implemented; DB-backed matrix gate remains open.**
Producer-owned parity streams and layers are persisted as read-only orderflow
facts. The required source-matched matrix still depends on resolving the
documented lifecycle and execution mismatches.

- Persist `context_location` from the canonical context owner with source
  observation IDs, causal parents, availability, and missing-evidence reasons.
- Run the DB-backed matrix for `2026-09-15` and `2026-10-01`, comparing manifests
  before records and stopping at the first deterministic mismatch.
- Apply only the existing Phase 3.5 ATR/prior-value variance rule; add no Phase 9
  fallback or tolerance.

Exit gate: all four final layers persist and validate without Paper metadata in
canonical hashes.

#### 9H - Retire The Temporary Replay Harness

Retire `scripts/orderflow/orderflow_replay.py` as a separate final step after
the production Paper replay adapter has replaced every responsibility that was
temporarily carried by the harness. This is removal work, not permission to
promote the harness into a second replay engine or to preserve it as a
compatibility path.

Required work:

- Confirm the production replay adapter loads bounded input, advances the shared
  canonical transition boundary, captures the unified artifact, and exposes the
  required evidence, replay facts, execution facts, and recovery snapshot.
- Migrate or remove any scripts, tests, documentation, and commands that invoke
  `scripts/orderflow/orderflow_replay.py`; production live and replay paths must
  not import or shell out to it.
- Delete `scripts/orderflow/orderflow_replay.py` and remove harness-only artifact
  assumptions, raw-row loading, prefix iteration, and evidence projection from
  the production acceptance path.
- Re-run the bounded replay, live-prefix, recovery, artifact, and host-isolation
  checks after deletion so the production adapter is the only replay entry
  point.

Retirement gate:

- The production Paper replay adapter reproduces the required parity evidence
  without the harness and passes the Phase 8/9 manifest-first comparison.
- No live callback, replay worker, API, CLI, test, or documentation path refers
  to the deleted script.
- Replay retry remains idempotent and isolated from live runtime, broker, and
  Paper account state.
- The deleted harness is not replaced by another Paper strategy owner,
  compatibility wrapper, or fallback implementation.

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
- Canonical snapshot recovery preserves the same state/event prefix, while an
  unrecoverable interrupted session is stopped with one audit event.
- Production replay emits the validated unified artifact through the Paper
  runtime/store boundary and is isolated from live and account state.
- `scripts/orderflow/orderflow_replay.py` is retired, together with any
  harness-only artifact assumptions. Its responsibilities are covered by the
  production Paper replay adapter and shared canonical transition boundary.

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

## Principle Going Forward

Port behavior from `NiftyZoning/src/orderflow`, especially `execution.py`,
`opportunity_coordinator.py`, and `state_machine_reporting.py`, rather than
adding comparator-driven shortcuts. Remaining gaps, including Phase 7 fill
ordering and the `2026-10-05` `+1` formation discrepancy, require canonical
tracing, not new Paper policy.
