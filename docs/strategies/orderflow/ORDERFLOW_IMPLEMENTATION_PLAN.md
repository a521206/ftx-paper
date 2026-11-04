# Paper Orderflow Implementation Plan

## Purpose

`C:\Users\agraw\Python\NiftyZoning` is the canonical orderflow implementation.
Paper must port that behavior, not maintain a second strategy implementation.
The pinned baseline is commit `8114f553392ad01dd032aecfed0031a3bc08e2f8` on
`main`.

Paper owns only transport, input adaptation, runtime composition, persistence,
API projections, and paper-account integration. It must not add strategy rules,
fallbacks, inferred evidence, or a second state machine. Missing evidence stays
`UNAVAILABLE` or `INCOMPLETE`.

Live and bounded replay must use the same ported strategy transition and
execution implementation. Its owner advances causal strategy state and emits
execution proposals and facts; it does not submit orders, mutate the Paper
account, persist data, or build artifacts. Paper host adapters perform those
operations and feed execution outcomes back through the defined owner boundary.
Neither mode submits broker orders.

NiftyZoning is the behavioral reference and its `ReplayMachine` is a
session-oriented replay harness, not Paper's live runtime API. Paper is
live-first: one in-memory owner advances incrementally for each completed input.
Bounded replay constructs the same owner and drives the same per-input
advancement contract from stored data. Live and replay may differ in transport,
clock, capture, persistence, recovery, and Paper execution adapter, but not in
strategy transitions or the facts those transitions produce. Paper ports
canonical behavior into its own repository; NiftyZoning is not a runtime
dependency.

Paper's host adapter may normalize a completed bar and attach explicitly
available, provenance-bearing references. It must not rebuild current-session
strategy inputs from the full session on each live bar, infer missing
publications, or own strategy policy. The strategy owner decides and records
canonical execution lifecycle transitions; the Paper host adapter performs
account-facing execution and accounting. Persistence and artifact construction
remain host responsibilities over emitted facts, not recomputed decisions.

## Current Status

Implemented in the Paper source tree:

- canonical source/observation, context/location, interaction/response,
  opportunity, intent, allocation, and execution contracts;
- shared transition, replay, and host-input boundaries;
- canonical-shaped evidence and execution fact projections;
- unified parity-artifact validation, hashing, persistence, and pagination;
- Paper host execution binding and versioned input/output envelopes;
- runtime event persistence and read-only orderflow fact projections.

These are implementation seams and deterministic contract coverage. They do not
prove production parity.

Still open (the items below are not implied complete by existing contracts,
scaffolding, or same-class live/replay composition):

- prioritized production work is tracked in Phase 10B; the phase sections below
  define scope and acceptance gates, not additional independent task lists.

The temporary script `scripts/orderflow/orderflow_replay.py` is comparison
tooling only. It must not be called by live callbacks or become the production
 replay engine.

## Acceptance Rules

Every phase requires all of these checks:

1. Structure matches the pinned canonical symbols and schemas.
2. Behavior and causal ordering match canonical output.
3. Observed/available timestamps, lineage, identity, and unavailable reasons are
   preserved.
4. Paper owns no duplicate policy, lifecycle, execution, or reporting logic.
5. Live and replay call the same ported implementation.
6. Canonical tests, focused Paper tests, static checks, and a cross-repository
   comparison pass where applicable.
7. Live input preparation consumes only the current completed input plus
   explicitly published causal references; replay drives that same advancement
   API rather than a parallel strategy path.
8. Obsolete production providers, duplicate transition owners, and replay-only
   strategy paths are removed when their replacements pass the relevant gates.

Bounded `INPUT_VARIANCE` is acceptable only for the existing documented
ATR/prior-value input rule. It must be recorded explicitly and must not hide a
control-flow, identity, timestamp, lifecycle, or execution mismatch. A final
artifact may be accepted with bounded input variance only when the comparator
marks that variance as accepted; `MISMATCHED`, `INCOMPLETE`, and `UNAVAILABLE`
remain failures for the relevant gate.

## Phase 1: Remove Paper-Native Behavior

**Status: Complete.**

Paper-specific orderflow policy has been removed from the production scaffold.

## Phase 2: Define The Runtime Boundary

**Status: Contract scaffold exists; production live-first ownership is open.**

`runtime_boundary.py` defines one-observation transition, live/replay adapters,
event envelopes, and a versioned snapshot shape. Prefix replay and incremental
feeding produce the same boundary state/event prefix. This only validates that
boundary scaffold; it does not prove that production uses one incremental
canonical owner, that live preparation avoids full-session rebuilds, that
behavior matches NiftyZoning, or that canonical state can be fully restored.

## Phase 3: Source And Observation

**Status: Incremental input and source preparation implemented; canonical
parity remains open.**

Live and bounded replay use the incremental stream, selected continuous futures
source, and shared validated setup loader. Per-prefix canonical comparison,
proof against future-row influence, and live refresh for artifacts created after
startup remain open.

One deliberate exception to future-row influence is recorded here: session
levels derive the opening range from the session's loaded rows
(`OrderflowInputStream._build_session_levels`, mirroring canonical
`derive_session_levels`, which is built from the full session's aggregates at
session start). Bounded replay loads the whole session, so its derived opening
range is session-wide by canonical design; a mid-session restart derives the
same opening range from the stored row prefix alone. All other per-input
preparation must remain future-row free.

## Phase 4: Context And Locations

**Status: Complete for deterministic/runtime-neutral contracts.**

Deterministic context and location contracts are implemented. Runtime ownership
and DB-backed parity remain open.

## Phase 5: Interactions And Responses

**Status: Complete for deterministic contract and replay comparison.**

The interaction and response contracts are implemented; production ownership
and parity remain open.

## Phase 6: Opportunities And Setup Lifecycle

**Status: Complete for deterministic contract.**

The opportunity lifecycle contract is implemented. Runtime/database parity
remains open.

## Phase 7: Intent, Admission, Allocation, And Execution

**Status: Contract implementation complete; parity gate open.**

The deterministic execution contracts are implemented, with Paper accounting
updated by the host after canonical transitions. The phase remains open until
no-trade and execution-bearing sessions pass canonical stream comparison;
execution differences are not parity.

## Phase 8: Unified Reporting And Parity Artifacts

**Status: Artifact contract implemented; cross-repository gate open.**

The unified artifact contract is implemented. Cross-repository parity remains
the acceptance gate; legacy comparison helpers are diagnostics only.

The unified artifact must map all execution facts explicitly:

- decision substreams: `source_observation`, `context_location`,
  `interaction_response`, `opportunity_lifecycle`, `intent_admission`, and
  `trigger_exit`;
- execution/settlement layer: reservations, authorizations, submissions,
  intents, fills, exits, cancellations, and settlement;
- portfolio-ledger layer: reservations, releases, fills, cancellations, exit
  settlement, reset, and closeout.

The execution streams are therefore represented in the final layers, not silently
dropped from the Phase 7 contract. Each record retains identity, causal parent,
timestamps, sequence, values, and unavailable state.

Phase 8 is accepted only when the eligible canonical/Paper artifacts compare in
stream and layer order. A bounded `INPUT_VARIANCE` may be accepted only when the
comparison result explicitly says so; downstream lifecycle or execution
`MISMATCHED` results keep this gate open.

## Phase 9: Complete The Paper Host

**Status: In progress. Incremental source preparation is implemented; complete
production integration is not.**

Phase 9 work may prepare interfaces and tests before the Phase 8 gate, but no
production parity or completion claim is allowed until Phase 8 is accepted.

### 9B. Define The Strategy Owner And Host Boundaries

**Status: Complete for the implementation scope; production parity acceptance
remains governed by Phase 8 and Phase 10.**

- use one strategy owner for each completed input to advance source observation,
  context/location, interactions/responses, opportunity lifecycle, intent and
  admission, triggers, and canonical execution/settlement state; emit ordered,
  immutable facts and execution proposals from that causal transition;
- keep Paper order submission, account mutation, persistence, and artifact
  construction outside the strategy owner. The Paper execution adapter must
  consume only owner proposals, return fills/outcomes through the defined
  boundary, and capture host ledger facts without recomputing strategy policy;
- use the same owner and per-input advancement contract in live and bounded
  replay. Capture both owner facts and Paper execution outcomes in the same
  artifact contract;
- retain `transition.py`, context/location replay, and comparison harnesses only
  as implementation seams or diagnostics, not parallel production owners; and
- remove the production Phase 4 -> interaction -> Phase 4 double pass;
- remove obsolete compatibility shims (e.g., the unused `state_machine.py`
  `ReplayState` alias) and confirm the `_record_phase4_*` parity-fact recorders
  are single-pass within the shared owner, not a second Phase 4 owner.

The live `RuntimeSession` and bounded `ReplayWorker` now advance the same
orderflow owner once per completed input. The owner emits Paper execution
proposals without authorizing them; host adapters drain and authorize those
proposals, with replay remaining diagnostic-only. Focused live/replay prefix
and host-ownership tests pass. This does not close the cross-repository parity,
complete recovery, or production replay migration gates.

### 9C. Complete Snapshot And Recovery

Persist and restore the complete canonical mutable graph: context and location
generations, interaction and opportunity state, intents, reservations, pending
exits, positions, causal cursors, parity state, and idempotency identities.
Recovery must resume an explicit suffix without rereading future bars. An
unrecoverable interrupted session becomes `STOPPED` with one audit event.
Pending execution proposals are established as not part of recoverable state:
`context.execution_proposals` is a within-transition scratch buffer, cleared at
the start of each bar and consumed by selection and host publication within the
same synchronous `process_execution_bar` call, so it is always empty at snapshot
boundaries. Document that invariant (assertion/comment) rather than serializing
a transient buffer.

### 9D. Bind Paper Execution

Map only canonical admitted/triggered proposals and fills to the existing Paper
execution/accounting service. Diagnostic replay must not mutate the Paper
account. Broker integrations remain market-data only.

### 9E. Persist And Serve Facts

Persist canonical events, snapshots, and notifications idempotently, and expose
read-only API/CLI projections. API and persistence code must not recompute
decisions from trades, counters, reports, or terminal snapshots. Live fact
capture is the per-fact persistence of producer-owned streams and layers as
`ORDERFLOW_FACT` rows (idempotent per source/category/stream/identity), served
read-only by the orderflow facts API. The unified parity artifact is replay-gate
tooling: it is built and persisted for bounded replay only, has no live consumer
(canonical comparison and the API parity endpoint read replay runs), and must
not be emitted or persisted at write time on the live path. A future live parity
report would be built on demand from the persisted facts, not captured as a
parallel artifact.

## Phase 10: Close Live/Replay Execution Parity

**Status: In progress. The live/replay input owner is shared; execution,
recovery, and cross-repository parity gates remain open.**

### 10A. Accepted operational differences

Live and bounded replay may differ in the following host concerns only:

- input transport: broker-normalized incremental bars versus stored bounded bars;
- clock driving: feed completion and wall-clock deadlines versus deterministic
  historical timestamps;
- capture and persistence: live per-fact `ORDERFLOW_FACT` runtime event storage
  versus run-scoped replay artifacts that include the unified parity artifact;
- recovery: persistent runtime recovery versus fresh isolated replay state;
- Paper execution adapter: live PaperBroker/execution-service submission versus
  deterministic in-process replay settlement. Both adapters consume the same
  canonical proposals and report outcomes through the same owner boundary.

Neither path may submit broker orders. These differences must not change
canonical decisions, causal evidence, lifecycle transitions, execution facts, or
ledger semantics. Synthetic output remains reporting-only where the host
contract requires it.

### 10B. Implementation gaps to close

The following backlog is ordered by dependency. Repeated references in Phases
9C–9E, 10C, and the Completion Gate are acceptance conditions for these
items, not separate tasks.

1. **Prove causal input parity.** Verify live setup publication for valid,
   missing, and unavailable artifacts, and for a source change handled by an
   explicit rebuild, including startup and subsequent updates. Prove incremental live preparation uses only the current completed
   input and explicitly published causal references, with no future-row
   influence aside from the recorded session-levels exception (Phase 3).
   Compare each input prefix against the pinned NiftyZoning behavior.
   Reconcile availability/decision timestamps and the supporting-bar deadline;
   any accepted input variance must be documented and shown not to alter
   decisions.
2. **Complete the shared strategy owner and host boundary.** **Implemented for
   the current Paper boundary; parity acceptance remains open.** Route every live
   and replay input through the same strategy transition/execution owner. Keep
   Paper execution, account mutation, persistence, and artifact construction in
   host adapters; remove duplicate production orchestration only after the
   shared path passes its focused gates. Add a focused test proving live and
   replay produce identical parity-stream prefixes for the same completed
   inputs through that owner, with execution/account mutation occurring only in
   the host adapter.
3. **Pass execution-bearing parity and complete fact capture.** Rerun eligible
   source-matched sessions after UTC normalization. Compare no-trade and
   execution-bearing decision, execution/settlement, and portfolio-ledger
   streams against canonical output. Accept only the documented ATR/prior-value
   variance, explicitly classified as `INPUT_VARIANCE`. Persist DB-backed live
   and replay facts. The unified parity artifact is emitted and persisted for
   bounded replay only; live captures the same producer-owned facts as
   idempotent `ORDERFLOW_FACT` rows, and a live parity report would be built on
   demand from those facts rather than persisted as a parallel artifact.
   Resolve lifecycle and execution mismatches before accepting the parity gate.
4. **Complete snapshot recovery.** Persist and restore the complete canonical
   mutable graph and idempotency identities, and establish that pending
   execution proposals are not part of recoverable state (`execution_proposals`
   is a within-transition buffer that is always empty at snapshot boundaries).
   Prove recovery resumes the same causal prefix without rereading future bars;
   retain the existing `STOPPED` plus one audit event behavior for unrecoverable
   interrupted sessions.
5. **Migrate production replay and retire the harness.** Reduce `ReplayWorker`
   to bounded loading, cancellation, clock advancement, capture, and fresh owner
   construction; route each observation through the shared owner. Delete
   `scripts/orderflow/orderflow_replay.py` and production references only after
   required facts, unified artifacts, and recovery snapshots are captured by the
   production adapter.

### 10C. Acceptance criteria

Phase 10 is accepted only when all of the following hold:

- no-trade and execution-bearing sessions match canonical decision,
  execution/settlement, and portfolio-ledger streams;
- any variance is limited to the documented ATR/prior-value input rule and is
  explicitly classified as `INPUT_VARIANCE`;
- live and replay advance the same strategy owner once per completed input, and
  host execution outcomes are captured without introducing parallel strategy
  policy;
- replay does not reconstruct facts from terminal snapshots or duplicate
  canonical policy/execution logic;
- snapshot recovery reproduces the same causal prefix without rereading future
  bars; and
- the comparison gate passes with no `MISMATCHED`, `INCOMPLETE`, or
  `UNAVAILABLE` downstream phase.

## Completion Gate

The replacement is complete only when:

- the old Paper-native strategy is gone;
- every ported symbol is traceable to the pinned canonical commit;
- live and bounded replay share the same per-input strategy transition and
  execution implementation, with Paper execution, persistence, and artifact
  construction remaining explicit host responsibilities;
- eligible no-trade and execution-bearing artifacts pass manifest-first parity;
- snapshot recovery reproduces the canonical causal prefix;
- Paper accounting is reached only after canonical fill transitions;
- the temporary replay harness and split production paths are removed; and
- Paper remains loopback-bound and paper-only.

## Validation

From the Paper repository:

```text
.venv\Scripts\python.exe -m pytest tests/strategies -k orderflow
.venv\Scripts\ruff.exe check .
.venv\Scripts\pyright.exe <changed Python files>
git diff --check
```

Run canonical orderflow tests from the pinned NiftyZoning checkout separately.
Do not treat Paper-only tests as canonical parity evidence.
