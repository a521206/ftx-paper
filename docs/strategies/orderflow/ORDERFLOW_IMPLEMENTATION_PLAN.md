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

Live and bounded replay must use the same canonical transition and execution
owners. They may differ only in input, clock, capture, persistence, recovery,
and the selected Paper execution adapter. Neither mode submits broker orders.

NiftyZoning is the behavioral reference and its `ReplayMachine` is a
session-oriented replay harness, not Paper's live runtime API. Paper is
live-first: one in-memory owner advances incrementally for each completed input.
Bounded replay constructs the same owner and drives the same per-input
advancement contract from stored data. Paper ports canonical behavior into its
own repository; NiftyZoning is not a runtime dependency.

Paper's host adapter may normalize a completed bar and attach explicitly
available, provenance-bearing references. It must not rebuild current-session
strategy inputs from the full session on each live bar, infer missing
publications, or own strategy policy. Canonical transition ownership includes
causal observation through execution/settlement; persistence and Paper account
mutation remain host responsibilities after canonical transitions.

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

- cross-repository parity for the unified artifact, including execution-bearing
  sessions;
- one complete canonical owner for context through settlement;
- complete canonical snapshot restore and recovery;
- production replay migration away from the temporary harness;
- DB-backed live/replay parity and retirement of transitional production paths;
- incremental live input preparation and proof that no future session rows
  influence a transition;
- one-input-at-a-time parity between live advancement, bounded replay, and the
  pinned NiftyZoning behavior;
- live ATR, prior-value, and prior-session-level publication for startup;
- unified parity-artifact capture and persistence on the live path, equivalent
  in contract to bounded replay.

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

### 9A. Close Artifact Parity

- rerun eligible source-matched dates after UTC normalization;
- classify only the documented ATR/prior-value variance as accepted;
- resolve lifecycle and execution mismatches before enabling host parity.

### 9B. Use One Canonical Owner

- make one owner perform observation, context, interaction, opportunity,
  execution, settlement, and parity capture in one causal transition;
- keep `transition.py`, context/location replay, and comparison harnesses as
  host seams or diagnostics until they delegate to that owner;
- remove the production Phase 4 -> interaction -> Phase 4 double pass.

### 9C. Complete Snapshot And Recovery

Persist and restore the complete canonical mutable graph: context and location
generations, interaction and opportunity state, intents, reservations, pending
exits, positions, causal cursors, parity state, and idempotency identities.
Recovery must resume an explicit suffix without rereading future bars. An
unrecoverable interrupted session becomes `STOPPED` with one audit event.
The current owner restore also omits serialized pending execution proposals;
restore must reproduce those pending inputs or explicitly establish that they
are not part of recoverable state before this phase can be accepted.

### 9D. Migrate Replay And Retire The Harness

`ReplayWorker` may own only bounded loading, cancellation, clock, capture, and
fresh owner construction. Route every replay observation through the same owner
used by live. After the production adapter captures all required facts,
artifacts, and recovery snapshots, delete `scripts/orderflow/orderflow_replay.py`
and all production references to it.

### 9E. Bind Paper Execution

Map only canonical admitted/triggered proposals and fills to the existing Paper
execution/accounting service. Diagnostic replay must not mutate the Paper
account. Broker integrations remain market-data only.

### 9F. Persist And Serve Facts

Persist canonical events, snapshots, notifications, and artifacts idempotently.
Expose read-only API/CLI projections. API and persistence code must not
recompute decisions from trades, counters, reports, or terminal snapshots.
The unified artifact builder is currently exercised by bounded replay; the live
path persists runtime/orderflow facts but does not yet emit and persist the same
unified artifact contract.

## Phase 10: Close Live/Replay Execution Parity

**Status: In progress. The live/replay input owner is shared; execution,
recovery, and cross-repository parity gates remain open.**

### 10A. Accepted operational differences

Live and bounded replay may differ in the following host concerns only:

- input transport: broker-normalized incremental bars versus stored bounded bars;
- clock driving: feed completion and wall-clock deadlines versus deterministic
  historical timestamps;
- capture and persistence: runtime event storage versus run-scoped replay
  artifacts;
- recovery: persistent runtime recovery versus fresh isolated replay state;
- Paper execution adapter: live PaperBroker/execution-service submission versus
  deterministic in-process replay settlement.

Neither path may submit broker orders. These differences must not change
canonical decisions, causal evidence, lifecycle transitions, execution facts, or
ledger semantics. Synthetic output remains reporting-only where the host
contract requires it.

### 10B. Implementation gaps to close

1. Verify live setup publication. Startup backfill may contain multiple
   contracts by design; raw bars remain retained while the runtime resolves one
   canonical futures source per session and binds only its continuous bars to
   orderflow. Live and replay now load ATR, prior value, and prior-session levels
   through the same source-validated setup loader, or publish explicit
   unavailable states. Verify valid/stale/missing artifact cases through live
   startup and the subsequent update path.
2. The former provider's append operation rebuilt the
   observation artifact, aggregates, evidence, and published envelope history
   for the session after every appended bar. `OrderflowInputStream` now
   publishes one completed input incrementally. Verify its output against the
   pinned canonical replay at every input prefix and against live production
   ingress before accepting the replacement; per-session behavior remains a
   ported responsibility, not Paper policy.
3. Test live setup artifact publication against bounded replay for valid,
   stale, missing, and unavailable artifacts. Do not derive or infer them inside
   strategy policy.
4. `ReplayWorker` currently performs host-level entry deferral, authorization,
   reservation, fills, exits, settlement, notifications, and ledger projection.
   Reduce it to bounded loading, cancellation, clock advancement, capture, and
   fresh owner construction; route execution through the same canonical owner
   and Paper host adapter used by live.
5. Live uses the current bundle timestamp while replay advances
   `available_at`/`decision_at` to the next futures bar. Define and test one
   causal availability contract so this cannot alter admission, timeout,
   trigger, or settlement ordering.
6. Live uses a ten-second supporting-bar deadline while replay uses a zero-second
   deadline. Align the resulting canonical bundle/evidence behavior or record a
   formally accepted clock/input variance with proof that decision streams are
   unchanged.
7. Complete the canonical owner from context publication through settlement and
   remove transitional duplicate orchestration paths.
8. Complete DB-backed live/replay fact and unified-artifact capture, snapshot /
   recovery of the mutable canonical graph (including pending execution
   proposals), and retirement of the temporary replay harness.

### 10C. Acceptance criteria

Phase 10 is accepted only when all of the following hold:

- no-trade and execution-bearing sessions match canonical decision,
  execution/settlement, and portfolio-ledger streams;
- any variance is limited to the documented ATR/prior-value input rule and is
  explicitly classified as `INPUT_VARIANCE`;
- live and replay use the same complete transition and execution owner;
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
- live and bounded replay share the complete canonical state/transition/
  execution owner;
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
