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

Still open:

- cross-repository parity for the unified artifact, including execution-bearing
  sessions;
- one complete canonical owner for context through settlement;
- complete canonical snapshot restore and recovery;
- production replay migration away from the temporary harness;
- DB-backed live/replay parity and retirement of transitional production paths.

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

Bounded `INPUT_VARIANCE` is acceptable only for the existing documented
ATR/prior-value input rule. It must be recorded explicitly and must not hide a
control-flow, identity, timestamp, lifecycle, or execution mismatch. A final
artifact may be accepted with bounded input variance only when the comparator
marks that variance as accepted; `MISMATCHED`, `INCOMPLETE`, and `UNAVAILABLE`
remain failures for the relevant gate.

## Phase 1: Remove Paper-Native Behavior

**Status: Complete.**

The old Paper-specific orderflow policy and behavior tests were removed. The
strategy package retains the minimal boundary and canonical-support modules.
No deleted lifecycle, producer, replay, or reporting path is part of the
production scaffold.

## Phase 2: Define The Runtime Boundary

**Status: Complete for the host-neutral contract.**

`runtime_boundary.py` defines one-observation transition, live/replay adapters,
event envelopes, and a versioned snapshot shape. Prefix replay and incremental
feeding produce the same boundary state/event prefix. This phase does not claim
behavioral parity or complete canonical restore.

## Phase 3: Source And Observation

**Status: Complete for the ported contract.**

The port covers source identity, ordering, duplicate rejection, session bounds,
completed aggregates, gaps, availability, and lineage. Paper adapters project
the canonical input shape and do not calculate parallel policy or aggregates.

## Phase 4: Context And Locations

**Status: Complete for deterministic/runtime-neutral contracts.**

The port covers value publication, opening-gap policy, immutable context
versions, location generations, causal VWAP, permissions, and explicit
unavailable evidence. Runtime ownership and DB-backed parity remain open.

## Phase 5: Interactions And Responses

**Status: Complete for deterministic contract and replay comparison.**

Independent 1m/5m interaction state, frozen provenance, response windows,
policy dispatch, arbitration, invalidation, expiry, and retry idempotence are
ported. No terminal snapshot may be used to invent a response outcome.

## Phase 6: Opportunities And Setup Lifecycle

**Status: Complete for deterministic contract.**

Opportunity identity, formation, timeout, retest, invalidation, supersession,
execution-group eligibility, and causal evidence are covered. Runtime/database
parity is not complete.

## Phase 7: Intent, Admission, Allocation, And Execution

**Status: Contract implementation complete; parity gate open.**

The port contains canonical admission, allocation, intent, reservation, trigger,
fill, exit, settlement, and orderflow-ledger contracts. Paper accounting is
updated only through the host adapter after canonical transitions.

The phase is not accepted until both a no-trade session and an
execution-bearing session match canonical streams for admission, trigger/exit,
fills, transitions, and ledger mutations. Current execution differences remain
downstream mismatches or accepted input variance and must not be called parity.

## Phase 8: Unified Reporting And Parity Artifacts

**Status: Artifact contract implemented; cross-repository gate open.**

`parity_artifacts.py` defines the canonical artifact envelope, six decision
substreams, four final layers, coverage states, validation, hashing, JSON/JSONL
persistence, and bounded reads. The former Phase 4/Phase 7 comparison helpers
are diagnostics only.

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

**Status: In progress. Preparation is implemented; production integration is not.**

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

## Phase 10: Close Live/Replay Execution Parity

**Status: Not started. The canonical strategy boundary is shared, but the host
execution paths still contain unresolved causal and ownership differences.**

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

1. `ReplayWorker` currently performs host-level entry deferral, authorization,
   reservation, fills, exits, settlement, notifications, and ledger projection.
   Reduce it to bounded loading, cancellation, clock advancement, capture, and
   fresh owner construction; route execution through the same canonical owner
   and Paper host adapter used by live.
2. Live uses the current bundle timestamp while replay advances
   `available_at`/`decision_at` to the next futures bar. Define and test one
   causal availability contract so this cannot alter admission, timeout,
   trigger, or settlement ordering.
3. Live uses a ten-second supporting-bar deadline while replay uses a zero-second
   deadline. Align the resulting canonical bundle/evidence behavior or record a
   formally accepted clock/input variance with proof that decision streams are
   unchanged.
4. Complete the canonical owner from context publication through settlement and
   remove transitional duplicate orchestration paths.
5. Complete DB-backed live/replay fact capture, snapshot/recovery of the mutable
   canonical graph, and retirement of the temporary replay harness.

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
