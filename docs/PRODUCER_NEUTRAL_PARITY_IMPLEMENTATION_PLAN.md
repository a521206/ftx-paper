# Producer-Neutral Parity Implementation Plan

Status: proposed implementation plan for `docs/PRODUCER_NEUTRAL_PARITY.md`.

## Canonical References

The authoritative replay behavior and acceptance sequence live in the
NiftyZoning repository. Paper uses these files as read-only comparison
references and must not import their runtime code, state, databases, or
generated outputs:

- `C:\Users\agraw\Python\NiftyZoning\docs\orderflow\ORDERFLOW_ARCHITECTURE.md`
- `C:\Users\agraw\Python\NiftyZoning\docs\orderflow\PAPER_ORDERFLOW_PARITY_UPDATE_PLAN.md`
- `C:\Users\agraw\Python\NiftyZoning\scripts\ftx\live\parity\docs\PARITY_CONTRACT.md`

The Paper implementation target is the registered `ftx-paper-orderflow`
strategy replay path, `PaperEngine`, execution coordinator, portfolio state,
replay worker, and persisted parity artifact. Paper's diagnostic
`strategy.orderflow_replay.ReplayMachine` remains a detached diagnostic capture
boundary and is not an authoritative execution comparison source.

The goal is simple: organize Paper's existing orderflow facts and reporting
boundaries so the equivalent NiftyZoning behavior can be compared directly. This
plan does not add a second runtime, lifecycle store, checkpoint database, or
strategy implementation. It makes the existing module boundaries and detached
replay facts usable by a neutral projector.

Phase 9 paired replay remains a separate Paper-internal validation of diagnostic
and executable projections. It supplies producer facts but does not produce the
cross-producer parity verdict; that verdict is produced only by this plan's
four-layer projection and comparison workflow.

## Relationship To Alignment

`docs/ORDERFLOW_ALIGNMENT_IMPLEMENTATION_PLAN.md` owns Paper code organization,
behavioral alignment, and focused Paper-owned regression fixtures.

This plan owns only:

- the producer-neutral projection of already-recorded Paper and canonical facts;
- the four-layer artifact and manifest;
- cross-producer comparison and parity API behavior.

The alignment plan's module map is the comparison map. Do not create a parallel
parity implementation of source, context, interaction, response, opportunity,
admission, execution, or accounting behavior.

## Comparison Organization

Each Paper module is compared with the corresponding NiftyZoning seam through
the module map in `ORDERFLOW_ALIGNMENT_IMPLEMENTATION_PLAN.md`. That map is the
single source of comparison ownership; this plan must not duplicate it. Paper
remains the sole owner of its runtime and state. NiftyZoning remains read-only
reference data.

Parity evidence follows the same order as the map: source and coverage facts,
diagnostic checkpoints for intermediate lifecycle stages, structural decision
records for interaction/candidate/admission/trigger/exit stages and outcomes,
and execution/settlement/ledger facts for the existing execution owner.

## Fact And Checkpoint Boundary

Paper's existing reporting and replay paths produce detached producer facts. A
fact may contain producer-local IDs, intermediate observations, interactions,
response updates, predicates, execution notifications, and unavailable reasons.
These facts are comparison evidence, not parity records.

Diagnostic checkpoints are organized by the same module map. They must preserve:

- Paper-owned diagnostic identity and causal sequence;
- observed, available, and decision timestamps;
- source and evidence IDs;
- context, permission, generation, and policy versions;
- outcome, structured reason, and unavailable boundary;
- detached diagnostic values.

Checkpoints must not become a new runtime event schema, parity layer, or public
API. The neutral projector consumes recorded facts and selects only structural
records required by the parity contract. It must not recompute decisions from
future rows, summaries, or producer-local storage.

## Causal Parity Gate

Artifact parity is downstream of causal parity. Before projecting or comparing
the four layers, freeze and record the exact replay scope and decision clock:

- source contract and input identity;
- requested and effective sessions;
- timezone and session-end cutoff, including inclusive/exclusive semantics;
- vehicles, capital profile, strategy configuration, and capture mode;
- selected, filtered, completed, gap, and processed decision-bar counts.

When scope is equivalent, compare a replay-only causal trace in this order:

```text
source row
-> completed observation
-> context publication
-> location publication
-> interaction transition
-> response hypothesis transition
-> opportunity transition
-> admission predicate
-> trigger observation
-> execution/settlement
```

Trace entries are detached facts with observed/available timestamps, source and
generation IDs, status, reason, and parent identity. The trace is diagnostic
evidence, not a fifth parity layer. Stop investigation at the first divergent
phase; do not compare final trades while an earlier phase differs.

## Neutral Projection Rules

The Paper adapter is a pure mapping from recorded facts to the four parity
layers. It must use explicit mappings, not broad `POLICY_DECISION` inclusion.

Structural decision mappings use `kind: "decision"` and preserve the causal
stage:

- interaction observation or closure that affects a decision -> `interaction`;
- setup formation -> `candidate`;
- admission acceptance or denial -> `admission`;
- trigger eligibility or denial -> `trigger`;
- explicit management or terminal exit -> `exit`.

Lifecycle outcomes are values on the decision record: `accepted`, `rejected`,
`cancelled`, `invalidated`, `expired`, `observed`, or `unavailable`. This keeps
the causal stage comparable without collapsing interaction closure into response
invalidation.

Response snapshots, predicates, reference replacements, and per-bar evidence
remain diagnostic facts when they do not create a decision-bearing stage.
Unavailable facts remain unavailable; they are not converted into decisions or
values.

Execution projection preserves distinct published lifecycle facts using the
shared kinds `reservation`, `authorization`, `submission`, `order_intent`,
`fill`, `pending_exit`, `exit`, `exit_fill`, `cancellation`, `settlement`, and
`synthetic_settlement`. Ledger projection preserves mutation kinds `reservation`,
`reservation_release`, `authorization`, `fill`, `partial_fill`, `cancellation`,
`exit_settlement`, `daily_reset`, and `closeout`. A closeout summary never
replaces published mutation records.

Neutral identity is constructed only by the parity projector from normalized UTC
time, structural kind, shared dimensions, an explicit neutral event key, and a
neutral parent identity when applicable. Paper IDs, NiftyZoning IDs, storage IDs,
and array positions are never cross-producer identities.

Facts retain their recorded causal order. Each parity layer preserves the
subsequence of facts mapped to it. Synthetic settlement remains at its recorded
causal position rather than being appended after execution events. Missing event
keys and identity collisions are projection errors and produce `INCOMPLETE`.

## Implementation Phases

### Phase 0: Freeze the comparison map and fixtures

Use the alignment plan's module map as the single reference manifest. Add
Paper-owned fixtures under `tests/fixtures/orderflow/` with explicit timestamps,
source IDs, availability, expected causal sequence, and unavailable reasons.
Every active module boundary gets at least one fixture; fixture names identify
the comparable behavior, not internal class names.

Acceptance tests:

- every module mapping has a named fixture;
- fixtures cover complete, delayed, missing, duplicate, and replacement cases;
- diagnostic and executable paths remain distinguishable;
- no fixture imports NiftyZoning runtime objects.

The Sep 29 baseline counts are diagnostic evidence only. A `375` versus `356`
processed-bar difference is a scope or decision-clock mismatch, not a
decision-layer mismatch, until the cutoff and coverage contract agree.

### Phase 1: Stabilize Paper reporting and replay facts

Align `orderflow_reporting.py` and the existing replay/event boundaries with the
module map. Deep-detach nested payloads, preserve causal order, and retain
rejected and unavailable facts. Do not add a second persistence owner or move
policy decisions into reporting.

The replay path must expose the causal trace phases defined above. Repeated
location observations remain diagnostic; structural interaction facts include
creation, closure, replacement, gap, and other lifecycle transitions. Response
hypothesis facts are retained when evidence, arming, availability, confirmation,
invalidation, or expiry changes. Opportunity identity and conflict-group identity
must remain stable through the selected response template.

Acceptance tests:

- repeated replay produces identical detached fact order;
- same-bar ordering is explicit and is not repaired by sorting after evaluation;
- facts do not change when live state mutates later;
- no future rows are consulted when producing replay facts.
- the first divergent causal phase is identifiable without inspecting final trades.

### Phase 2: Implement the Paper neutral projector

Move parity-specific filtering and identity construction out of the generic
artifact helper into a Paper adapter. Consume existing replay facts only. Keep
the adapter pure and test every mapping in the Neutral Projection Rules section.

Acceptance tests:

- diagnostic interaction/response observations do not inflate structural records;
- interaction, candidate, admission, trigger, and exit records map
  deterministically with explicit outcomes;
- interaction closure is not collapsed into response invalidation;
- execution and ledger event kinds remain distinct through settlement;
- execution and ledger records link to neutral decision identities;
- missing parent links, event keys, and values are explicitly unavailable;
- layer order is stable and synthetic settlement is not reordered;
- diagnostic replay produces zero orders, fills, reservations, exits, and
  portfolio mutations;
- any reported trade has authoritative order/fill/ledger attribution from the
  existing Paper execution owner.

### Phase 3: Implement the canonical projection

Add an adapter for the canonical
`orderflow_state_machine_multi_session_replay` envelope. It may project only
facts the canonical replay actually published. Summary counters and research-only
metadata cannot create records. Until this adapter exists, canonical comparison
is `INCOMPLETE` at `run_scope`.

Acceptance tests:

- canonical research envelopes are not mistaken for parity artifacts;
- equivalent Paper and canonical scopes normalize identically;
- missing canonical execution or ledger facts remain unavailable;
- canonical records map through the same neutral vocabulary and identity rules.

### Phase 4: Validate artifacts and build manifests

Implement pure validation and manifest services. Normalize timestamps to UTC,
validate contiguous layer sequences, compute independent per-layer coverage, and
hash canonical neutral records. The top-level normalized scope is authoritative;
the `run_scope` record is its checked projection.

Acceptance tests:

- unsupported schema/contract versions are rejected;
- equivalent timestamp offsets produce equal normalized identities;
- gaps and incomplete requested scope produce `INCOMPLETE`;
- observed-empty layers are matchable only with explicit proof;
- unobserved layers are `UNAVAILABLE`.

### Phase 5: Compare and expose persisted artifacts

Keep comparison separate from projection and keep API handlers thin. Compare
contract and scope, then coverage/status, count/hash, and finally records in the
first mismatching layer. Return bounded context with structured scope, coverage,
ordering, identity, value, unavailable, and parent-link reasons.

Use the existing persisted artifact endpoints:

```text
GET /api/v1/replay/{id}/parity/manifest
GET /api/v1/replay/{id}/parity/{layer}?offset=&limit=
POST /api/v1/replay/compare
```

Acceptance tests:

- extra, missing, reordered, and changed records stop at the first layer;
- verdict precedence is `MISMATCHED`, `INCOMPLETE`, `UNAVAILABLE`, `MATCHED`;
- same-producer comparisons cannot claim contract `MATCHED`;
- APIs expose persisted parity records only and return bounded pages.

### Phase 6: Conformance rollout

Run Paper-only projection and artifact validation first. Then compare equivalent
Paper and canonical fixtures. Use the documented replay baseline as diagnostic
evidence, not as a passing parity result. Never filter Paper records or weaken
the comparator to force agreement.

Release gates:

- alignment plan's focused orderflow tests pass;
- Paper facts and artifacts are deterministic and independently valid;
- canonical projection exists for the requested scope;
- both run scopes are complete before decision comparison;
- first mismatching layer and bounded context are stable across reruns.

## Explicit Non-Goals

- No second runtime, lifecycle store, checkpoint database, or strategy owner.
- No live broker execution or external position reconciliation.
- No shared Paper/NiftyZoning implementation code or state.
- No parity requirement for every intermediate diagnostic observation.
- No inference of unavailable execution, settlement, or ledger facts.
