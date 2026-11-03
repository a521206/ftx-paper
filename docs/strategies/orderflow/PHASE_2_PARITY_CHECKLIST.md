# Phases 2-4 Parity Checklist

Authority: the pinned NiftyZoning commit recorded in
`ORDERFLOW_IMPLEMENTATION_PLAN.md` and
`C:\Users\agraw\Python\NiftyZoning\docs\orderflow\PARITY_CONTRACT.md`.
This is a contract and dependency map, not an implementation of later policy.

## Shared Runtime Boundary

- Canonical owners: `timestamps.py` (`parse_nifty_label`, `parse_utc`,
  `require_utc`, `serialize_utc`), `orderflow_common.py` (`Aggregate`,
  `OrderflowEvent`), `state_machine_types.py` (`ReplayContext`, `SourceState`,
  `ObservationState`, `MarketContextStore`), `state_machine.py`
  (canonical replay stepping and bar processing), and `state_engine.py`
  (`ReplayRecorder`, `CaptureMode`).
- Direct closure: `hierarchy.py`, `lifecycle.py`, `interactions.py`,
  `location_geometry.py`, `response.py`, `templates.py`, `opening_gap_policy.py`,
  `replay_common.py`, and `orderflow_common.py`. The closure is carried by the
  orchestration shell, but Phase 2 exposes no policy outputs.
- Paper scaffold: `runtime_boundary.py` defines `CompletedObservation`,
  `BoundaryEvent`, `BoundaryState`, `transition_one`, `LiveInputAdapter`, and
  `ReplayInputAdapter`. `transition_one` is the single host-neutral boundary.
- State and envelope: input identity prefix, recursively immutable event prefix, last
  available timestamp, schema version 1; every event has `kind`, `occurred_at`,
  `observed_at`, `available_at`, `sequence`, `identity`, `causal_parent_id`,
  `values`, and explicit `unavailable` reasons. Timestamps are timezone-aware
  UTC at the artifact boundary and availability cannot precede observation.
- Recovery: restore only from a versioned snapshot, then process an explicit
  bounded suffix. Duplicate identities are idempotent only when all causal
  evidence matches; conflicting duplicates and earlier available times are
  rejected. Paper owns snapshot persistence and recovery orchestration.
- Timestamp mapping: every Paper causal field is an aware UTC `datetime`.
  NiftyZoning `YYYY-MM-DDTHH:MM` values are external labels only and are parsed
  as `Asia/Kolkata` by the replay/source ingress adapter, then normalized once
  to UTC. Original session/bar labels remain separate identity/grouping
  metadata. API, artifact, and parity serialization uses UTC ISO-8601 strings
  ending in `Z`; causal ordering always compares UTC datetimes, never strings.
  Naive timestamps are rejected everywhere except the explicit NiftyZoning
  ingress parser.
- Live calls one completed observation at a time. Replay processes a bounded,
  timestamp-ordered iterable. Neither adapter invokes the other or evaluates
  policy.
- Comparable candle identity is an explicit reference with `session_date`,
  `timeframe`, `start_at`, and `end_at`. The timestamp members are UTC
  ISO-8601 values ending in `Z`; producer-local candle IDs remain diagnostic
  metadata and are never normalized as generic timestamps.
- Diagnostic replay artifacts include symmetric `execution_lifecycle_tick`
  records keyed by lifecycle and causal clock. These records explain the first
  execution divergence but are not parity contract values.
- `build_runtime_boundary_artifact` emits the versioned
  `orderflow_runtime_boundary` envelope with `input_prefix`, `replay`,
  `incremental`, `recovery`, and `coverage`. Each execution reports state and
  event-prefix SHA-256 hashes, last availability, and `MATCHED`/`MISMATCHED`.

## Layer Checklist

Each row records the required canonical closure, contract facts, ownership,
entry behavior, and tests. `PENDING` is intentional until the corresponding
phase is ported and its canonical tests pass.

| Layer | Canonical modules and symbols | Direct closure and contract facts | Host boundary and entry behavior | Tests / status |
|---|---|---|---|---|
| Source and replay | `state_machine_source.py`: `source_fingerprint`, `canonical_source_rows`, `source_metadata`; `market_observation.py`: `MarketObservation`, `ObservationGap`, `observe_market`, `rows_available_for_observation`, `derive_session_levels`; `orderflow_common.py`: `Aggregate`, `ObservationIssue`, `build_aggregate_result`, `canonical_timestamp`; canonical replay stepping and bar processing | Ordered unique source rows; duplicate rejection; completed 1m/5m aggregates; gap and incomplete-parent issues; observed/available timestamps and parent lineage; unavailable reasons remain explicit | Paper supplies normalized market input only. Live publishes one completed observation; replay supplies bounded stored input. Adapters do not aggregate, score, or repair gaps. | Phase 3 source/observation parity and Paper adapter gates: **COMPLETE** |
| Context and locations | `value_publication.py`; `opening_gap_policy.py`; `location_observation.py`: `LocationObservation`; `location_geometry.py`: `LocationBand`; `vwap_location.py`: `causal_vwap_prefix`; `state_machine_types.py`: `MarketContextStore`, `LocationRegistry`, `LocationRecord`, `SessionLevels`; artifact `market_context_history.py` | Immutable context versions; location generations, aliases, geometry, ownership, priority, as-of resolution, permissions, gap transitions, source lineage, availability, and unavailable evidence | Canonical owns replacement and permission rules. Paper may provide prior-session inputs and persist snapshots, never fallback values or priority. Same boundary in live/replay. | Phase 4 contracts and runtime-neutral replay are covered; host publication wiring and DB parity remain **INCOMPLETE** |
| Interactions and responses | `interactions.py`: `InteractionSnapshot`, lifecycle enums; `interaction_detector.py`: `InteractionDetector`, transitions; `response.py`: response status/policy symbols | Independent 1m/5m episode clocks; frozen contact/reference lineage; departure/close/expiry; response arming, acceptance, rejection, confirmation, invalidation, expiry; terminal interaction never implies response | Paper supplies canonical inputs and consumes events. No Paper detector, response policy, or location wrapper. Live incremental and replay bounded calls share implementation. | Canonical interaction/response tests plus boundary scaffold; `PENDING` |
| Opportunities and setup lifecycle | `opportunity.py`: `SetupOpportunity`; `opportunity_timeout.py`: timeout advancement; `opportunity_coordinator.py`; `templates.py`: canonical templates; `lifecycle.py` | Candidate/setup identity, parent context and response lineage, formation availability, counted timeout evidence, invalidation, supersession, retest, expiry, execution-group eligibility, unavailable outcomes | Canonical owns lifecycle and identity. Paper accounting is outside this layer; adapters only transport facts. | Canonical opportunity/lifecycle tests plus boundary scaffold; `PENDING` |
| Intents, admission, allocation, execution | `intent_policy.py`; `active_position_gate.py`; `allocation.py`; `execution.py`: `ExecutionCoordinator`; `lifecycle.py`: execution state; `state_machine_types.py`: `ExecutionProposal`, `ExecutionConstraints` | Admission predicates and unavailable results; permission/active-position/allocation/risk gates; reservation, authorization, trigger, fills, partial fills, stops, settlement and orderflow ledger mutations with causal timing | Canonical execution owner is authoritative. Paper owns only paper-account mapping; no second coordinator, broker submission, or capital-ledger duplicate. | Canonical intent/execution tests plus boundary scaffold; `PENDING` |
| Source/run reporting | `state_machine_reporting.py`, `canonical_projection.py`, `parity_artifacts.py` | `run_scope` identity, normalized scope/timezone/source/capture/coverage and counts; manifests precede records; explicit `INCOMPLETE`/`UNAVAILABLE` | Paper persists/serves artifacts without reconstructing them; transport metadata is outside canonical values. | Canonical reporting/artifact tests plus Paper scaffold; `PENDING` |
| Decision reporting | `state_machine_reporting.py`, `canonical_projection.py`, `parity_artifacts.py` | Ordered decision projection stages: interaction, candidate, admission, trigger, exit; exact identity, evidence, timestamps, causal parent and ordering | Paper exposes canonical facts only and does not recompute decisions. | Canonical reporting tests plus Paper scaffold; `PENDING` |
| Execution/settlement reporting | `state_machine_reporting.py`, `canonical_projection.py`, `parity_artifacts.py` | Separate reservation, authorization, submission, order intent, fill, pending exit, exit, exit fill, cancellation, settlement, synthetic settlement records; no closed-trade substitution | Paper may capture diagnostic/paper facts; it must not fabricate broker acknowledgements or collapse lifecycle records. | Canonical execution/reporting tests plus Paper scaffold; `PENDING` |
| Portfolio-ledger reporting | `state_machine_reporting.py`, `canonical_projection.py`, `parity_artifacts.py` | Mutation records: reservation, release, fill, partial fill, cancellation, exit settlement, daily reset, closeout; gross/net and unavailable cost policy explicit | Orderflow owns lifecycle ledger facts; Paper capital/equity ledger remains a separate host owner. | Canonical ledger/reporting tests plus Paper scaffold; `PENDING` |

## Phase 2 Gate

Verified scaffold checks: Paper live-style incremental input and bounded replay
prefix invoke `transition_one` and produce identical state/event prefixes;
snapshot recovery preserves that prefix; duplicate input is idempotent; replay
ordering is explicit. No policy behavior, fallback, translation wrapper, second
state machine, replay file orchestration, or live feed callback was added.

The contract/runtime-boundary gate is recorded in the implementation plan only
after the targeted tests and static checks pass.

## Phase 3 Gate: Source And Market Observation

Canonical source: the pinned NiftyZoning `state_machine_source.py`,
`market_observation.py`, and aggregate portions of `state_machine_types.py`.
Paper must project this source contract and must not create a second market
observation policy.

Required parity facts:

- Source identity, fingerprint, metadata, strict ordering, and duplicate
  rejection match the canonical contract.
- Session filtering, session boundaries, completed 1m bars, aligned 5m
  aggregates, incomplete-parent issues, and source gaps remain explicit.
- Observed timestamps, available timestamps, and parent lineage are preserved.
- Live publishes one completed observation at a time; replay consumes a bounded,
  ordered aggregate artifact. Neither path evaluates context or trading policy.
- Phase 3 does not calculate ATR, VWAP, context scores, locations, permissions,
  interactions, opportunities, or execution decisions.

Phase 3 verification status:

- Canonical and Paper source/observation tests pass with package-root import
  changes only where required.
- Paper live and replay adapters expose the same canonical input shape.
- Source parity manifests are compared before detailed records and report
  `MATCHED`, `MISMATCHED`, or `INCOMPLETE` explicitly.
- Extended multi-date edge cases and any uncommitted repair work remain follow-up
  items unless recorded in `ORDERFLOW_IMPLEMENTATION_PLAN.md`.

## Phase 4 Gate: Context, Values, Permissions, And Locations

Canonical source: the pinned NiftyZoning context, value, permission, geometry,
VWAP, and state-machine publication contracts. Paper may translate supplied
canonical or host-stored publications, but it must not reproduce strategy rules.

Required ownership rules:

- `OpeningGapPolicy`, value-profile completeness, location replacement, VWAP
  calculation, permissions, and location priority remain canonical-owned.
- Missing ATR, value, context, level, or location evidence stays explicitly
  unavailable. Paper must not infer it from snapshots or presentation output.
- ATR numeric values are source-dependent when Paper and NiftyZoning use different
  continuous-futures history. The parity harness reports this as `INPUT_VARIANCE`;
  it does not remove ATR from the evidence contract or apply a numeric tolerance.
- Canonical `LocationRecord` and `MarketContext` stream identities are preserved
  through the Paper evidence boundary; records are not rewritten to fit Paper
  identifiers.
- Live and replay resolve immutable publications as-of their availability and
  share the same causal evidence contract.

### Phase 4A: Evidence Publication

Input is the Phase 3 aggregate artifact plus explicit ATR, prior-session value,
prior-session level, location, VWAP, and context publications. The evidence
adapter validates envelopes, provenance, stream identity, finite geometry inputs,
and unavailable reasons only. It does not derive missing levels, fallback values,
permissions, or replacement generations.

### Phase 4B: Causal Context/Location Replay

Replay advances one completed 1m/5m clock and emits `source_observation` and
`context_location` facts. Every fact must preserve:

- as-of context and location generation resolution;
- source aggregate and source observation IDs;
- causal parent and child lineage, including exactly five causally available 1m
  children for a 5m location observation;
- observed and available timestamps, with availability never preceding
  observation; and
- explicit unavailable reasons rather than inferred strategy state.

Presentation/history projections may validate and display captured facts, but may
not reconstruct runtime context or location evidence.

### Phase 4C: Combined Parity Harness

The Phase 4C entry point runs the Phase 3 source exporter, supplies its artifact
to the Phase 4A evidence boundary, runs Phase 4B replay, and compares source and
context/location streams. It must not require a manually prepared Paper facts
artifact or reconstruct facts from snapshots.

Phase 4 verification status:

- Deterministic context, value, permission, geometry, VWAP, immutable-history,
  and causal replay contracts are covered by focused tests.
- The runtime-neutral replay shell is available when explicit evidence
  publications are supplied.
- Host-runtime publication wiring and DB-backed context/location parity remain
  `UNAVAILABLE` until the producer adapter is complete.
- No Phase 5 interaction, response, opportunity, lifecycle, or execution policy
  is enabled by this checklist.
