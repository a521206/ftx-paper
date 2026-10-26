# Orderflow Strategy for `ftx-paper`

Status: proposed design and implementation plan, October 2026

This document describes how orderflow can be added as a strategy to the
independent `ftx-paper` paper-trading application. It is based on the strategy
contract in `ftx-paper/docs/STRATEGY_DEVELOPMENT.md` and the causal orderflow
work in this repository.

The implementation must preserve the boundary between the two repositories:
`ftx-paper` must not import `NiftyZoning` orderflow modules, databases,
configuration, or generated artifacts. The behavior and contracts may be
reproduced independently, but the implementations remain separate.

## Design Goals

The orderflow strategy must:

- consume completed, causally available market observations;
- produce deterministic `DecisionEvent` and `OrderIntent` values;
- retain source, availability, and evidence provenance;
- reuse the existing `ftx-paper` runtime, risk, sizing, exits, replay, and
  attribution services;
- remain independently selectable through `StrategyFactory`;
- support diagnostic-only operation before executable policy use; and
- remain compatible with `MultiStrategyEngine` and shared runtime ownership.

It must not create a second runtime, broker session, portfolio, execution
engine, database, or persistence path.

## Architectural Prerequisites

Orderflow implementation depends on the strategy reuse refactor described in
`docs/STRATEGY_REFACTORING.md`. That refactor is not optional plumbing: it
defines the boundary that prevents an orderflow policy from copying the
research strategy's orchestration.

Before orderflow policy implementation begins, the repository must provide:

- a typed `StrategyPolicy` protocol for feature requirements, candidate
  evaluation, admission, and deterministic state transitions;
- a runtime-owned shared coordinator that converts policy results into
  `DecisionEvent` and, only when authorized, `OrderIntent` values;
- a typed market-feature requirement registry whose union is composed before
  the decision clock starts;
- an immutable orderflow context attached to `DecisionBundle` supporting
  observations, provenance, availability cutoffs, and completed child graphs;
- strategy-owned snapshot adapters dispatched by stable `strategy_id`; and
- an explicit diagnostic mode in which the coordinator rejects executable
  output from the policy.

The existing research strategy must be migrated behind these seams first. The
orderflow adapter must not target `IndependentLiveDecisionEngine` directly or
create a second coordinator.

## Recommended Strategy Identity

```text
name: ftx-paper-orderflow
version: 0.1.0
strategy_id: orderflow-research
```

The strategy should be implemented under:

```text
src/ftx_paper/strategy/orderflow.py
```

It should implement the existing `Strategy` protocol as a thin adapter rather
than extending the current configured location strategy with orderflow-specific
branches. The adapter is subject to the shared-coordinator refactor: orderflow
policy owns feature requirements, candidate evaluation, admission, and
strategy-specific state transitions; the coordinator converts policy decisions
into `DecisionEvent` and authorized `OrderIntent` values. The orderflow plan
must not introduce a second coordinator or duplicate orchestration while that
refactor is being completed.

## Architecture

The strategy evaluates each completed decision bundle through this pipeline:

```text
DecisionBundle
  -> causal orderflow observation
  -> location interaction
  -> response evaluation
  -> opportunity formation
  -> trigger observation
  -> admission policy
  -> shared risk and sizing
  -> OrderIntent
```

The runtime remains responsible for:

- decision-clock scheduling;
- account and portfolio context;
- order authorization;
- execution and fills;
- persistence and audit storage;
- strategy attribution; and
- end-of-day runtime lifecycle.

The strategy policy is responsible only for deterministic market hypothesis,
orderflow state, candidate formation, and strategy-specific admission. The
shared coordinator remains the sole owner of risk authorization, sizing,
reservations, `ExitStateMachine` instances, execution conversion, and account
mutation. Diagnostic orderflow policies may emit candidates and trigger
observations, but cannot emit executable orders.

The strategy may retain only deterministic state required to evaluate the next
bundle and to restore an equivalent decision state from a snapshot.

## Causal Orderflow Input Contract

Add a versioned, runtime-independent contract in `ftx-paper`, for example:

```text
src/ftx_paper/strategy/orderflow_contract.py
```

The contract should expose an observation equivalent to:

```python
@dataclass(frozen=True, slots=True)
class OrderflowObservation:
    observation_id: str
    source_observation_id: str | None
    feature_version: str
    timeframe: str
    instrument: str
    observed_at: datetime
    available_at: datetime
    features: Mapping[str, float | int | str | None]
    location: str | None
    interaction_id: str | None
    response_status: str | None
    evidence_ids: tuple[str, ...]
```

The observation is accompanied by an immutable envelope/context containing the
guarantees that are not feature values:

```python
@dataclass(frozen=True, slots=True)
class OrderflowObservationContext:
    session_id: str
    completeness: str
    availability_status: str
    replay_provenance: Mapping[str, str]
    generation: int | None
    child_observation_ids: tuple[str, ...]
    child_intervals: tuple[tuple[datetime, datetime], ...]
    coverage_status: str | None
    causal_sequence: int | None
```

Every observation must carry:

- instrument and timeframe identity;
- source candle or aggregate identity;
- feature or calculation version;
- `observed_at` and `available_at`;
- location and interaction identity where applicable;
- response status and evidence IDs where applicable;
- explicit missing or unavailable state; and
- artifact or replay provenance in the immutable context.

The orderflow policy must also expose its typed `required_features` before
runtime composition. The market aggregator uses the union of enabled policies'
requirements and reports missing or unavailable features explicitly; it must
not treat either condition as a valid value. For a complete 5m observation,
the context must additionally validate exactly five distinct child IDs whose
ordered intervals cover the parent and whose generation, completeness,
availability, instrument, and causal sequence match.

The strategy must reject or fail closed for observations that are:

- available after the bundle decision time;
- incomplete or from an unexpected timeframe;
- missing required provenance;
- from another instrument or session; or
- reconstructed from a later terminal snapshot.

Raw `NiftyZoning` objects must not cross the repository boundary. The input can
be calculated natively in `ftx-paper`, or supplied through an explicit,
versioned serialized observation contract.

The current `ftx-paper` `DecisionBundle` contains synchronized bars for one
decision minute. It does not by itself contain the full 1m/5m history needed to
reconstruct interactions. Therefore the market layer must supply the dedicated
immutable orderflow context described below before strategy evaluation. The
context may be produced natively by an orderflow market service or decoded from
an explicit versioned serialized observation contract, but it must not be
smuggled through the untyped supporting-input mapping.

The strategy must not silently rebuild missing history from raw rows or from a
terminal registry snapshot.

### Bundle Integration

Extend `DecisionBundle` with an optional immutable orderflow context rather
than expanding the untyped `supporting_inputs` mapping. The context should
contain the completed 1m/5m prefixes, observations, interactions, and the
decision-time availability cutoff. It must be produced by the market layer or
an explicit serialized observation adapter before policy evaluation.

The market aggregator must compose required inputs from the union of all
enabled policies before runtime startup. Missing and unavailable features must
remain distinct typed states. A policy must receive an unavailable result when
the required producer cannot meet the bundle cutoff; it must never infer a
value from a later bar, carried-forward terminal snapshot, or raw historical
rows.

For a complete 5m location observation, preserve the existing child-graph
invariant: exactly five distinct, chronologically ordered 1m observations must
cover the parent interval, with matching instrument, location, generation,
completeness, availability, and causal sequence.

## Initial Diagnostic Hypothesis

The first hypothesis should be intentionally narrow and diagnostic-only:

```text
5m VWAP rejection or recovery
```

Suggested diagnostic behavior:

- location: VWAP with a frozen interaction band;
- response timeframe: 5m;
- direction: determined by the confirmed response;
- response: confirmed rejection evidence, with recovery retained as a separate
  future response type;
- trigger: recorded as a diagnostic `TriggerObservation`, not an executable
  order trigger; and
- entry, stop, and exit: not created during the initial diagnostic phase.

The current `NiftyZoning` replay intentionally treats VWAP as diagnostic
context. It publishes VWAP observations and rejection evidence, but excludes
VWAP interactions from executable opportunity evaluation. Executable VWAP use
requires a separate policy decision and a later phase covering templates,
direction rules, trigger semantics, admission, and paired replay.

Do not begin by combining every orderflow feature. Delta divergence, CLV,
profile skew, POC movement, liquidity sweeps, and volume/range features should
initially be diagnostic or admission inputs. They should qualify or reject a
candidate, not independently create trades.

## Current Capability Mapping

The current `NiftyZoning` implementation already provides the following
behavior. The future `ftx-paper` implementation should reproduce the relevant
contracts independently rather than import these modules:

| Current capability | Current implementation | `ftx-paper` design use |
|---|---|---|
| Prefix-safe VWAP | `vwap_location.causal_vwap_prefix` and `state_machine._publish_vwap_locations` | Native causal producer or serialized adapter input |
| Location observations | `LocationObservation` with source IDs, availability, generation, and child IDs | Immutable strategy input contract |
| Frozen interactions | `LocationInteraction` and `InteractionSnapshot` | Independent strategy-local state with equivalent frozen fields |
| Response outcomes | `OpportunityOutcome` and response evaluators | Policy result, not execution state |
| Trigger evidence | `TriggerObservation` and `observe_trigger` | Preserve the same trigger-observation semantics |
| Replay capture | `ReplayRecorder` and full replay artifacts | Full-capture source for causal comparison |
| History projection | `project_history` | Artifact-only projection; never recalculate evidence |

The `runner.OrderflowObservation` feature experiment is a separate session-level
summary. It is not sufficient as the executable strategy input because it does
not contain per-candle timestamps, source IDs, timeframe, interaction IDs, or
evidence lineage.

## State Ownership

The strategy may own:

- current trading date;
- completed 1m and 5m causal prefixes, only if supplied by an orderflow market
  service or immutable bundle input;
- active interaction episodes;
- frozen VWAP level, band, generation, and source observation;
- pending response state;
- formed opportunity IDs; and
- strategy cooldown state.

The strategy must not own:

- the runtime portfolio;
- order authorization;
- execution arbitration across strategies;
- broker state;
- SQLite or other persistence;
- external clock access; or
- a separate fill simulator.

Use `ExitStateMachine`, adaptive stops, shared sizing, shared risk guards,
session square-off, and `OrderIntent` from `ftx-paper`. Do not port the
`NiftyZoning` `ExecutionEngine` into the strategy.

## Interaction, Response, and Execution Lifecycles

Keep these lifecycles separate rather than combining them into one state
machine:

- interaction lifecycle: location contact and departure;
- response lifecycle: pending, confirmed, invalidated, or expired response;
- opportunity lifecycle: candidate formation, admission, suppression, or
  invalidation; and
- execution lifecycle: armed, triggered, filled, cancelled, or exited.

The strategy may implement deterministic interaction and response state, but
must not embed the full orderflow execution engine.

The interaction lifecycle is limited to the location contact episode:

```text
CREATED
ACTIVE
ENDED
```

The response lifecycle is tracked separately:

```text
PENDING_RESPONSE
CONFIRMED
INVALIDATED
EXPIRED
```

When an interaction is created, freeze:

- reference level;
- band bounds;
- source observation ID;
- timeframe;
- generation; and
- interaction ID.

Later VWAP revisions must never mutate an earlier episode. Independent 1m and
5m interactions must remain separate.

Response evaluation must use only completed candles available at the evaluation
cutoff. It must preserve the response state, evidence IDs, and causal
availability timestamps independently of the interaction state.

The current implementation represents interaction state as `ACTIVE` or
`ENDED`, while response state is represented separately by
`OpportunityOutcomeStatus`. The `ftx-paper` strategy should preserve this
separation unless a deliberate compatibility mapping is documented.

## Trigger Policy

Use one canonical trigger evaluator. Do not add independent
`trigger_satisfied()` methods to templates.

The trigger evaluator should return a structured observation containing:

- whether the boundary was crossed or touched;
- whether the evidence is eligible;
- trigger price;
- source candle or observation ID;
- observed and available timestamps; and
- ineligibility reason.

The evaluator must apply the configured trigger semantics for breakout,
reclaim, pullback, or immediate entry. Trigger evidence must never be inferred
from a future candle or from a terminal interaction snapshot. Its output should
preserve the existing `TriggerObservation` semantics: crossed, eligible,
price, reason, ineligible reason, and retest touch/confirmation evidence.

## Configuration and Identity

Add an immutable orderflow configuration, separate from the existing generic
strategy configuration. It should define:

- strategy name and version;
- enabled timeframes;
- session windows;
- VWAP calculation version;
- price and volume inputs;
- session reset behavior;
- interaction-band configuration;
- response timeframe and response type;
- trigger type;
- confirmation and invalidation rules;
- feature thresholds;
- admission policy version;
- stop and exit policies; and
- enabled vehicles.

It must also define `execution_mode` with at least `diagnostic` and
`executable` values. The initial implementation supports only `diagnostic`.
The shared coordinator must enforce this mode independently of policy code so
an accidental order returned by a diagnostic policy is rejected and recorded.

All values must participate in `config_hash`. A change to a feature
definition, threshold, template, timeframe, or causal rule must produce a new
configuration identity. This includes the selected feature set, feature alias
mapping, session/window cutoff, minimum-bar rule, signed OHLCV delta-proxy
definition, and calculation version.

## Decision Events and Provenance

Every candidate decision should retain enough information to reconstruct why
it was accepted or rejected:

- strategy ID, name, version, and configuration hash;
- decision and bundle IDs;
- opportunity ID;
- template ID and version;
- interaction ID;
- source observation IDs;
- response evidence IDs;
- decision timestamp;
- availability cutoff;
- feature values and feature version;
- admission result and reason; and
- risk or sizing result and reason.

Accepted orders should carry the same provenance in their order metadata. A
rejected or pending candidate should remain observable through the existing
`CandidateDecision` event contract.

## Implementation Plan

### Phase 0: Complete the Shared Strategy Boundary

Complete the prerequisite refactor before adding orderflow behavior:

- define `StrategyPolicy`, policy decision, admission, and state-transition
  protocols;
- extract the reusable coordinator from `IndependentLiveDecisionEngine`;
- move research-specific cell rules and feature requirements behind a research
  policy adapter;
- move risk, sizing, reservations, and exit ownership into the coordinator;
- add the strategy snapshot envelope and adapter dispatch;
- add policy feature-requirement composition to the market setup; and
- prove the refactor with a minimal non-research policy and unchanged research
  characterization fixtures.

The coordinator must preserve existing event payloads, order IDs, exception
behavior, causal timestamps, and legacy research snapshots.

### Phase 1: Freeze the Orderflow Contract

Document and test the following before producer implementation:

- target instrument and contract selection;
- 1m and 5m source streams;
- VWAP typical-price and volume definitions;
- session reset behavior;
- interaction-band calculation;
- availability cutoffs;
- complete 5m-to-1m child-observation validation;
- response confirmation and invalidation rules;
- trigger semantics;
- missing-input behavior;
- strategy version and configuration hash; and
- replay artifact and provenance fields.

Also freeze:

- the `DecisionBundle` orderflow-context field and serialization boundary;
- the typed feature requirement names and unavailable-state semantics;
- diagnostic versus executable policy mode; and
- the coordinator rule that diagnostic mode cannot produce orders.

Do not introduce a new research or validation split as part of this work.

### Phase 2: Causal Observations

Add the orderflow contract and prefix-safe calculations. For authoritative
causal history, use full replay capture. The replay diagnostic capture mode is
not a substitute for full capture because it does not retain the complete
location-observation graph.

Required tests:

- no future-bar access;
- incomplete candle rejection;
- session reset;
- deterministic repeated calculation;
- missing versus unavailable inputs;
- exact availability cutoffs;
- stable observation IDs; and
- feature-version provenance;
- exact five-child 5m observation coverage; and
- parent/child generation and availability matching.

This phase must not create executable orders.

### Phase 3: Interaction State

Add strategy-local interaction state and frozen geometry.

Required behavior:

- independent 1m and 5m episodes;
- deterministic interaction IDs;
- immutable reference level and band;
- generation-aware source linkage;
- departure and re-arm behavior; and
- interaction state (`CREATED`, `ACTIVE`, or `ENDED`);
- separately tracked response state (`PENDING_RESPONSE`, `CONFIRMED`,
  `INVALIDATED`, or `EXPIRED`).

Map interaction state and response state separately. Do not make
`CONFIRMED` or `INVALIDATED` interaction statuses when the underlying
interaction is only `ENDED`; those are response or opportunity outcomes.

### Phase 4: Opportunity and Trigger Policy

Add policy and trigger modules, for example:

```text
src/ftx_paper/strategy/orderflow_policy.py
src/ftx_paper/strategy/orderflow_trigger.py
```

The policy must form opportunities only from causal response evidence. The
trigger layer must create a trigger observation before an order can be formed.

Test pending, resolved, contradicted, expired, rejected, and eligible trigger
outcomes independently.

### Phase 5: Diagnostic Strategy Adapter

Implement `OrderflowPolicy` behind the shared policy contract, then expose it
through an `OrderflowStrategy` adapter with:

- `metadata`;
- `on_bundle`;
- `evaluate`;
- `on_execution_event`;
- `snapshot`; and
- a strategy-owned snapshot adapter hook.

The adapter must run in `diagnostic` mode initially. It should return
diagnostic candidate and trigger records with no `OrderIntent`. The coordinator
must independently validate this invariant. The strategy should use shared
cooldown and lifecycle services but must not duplicate event conversion,
authorization, sizing, reservations, execution, or exit orchestration.

### Phase 6: Factory and Runtime Composition

Extend the factory boundary to select the orderflow strategy explicitly:

```text
ftx-paper-research
ftx-paper-orderflow
```

Run it beside the existing strategy through `MultiStrategyEngine` when
comparison is required. Use separate stable strategy IDs while sharing the
runtime-owned portfolio and capital context. The coordinator remains the single
owner of event conversion, authorization, execution, and exit orchestration;
factory composition must not create an orderflow-specific coordinator.

### Phase 7: Snapshot and Replay

Version the orderflow snapshot schema, for example:

```text
orderflow_snapshot_schema: 1
```

Persist only JSON-compatible deterministic state.

Run paired replays with identical:

- session date;
- input manifest;
- selected contract;
- capital profile;
- session configuration; and
- runtime execution settings.

Baseline-versus-orderflow comparisons must keep those shared runtime and input
settings identical while recording each strategy's own configuration and
configuration hash separately.

Compare observations, interactions, responses, opportunities, triggers,
admissions, orders, fills, exits, and realized P&L as separate populations.

A changed input fingerprint, feature version, or configuration hash must be
reported as a comparison difference, not unexplained strategy improvement.

### Phase 8: Diagnostic Enablement

Enable the strategy in this order:

1. Diagnostic observations only.
2. Diagnostic interactions and response records.
3. Candidate decisions without orders.
4. Paired deterministic replay against the baseline with full capture.
5. Diagnostic operation beside the existing strategy with stable attribution.

### Phase 9: Optional Executable VWAP Policy

This phase is not part of the initial implementation. It requires explicit
approval and a new paired validation design. It must add:

- VWAP trade templates and direction rules;
- executable response-to-opportunity mapping;
- trigger and entry semantics;
- stop and exit policy;
- interaction/opportunity arbitration;
- executable-output comparison against the diagnostic baseline; and
- a versioned strategy configuration and replay artifact.

Phase 9 must also add executable-only tests for accepted `OrderIntent`
construction and shared sizing/risk behavior. These tests do not belong to the
diagnostic implementation's acceptance criteria.

Only after those gates pass may the strategy produce VWAP entry orders.

## Required Tests

Add focused tests for:

- causal feature calculation;
- availability and provenance validation;
- VWAP session reset;
- frozen interaction geometry;
- independent 1m and 5m interactions;
- pending, confirmed, invalidated, and expired responses;
- deterministic opportunity identity;
- trigger eligibility and rejection;
- admission rejection;
- execution notification handling;
- snapshot round-trip and invalid snapshots;
- `MultiStrategyEngine` attribution;
- paired replay determinism; and
- no baseline-output changes when orderflow is diagnostic-only.

Diagnostic acceptance additionally requires zero executable orders and no
changes to baseline execution outputs. Accepted order construction and shared
 sizing/risk tests are deferred to Phase 9 with executable VWAP policy.

## Replay Acceptance Gates

The implementation is not ready for executable use until:

- repeated replay produces deterministic observations and decisions;
- every order is traceable to causal source and response evidence;
- no decision uses evidence unavailable at its decision time;
- snapshot restore produces equivalent future decisions;
- shared runtime execution remains the only execution path;
- strategy attribution is stable; and
- diagnostic mode produces no executable-output delta against baseline.

The final acceptance comparison must distinguish diagnostic interaction counts
from admissions, orders, fills, exits, and trades.

## Cross-Document Contract Dependencies

This plan depends on the shared contracts described by the other design
documents. The development guide and refactoring plan should state that:

- orderflow is a strategy input and policy, not a runtime;
- `ftx-paper` remains independent from `NiftyZoning`;
- raw features require a versioned causal observation contract;
- interaction and response state may be strategy-owned deterministic state;
- execution remains owned by `RuntimeSession`;
- diagnostic orderflow must precede executable policy use; and
- every decision must retain source observation, response evidence,
  availability, template, and configuration provenance.

The central design decision is to reproduce the required contracts and causal
behavior in `ftx-paper`, rather than copy or import the `NiftyZoning`
implementation.
