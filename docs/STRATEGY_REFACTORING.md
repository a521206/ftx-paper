# Strategy Reuse Refactor

This document defines the small refactor required to make the reusable runtime
and strategy capabilities available to strategies beyond `ftx-paper-research`.
It is not a rewrite and does not change the research strategy's behavior.

This refactor is a prerequisite for the orderflow strategy described in
`FTX_PAPER_ORDERFLOW_STRATEGY_DESIGN_PLAN.md`. Orderflow must consume the
resulting policy and coordinator contracts rather than bypassing them.

## Current Problem

The reusable pieces already exist, including `RiskEngine`, `SizingPipeline`,
`ExitStateMachine`, adaptive stops, snapshots, replay, and runtime attribution.
However, `IndependentLiveDecisionEngine` still combines reusable orchestration
with research-specific cell policies, session rules, feature requirements, and
Nifty-oriented assumptions. Snapshot restoration also assumes the research
strategy's state shape.

## Target Design

Keep the stable strategy boundary:

```text
DecisionBundle
    -> strategy policy
    -> shared decision/risk/sizing coordinator
    -> DecisionEvent / OrderIntent
```

### Shared Coordinator

The coordinator is runtime-owned and is the only component allowed to apply
account or execution state. It owns reusable mechanics:

- decision event construction and causal timestamps;
- decision context and portfolio inputs;
- risk authorization and sizing orchestration;
- vehicle and exposure validation;
- exit state-machine integration;
- execution-notification forwarding;
- deterministic event and diagnostic output.

The coordinator passes read-only decision context and portfolio inputs to the
policy. The policy may not mutate the portfolio, reservations, risk state, or
execution state. It must not import research policy tables or choose a
strategy's market hypothesis. Authorization, reservations, fills, and
settlement remain runtime operations after policy evaluation.

The coordinator must also enforce policy execution mode. A diagnostic policy
may produce observations, candidates, and rejection records, but any returned
`OrderIntent` is invalid and must become a deterministic coordinator rejection.

### Strategy Policy

Each strategy supplies its policy through an explicit object or protocol. The
policy owns:

- configuration and metadata;
- required market features and input availability rules;
- entry and session conditions;
- direction, hypothesis, and exit-mode selection;
- strategy-specific admission predicates;
- strategy-specific deterministic state transitions.

The policy protocol must expose a typed `required_features` value before market
composition, plus an `execution_mode` value. Candidate evaluation returns a
strategy decision value; it does not construct or authorize orders. This keeps
diagnostic strategies useful without giving them an accidental execution path.

The policy contract must expose typed operations for feature requirements,
candidate evaluation, admission, and state transitions. Candidate evaluation
returns a strategy decision value containing the hypothesis, direction,
optional exit mode, and candidate data; the coordinator converts that value
into the existing `DecisionEvent` and, when authorized, `OrderIntent`. Policy
state transitions consume coordinator notifications and cannot apply account
mutations. The coordinator owns the `ExitStateMachine` instances and invokes
policy hooks only for strategy-specific state.

Research policy should contain the current cell tables, PCR/VIX and momentum
gates, and Nifty-specific assumptions. A future strategy can provide a
different policy without subclassing or copying the coordinator. Required
features are declared before runtime composition; the market aggregator is
configured from the union of all enabled policies' requirements. A missing
feature is reported to that policy as unavailable and cannot silently be
treated as a valid value.

### Snapshots

Keep the runtime envelope and `strategy_id` mapping common. Move strategy state
serialization behind a strategy-owned snapshot adapter:

- common envelope: envelope schema version, strategy ID, enabled state, and
  strategy metadata;
- strategy payload: adapter/schema version and serializable policy state;
- no snapshot owns or creates a second runtime or shared portfolio.

The decoder must first identify the common envelope, then dispatch by
`strategy_id` to the strategy adapter. It must also accept the existing
research-only snapshot as an explicit legacy format and decode it into the new
envelope without changing its payload validation. Unsupported envelope or
payload versions must fail clearly. Snapshot decoding is compatibility for the
existing startup/runtime restore path, not a new session-resumption feature.

## Implementation Plan

1. Add a typed policy protocol for feature requirements, candidate evaluation,
   admission, exit-mode selection, and deterministic state transitions. Define
   the decision value and unavailable-input behavior at this boundary.
2. Define the immutable `DecisionBundle` context extension and feature
   requirement composition needed by policies with multi-timeframe inputs.
3. Extract the orchestration portions of `IndependentLiveDecisionEngine` into a
   shared coordinator without changing event payloads or order semantics.
4. Implement an adapter for the existing research policy. Initially, all
   research behavior should be moved behind this adapter unchanged.
5. Inject the policy into `ConfiguredLiveStrategy` and preserve its factory and
   `Strategy` protocol interfaces.
6. Extract snapshot encode/decode hooks. Define the common envelope and legacy
   research decoder; keep the current research payload schema and validation as
   the first adapter implementation.
7. Keep `RiskEngine`, `SizingPipeline`, `ExitStateMachine`, replay, and runtime
   attribution as shared services; do not duplicate them in the adapter.
8. Add a minimal second test policy with a different feature requirement and
   simple entry rule. Run it through the same coordinator, aggregator
   composition, `Strategy` boundary, and factory to prove that research policy
   is not required by the shared path.
9. Add characterization fixtures for the research strategy before extraction,
   then compare event types and payloads, causal timestamps, order IDs,
   decisions, fills, exits, ledger, parity artifact, and exception behavior
   after extraction.

## Acceptance Criteria

- Existing `ftx-paper-research` unit and integration tests pass unchanged.
- A deterministic replay produces the same decisions, fills, exits, ledger, and
  parity artifact for the research strategy before and after extraction.
- The coordinator has no imports of research policy constants or Nifty-specific
  entry rules.
- A second policy can run through the `Strategy` and factory boundaries without
  copying research decision code, and its declared features are supplied by
  the composed decision clock.
- Risk, sizing, exits, snapshots, replay, and multi-strategy attribution remain
  available through the shared interfaces.
- Strategy exceptions retain the existing runtime failure behavior.
- Existing event types, payload shapes, causal timestamps, order ID semantics,
  rejection reasons, and legacy research snapshot payloads remain compatible.

## Non-Goals

- New trading rules or changed research parameters.
- Separate strategy portfolios, P&L, or databases.
- Dynamic strategy loading or hot reload.
- Live broker order routing or external-position reconciliation.
- Replacing the existing replay or parity artifact format.
