# Strategy Reuse Refactor

This document defines the small refactor required to make the reusable runtime
and strategy capabilities available to strategies beyond `ftx-paper-research`.
It is not a rewrite and does not change the research strategy's behavior.

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

The coordinator owns reusable mechanics:

- decision event construction and causal timestamps;
- decision context and portfolio inputs;
- risk authorization and sizing orchestration;
- vehicle and exposure validation;
- exit state-machine integration;
- execution-notification forwarding;
- deterministic event and diagnostic output.

It must not import research policy tables or choose a strategy's market
hypothesis.

### Strategy Policy

Each strategy supplies its policy through an explicit object or protocol. The
policy owns:

- configuration and metadata;
- required market features and input availability rules;
- entry and session conditions;
- direction, hypothesis, and exit-mode selection;
- strategy-specific admission predicates;
- strategy-specific state transitions.

Research policy should contain the current cell tables, PCR/VIX and momentum
gates, and Nifty-specific assumptions. A future strategy can provide a
different policy without subclassing or copying the coordinator.

### Snapshots

Keep the runtime envelope and `strategy_id` mapping common. Move strategy state
serialization behind a strategy-owned snapshot adapter:

- common envelope: schema version, strategy ID, enabled state, and metadata;
- strategy payload: versioned, serializable policy state;
- no snapshot owns or creates a second runtime or shared portfolio.

Existing research snapshots must remain readable during the migration. Do not
add session resumption as part of this refactor.

## Implementation Plan

1. Add a policy protocol for feature requirements, candidate evaluation, and
   strategy-specific admission decisions.
2. Extract the orchestration portions of `IndependentLiveDecisionEngine` into a
   shared coordinator without changing event payloads or order semantics.
3. Implement an adapter for the existing research policy. Initially, all
   research behavior should be moved behind this adapter unchanged.
4. Inject the policy into `ConfiguredLiveStrategy` and preserve its factory and
   `Strategy` protocol interfaces.
5. Extract snapshot encode/decode hooks. Keep the current research snapshot
   schema and validation as the first adapter implementation.
6. Keep `RiskEngine`, `SizingPipeline`, `ExitStateMachine`, replay, and runtime
   attribution as shared services; do not duplicate them in the adapter.
7. Add a minimal second test policy using the same coordinator to prove that
   research-specific policy is no longer required by the shared path.

## Acceptance Criteria

- Existing `ftx-paper-research` unit and integration tests pass unchanged.
- A deterministic replay produces the same decisions, fills, exits, ledger, and
  parity artifact for the research strategy before and after extraction.
- The coordinator has no imports of research policy constants or Nifty-specific
  entry rules.
- A second policy can run through the `Strategy` and factory boundaries without
  copying research decision code.
- Risk, sizing, exits, snapshots, replay, and multi-strategy attribution remain
  available through the shared interfaces.
- Strategy exceptions retain the existing runtime failure behavior.

## Non-Goals

- New trading rules or changed research parameters.
- Separate strategy portfolios, P&L, or databases.
- Dynamic strategy loading or hot reload.
- Live broker order routing or external-position reconciliation.
- Replacing the existing replay or parity artifact format.
