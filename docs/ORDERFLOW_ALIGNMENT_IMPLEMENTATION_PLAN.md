# Orderflow Code Organization Alignment Plan

## Goal

Organize FTX Paper's orderflow code so each NiftyZoning behavior has an obvious
Paper counterpart that can be reviewed and tested side by side.

This is an organization plan, not a second strategy or parity implementation
plan. Detailed behavior remains in:

- `docs/ORDERFLOW_IMPLEMENTATION_PLAN.md`
- `docs/PRODUCER_NEUTRAL_PARITY.md`
- `docs/PRODUCER_NEUTRAL_PARITY_IMPLEMENTATION_PLAN.md`

NiftyZoning remains a read-only reference. Paper does not import its code or
share its runtime state.

## Organization Rules

1. Use one Paper module for each major behavioral responsibility.
2. Keep pure calculations separate from stateful lifecycle owners.
3. Keep `OrderflowPolicy` as the live lifecycle orchestrator, not the home for
   every implementation detail.
4. Keep replay orchestration separate from live policy state.
5. Keep execution and accounting in the existing Paper execution owner.
6. Use matching concepts and names where that makes comparison clearer.
7. Keep Paper-specific adapters and persistence at Paper boundaries.

## Module Map

| Paper module | Responsibility | NiftyZoning comparison area |
| --- | --- | --- |
| `orderflow_source.py` | source identity and source selection | `state_machine_source.py`, `research_replay.py` |
| `orderflow_producer.py` | completed 1m/5m aggregates | `market_observation.py`, `orderflow_common.py` |
| `orderflow_observation.py` | availability, gaps, and causal prefixes | `replay_common.py`, observation contracts |
| `orderflow_context.py` | context, values, permissions, opening gap | `market_context_history.py`, `value_publication.py`, `opening_gap_policy.py`, `regime_classification.py` |
| `orderflow_location.py` | location records, bands, generations | `state_machine_types.py`, `location_geometry.py`, `location_observation.py` |
| `orderflow_interaction.py` | interaction episodes and transitions | `interaction_detector.py`, `interactions.py` |
| `response.py` | reusable response geometry and evaluation helpers | `response.py`, `templates.py` |
| `orderflow_response.py` | orderflow-specific response adaptation and state | `response.py`, `policies/` |
| `orderflow_opportunity.py` | opportunities, setups, conflict groups | `opportunity.py`, `lifecycle.py` |
| `opportunity_timeout.py` | opportunity timeout rules | `opportunity_timeout.py` |
| `orderflow_admission.py` | ordered admission predicates and diagnostic intent | `intent_policy.py`, `active_position_gate.py`, `allocation.py` |
| `orderflow_trigger.py` | trigger observations and eligibility | `execution.py` |
| `orderflow_execution.py` | Paper-specific execution helpers | `execution.py`, `lifecycle.py` |
| `orderflow_reporting.py` | Paper diagnostic projections | `state_machine_reporting.py`, `state_machine_evidence.py` |
| `orderflow_replay.py` | replay-only orchestration | `runner.py`, `ReplayMachine` |
| `orderflow.py` | live orchestration and public strategy facade | `state_machine.py` orchestration only |

## Causal Comparison Order

Align the decision scope before comparing final populations. Freeze source
contract, requested sessions, timezone, session-end cutoff, vehicles, capital
profile, strategy configuration, and inclusive/exclusive decision-clock rules.
Resolve selected, filtered, completed, gap, and processed decision-bar counts
before comparing decisions.

Use the existing replay path to expose detached diagnostic checkpoints in this
order:

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

Compare the first divergent checkpoint phase before final trades. Repeated
location observations remain diagnostic; decision-bearing interaction creation,
closure, replacement, and gap transitions remain distinguishable. Response
hypothesis changes in evidence, arming, availability, confirmation,
invalidation, or expiry must be retained. Diagnostic execution must produce no
orders, fills, reservations, exits, or portfolio mutations; any reported trade
requires attribution through the existing execution and ledger owners.

## Refactoring Steps

1. Keep the module map above as the review checklist.
2. Move code out of `orderflow.py` only when it belongs to an existing
   responsibility module; do not create new runtime owners.
3. Keep each module's public functions and records close to the corresponding
   NiftyZoning concept.
4. Add short docstrings naming the corresponding reference concept where the
   mapping is not obvious.
5. Keep `OrderflowPolicy.evaluate()` as the readable causal sequence that calls
   the responsibility modules in order.
6. Keep `ReplayMachine` and replay capture out of the live strategy callback.
7. Keep Paper execution, persistence, and snapshots behind Paper-owned
   interfaces even when the behavior matches NiftyZoning.

## Comparison-Friendly Tests

Organize tests using the same responsibility names:

```text
tests/test_orderflow_source.py
tests/test_orderflow_observation.py
tests/test_orderflow_context.py
tests/test_orderflow_location.py
tests/test_orderflow_interaction.py
tests/test_orderflow_response.py
tests/test_orderflow_opportunity.py
tests/test_orderflow_admission.py
tests/test_orderflow_execution.py
tests/test_orderflow_reporting.py
tests/test_orderflow_replay.py
```

The canonical Sep 29 API replay comparison is intentionally outside the unit
test suite. With the Paper API already running, execute:

```powershell
.venv\Scripts\python.exe scripts/orderflow/api_replay_regression.py
```

The script submits `2026-09-29` over HTTP, waits for completion, compares the
futures replay against the canonical totals (17 setup candidates, 4 filled
trades, 74 location interactions, 155.2143 gross points, and 6.1443 achieved
R:R), prints the observed values, and exits nonzero on mismatch or unavailable
API metrics. It is an opt-in integration regression, not a pytest test.

Within each test module, prefer names that identify the comparable behavior,
such as `test_response_acceptance_uses_completed_candle` or
`test_opportunity_timeout_precedes_formation`.

Fixtures should be Paper-owned and use explicit timestamps, source IDs,
availability, and expected transitions. They do not need to contain copied
NiftyZoning runtime objects.

## Completion Criteria

The organization is aligned when:

- every major NiftyZoning behavior has one obvious Paper module and test module;
- `orderflow.py` is an orchestrator rather than a catch-all implementation;
- live state, replay state, and execution state have clear owners;
- a reviewer can compare corresponding Paper and NiftyZoning concepts without
  searching unrelated modules;
- no NiftyZoning runtime code or state has been introduced.
