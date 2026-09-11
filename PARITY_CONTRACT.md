# Canonical FTX Decision Parity Contract

Status: P0 specification. The current `ftx-paper` implementation does not
yet satisfy this contract; Tracks P1-P10 are the implementation work required
to do so.

## Scope

Parity means that `ftx-paper` and the canonical FTX implementation produce the
same decision stream when given the same completed market inputs. The
canonical reference is:

- Historical extraction: `src/ftx/signal_extraction.py`
- Canonical event features: `src/ftx/event_features.py`
- Canonical live session: `src/ftx/live_session.py`
- Session policies: `src/ftx/live_strategy_config.py`

The contract covers candidate detection, eligibility, rejection, acceptance,
order intent, stop, quantity, and exit decisions. Broker acknowledgements,
fill prices, network timing, and persistence timestamps are outside decision
parity and are covered by the ledger/runtime tracks.

## Input contract

Each decision minute is evaluated from exactly one completed futures bar and a
causal prefix containing only bars at or before that minute. Supporting inputs
are:

| Input | Contract |
|---|---|
| Futures | Completed OHLCV bar, ordered by canonical IST minute; current bar is the entry bar. |
| VIX | Opening value from the first available 09:15 bar; event value is the latest completed value at or before the decision minute. Missing VIX follows the canonical rejection path. |
| Option PCR | Value associated with the decision minute, or `None` when unavailable according to the canonical lookup cutoff. |
| Prior-day levels | Previous trading day high/low, reset at each trading-date boundary; `None` when unavailable. |
| Expiry calendar | Canonical weekly-expiry membership used for stop adjustment. |
| Configuration | Strategy version, session windows, cells, directions, exit modes, stability, cooldown, sizing, and risk settings. |

All timestamps used for decisions are timezone-aware instants. Session and
minutes-from-open calculations are performed in Asia/Kolkata. `created_at`
and other persistence timestamps must never influence a decision.

## Determinism rules

- Inputs are processed in canonical minute order.
- A minute is processed at most once.
- Late supporting inputs cannot mutate an already emitted decision.
- Features and eligibility use no future bars or future supporting values.
- Missing values use canonical defaults or canonical rejection reasons; paper
  must not invent a substitute value silently.
- Numeric values are serialized using the same field names and normalized
  scalar types. Parity comparison uses exact equality after normalization;
  no tolerance is permitted for score, stop, quantity, or decision fields.
- Decision IDs may differ only if the contract explicitly defines an equivalent
  deterministic ID formula; otherwise IDs must also compare exactly.

## Decision output contract

For every candidate minute and cell, the comparator must be able to compare:

```text
decision_at
bundle/event identity
sequence
cell
direction
entry price
setup type
score
score factors
VIX at event and VIX open
PCR at event
structural proximity
stop basis / hard stop
requested quantity
accepted or rejected outcome
rejection reason
exit mode and exit result, when accepted
```

The ordered decision stream must include the same candidate, rejection, warmup,
sizing-rejection, acceptance, and exit events in the same order. A missing
event, extra event, changed field, changed ordering, or changed timestamp is a
parity failure.

## Component mapping

`ftx-paper` remains independently runnable and must not import NiftyZoning
implementation modules. Its independently maintained components map to the
canonical contract as follows:

| Canonical responsibility | Independent paper responsibility | Required result |
|---|---|---|
| Completed-bar decision clock | `ftx_paper.core.bundles` and runtime feed | Same causal prefix, ordering, warmup, and missing-input semantics |
| Market features | `ftx_paper.core.features` and bundle feature path | Same VWAP, session levels, ATR, opening range, VIX, PCR, and prior-day values |
| Location/cell detection | `ftx_paper.core.policy` / new independent detector | Same simultaneous locations and composite cell names |
| Selling structure and setup score | `ftx_paper.core.scoring` | Same factor booleans, score, and setup tier |
| Session policy | `ftx_paper.strategy.config` and decision engine | Same half-open windows, cell eligibility, fixed directions, and transition rules |
| Stops and sizing | `ftx_paper.core.risk` | Same stop distance, expiry/VIX adjustments, quantity, and rejection behavior |
| Risk gates | paper runtime/core risk state | Same drawdown, concurrency, thesis, cooldown, and reset behavior |
| Exits | `ftx_paper.core.exits` | Same per-cell exit mode, intrabar ordering, trail, hard stop, and EOD result |
| Audit output | `ftx_paper` decision/order events | Same normalized decision fields; storage metadata may differ |

This mapping is a responsibility map, not permission to share code. The
implementations remain duplicated and isolated by design.

## Acceptance gate

P0 is complete when this contract is approved as the working specification.
The implementation cutover gate is stricter:

1. Run both implementations against the same deterministic fixture set.
2. Run both implementations against the same holdout replay.
3. Compare the complete ordered decision stream field-by-field.
4. Compare exits and ledgers separately for futures and synthetic vehicles.
5. Require zero unexplained differences.
6. Record any intentional exception with the exact field, reason, expected
   value on each side, and explicit approval before cutover.

No production switch or removal of the canonical implementation is allowed
until the acceptance gate passes.
