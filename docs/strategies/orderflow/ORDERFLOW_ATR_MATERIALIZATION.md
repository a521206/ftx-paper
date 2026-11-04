# Orderflow ATR And Prior-Session Level Materialization

## Purpose

This document defines orderflow daily setup tasks built on the shared setup
materialization design in
`docs/strategies/shared/DAILY_SETUP_MATERIALIZATION.md`.

The daily-ATR task removes recursive ATR history reconstruction from replay.
The prior-session-levels task publishes completed-session high/low facts so
replay need not reload and scan the prior session. Both preserve source lineage
and causal availability. This document records each contract and its status.

## Status

The `daily-atr` task is implemented in the Paper runtime. It produces immutable,
validated artifacts with chronological Wilder state, source provenance, target
availability, and downstream invalidation on source correction. Replay consumes
the artifact and does not reconstruct ATR from historical warmup bars.

The `prior-session-levels` task is registered and implemented as an independent
setup task. It publishes immutable high/low artifacts with source-bar lineage,
and replay validates and consumes them without rescanning prior-session bars for
those extrema. Canonical parity acceptance remains pending. These levels are
direct facts from one completed source session, not derived indicators.

Focused strategy/infrastructure validation passes. Remaining acceptance work is
canonical cross-repository parity and production operational readiness.

## Canonical Contract

The source of truth is NiftyZoning's orderflow ATR implementation:

- source: continuous futures for the configured symbol;
- session OHLC: high and low across the completed session, and the final close;
- true range for the first session: `high - low`;
- true range thereafter:
  `max(high - low, abs(high - prior_close), abs(low - prior_close))`;
- period: 14 completed sessions;
- seed: arithmetic mean of the first 14 true ranges;
- update: `((previous_atr * 13) + current_true_range) / 14`;
- target-session ATR: the ATR value from the immediately preceding completed
  trading session.

The canonical producer uses the complete continuous-futures history and carries
the Wilder state forward chronologically. A replay lookback of 14 sessions is
not equivalent: it reseeds the ATR and produces a different value.

## Task Declaration

The orderflow registry should declare:

```text
strategy_id:       ftx-paper-orderflow
task_id:           daily-atr
algorithm_version: orderflow.daily-atr.wilder.v1
schema_version:    1
instrument:        NIFTY
```

The task is strategy-owned, while source reads, artifact persistence, job
lifecycle, scheduling, and catalog lookup remain owned by the shared setup
worker and its ports.

## Prior-Session Levels

The setup registry declares this separate task:

```text
strategy_id:       ftx-paper-orderflow
task_id:           prior-session-levels
algorithm_version: orderflow.prior-session-levels.v1
schema_version:    1
instrument:        NIFTY
```

This task publishes `PD_HIGH` and `PD_LOW` for each eligible target session
from the selected continuous-futures bars of the immediately preceding completed
trading session. The task runs in the same initial catch-up and incremental setup
workflow as `daily-atr`; it is not calculated in the replay hot path. Keep it
separate from ATR because it is a pair of observed extrema, not an indicator
derived from a multi-session formula. Keep it separate from prior-session value
because value-area geometry has a different canonical producer and contract.

The available artifact uses the shared `strategy_daily_setup` envelope with:

```text
contract.id:       ftx-paper-orderflow.prior-session-levels
task_id:           prior-session-levels
algorithm_version: orderflow.prior-session-levels.v1
```

Its source section identifies exactly one source session and records the
selected storage contract, normalized continuous-source fingerprint and recipe,
source bar count, and session finalization boundary. Its payload contains:

```json
{
  "target_session": "2026-10-05",
  "source_session": "2026-10-01",
  "symbol": "NIFTY",
  "high": 25250.0,
  "low": 24910.0,
  "observed_at": "2026-10-01T10:00:00Z",
  "available_at": "2026-10-05T03:45:00Z",
  "lineage": [
    "source:normalized-source-fingerprint",
    "bar:2026-10-01T09:15:00Z",
    "bar:2026-10-01T09:16:00Z",
    "..."
  ]
}
```

`high` and `low` are the maximum source-bar high and minimum source-bar low,
respectively, over the same selected, normalized completed session. `observed_at`
is the final source observation used to establish the extrema; `available_at` is
the target session open. Artifact creation time must not substitute for either
domain timestamp. If the prior session is missing, incomplete, gapped, or its
selected source is invalid, publish an explicit unavailable result or fail
according to the task policy; do not use a different session or partial range.

Replay validates the artifact contract, digest, target/source session identity,
selected source provenance, payload values, and causal timestamps, then injects
the levels into the feature input before today's first decision. It must not
rescan prior-session intraday bars for these extrema. Setup
invalidation/rebuild owns source corrections; stale, missing, or corrupt level
artifacts remain unavailable rather than triggering an implicit replay-time
recomputation. The published locations must retain the prior source session and
artifact/source lineage so the durable value does not obscure the underlying
market evidence. Lineage must identify the exact contributing source bars (by
stable source IDs or timestamps) as well as the normalized source fingerprint;
the session identifier alone is insufficient.

Unlike ATR, a correction to this task's one source session invalidates only
artifacts whose target session uses that source; it does not recursively
invalidate later sessions. The task's parity check compares exact high/low,
source session and identity, observed/available times, lineage, and explicit
unavailable status against extrema recomputed from the same canonical source
bars.

## Ownership Boundary

The shared worker owns scheduling, artifact lifecycle, persistence, and lookup.
The orderflow task owns ATR normalization, the canonical Wilder recurrence,
provenance, and invalidation. Replay only consumes validated target-session
publications. A source correction invalidates the affected session and all later
ATR values that depend on its recursive state.

## Materialization Scope

For each eligible target session `D`, the task publishes the ATR from the last
completed source session before `D`. The materializer must:

1. Read the canonical continuous-futures source, not an arbitrary option or
   dated-contract surface.
2. Normalize and order source bars using the same session calendar and window
   as NiftyZoning.
3. Build one daily OHLC row per completed source session.
4. Walk source sessions in chronological order, retaining the recursive Wilder
   state.
5. Publish no value until 14 true ranges are available.
6. Publish the prior-session ATR for each eligible target session.

The implementation may persist one artifact per target session or a versioned
chronological ATR series, but lookup must resolve one immutable value for one
target session. A series artifact must include the exact target-session map and
must not require replay to rerun the recurrence.

## Artifact Contract

The task uses the shared `strategy_daily_setup` envelope with:

```text
artifact_type:     strategy_daily_setup
contract.id:       ftx-paper-orderflow.daily-atr
task_id:           daily-atr
algorithm_version: orderflow.daily-atr.wilder.v1
```

The payload for an available target session contains:

```json
{
  "target_session": "2026-10-05",
  "source_session": "2026-10-01",
  "symbol": "NIFTY",
  "period": 14,
  "method": "wilder",
  "value": 223.449924185514,
  "observed_at": "2026-10-01T10:00:00Z",
  "available_at": "2026-10-05T03:45:00Z",
  "lineage": ["daily_atr:NIFTY:2026-10-05:223.449924185514"]
}
```

The source section must record:

- continuous-futures identity and selected storage contract identity;
- source sessions contributing to the recurrence, or the series artifact's
  covered range;
- normalized source fingerprint and fingerprint recipe;
- source bar count and completed-session count;
- session calendar and finalization boundary.

The configuration section must record the symbol, period, session specification,
algorithm version, and task contract version. Artifact and payload digests use
the shared canonical serialization rules.

## Causal and Availability Rules

- `observed_at` is the final source observation used to complete the source
  session, not artifact creation time.
- `available_at` is the target session's earliest permitted decision time,
  normally the target session open.
- A target session cannot consume an ATR sourced from that same session.
- Missing, incomplete, gapped, stale, or identity-mismatched source data makes
  the artifact unavailable or failed according to the task policy; it never
  triggers a 14-session fallback.
- An unavailable artifact contains no numeric ATR, timestamps, or lineage.

## Replay Integration

The migration occurs after the shared setup worker and artifact reader are
available:

1. Load the target session's validated `daily-atr` artifact.
2. Verify its source-session lineage and causal timestamps against the target
   session contract.
3. Reconstruct a typed ATR publication/domain value.
4. Inject the publication into the orderflow input provider or transition
   owner before the first target-session decision.
5. Replay only target-session bars for ATR advancement that is explicitly
   required by the canonical contract; do not reseed from a bounded lookback.
6. Preserve canonical decision-clock availability, timestamps, source identity,
   unavailable reasons, and lineage.

Paper materializes ATR from its own resolved continuous-futures history. Matching
the canonical value therefore also requires that the two repositories provide
the same normalized daily OHLC history; replay must not substitute a canonical
value to conceal source-history variance. Rebuild the setup artifact after source
history corrections; replay validates artifact semantics without reloading and
recomputing the full history.

Normal replay must not call `daily_atr()` over a truncated `rows_by_date` map.
It must not load the entire intraday history merely to reconstruct a value that
has already been materialized. The 14-session warmup path is temporary
diagnostic behavior only and must not be used for accepted parity.

## Invalidation and Rebuild

An ATR artifact is stale when any of these change:

- source bars, source contract, continuous-roll policy, or source fingerprint;
- session calendar or session window;
- symbol or period;
- algorithm or task contract version;
- artifact schema or configuration identity.

Rebuilds are chronological and idempotent. A source correction invalidates the
affected session and all later ATR values whose recursive state depends on it.
The setup worker writes replacement immutable artifacts and publishes the new
catalog only after validation succeeds.

Startup and replay do not recompute the history fingerprint on every load. That
recomputation rescans the complete completed-session history, and the source
rarely changes, so load validates the artifact's structural source lineage and
causal timestamps instead. The setup worker still detects a source change when
it materializes, and an operator can force replacement with
`--mode rebuild`.

## Parity Acceptance

For every eligible comparison date, the canonical and Paper artifacts must
match on:

- ATR value within zero numerical tolerance for the same normalized source;
- source session and source identity;
- observed and available timestamps;
- period and algorithm version;
- lineage and fingerprints;
- explicit unavailable status and reason when no value is publishable.

The existing bounded ATR `INPUT_VARIANCE` rule may classify repository source
history differences only when the difference is documented, numeric, and within
the configured tolerance. A different Wilder seed caused by a truncated replay
warmup is not input variance; it is an implementation mismatch.

## Validation Status

Focused coverage includes recurrence seeding, recursive updates, source
correction invalidation, missing-history unavailability, artifact round trips,
replay integration, and cross-repository comparison fixtures. The remaining
gate is acceptance against the canonical NiftyZoning output for representative
no-trade and execution-bearing sessions.

## Migration Status

The shared worker and artifact contract are complete, the task is registered, and
artifact-backed replay is the normal path. Historical warmup reconstruction and
its fallback have been removed. Production acceptance depends on canonical parity
and operational scheduling rather than additional replay implementation.
