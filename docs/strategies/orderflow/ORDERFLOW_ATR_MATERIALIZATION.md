# Orderflow ATR Materialization

## Purpose

This document defines the orderflow-specific ATR task built on the shared daily
setup materialization design in
`docs/strategies/shared/DAILY_SETUP_MATERIALIZATION.md`.

The task removes recursive ATR history reconstruction from replay while
preserving the canonical NiftyZoning calculation and causal publication rules.
This document records the implementation contract and current acceptance status.

## Status

Implemented in the Paper runtime. The `daily-atr` task produces immutable,
validated artifacts with chronological Wilder state, source provenance, target
availability, and downstream invalidation on source correction. Replay consumes
the artifact and no longer reconstructs ATR from historical warmup bars.

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
2. Reconstruct a typed ATR publication/domain value.
3. Inject the publication into the orderflow input provider or transition
   owner before the first target-session decision.
4. Replay only target-session bars for ATR advancement that is explicitly
   required by the canonical contract; do not reseed from a bounded lookback.
5. Preserve canonical decision-clock availability, timestamps, source identity,
   unavailable reasons, and lineage.

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
