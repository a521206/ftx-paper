# Daily Setup Materialization

This document defines the production design for strategy-owned daily setup
materialization. It moves static, prior-session features out of replay and
live decision hot paths while preserving the repository's runtime ownership,
causal availability, and artifact provenance rules.

The first consumer is orderflow prior-session value publication. The worker is
strategy-neutral: each strategy declares the daily tasks it needs, and one
application-owned worker executes those tasks after market data is finalized
and before the next session.

## Goals

- Materialize static prior-session features once, outside replay.
- Make replay consume immutable, versioned derived artifacts.
- Support one initial historical catch-up followed by incremental daily runs.
- Preserve source, configuration, algorithm, and lineage fingerprints.
- Fail closed on missing, stale, or corrupt production evidence.
- Keep the API as the only owner of the runtime process and runtime state.
- Keep research rebuilds explicit and isolated.

## Non-goals

- This worker does not own market data ingestion, a broker feed, portfolio
  state, simulated execution, or the decision clock.
- This worker does not replace canonical strategy policy or invent fallback
  values.
- This design does not add a database materialized view.

## Current State

### Orderflow replay hot path

The replay input pipeline constructs a `CanonicalInputProvider`. Its current
`_build()` path constructs a `PaperFeatureAdapter` with
`build_prior_values(rows_by_date)`. `build_prior_values()` walks every supplied
session, finds each preceding trading session, rebuilds the canonical value
profile, and converts the resulting reference into `ValuePublication`.

This has three undesirable properties:

1. Static prior-session data is recalculated for every replay.
2. Replay latency grows with historical input scope.
3. Replay depends on historical source availability even when only one
   immutable prior-session publication is required.

The current fallback is an explicit unavailable publication with reason
`missing_prior_session_value_evidence`. That explicit unavailable behavior is
preserved; it must not become a silent recomputation or guessed value.

Replay has other historical dependencies that are not replaced by this task.
Warmup bars remain available for recursive ATR and any other explicitly
declared causal inputs. The prior-value artifact replaces only the historical
profile/publication calculation. The replay provider must not call
`build_prior_values()` or rebuild daily value profiles when the artifact is
available or when it is missing.

### Existing canonical contracts

The canonical value-profile implementation provides the domain contracts and
provenance primitives used by the task:

- `DailyProfileRecord`
- `ValueProfileSnapshot`
- `ValueAreaReference`
- `PriorSessionValueReference`
- `ValuePublication`

The existing profile code also provides:

- aggregate source fingerprints (the artifact adds normalized-bar and selected
  contract identity);
- profile configuration/session-window fingerprints;
- profile algorithm versions;
- deterministic daily snapshot identities;
- completeness and explicit unavailable reasons.

`ensure_daily_profile()` is the closest existing setup pattern. It reuses a
record when its source fingerprint matches and rebuilds it when the source
changes.

### Runtime and persistence boundaries

The API owns one in-process `RuntimeSession` for a runtime directory. Runtime,
portfolio, broker, and execution mutations occur in memory and are persisted
afterward. Strategies do not read SQLite, own clocks, call brokers, or create
runtime owners.

Replay is a bounded host operation. The replay CLI is a thin loopback API
client and must remain so.

The existing SQLite daily-profile repository is useful for canonical profile
records, but its mutable upsert semantics and coupling to the runtime database
make it unsuitable as the immutable replay artifact contract.

## Architecture

```text
scheduler or setup CLI
        |
        v
loopback setup API
        |
        v
API-owned SetupWorker
        |
        +--> SetupTaskRegistry
        +--> SourceReader port
        +--> SessionCalendar port
        +--> DerivedArtifactStore port
        +--> setup job lifecycle persistence
```

The worker is a generic application/runtime host. Strategies provide task
definitions through the composition root. Runtime code does not branch on
strategy names or instantiate strategy-specific producers.

### Strategy task declaration

Each task declares:

- stable `strategy_id`;
- stable `task_id`;
- task algorithm version;
- input requirements and source identity;
- session eligibility/finalization rule;
- output contract and schema version;
- materialization function;
- whether unavailable output is valid or fatal;
- task dependencies, if any.

Conceptually:

```text
DailySetupTask(
    strategy_id,
    task_id,
    algorithm_version,
    input_requirements,
    eligibility_policy,
    materialize,
    unavailable_policy,
)
```

The task implementation remains under `ftx_paper.strategies`. Source access,
artifact I/O, job lifecycle, and scheduling remain outside the strategy.

The registry is an explicit composition-time mapping of `(strategy_id, task_id)`
to a trusted task object. Requests never import or execute a task by module
path. Unknown strategy/task pairs are rejected before a job is queued. Task
execution is single-owner within the API process; concurrent jobs for the same
task, configuration identity, and overlapping session scope are serialized.

### Worker execution modes

The worker supports three operational modes:

- `catch_up`: process every eligible historical session missing valid output;
- `incremental`: reconcile the latest finalized session and any invalidated
  sessions;
- `rebuild`: explicitly recompute a selected scope.

The first deployment runs one large `catch_up`. Normal scheduling runs
`incremental`, which first verifies historical coverage and then processes the
latest finalized session. Processing is idempotent and chronological.

`catch_up`, `incremental`, and `rebuild` are the only mode values. `research` is
not a mode: it is an isolated output profile selected by `output_root` and may
only be used with `rebuild`. CLI and API spellings use the same underscore
values.

## Orderflow Materializer

The initial task is:

```text
strategy_id:       ftx-paper-orderflow
task_id:           prior-session-value
algorithm_version: orderflow.prior-session-value.v1
```

For finalized session `D`, the materializer creates the daily profile record
for `D`. It then creates the publication artifact consumed by target session
`D+1`.

`D+1` means the next date in the authoritative trading-session calendar, not
the next calendar date. A source session is eligible only when its selected
source reaches the configured finalization boundary: the last completed bar
ends at the session close, the expected bar timestamps are present, and the
source contains no gaps. Missing or late data leaves the session pending; it
does not create an available profile. An explicitly incomplete profile may be
written only when the task policy declares unavailable output valid.

A session remains pending while its source may still be finalized. Once the
configured finalization deadline has passed, the source reader evaluates the
same completeness rules again. If the source is still missing, late, or gapped,
the task writes an explicit unavailable artifact when its policy permits that
output; otherwise the session is recorded as failed and no available catalog
entry is published. A later source correction invalidates that unavailable
artifact and may replace it through the normal immutable rebuild path.

The implementation must use the canonical daily-profile/value-publication
contracts. It must not copy policy into the generic worker or recreate a
second value-profile algorithm.

The target publication should be derived from the finalized daily profile of
the immediately preceding trading session. If exact canonical behavior still
requires active-session input, the canonical producer boundary must be
adapted explicitly; the materializer must not approximate it.

The parity criterion is that a publication produced from the finalized `D`
record is equivalent in domain values, lineage, observed time, and target-
session availability to the current canonical `publish_value_references()`
result. Until that criterion is demonstrated, the orderflow task remains in
validation mode and replay may use the legacy path only in an explicit
comparison command.

## Artifact Model

### Artifact envelope

Every derived artifact uses a versioned JSON envelope:

```json
{
  "artifact_type": "strategy_daily_setup",
  "schema_version": 1,
  "contract": {
    "id": "ftx-paper-orderflow.prior-session-value",
    "version": 1
  },
  "strategy_id": "ftx-paper-orderflow",
  "task_id": "prior-session-value",
  "algorithm_version": "orderflow.prior-session-value.v1",
  "target_session": "2026-11-02",
  "status": "available",
  "created_at": "2026-11-02T03:45:00Z",
  "source": {
    "sessions": ["2026-11-01"],
    "selected_source": {
      "exchange": "NFO",
      "symbol": "NIFTY",
      "expiry": "2026-11-26",
      "contract_type": "FUTURES",
      "rollover_policy_version": "dated-front.v1"
    },
    "source_fingerprints": ["..."],
    "source_fingerprint_recipe": "ordered-canonical-bars-v1",
    "source_bar_count": 375,
    "session_finalization": {
      "timezone": "Asia/Kolkata",
      "close": "15:30",
      "finalized_at": "2026-11-01T10:00:00Z"
    }
  },
  "configuration": {
    "config_fingerprint": "...",
    "profile_version": "value_profile.v1",
    "session_spec": {
      "timezone": "Asia/Kolkata",
      "start": "09:15",
      "end": "15:30"
    }
  },
  "payload": {},
  "manifest": {
    "payload_sha256": "...",
    "artifact_sha256": "..."
  }
}
```

Timestamps are UTC ISO-8601 values. JSON uses sorted keys, stable ordering,
compact canonical serialization for hashing, and readable indentation only for
the stored representation if desired.

The payload digest is the SHA-256 of the canonical compact serialization of
`payload` only. `artifact_sha256` is the SHA-256 of the canonical compact
serialization of the complete envelope with the `manifest` member omitted.
`manifest.payload_sha256` and `manifest.artifact_sha256` are then inserted into
the stored envelope. Readers recreate these two preimages exactly; neither
digest includes its own manifest member or formatting whitespace.

`source.sessions` contains the exact trading sessions contributing to the
payload. `target_session` is the session that consumes the artifact. The
profile's `observed_at` is the last source observation used to finalize `D`;
the publication's `available_at` is the earliest target-session clock at which
the reference is allowed to be consumed. Materialization `created_at` is
operational metadata and is not substituted for either domain timestamp.

### Available and unavailable output

Unavailable output is a valid, explicit artifact:

```json
{
  "status": "unavailable",
  "unavailable_reason": "prior_session_incomplete",
  "payload": null
}
```

The task contract determines whether an unavailable artifact is acceptable.
Orderflow prior-session value publication remains explicit unavailable
evidence; it does not produce a replacement value.

### `ValuePublication` serialization

An available orderflow payload is serialized as:

```json
{
  "reference": {
    "session_date": "2026-11-01",
    "value_area_low": 24900.0,
    "point_of_control": 25010.0,
    "value_area_high": 25120.0,
    "observed_at": "2026-11-01T10:00:00Z",
    "available_at": "2026-11-02T03:45:00Z",
    "lineage": [
      "vp:daily:2026-11-01:...",
      "..."
    ]
  },
  "observed_at": "2026-11-01T10:00:00Z",
  "available_at": "2026-11-02T03:45:00Z",
  "lineage": [
    "vp:daily:2026-11-01:...",
    "..."
  ],
  "unavailable_reason": null
}
```

An unavailable publication has `reference: null`, no timestamps, an empty
lineage, and a required reason.

The artifact reader must reject an unavailable payload if `reference` is not
null, either timestamp is present, lineage is non-empty, or the reason is
missing. These are payload invariants, not advisory serialization conventions.

Replay reconstructs domain objects in this order:

1. Validate and parse `PriorSessionValueReference`.
2. Construct `ValuePublication`.
3. Inject the immutable publication into the evidence adapter.

Raw dictionaries must not enter strategy policy code.

### Artifact catalog

Artifacts are immutable. A small catalog is the only mutable pointer:

```json
{
  "schema_version": 1,
  "artifact_type": "strategy_daily_setup_catalog",
  "strategy_id": "ftx-paper-orderflow",
  "task_id": "prior-session-value",
  "algorithm_version": "orderflow.prior-session-value.v1",
  "config_fingerprint": "...",
  "instrument": "NIFTY",
  "entries": {
    "2026-11-02": {
      "status": "available",
      "path": "2026-11-02/source-.../artifact.json",
      "artifact_sha256": "..."
    }
  }
}
```

The catalog identity is `(strategy_id, task_id, algorithm_version,
config_fingerprint, instrument)`. Each catalog entry is additionally keyed by
target session and selected source-contract identity. The catalog must never
point to a partially written artifact. API artifact lookup must provide the
instrument and configuration identity (or resolve them from an unambiguous
task declaration); target session alone is not a sufficient key.

### Directory layout

Derived artifacts live under persistent `FTX_PAPER_HOME`, not in the source
repository or runtime database:

```text
derived/
  setup/
    <strategy_id>/
      <task_id>/
        <algorithm_version>/
          <config_fingerprint>/
            <instrument>/
              <target_session>/
                source-<source_fingerprint>/
                  artifact.json
            catalog.json
            runs/
              <job_id>.json
```

This layout supports immutable historical versions, deterministic lookup, and
separate research output roots.

## Fingerprints and Invalidation

An artifact is valid only when all identity inputs match:

- source fingerprint;
- source fingerprint recipe;
- source session set and bar count;
- selected exchange, symbol, expiry, contract type, and rollover-policy version;
- profile configuration fingerprint;
- session specification;
- profile version;
- materializer algorithm version;
- artifact schema version;
- task contract version;
- artifact and payload digests.

The source fingerprint must be computed over normalized, ordered source bars,
including normalized OHLCV/open-interest values and source timestamps. The
selected storage contract identity is stored separately from the virtual
continuous identity, and both identities participate in validation.

Any mismatch makes the artifact stale. A rebuild writes a new immutable
artifact and atomically changes the catalog pointer. It never overwrites the
previous artifact in place.

## Atomic Writes and Recovery

The writer performs these steps:

1. Compute and validate the complete payload in memory.
2. Serialize canonical JSON.
3. Write a uniquely named temporary file in the target directory.
4. Flush and `fsync` the file.
5. Re-read and validate the temporary artifact and digest.
6. Atomically rename it to the immutable artifact path.
7. Write, flush, and validate a temporary catalog.
8. Atomically replace `catalog.json`.

The catalog is published last. A crash can leave an unreferenced immutable
artifact or a temporary file, but cannot expose a partially written artifact.
Temporary files are ignored during reads and cleaned during worker startup.

On startup:

- queued setup jobs may be requeued;
- running jobs from a previous process are marked interrupted;
- valid published artifacts are reused;
- interrupted scopes are resumed idempotently;
- the previous valid catalog entry remains available until replacement
  succeeds.

## Missing, Stale, and Corrupt Artifacts

### Missing

Normal replay does not rebuild a missing artifact. It returns an explicit
`derived_artifact_unavailable` result. This is an intentional behavior change
from the current path, which recomputes from historical rows. A production
strategy that requires the artifact must fail closed or remain unavailable
according to its task policy. The replay run remains `completed` when the
strategy contract permits explicit unavailable evidence; it is `failed` when
the task policy marks the artifact as required.

### Stale

Stale artifacts are treated as unavailable. Scheduled setup runs rebuild them
when finalized source data is available. Replay never silently rebuilds.

### Corrupt

Invalid JSON, unsupported schema, invalid domain values, timestamp violations,
digest mismatches, and catalog/path mismatches are corruption.

The reader must quarantine the artifact, mark the catalog entry unavailable,
record the failure, and require explicit rebuild or repair.

## Setup Job and CLI Contracts

### Request

```json
{
  "schema_version": 1,
  "strategy_id": "ftx-paper-orderflow",
  "task_id": "prior-session-value",
  "mode": "incremental",
  "through_session": "2026-11-01",
  "rebuild": false,
  "requested_by": "scheduler"
}
```

Supported modes are `catch_up`, `incremental`, and `rebuild`. Research rebuilds
use `mode: "rebuild"` with an isolated `output_root`; they must not replace
production pointers. `--mode catch-up` and `--mode research` are not accepted
spellings.

### Result

```json
{
  "schema_version": 1,
  "job_id": "setup-...",
  "status": "completed",
  "strategy_id": "ftx-paper-orderflow",
  "tasks": {
    "prior-session-value": {
      "processed_sessions": 120,
      "materialized": 3,
      "reused": 117,
      "unavailable": 0,
      "failed": 0
    }
  },
  "through_session": "2026-11-01"
}
```

Job statuses are `queued`, `running`, `completed`, `partial`, `failed`, and
`cancelled`.

### API

The API should expose generic setup operations:

```text
POST /api/v1/setup-runs
GET  /api/v1/setup-runs/{job_id}
POST /api/v1/setup-runs/{job_id}/cancel
GET  /api/v1/strategies/{strategy_id}/setup-artifacts/{task_id}/{instrument}/{session_date}?config_fingerprint=...
```

### CLI

The CLI is a thin loopback API client, analogous to `ftx-paper-replay`:

```text
ftx-paper-setup --strategy-id ftx-paper-orderflow \
  --task-id prior-session-value \
  --mode catch_up \
  --through 2026-11-01
```

The scheduler normally invokes:

```text
ftx-paper-setup --mode incremental
```

The CLI must not instantiate `RuntimeSession`, a broker, or a second replay
engine.

The worker persists job state transitions before and after each session. An
artifact publish is idempotent: an immutable artifact is validated before the
catalog pointer changes. If artifact publication succeeds but job persistence
fails, the next run re-reads and validates the artifact, then repairs the job
state without recomputing it. Cancellation stops between sessions and never
removes a previously valid catalog entry.

## Scheduling and Ownership

The operating-system scheduler should invoke the loopback setup CLI after the
configured market-data finalization deadline and before the next session open.
The CLI submits work to the API-owned setup worker and polls its status.

There is no separate market process and no second runtime owner.

Setup job metadata may use the runtime store's operational persistence, but
derived feature payloads remain filesystem JSON artifacts. The worker must not
mutate runtime market bars, portfolio state, broker state, or execution state.

The API process lease continues to protect runtime-directory ownership. Setup
job idempotency is additionally keyed by task, scope, algorithm version, and
configuration identity.

## Replay Integration

The replay input pipeline should receive a derived-artifact reader. The normal
replay sequence becomes:

1. Load the requested market session.
2. Load the target session's setup artifact from the catalog using the task's
   instrument and configuration identity.
3. Use the artifact's declared source session set and selected contract
   identity to derive the expected normalized source-bar fingerprint, source
   session set, and bar count through the `SourceReader` port. The target
   session's replay bars alone are not sufficient when the source is the prior
   trading session.
4. Validate schema, contract, algorithm version, source/configuration identity,
   the derived source identity, and digest.
5. Reconstruct `ValuePublication`.
6. Inject the publication into `PaperFeatureAdapter`.
7. Run chronological evidence and decision replay.

Replay must not call `build_prior_values()` in the normal path and must not rebuild
static value profiles from historical rows.

The existing as-of rule remains active: if a publication's `available_at` is
later than the decision clock, evidence reports
`prior_value_not_yet_available`.

## Research Replay and Explicit Rebuilds

Research replay defaults to consuming the same validated production artifact
contract. If an artifact is absent or stale, research replay fails explicitly.

An operator may request an explicit rebuild:

```text
ftx-paper-setup --mode rebuild \
  --strategy-id ftx-paper-orderflow \
  --task-id prior-session-value \
  --from 2024-01-01 \
  --through 2026-11-01 \
  --rebuild \
  --output-root <isolated-directory>
```

Research output is isolated by root and algorithm/configuration identity. It is
never silently promoted to production artifacts.

## Migration from `build_prior_values()`

### Phase 1: Generic infrastructure

Add setup task contracts, registry, worker lifecycle, source reader, artifact
reader/writer ports, canonical serialization, validation, catalog handling,
quarantine, and atomic replacement.

### Phase 2: Orderflow task

Implement `prior-session-value` using canonical daily profile records and
preserve completeness, identity, fingerprints, scope, lineage, availability,
and unavailable reasons.

### Phase 3: Initial catch-up

Run one explicit historical catch-up and compare materialized publications
against the current `build_prior_values()` output and canonical replay artifacts.

### Phase 4: Dual-read validation

In non-production validation, read both paths and compare reconstructed
`ValuePublication` values. Differences are failures unless explained by an
approved canonical algorithm/version change.

### Phase 5: Replay cutover

Change replay to read artifacts and remove `build_prior_values()` from the normal
path. Keep it only behind an explicit migration/rebuild command temporarily.

### Phase 6: Remove legacy path

After coverage and parity are established, delete `build_prior_values()` and any
replay-only static value-profile reconstruction.

## Testing and Validation

Tests should cover:

- deterministic artifact serialization;
- `ValuePublication` round trips;
- available and unavailable publications;
- schema and contract rejection;
- source/config/algorithm invalidation;
- digest mismatch and quarantine;
- atomic catalog replacement;
- temporary-file recovery;
- catch-up ordering and latest-session reconciliation;
- idempotent reruns;
- interrupted job recovery;
- replay not invoking `build_prior_values()`;
- explicit research rebuild isolation;
- canonical parity for prior-session publication.

For Python implementation changes, run the relevant focused tests, Ruff, and
Pyright:

```powershell
.venv\Scripts\python.exe -m pytest tests/strategies tests/infrastructure
.venv\Scripts\ruff.exe check .
.venv\Scripts\pyright.exe <changed-python-files>
```

Relevant existing coverage includes:

- `tests/infrastructure/test_daily_profile_repository.py`;
- `tests/strategies/test_orderflow_replay_clock.py`;
- `tests/strategies/test_orderflow_features.py`;
- `tests/strategies/test_orderflow_phase_comparison.py`.

## Replay Performance Design

Daily setup materialization removes static prior-session profile work from the
replay path, but it does not by itself solve the full
`CanonicalInputProvider.from_market_bars()` cost. The provider must also avoid
rebuilding canonical evidence independently for every decision-clock prefix.

The clean runtime split is:

```text
setup materializer
    -> immutable daily setup artifacts

target session bars + validated setup artifact
    -> one-pass canonical session evidence stream
        -> orderflow replay
```

### Static setup artifacts

The setup worker owns features that are finalized once per session, including
prior-session value publication. Replay loads and validates these artifacts,
then injects reconstructed domain objects into the strategy feature adapter.
Replay must not call `build_prior_values()` or rebuild daily value profiles.

### Incremental session evidence

`CanonicalInputProvider` remains responsible for causal target-session input,
but its build operation must be incremental rather than prefix-rebuilding:

- initialize recursive state from validated setup artifacts and the explicitly
  required ATR warmup state;
- consume the selected target-session futures bars in chronological order;
- update ATR, VWAP, session extrema, location publications, and causal
  observations once per completed bar;
- emit one `CanonicalInputEnvelope` per decision clock;
- retain the same availability cutoffs, lineage, unavailable reasons, and
  source identities as the canonical producer.

The provider must not call `produce_evidence()` once for every growing prefix
of the session. If a producer cannot be advanced incrementally without
changing causal behavior, that producer should expose an explicit streaming
boundary or materialize a versioned session-evidence artifact; replay must not
silently approximate it by using the final snapshot.

### Ownership and validation

The setup worker owns static artifact production and persistence. The replay
input provider owns only target-session chronological evidence assembly. The
strategy transition owner remains the sole policy owner. Both paths use the
same domain contracts and provenance rules.

Performance changes are valid only when they preserve:

- the exact decision-clock publication order;
- as-of availability and observation timestamps;
- source and causal lineage;
- explicit unavailable evidence;
- normalized source and configuration fingerprints;
- parity with the canonical replay output.

The migration therefore has two independent cutovers: first replace historical
profile reconstruction with validated setup artifacts, then replace repeated
prefix evidence construction with the incremental session provider. Measuring
only the first cutover must not be treated as completion of replay
optimization.

## Remaining Decisions

- Define retention and garbage collection for old source/config/version
  artifacts.
- Decide whether missing required setup evidence blocks runtime startup or only
  the affected strategy.
- Decide whether setup job lifecycle rows belong in the existing runtime store
  or a separate operational store. The first implementation should use the
  existing operational store unless isolation is required by deployment.
- Begin with a deterministic ordered task list; add a dependency DAG only when
  a task requires it.
- Test Windows rename and flush behavior explicitly before production use.
