# Minimal Producer-Neutral Parity Contract

Status: normative first-pass contract. This is the producer-neutral counterpart
to the canonical FTX parity contract. It keeps the comparison surface small and
does not require sharing strategy or runtime code. The implementation roadmap is
in `PRODUCER_NEUTRAL_PARITY_IMPLEMENTATION_PLAN.md`.

## Four comparison layers

Each producer owns its replay facts. A pure projection step converts those facts
into one `parity_artifact.json`; the comparator never reconstructs facts from
summaries, diagnostics, or producer-local storage. The artifacts compare these
layers in order:

1. **Run and scope** — contract, input, range, and coverage identity.
2. **Decision** — ordered causal candidates and outcomes.
3. **Execution and settlement** — locally published orders, fills, exits, and
   synthetic settlement facts.
4. **Portfolio and ledger** — ordered reservations and accounting mutations.

Intermediate observations, non-decision-bearing interactions, response updates,
gate predicates, and policy details are producer diagnostics in this version.
Decision-bearing lifecycle stages are shared records when they meet the Layer 2
rules; they must not be promoted merely because they are present in a producer
event stream.

Before artifact comparison, both producers must use an identical replay scope and
decision clock: source contract, requested sessions, timezone, session end,
vehicles, capital/strategy configuration, and inclusive/exclusive cutoff rules.
Scope and coverage differences are resolved before comparing decisions. A causal
trace may be used to find the first divergent phase, but it is diagnostic evidence
and is not an additional parity layer.

## Artifact and records

New artifacts use:

```text
artifact_type: "producer_parity"
schema_version: 2
contract: {id: "producer_replay_parity", version: 1}
producer: {name, version}
run_id
scope
coverage
layers
```

`scope` includes the requested/effective range, applicable partitions, timezone,
and input identity. `coverage` includes run-level issues and independent
per-layer coverage. A missing or unobserved scope is `INCOMPLETE`, not an empty
match.

Each layer contains `count`, `sha256`, status, and ordered `records`. Layer status
is one of `AVAILABLE`, `INCOMPLETE`, or `UNAVAILABLE`. An empty layer is
`AVAILABLE` only when the producer proves that the scoped layer was observed and
empty; otherwise it is `INCOMPLETE` or `UNAVAILABLE`.

A record contains:

```text
kind, occurred_at, sequence, identity, values, unavailable
```

Decision records additionally contain `stage`, `outcome`, and `reason` when
applicable. Execution and ledger records use their layer-specific event kinds
and may contain `causal_parent_identity`.

Timestamps are timezone-aware and normalized to UTC in the artifact. `sequence`
is contiguous, zero-based projection order. `identity` is a hash of a published,
producer-neutral identity basis; producer-local IDs cannot participate in it.
Values compare exactly after JSON normalization. Missing values are declared in
`unavailable`; they are not converted to zero, false, or an empty value.

Execution and ledger records include `causal_parent_identity` when a decision
parent exists. A producer-local decision or order ID remains in the producer's
detached replay facts only; it is never copied into a parity record.

## Layer definitions

### Layer 1: Run and scope

Compare contract ID/version, normalized scope, input identity, timezone, and
coverage before comparing records. A difference is a contract/scope result, not
a strategy result.

An opaque producer configuration payload and hash may be included as provenance.
Configuration is not part of record identity, and a hash alone does not prove
configuration parity.

### Layer 2: Decision

Every decision record uses `kind: "decision"` and a neutral `stage`:

```text
interaction, candidate, admission, trigger, exit
```

`outcome` carries the lifecycle result, such as `accepted`, `rejected`,
`cancelled`, `invalidated`, `expired`, `observed`, or `unavailable`. A producer
native event kind may remain in producer facts, but it must not replace the
neutral stage/outcome fields. Unavailable evidence is represented in
`unavailable` and coverage, not fabricated into a successful decision.

Identity uses normalized producer-neutral UTC date/minute, stage, outcome, event
kind, reason code, and shared dimensions such as session, cell, direction, and
vehicle when applicable. Decision records include direction, entry information,
and quantity when applicable, plus a terminal or rejection reason when available.

The projection includes every decision-bearing interaction, candidate, admission,
trigger, and exit record. Response lifecycle traces, repeated observations,
predicates, and reference replacements remain diagnostics only when they do not
produce a comparable decision record. Interaction closure must not be silently
collapsed into response invalidation.

Records are compared in producer sequence order. Extra, missing, reordered, or
changed records are mismatches. Only stable shared evidence belongs in comparable
`values`; producer extensions remain producer diagnostics.

### Layer 3: Execution and settlement

The execution kinds are `reservation`, `authorization`, `submission`,
`order_intent`, `fill`, `pending_exit`, `exit`, `exit_fill`, `cancellation`,
`settlement`, and `synthetic_settlement`. Records link to the Layer 2 identity
when a decision parent exists. Shared values are instrument, side, quantity,
price, timestamp, and status when available. A missing parent link is explicitly
unavailable and is a coverage issue, not a producer-local identity fallback.
Unavailable broker acknowledgements or synthetic premiums remain unavailable and
must not create or change a decision.

Diagnostic mode must publish zero executable orders, fills, reservations, exits,
and portfolio mutations. Trades or closed positions must never be derived from
policy decisions or lifecycle diagnostics; they require authoritative execution
and portfolio facts from the existing execution owner.

### Layer 4: Portfolio and ledger

The ledger kinds are `reservation`, `reservation_release`, `authorization`,
`fill`, `partial_fill`, `cancellation`, `exit_settlement`, `daily_reset`, and
`closeout`. Shared values are realized P&L, costs, open margin/reservations,
available capital, and directional exposure when published by both producers.
A daily closeout is an aggregate diagnostic and does not replace published
mutation-level records. Each producer has one authoritative portfolio state.

## Comparison and API

- A producer artifact is evidence only; creating or validating one does not
  produce a parity verdict.
- Compare two producer artifacts, and compare the manifest and Layer 1 before
  other layers.
- Validate the envelope and normalized scope before comparing layer status.
- Compare each layer's count and hash before fetching records.
- On a mismatch, inspect only that layer and stop at the first mismatch.
- Verdict precedence is `MISMATCHED`, `INCOMPLETE`, `UNAVAILABLE`, then
  `MATCHED`.
- A zero-row layer is incomplete unless coverage proves it was observed and
  empty.
- No numeric tolerance is used in this first version.
- Producer-local IDs and storage metadata are diagnostics, not identities.

Projection and comparison are separate responsibilities. A producer adapter may
reject insufficient facts or mark them unavailable, but it must not infer a
record from a summary counter, substitute a local ID for a neutral identity, or
filter a producer's records to force a match. Same-producer regression comparison
is a useful diagnostic, but `MATCHED` in this contract remains reserved for an
explicit comparison of distinct producer identities.

`MATCHED` is reserved for the result of an explicit cross-producer comparison.
It must never be assigned to a producer's own artifact merely because its
records are internally valid or its coverage is complete.

Paper APIs expose persisted data only:

```text
GET /api/v1/replay/{id}/parity/manifest
GET /api/v1/replay/{id}/parity/{layer}?offset=&limit=
```

Artifacts with an unsupported schema or contract version are not parity inputs.
They must be migrated offline into a new versioned artifact using producer facts;
readers must not infer records or values from an old summary shape.

## Orderflow Replay Conformance

The `ftx-paper-orderflow` replay must emit the same producer-neutral decision
population as the canonical orderflow replay for the requested scope. The
Paper strategy must not fall back to the generic VWAP candidate path.

The canonical research replay is not itself a parity artifact. It uses the
`orderflow_state_machine_multi_session_replay` envelope and declares
`research_only: true`; it has no `producer_parity` envelope or four comparable
layers. A canonical adapter must first project its recorded facts into this
contract. The adapter must not infer records from summary counters or invent
execution/ledger values that the canonical replay did not publish. Until that
projection exists, a comparison is `INCOMPLETE` at `run_scope`, not a decision
or execution mismatch.

The current Paper artifact builder is not the contract boundary yet. It combines
event filtering, identity construction, coverage calculation, and projection in
one runtime helper. The implementation plan separates those concerns and treats
the current replay counts below as baseline evidence, not a conformance result.

The latest Sep 29, 2026 Paper replay after lifecycle integration reported:

- Source futures bars: 375.
- Causal Paper bars: 375.
- Structural candidates: 31.
- Accepted entries, fills, exits, and closed trades: 14 each.
- Paper parity records: 4,680 decision, 98 execution/settlement, and 1 ledger.
- Canonical summary: 356 processed bars, 17 setup candidates, 4 filled entries,
  and 4 exits.

These results expose two producer conformance failures after normalization:

1. Run scope differs. The adapter and Paper must expose selected source rows,
   filtered rows, completed causal bars, gap/incomplete bars, and processed
   decision bars separately. `source_bar_count` must not be compared as
   `processed.bar_count`.
2. Decision and lifecycle populations differ. Paper must emit one record for
   each decision-bearing interaction, candidate, admission, trigger, and exit
   in causal sequence, with lifecycle outcomes such as rejection, cancellation,
   invalidation, or expiry. Repeated location observations remain diagnostic;
   decision-bearing interaction closures are structural interaction records.

For `ftx-paper-orderflow`, the Paper producer must:

1. Dispatch explicitly to the orderflow strategy implementation.
2. Emit every decision-bearing interaction, candidate, admission, trigger, and
   exit record in causal sequence order, including non-executable outcomes.
3. Preserve producer-neutral identity fields, normalized decision timestamps,
   stage, outcome, direction, cell, and rejection reason when available.
4. Preserve orderflow evidence and lifecycle details in producer facts, without
   replacing the shared decision record with one record per bar or promoting
   every location observation to a decision.
5. Keep execution and ledger facts in their respective layers; they must not be
   represented as additional decision records.

The parity workflow must compare in this order:

1. Normalize both artifacts into the same envelope and compare `run_scope`.
2. Compare decision counts, stages, outcomes, identities, timestamps, directions,
   cells, and rejection reasons.
3. Compare accepted intents, reservations, fills, exits, and settlements.
4. Compare trade-level ledger mutations before daily closeout totals.

The first mismatching layer is the diagnostic boundary. A missing canonical
projection is an artifact-coverage problem; it must not be resolved by filtering
Paper records or weakening the comparator.

The parity artifact remains four-layer. Producer diagnostic stages are not parity
layers and must not be mapped into, duplicated across, or substituted for the
four contract layers. A decision-count mismatch is a producer conformance
failure and must be fixed in Paper's replay/strategy path, not hidden by the
comparator or by filtering canonical records.

## Non-goals

This version does not require producer-neutral parity for every feature,
interaction, intermediate gate, broker acknowledgement, network timestamp, or
diagnostic extension. It does not share implementation code or change replay and
trading behavior. Additional fields require a later contract revision.
