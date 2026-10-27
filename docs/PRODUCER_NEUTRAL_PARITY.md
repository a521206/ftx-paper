# Minimal Producer-Neutral Parity Contract

Status: first-pass contract. This is the producer-neutral counterpart to the
canonical FTX parity contract. It keeps the initial comparison small and does
not require sharing strategy or runtime code.

## Four comparison layers

Both producers write one `parity_artifact.json` and compare these layers in
order:

1. **Run and scope** — contract, input, range, and coverage identity.
2. **Decision** — ordered causal candidates and outcomes.
3. **Execution and settlement** — locally published orders, fills, exits, and
   synthetic settlement facts.
4. **Portfolio and ledger** — ordered reservations and accounting mutations.

Intermediate observations, features, interactions, and policy details are
optional diagnostics in this version. They are not required shared records.

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
and input identity. `coverage` includes `complete` and `issues`. A missing or
unobserved scope is `INCOMPLETE`, not an empty match.

Each layer contains `count`, `sha256`, and ordered `records`. A record contains:

```text
kind, occurred_at, sequence, identity, values, unavailable
```

Timestamps are timezone-aware. `sequence` is contiguous, zero-based producer
order. `identity` is producer-neutral. Values compare exactly after JSON
normalization. Missing values are declared in `unavailable`; they are not
converted to zero, false, or an empty value.

## Layer definitions

### Layer 1: Run and scope

Compare contract ID/version, normalized scope, input identity, timezone, and
coverage before comparing records. A difference is a contract/scope result, not
a strategy result.

An opaque producer configuration payload and hash may be included as provenance.
Configuration is not part of record identity, and a hash alone does not prove
configuration parity.

### Layer 2: Decision

The minimum decision kinds are `candidate`, `accepted`, `rejected`, and `exit`.
Identity uses the producer-neutral date/minute and event kind, plus native
dimensions such as cell when applicable. Accepted and rejected records include
direction and quantity when applicable; rejected records include a reason.

Records are compared in producer sequence order. Extra, missing, reordered, or
changed records are mismatches. Feature and gate details remain optional values.

### Layer 3: Execution and settlement

The minimum kinds are `order_intent`, `fill`, `exit`, and
`synthetic_settlement`. Records link to the Layer 2 identity. Shared values are
instrument, side, quantity, price, timestamp, and status when available.
Unavailable broker acknowledgements or synthetic premiums remain unavailable and
must not create or change a decision.

### Layer 4: Portfolio and ledger

The minimum kinds are `reservation`, `fill`, `cancellation`, `exit_settlement`,
`daily_reset`, and `closeout`. Shared values are realized P&L, costs, open
margin/reservations, available capital, and directional exposure when published
by both producers. Each producer has one authoritative portfolio state.

## Comparison and API

- A producer artifact is evidence only; creating or validating one does not
  produce a parity verdict.
- Compare two producer artifacts, and compare the manifest and Layer 1 before
  other layers.
- Compare each layer's count and hash before fetching records.
- On a mismatch, inspect only that layer and stop at the first mismatch.
- Verdict precedence is `MISMATCHED`, `INCOMPLETE`, `UNAVAILABLE`, then
  `MATCHED`.
- A zero-row layer is incomplete unless coverage proves it was observed and
  empty.
- No numeric tolerance is used in this first version.
- Producer-local IDs and storage metadata are diagnostics, not identities.

`MATCHED` is reserved for the result of an explicit cross-producer comparison.
It must never be assigned to a producer's own artifact merely because its
records are internally valid or its coverage is complete.

Paper APIs expose persisted data only:

```text
GET /replay/{id}/parity/manifest
GET /replay/{id}/parity/{layer}?offset=&limit=
```

Legacy artifacts may remain readable, but they are comparable only after being
represented in this four-layer shape with equivalent scope and coverage. Readers
must not invent missing records or values.

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
   each decision-bearing candidate, acceptance, rejection, cancellation,
   invalidation, expiry, and exit in causal sequence. Repeated location
   observations and interaction closures are diagnostic values, not structural
   candidates or replacement decision records.

For `ftx-paper-orderflow`, the Paper producer must:

1. Dispatch explicitly to the orderflow strategy implementation.
2. Emit every decision-bearing candidate, accepted, rejected, and exit record
   in causal sequence order, including non-executable outcomes.
3. Preserve producer-neutral identity fields, decision timestamps, direction,
   cell, outcome, and rejection reason when available.
4. Preserve orderflow evidence and lifecycle details as diagnostic `values`,
   without replacing the shared decision record with one record per bar or
   promoting every location interaction to a `candidate`.
5. Keep execution and ledger facts in their respective layers; they must not be
   represented as additional decision records.

The parity workflow must compare in this order:

1. Normalize both artifacts into the same envelope and compare `run_scope`.
2. Compare decision counts, kinds, identities, timestamps, directions, cells,
   outcomes, and rejection reasons.
3. Compare accepted intents, reservations, fills, exits, and settlements.
4. Compare trade-level ledger mutations before daily closeout totals.

The first mismatching layer is the diagnostic boundary. A missing canonical
projection is an artifact-coverage problem; it must not be resolved by filtering
Paper records or weakening the comparator.

The parity artifact remains four-layer. P1-P10 diagnostic stages are not
parity layers and must not be mapped into, duplicated across, or substituted for
the four contract layers. A decision-count mismatch is a producer conformance
failure and must be fixed in Paper's replay/strategy path, not hidden by the
comparator or by filtering canonical records.

## Non-goals

This version does not require producer-neutral parity for every feature,
interaction, intermediate gate, broker acknowledgement, network timestamp, or
diagnostic extension. It does not share implementation code or change replay and
trading behavior. Additional fields require a later contract revision.
