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

Each layer contains `status`, `count`, `sha256`, and ordered `records`. A record
contains:

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

- Compare the manifest and Layer 1 before other layers.
- Compare each layer's status, count, and hash before fetching records.
- On a mismatch, inspect only that layer and stop at the first mismatch.
- Verdict precedence is `MISMATCHED`, `INCOMPLETE`, `UNAVAILABLE`, then
  `MATCHED`.
- A zero-row layer is incomplete unless coverage proves it was observed and
  empty.
- No numeric tolerance is used in this first version.
- Producer-local IDs and storage metadata are diagnostics, not identities.

Paper APIs expose persisted data only:

```text
GET /replay/{id}/parity/manifest
GET /replay/{id}/parity/{layer}?offset=&limit=
```

Legacy artifacts may remain readable, but they are comparable only after being
represented in this four-layer shape with equivalent scope and coverage. Readers
must not invent missing records or values.

## Non-goals

This version does not require producer-neutral parity for every feature,
interaction, intermediate gate, broker acknowledgement, network timestamp, or
diagnostic extension. It does not share implementation code or change replay and
trading behavior. Additional fields require a later contract revision.
