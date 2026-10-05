# Paper Parity Artifact Schema v1

`ftx_paper.runtime.parity_artifacts` emits a deterministic JSON envelope with
`artifact_type`, `schema_version`, `producer`, `run_id`, `scope`, `strategy`,
`coverage`, `p9`, and `p10`. Strategy payload hashing uses sorted, compact JSON
and SHA-256 over strategy configuration only.
All emitted timestamps are timezone-aware UTC instants. P9 decisions and P10
execution, exits, synthetic settlements, and ledger facts are separate arrays.
Records use zero-based `stream_sequence` values. Coverage includes completeness,
issues, field availability, and unavailable fields; missing required data is
never represented as zero or omitted. Scope contains requested/effective dates,
sessions, vehicles, replay-input identity/provenance, and processed counts.
