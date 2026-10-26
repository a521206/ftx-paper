# Changelog

## Unreleased

Changes intended for the next production release go here. Move them into a
dated version section when production is pushed.

## 0.2.1 - 2026-10-26

Minor IIFL XTS market-data fixes.

### Fixed

- Corrected XTS REST and WebSocket deployment paths for IIFL market-data sessions.
- Added typed XTS authentication and transport failures with one-time token refresh.
- Made startup backfill and runtime shutdown cancellation-safe for XTS sessions.

## 0.2.0 - 2026-10-24

This release records the changes brought in by the latest merge/pull, including
the strategy-rules documentation update.

### Added

- Multi-broker market-data support for IIFL XTS and Shoonya, alongside Zerodha.
- Broker-specific authentication, WebSocket feeds, historical backfill, and
  instrument/option-chain discovery.
- Runtime contract persistence keyed by exchange, underlying, role, and
  trading symbol.
- Deterministic replay and parity artifacts with causal decision traces.

### Changed

- Unified session boundaries and IST handling across live and replay paths.
- Shared the canonical futures rollover policy between live and replay.
- Hardened Zerodha contract persistence and startup backfill behavior.
- Preserved unavailable PCR values in parity artifacts instead of treating them
  as favorable inputs.
- Expanded strategy documentation for admission gates, signal exits, risk
  budgets, drawdown controls, and margin limits.

### Fixed

- Improved runtime SQLite schema and migration handling for contract keys,
  leases, market data, replay runs, and simulated order lifecycle state.
- Kept paper execution and runtime audit behavior explicit across broker feeds.

### Upgrade Notes

Existing runtime databases with the legacy `runtime_contracts` primary key must
run this one-time migration before starting the new build:

```powershell
.venv\Scripts\python.exe scripts\migrate_runtime_contracts_key.py <runtime-dir-or-db>
```
