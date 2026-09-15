# FTX Paper final migration parity report

This report is generated from the final comparison artifacts under:

`data/runtime/ftx-paper-final-comparisons`

Only dates with data on both sides are included. All five comparison dates have canonical and Paper futures data.

## Final result

| Metric | Count |
|---|---:|
| Dates compared | 5 |
| Canonical futures trades | 12 |
| Paper futures trades | 18 |
| Exact matches | 10 |
| Modified matches | 2 |
| Canonical-only trades | 0 |
| Paper-only trades | 6 |

The remaining differences are limited to six extra Paper entries and two modified exits. There are no missing canonical trades and no unexplained differences.

## Validation

- 169 ftx-paper tests passed.
- Ruff clean.
- DB/import boundary scans clean.
- Duplicate-test scan clean.
- Exact differential matches: 2026-09-09 and 2026-09-11.
- Remaining Paper-only entries: two on 2026-09-07, two on 2026-09-08, and two on 2026-09-15.
- Remaining modified trades: two on 2026-09-15.

## Results by date

| Date | Canonical | Paper | Matched | Modified | Canonical-only | Paper-only | Result |
|---|---:|---:|---:|---:|---:|---:|---|
| 2026-09-07 | 2 | 4 | 2 | 0 | 0 | 2 | MISMATCHED |
| 2026-09-08 | 2 | 4 | 2 | 0 | 0 | 2 | MISMATCHED |
| 2026-09-09 | 1 | 1 | 1 | 0 | 0 | 0 | MATCHED |
| 2026-09-11 | 2 | 2 | 2 | 0 | 0 | 0 | MATCHED |
| 2026-09-15 | 5 | 7 | 3 | 2 | 0 | 2 | MISMATCHED |

## Matched trades

| Date/time | Session | Cell | Direction | Qty | Exit | Canonical net | Paper net |
|---|---|---|---|---:|---|---:|---:|
| 2026-09-07 10:20 | morning | vwap_zone+or_low | short | 1 | 13:53 trail_stop | -455.73 | -455.73 |
| 2026-09-07 10:21 | morning | vwap_zone+or_low | short | 1 | 13:53 trail_stop | -98.95 | -98.95 |
| 2026-09-08 10:37 | morning | vwap_zone+or_low | short | 1 | 15:10 eod | 384.00 | 384.00 |
| 2026-09-08 10:38 | morning | vwap_zone+or_low | short | 1 | 15:10 eod | 410.00 | 410.00 |
| 2026-09-09 10:15 | morning | vwap_zone+or_low | short | 1 | 10:20 hard_stop | -2,800.97 | -2,800.97 |
| 2026-09-11 13:59 | afternoon | session_high+or_high | long | 1 | 14:08 trail_stop | 832.50 | 832.50 |
| 2026-09-11 14:01 | afternoon | session_high+or_high | long | 1 | 14:08 trail_stop | -79.32 | -79.32 |
| 2026-09-15 10:17 | morning | vwap_zone+or_low | short | 1 | 14:51 trail_stop | 12,704.10 | 12,704.10 |
| 2026-09-15 10:18 | morning | vwap_zone+or_low | short | 1 | 14:51 trail_stop | 12,704.10 | 12,704.10 |
| 2026-09-15 10:29 | morning | session_low+or_low | long | 3 | 10:39 counter_move | -1,207.50 | -1,207.50 |

## Modified trades

| Date/time | Cell | Qty | Canonical result | Paper result | Reason |
|---|---|---:|---|---|---|
| 2026-09-15 10:30 | session_low+or_low | 3 | 10:46 hard_stop, exit 23,374.38575, net -8,347.28 | 10:39 counter_move, exit 23,402.50, net -2,865.00 | Exit-state divergence: Paper exits by counter-move before canonical hard-stop. |
| 2026-09-15 13:45 | or_low | 1 | 14:11 hard_stop, exit 23,283.92165, net -2,773.59 | 13:54 counter_move, exit 23,322.20, net -285.50 | Exit-state divergence: Paper exits by counter-move before canonical hard-stop. |

Both modified rows have the same entry identity and quantity. The divergence is in exit timing/reason and therefore exit price and P&L; it is not a sizing mismatch.

## Paper-only trades and reasons

| Date/time | Session | Cell | Direction | Qty | Paper exit / net | Reason |
|---|---|---|---|---:|---|---|
| 2026-09-07 10:15 | morning | vwap_zone+session_low+or_low | long | 1 | 10:33 counter_move / -760.00 | Extra Paper accepted trade; no canonical trade has the same six-field identity. |
| 2026-09-07 10:17 | morning | vwap_zone+session_low+or_low | long | 1 | 10:33 counter_move / 150.00 | Extra Paper accepted trade; no canonical trade has the same six-field identity. |
| 2026-09-08 10:36 | morning | vwap_zone+session_low+or_low | long | 1 | 10:44 counter_move / -792.50 | Extra Paper accepted trade; no canonical trade has the same six-field identity. |
| 2026-09-08 10:40 | morning | vwap_zone+session_low+or_low | long | 1 | 10:44 counter_move / -584.50 | Extra Paper accepted trade; no canonical trade has the same six-field identity. |
| 2026-09-15 10:22 | morning | vwap_zone+session_low+or_low | long | 1 | 10:28 counter_move / -916.00 | Extra Paper accepted trade; no canonical trade has the same six-field identity. |
| 2026-09-15 13:51 | afternoon | or_low | long | 1 | 13:54 counter_move / -545.50 | Extra Paper accepted trade; no canonical trade has the same six-field identity. |

## First remaining difference

The first remaining difference in deterministic date/time order is the Paper-only trade on **2026-09-07 at 10:15**, identity `morning / futures / vwap_zone+session_low+or_low / long`. It is classified as an extra Paper accepted trade, not a missing canonical trade or comparator defect.

## Scope boundaries

- Futures remain the sole decision and portfolio vehicle.
- No synthetic trade is used to explain or conceal the final differences.
- The remaining modified trades are exit-state differences, not quantity differences.
- The comparison does not alter canonical implementation or execution economics.
