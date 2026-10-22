# FTX Paper Research Strategy & System Rules

## 1. Overview & Identity

- **Strategy Name:** `ftx-paper-research`
- **Version:** `0.4.0-pipeline-research-aligned`
- **Target Instrument:** Nifty 50 Index Futures; options provide supporting PCR data.
- **Execution Mode:** Paper trading only (in-memory simulation with SQLite audit trail)

---

## 2. Core Concepts: Structural Locations & Cells

The strategy evaluates price against structural levels calculated causally from completed 1-minute bars:

| Level Name | Description |
|---|---|
| `VWAP_ZONE` | Price trading within 0.25% of the volume-weighted average price |
| `OR_HIGH` | High of the first 15 completed session bars |
| `OR_LOW` | Low of the first 15 completed session bars |
| `SESSION_HIGH` | Price in the top 10% of the current day's running intraday range |
| `SESSION_LOW` | Price in the bottom 10% of the current day's running intraday range |
| `PRIOR_DAY_HIGH` | Previous trading session's high; proximity is within 0.25 ATR |
| `PRIOR_DAY_LOW` | Previous trading session's low; proximity is within 0.25 ATR |

### Composite Cells
A **`Cell`** represents the intersection of multiple simultaneous location conditions (e.g., price is near VWAP, at the Opening Range Low, and near Prior Day High). Implied locations may be normalized out of a cell.

---

## 3. Session Windows & Timing Rules

Entries are only permitted during two intraday regimes (IST), using half-open minute ranges:

1. **Morning Session Window:**
   - **Range:** Minutes `0` to `120` from market open (**09:15 AM to before 11:15 AM IST**).
2. **Afternoon Session Window:**
   - **Range:** Minutes `240` to `300` from market open (**01:15 PM to before 02:15 PM IST**).
3. **Outside Windows / Square-Off:**
   - No new positions are initiated outside the entry windows.
   - Open positions are exited at or after **15:10 IST**; no overnight positions are intended.

---

## 4. Entry Cell Policies

### A. Morning Session Policies (09:15 – before 11:15 IST)

| Policy # | Composite Location Cell | Direction | Trade Hypothesis | Exit Mode |
|:---:|---|:---:|---|:---:|
| **1** | `VWAP_ZONE` + `OR_LOW` + `PRIOR_DAY_HIGH` | **SELL** | Trend Continuation | `TRAIL` |
| **2** | `VWAP_ZONE` + `OR_HIGH` + `PRIOR_DAY_LOW` | **BUY** | Trend Continuation | `TRAIL` |
| **3** | `SESSION_HIGH` + `OR_HIGH` + `PRIOR_DAY_LOW` | **SELL** | Mean Reversion | `SIGNAL` |
| **4** | `VWAP_ZONE` + `SESSION_LOW` + `OR_LOW` + `PRIOR_DAY_LOW` | **SELL** | Trend Continuation | `TRAIL` |
| **5** | `VWAP_ZONE` + `SESSION_HIGH` + `PRIOR_DAY_LOW` | **BUY** | Trend Continuation | `SIGNAL` |

### B. Afternoon Session Policies (13:15 – before 14:15 IST)

| Policy # | Composite Location Cell | Direction | Trade Hypothesis | Exit Mode |
|:---:|---|:---:|---|:---:|
| **1** | `VWAP_ZONE` + `OR_LOW` + `PRIOR_DAY_HIGH` | **SELL** | Trend Continuation | `TRAIL` |
| **2** | `VWAP_ZONE` + `PRIOR_DAY_HIGH` | **SELL** | Mean Reversion | `TRAIL` |
| **3** | `OR_LOW` + `PRIOR_DAY_LOW` | **BUY** | Mean Reversion | `TRAIL` |

---

## 5. Admission, Momentum & Supporting Filters

Admission uses hypothesis-specific price, volume, PCR, and VIX conditions. Trend-continuation policies additionally require at least two confirmations from volume climax, prior-bar midpoint reclaim, and volume drying. PCR is derived from same-minute option data when eligible inputs are available; VIX is evaluated against its day-open value. A 2-bar per-cell cooldown applies after exit before re-entry.

---

## 6. Exit Mechanics & Stop Logic

### 1. Trailing Stop (`ExitMode.TRAIL`)
- **Activation Threshold:** Activates when unrealized gain reaches **`+20.0 bps`** (0.20%).
- **Trailing Distance:** Uses a **20.0 bps** distance relative to entry and the peak favorable price.

### 2. Signal Exit (`ExitMode.SIGNAL`)
- Uses price and volume reversal signals after a favorable move; it is not based on cell dissolution or reaching an opposing structural boundary.

### 3. Adaptive Stop Loss
- Sets an initial defensive stop from ATR and VIX, bounded to **15–50 bps** (widened on expiry days).

---

## 7. Sizing & Risk Management

- **Lot Sizing:** Dynamically computed according to risk budget, volatility, and distance to the adaptive stop, subject to margin and position limits.
- **Per-Order Maximum:** **8 lots** under the default research capital profile.
- **Maximum Directional Exposure:** Capped at **12 lots** net position under the default research capital profile.
- **Daily Loss Guardrail:** If equity falls below 95% of the daily baseline, new entries are halted for the remainder of the session.
- **Intraday Square-Off:** Open positions exit at or after **15:10 IST**; positions are not intended to carry overnight.

---

## 8. Source File Map

- [`src/ftx_paper/strategy/config.py`](../src/ftx_paper/strategy/config.py): Session constants, cell policy declarations, parameters.
- [`src/ftx_paper/strategy/decision.py`](../src/ftx_paper/strategy/decision.py): Main bar-by-bar decision loop and order intent generator.
- [`src/ftx_paper/strategy/admission.py`](../src/ftx_paper/strategy/admission.py): Admission predicates and hypothesis-specific gates.
- [`src/ftx_paper/strategy/exits.py`](../src/ftx_paper/strategy/exits.py): Trailing stop and signal exit evaluations.
- [`src/ftx_paper/strategy/adaptive_stop.py`](../src/ftx_paper/strategy/adaptive_stop.py): Dynamic stop-loss calculator.
- [`src/ftx_paper/strategy/risk.py`](../src/ftx_paper/strategy/risk.py): Risk sizing and position boundaries.
- [`src/ftx_paper/market/location.py`](../src/ftx_paper/market/location.py): Price level classification, OR calculation, and cell detection.
