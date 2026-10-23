# FTX Paper Research Strategy & System Rules

## 1. Overview & Identity

- **Strategy Name:** `ftx-paper-research`
- **Version:** `0.4.0-pipeline-research-aligned`
- **Target Instrument:** Nifty 50 Index Futures & Supporting Options
- **Primary Code Base:** [`src/ftx_paper/strategy/`](../src/ftx_paper/strategy/) and [`src/ftx_paper/market/location.py`](../src/ftx_paper/market/location.py)
- **Execution Mode:** Paper trading only (in-memory simulation with SQLite audit trail)

---

## 2. Core Concepts: Structural Locations & Cells

Rather than relying on lagging oscillators, the strategy evaluates price against **structural levels** calculated causally on completed 1-minute bars:

| Level Name | Description |
|---|---|
| `VWAP_ZONE` | Price trading within the volume-weighted average price band |
| `OR_HIGH` | High of the Opening Range (first 15/30 minutes) |
| `OR_LOW` | Low of the Opening Range (first 15/30 minutes) |
| `SESSION_HIGH` | Current day's running intraday high |
| `SESSION_LOW` | Current day's running intraday low |
| `PRIOR_DAY_HIGH` | Previous trading session's high |
| `PRIOR_DAY_LOW` | Previous trading session's low |

### Composite Cells
A **`Cell`** represents the intersection of multiple simultaneous location conditions (e.g., price is near VWAP, at the Opening Range Low, and near Prior Day High).

---

## 3. Session Windows & Timing Rules

Entries are only permitted during two specific intraday regimes (IST):

1. **Morning Session Window:**
   - **Range:** Minutes `0` to `120` from market open (**09:15 AM to 11:15 AM IST**).
   - Captures opening impulse and initial structural reversals/continuations.
2. **Afternoon Session Window:**
   - **Range:** Minutes `240` to `300` from market open (**01:15 PM to 02:15 PM IST**).
   - Captures midday breakouts or mean reversion heading into European open overlap.
3. **Outside Windows / Blackout:**
   - No new positions are initiated between 11:15 AM and 01:15 PM, or after 02:15 PM.
   - Mandatory intraday square-off begins before market close (**15:15 to 15:25 IST**).

---

## 4. Entry Cell Policies

### A. Morning Session Policies (09:15 – 11:15 IST)

| Policy # | Composite Location Cell | Direction | Trade Hypothesis | Exit Mode |
|:---:|---|:---:|---|:---:|
| **1** | `VWAP_ZONE` + `OR_LOW` + `PRIOR_DAY_HIGH` | **SELL** | Trend Continuation | `TRAIL` |
| **2** | `VWAP_ZONE` + `OR_HIGH` + `PRIOR_DAY_LOW` | **BUY** | Trend Continuation | `TRAIL` |
| **3** | `SESSION_HIGH` + `OR_HIGH` + `PRIOR_DAY_LOW` | **SELL** | Mean Reversion | `SIGNAL` |
| **4** | `VWAP_ZONE` + `SESSION_LOW` + `OR_LOW` + `PRIOR_DAY_LOW` | **SELL** | Trend Continuation | `TRAIL` |
| **5** | `VWAP_ZONE` + `SESSION_HIGH` + `PRIOR_DAY_LOW` | **BUY** | Trend Continuation | `SIGNAL` |

### B. Afternoon Session Policies (13:15 – 14:15 IST)

| Policy # | Composite Location Cell | Direction | Trade Hypothesis | Exit Mode |
|:---:|---|:---:|---|:---:|
| **1** | `VWAP_ZONE` + `OR_LOW` + `PRIOR_DAY_HIGH` | **SELL** | Trend Continuation | `TRAIL` |
| **2** | `VWAP_ZONE` + `PRIOR_DAY_HIGH` | **SELL** | Mean Reversion | `TRAIL` |
| **3** | `OR_LOW` + `PRIOR_DAY_LOW` | **BUY** | Mean Reversion | `TRAIL` |

---

## 5. Admission, Momentum & Supporting Filters

Before a signal triggers an order, it must pass checks in [`admission.py`](../src/ftx_paper/strategy/admission.py):

- **Location Stability & Transition:** Validates whether the price action at the level is stable (`stability > 0`) or an unstable false spike.
- **Option Volume PCR (Put-Call Ratio):** Examines the option chain sentiment up to the canonical 10:00 AM cutoff.
- **India VIX Volatility:** Compares event volatility to day-open volatility to prevent entries during dislocated shock events.
- **Post-Exit Cooldown:** Mandatory **2-bar cooldown** (`POST_EXIT_COOLDOWN_BARS = 2`) after an exit before entering a new trade on that instrument.

---

## 6. Exit Mechanics & Stop Logic

Defined in [`exits.py`](../src/ftx_paper/strategy/exits.py) and [`adaptive_stop.py`](../src/ftx_paper/strategy/adaptive_stop.py):

### 1. Trailing Stop (`ExitMode.TRAIL`)
- **Activation Threshold:** Activates when unrealized gain reaches **`+20.0 bps`** (0.20%).
- **Trailing Distance:** Trails peak favorable price by **`20.0 bps`**.
- Locks in profits as the trend extends.

### 2. Signal Exit (`ExitMode.SIGNAL`)
- Monitors the structural location state.
- Exits immediately when the qualifying cell dissolves, breaks down, or reaches the opposing structural boundary.

### 3. Adaptive Stop Loss
- Evaluates real-time ATR / volatility bounds to set a defensive initial stop price upon entry.

---

## 7. Sizing & Risk Management

Defined in [`risk.py`](../src/ftx_paper/strategy/risk.py) and [`sizing.py`](../src/ftx_paper/strategy/sizing.py):

- **Lot Sizing:** Dynamically computed according to volatility and distance to adaptive stop, subject to maximum position boundaries.
- **Maximum Directional Exposure:** Capped at **12 lots** net position.
- **Daily Loss Guardrail:** If cumulative daily drawdown reaches **5%** of initial capital, trading halts for the remainder of the session.
- **Intraday Square-Off:** No open positions carried overnight; positions are liquidated prior to market close.

---

## 8. Source File Map

- [`src/ftx_paper/strategy/config.py`](../src/ftx_paper/strategy/config.py): Session constants, cell policy declarations, parameters.
- [`src/ftx_paper/strategy/decision.py`](../src/ftx_paper/strategy/decision.py): Main bar-by-bar decision loop and order intent generator.
- [`src/ftx_paper/strategy/admission.py`](../src/ftx_paper/strategy/admission.py): Quality scoring and gate admission.
- [`src/ftx_paper/strategy/exits.py`](../src/ftx_paper/strategy/exits.py): Trailing stop and signal exit evaluations.
- [`src/ftx_paper/strategy/adaptive_stop.py`](../src/ftx_paper/strategy/adaptive_stop.py): Dynamic stop-loss calculator.
- [`src/ftx_paper/strategy/risk.py`](../src/ftx_paper/strategy/risk.py): Hard guardrails and portfolio boundaries.
- [`src/ftx_paper/market/location.py`](../src/ftx_paper/market/location.py): Price level classification, OR calculation, and cell detection.
