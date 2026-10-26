# Fresh Trade Engine - Architecture & Operational Guide

A high-performance, fault-proof algorithmic trading engine with a component-based microservice architecture running locally on a shared SQLite WAL database.

---

## 🏗️ System Architecture

```mermaid
graph TD
    subgraph UI ["Front End (Modern Web Terminal)"]
        DASH[Dashboard: Ticker, Client Cards, Strategy Lists]
        ADMIN_STRAT[Admin Strategies Repository]
        STATUS_PAGE[Status & 6-Segment Diagnostics]
        LOGS_PAGE[Logs & Events: 3-Part Views & Error Banner]
    end

    subgraph Core ["Local IPC & Shared Database"]
        DB[(Shared SQLite WAL Database)]
        BUS[Local Low-Latency Event Bus]
        MAPPER[Universal Symbol Mapper & Expiry Auto-Roll]
    end

    subgraph Microservices ["Component Microservices (Same Codebase / Zero Network Latency)"]
        FEED[Phase 1: Feed Manager Microservice<br/>Priority: XTS > SHOONYA > FYERS<br/>Segments: NSE, NFO, BSE, BSEFO, MCX, MCXFO]
        ORDER[Phase 2: Broker Order Manager<br/>XTS, KotakNeo, Shoonya, Zerodha, Dhan, Fyers<br/>Parallel Multi-Client Dispatch & Reconciler]
        STRAT[Phase 3: Strategy Manager Supervisor<br/>Isolated OS Process Containers<br/>Multi-Leg Tracking & Strategy Duplication]
    end

    FEED -->|Ticks / OHLC| BUS
    BUS -->|Real-Time Ticks| STRAT
    STRAT -->|Order Signals| ORDER
    ORDER -->|Reconciliation Reports| BUS
    BUS -->|WebSocket /ws/live| UI
    FEED <--> DB
    ORDER <--> DB
    STRAT <--> DB
    MAPPER <--> DB
```

---

## 📦 Component Breakdown

### 1. Phase 1: Feed Manager (Microservice)
- **Supported Brokers:** IIFL XTS, Shoonya (Finvasia), Fyers.
- **Priority Failover:** Automatic switchover based on priority list (`["XTS", "SHOONYA", "FYERS"]`). If the primary feed experiences heartbeat timeout or socket disconnect, it seamlessly switches to the standby broker and restores all subscriptions.
- **Supported Segments:** `NSE`, `NFO`, `BSE`, `BSEFO`, `MCX`, `MCXFO`.
- **Default Subscriptions:**
  - `NSE:NIFTY` (NIFTY 50)
  - `NSE:BANKNIFTY` (BANK NIFTY)
  - `BSE:SENSEX` (SENSEX)
  - `MCX:CRUDEOIL` (Current Month Future)
  - `MCX:NATURALGAS` (Current Month Future)
  - `MCX:GOLD` (Current Month Future)
  - `MCX:SILVER` (Current Month Future)
- **Automatic Expiry Auto-Roll:** The Expiry Engine detects when commodity futures reach expiration and automatically rolls the contract to the next active month on the following day.
- **Streaming OHLC Aggregator:** Real-time in-memory candle builder for `1m`, `5m`, `15m`, and `1h` resolutions.

Data Save

### 2. Phase 2: Broker Order Manager (Independent Module)
- **Supported Brokers:** XTS, Kotak Neo, Shoonya, Zerodha, Dhan, Fyers.
- **Universal Symbol Mapping:** Unified canonical token resolver (`NSE:NIFTY`, `NFO:...`, `MCX:...`) mapping to broker-specific trading symbols and exchange tokens across all 6 brokers.
- **Parallel Multi-Client Routing:** When a single strategy generates a signal mapped to multiple client accounts, orders are fired concurrently with zero head-of-line blocking.
- **Paper vs. Live Execution Mode:** Per-account mode switch with simulation of fills, slippage, and margins.
- **Order Reconciliation Engine:** Processes WebSocket and Webhook execution reports, syncing positions and trade records.
- **Prominent Emergency Square-Off:** Single-click square-off available at both Account level and Strategy level.

### 3. Phase 3: Strategy Manager (Independent Module) Core Module
- **Process Container Isolation:** Each strategy instance runs inside an isolated OS process (`multiprocessing.Process`). A fault, unhandled exception, or code modification in one strategy will **never** disturb another strategy or crash the engine.
- **Multi-Leg Price Tracking:** Tracks entry, exit, and real-time PnL across all legs (e.g. 0DTE Short Straddles with Call & Put legs).
- **Strategy Duplication:** Clones tested strategy templates with customized parameters or symbols while maintaining separate runtime state.
- **Chronological Decision Trail:**
  1. `Candle Fetch Success`
  2. `ENTRY CONDITION MATCH / NOT MATCH`
  3. `ORDER SENT TO BROKER`
  4. `ORDER FILLED / FAILED`
  5. `TRADE ACTIVE`
  6. `TRACKING SL / TARGET`

---

## 🖥️ Web Trading Dashboard Features

1. **Header:**
   - Consolidated PnL Widget: **U** (Unrealised), **R** (Running/Realised), **T** (Total).
   - Role Switcher: **ADMIN** (displays raw keys, internal names, and slugs) vs **USER** (displays clean strategy names without slugs).
   - Theme Switcher: Seamless Dark and Light theme toggle.
   - Live WebSocket pulse indicator.
2. **Streaming Ticker Row:**
   - Default instruments (`NIFTY`, `BANKNIFTY`, `SENSEX`, `CRUDEOIL`, `NATURALGAS`, `GOLD`, `SILVER`) with real-time green/red tick direction flashing.
3. **Client Cards Grid:**
   - Multi-broker account status, Paper/Live switcher, prominent **SQUARE OFF** button, and side drawer for:
     - Net & Day Positions
     - OrderBook
     - Executed Trades
     - Holdings
     - Margins & Balance Equity
4. **Strategy Lists Table:**
   - Client mapping, status, last trade state, start/end times, U/R/T PnL with multi-leg breakdown.
   - Action Menu: Enable/Disable, Square Off, Interactive Candlestick/Line Chart modal, Recent Trades modal, Decision Audit Timeline modal, Duplicate modal.
5. **Strategies Page (Admin Only):**
   - Catalog of registered strategy templates ready to deploy to client accounts.
6. **Status & Diagnostics Page:**
   - Feed Manager Health Card with latency, packets, and Manual Failover button.
   - **Diagnostic Test Cases Suite:**
     - Test 1: Symbol Subscription & OHLC fetch for 1 symbol across all 6 segments (`NSE`, `NFO`, `BSE`, `BSEFO`, `MCX`, `MCXFO`).
     - Test 2: Real-time order execution test across all 6 segments.
7. **Logs & Events Page:**
   - Clean, zero-noise interface with 3 tabs:
     - 1. Broker API Log (`ERROR` / `SUCCESS`)
     - 2. Feed Subscription (`ERROR` / `SUCCESS` / `DATA_RECEIVING`)
     - 3. Strategy Decision Log
   - **High-Visibility Prompt Error Banner:** Prominently highlights critical issues at the top of the page with a **DISMISS** action so errors are immediately resolved.
