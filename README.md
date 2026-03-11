# crypto-lag-monitor

Monitors the lag between significant Bitcoin price moves on Binance and the repricing reaction on Polymarket 5-minute BTC prediction markets. When the median lag exceeds ~3 seconds there may be an exploitable edge.

---

## Installation

**Requirements:** Python 3.11+

```bash
pip install -r requirements.txt
```

---

## Configuration

Copy the example env file and fill in your values:

```bash
cp .env.example .env
```

Edit `.env`:

```env
# Discord — create a webhook in each channel (Server Settings → Integrations → Webhooks)
DISCORD_WEBHOOK_RAW_TICKS=https://discord.com/api/webhooks/...
DISCORD_WEBHOOK_LAG_EVENTS=https://discord.com/api/webhooks/...
DISCORD_WEBHOOK_STATS=https://discord.com/api/webhooks/...
DISCORD_WEBHOOK_ALERTS=https://discord.com/api/webhooks/...

# Polymarket — optional, auto-discovered at startup if left empty
POLYMARKET_MARKET_ID=

# Thresholds
LAG_THRESHOLD_MS=500          # minimum lag to flag in logs
BINANCE_MOVE_THRESHOLD_USD=5  # minimum BTC price move to trigger measurement
```

All four Discord webhooks are **required**. The monitor will refuse to start with a clear error if any are missing or still set to the placeholder value.

`POLYMARKET_MARKET_ID` is **optional**: if empty, the monitor auto-discovers the soonest-expiring active 5-min BTC market via the Polymarket Gamma API at startup and every 60 seconds thereafter.

---

## Pre-flight check

Before running the main monitor, verify all connections:

```bash
python test_connections.py
```

This script runs 5 checks and prints a GO / NO-GO summary:

| Check | What it verifies |
|---|---|
| Binance WebSocket | Connects and receives 5 live BTC trades |
| Polymarket WebSocket | Subscribes to the active 5-min market and receives 3 price updates |
| Discord Webhooks | Sends a test embed to each of the 4 channels |
| Gamma API | Fetches and displays the active 5-min BTC market (question, token_id, expiry) |

Example output:
```
crypto-lag-monitor — pre-flight connectivity check
========================================================
  ✔  Question : Will Bitcoin be above $X at 17:45 UTC?
  ✔  token_id : 0xabc...
  ✔  Expires  : 17:45:00 UTC  (in 3m 12s)
...
========================================================
  SUMMARY
========================================================
  ✔  Binance WebSocket         Received 5 trades successfully
  ✔  Polymarket WebSocket      Received 3 price updates successfully
  ✔  Discord Webhooks (4 ch)   All 4 webhooks reachable
  ✔  Gamma API (5-min market)  Market found — expires in 3m 12s

  ✔  ALL CHECKS PASSED — GO
```

---

## Running the monitor

```bash
python main.py
```

On startup the monitor:
1. Validates config (aborts with a clear error if webhooks are missing)
2. Fetches the active 5-min BTC Polymarket market via Gamma API
3. Posts `🚀 Starting lag monitor | Market: ... | Expires: ...` to ALERTS
4. Starts four concurrent tasks: Binance stream, Polymarket stream, stats reporter, market refresher

The market refresher checks every 60 seconds whether the current market has expired and automatically subscribes to the next one without restarting the process.

Stop with `Ctrl+C`. A final verdict is posted to ALERTS on shutdown.

---

## Discord channels

### `RAW_TICKS` — every significant Binance move

Posted whenever BTC moves more than `BINANCE_MOVE_THRESHOLD_USD` over the last 5 seconds.

```
📊 BTC Move Detected
Direction  UP
Delta      +$18.50 (+0.026%)
Price      $70,634 → $70,652
Time       17:42:33.421 UTC
```

Color: green for UP, red for DOWN.

---

### `LAG_EVENTS` — every completed lag measurement

Posted 0–30 seconds after each significant Binance move once Polymarket reacts (or times out).

```
⏱️ Lag Measured
Binance Move    UP $70,652 (Δ+$18.50)
Lag             2,340 ms
Poly Before     0.5120
Poly After      0.5340
Direction Match ✅
Exploitable     —
ID              a1b2c3d4
```

Color coding:
- 🟢 Green — lag < 1 000 ms (Polymarket fast)
- 🟡 Yellow — lag 1 000–3 000 ms (moderate)
- 🔴 Red — lag > 3 000 ms (potential edge)
- ⬜ Grey — `❓ No Reprice (30s)` — Polymarket did not react within 30 s

`Exploitable ✅` is shown when lag > 3 s **and** direction matches Binance.

---

### `STATS` — rolling report every 5 minutes

```
📈 Lag Stats Report — 5min
Moves detected   12
Repriced         10/12
No reprice       2
Median lag       1,847 ms
P25 / P75        1,200 ms / 2,900 ms
P95              4,100 ms
Max lag          6,340 ms
Edge exploitable ⚠️ BORDERLINE
Avg move         $18.4
```

Edge label:
- `🔴 YES` — median lag > 3 000 ms
- `⚠️ BORDERLINE` — median lag 2 000–3 000 ms
- `🟢 NO` — median lag < 2 000 ms

---

### `ALERTS` — operational events

Plain-text messages for connection events, market changes, errors, and the final verdict:

```
🚀 Starting lag monitor | Market: Will BTC be above $X? | Expires: 17:45:00 UTC
🔴 Binance WS disconnected, reconnecting (attempt 2)...
🟢 Polymarket WS reconnected
⚠️ Market expired, looking for new 5-min market...
🟢 New market subscribed | Will BTC be above $Y? | Expires: 17:50:00 UTC
🏁 Monitor stopped | VERDICT: Edge EXPLOITABLE ✅ | Median lag: 3420ms
```

---

## Interpreting results

| Median lag | Meaning |
|---|---|
| < 1 000 ms | Polymarket reprices quickly — no exploitable edge |
| 1 000–2 000 ms | Moderate lag — market is somewhat slow |
| 2 000–3 000 ms | Borderline — worth monitoring closely |
| > 3 000 ms | **Potential edge** — Polymarket is consistently slow to react to Binance moves |

**Direction match** matters: if Polymarket moves in the *opposite* direction after a Binance move, the lag is not directionally exploitable even if it is large.

**No reprice** events (Polymarket does not move within 30 s) indicate either very low market activity, that the market price is already fully reflecting information, or that the move was not considered significant by market makers.

---

## Architecture

```
main.py
├── BinanceTradeStream        (binance_ws.py)
│   └─ on_significant_move → LagAnalyzer.on_binance_move()
│                           → DiscordLogger.log_binance_move()
├── PolymarketPriceStream     (polymarket_ws.py)
│   └─ price_history / get_current_mid() polled by LagAnalyzer
├── LagAnalyzer               (lag_analyzer.py)
│   └─ background task: poll every 100ms up to 30s → LagMeasurement
│   └─ on_measurement → DiscordLogger.log_lag_measurement()
├── DiscordLogger             (discord_logger.py)
│   └─ 4 async queues, each capped at 5 msg/s
├── stats_reporter()          every 5 min → log_stats_report()
└── market_refresher()        every 60s → swap_token() if expired
```
