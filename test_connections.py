"""
test_connections.py — Pre-flight connectivity checks

Runs 5 independent checks and prints a GO / NO-GO summary:
  1. Binance WebSocket  — receive 5 trades
  2. Polymarket WebSocket — receive 3 price updates
  3. Discord webhooks  — send a test message to each of the 4 channels
  4. Gamma API         — fetch the active BTC 5-min market
  5. Summary           — overall GO/NO-GO verdict

Usage:
    python test_connections.py
"""

import asyncio
import datetime
import json
import sys
import time
from typing import Optional

import aiohttp
import websockets

# ---------------------------------------------------------------------------
# Load config (and .env) before anything else
# ---------------------------------------------------------------------------
try:
    import config
except Exception as exc:
    print(f"[FAIL] Could not import config: {exc}")
    sys.exit(1)

# ---------------------------------------------------------------------------
# ANSI colour helpers (graceful fallback on Windows without VT mode)
# ---------------------------------------------------------------------------
_GREEN  = "\033[92m"
_RED    = "\033[91m"
_YELLOW = "\033[93m"
_BOLD   = "\033[1m"
_RESET  = "\033[0m"

def _ok(msg: str)   -> str: return f"{_GREEN}  ✔  {_RESET}{msg}"
def _fail(msg: str) -> str: return f"{_RED}  ✘  {_RESET}{msg}"
def _warn(msg: str) -> str: return f"{_YELLOW}  ⚠  {_RESET}{msg}"
def _hdr(msg: str)  -> str: return f"\n{_BOLD}{msg}{_RESET}"


# ---------------------------------------------------------------------------
# 1. Binance WebSocket
# ---------------------------------------------------------------------------

async def check_binance() -> tuple[bool, str]:
    url = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    trades: list[dict] = []
    timeout = 15

    print(_hdr("1/5  Binance WebSocket"))
    print(f"     Connecting to {url} …")

    try:
        async with websockets.connect(url, ping_interval=10, open_timeout=10) as ws:
            deadline = time.monotonic() + timeout
            while len(trades) < 5 and time.monotonic() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=5)
                    msg = json.loads(raw)
                    trade = {
                        "symbol":         msg.get("s"),
                        "price":          float(msg["p"]),
                        "quantity":       float(msg["q"]),
                        "timestamp_ms":   int(msg["T"]),
                        "is_buyer_maker": bool(msg["m"]),
                    }
                    trades.append(trade)
                    print(f"     trade #{len(trades):>2}  price=${trade['price']:,.2f}  "
                          f"qty={trade['quantity']}  "
                          f"{'maker' if trade['is_buyer_maker'] else 'taker'}")
                except asyncio.TimeoutError:
                    continue
    except Exception as exc:
        return False, f"Connection error: {exc}"

    if len(trades) >= 5:
        return True, f"Received {len(trades)} trades successfully"
    return False, f"Only received {len(trades)}/5 trades within {timeout}s"


# ---------------------------------------------------------------------------
# 2. Polymarket WebSocket
# ---------------------------------------------------------------------------

async def check_polymarket(token_id: str) -> tuple[bool, str]:
    url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    updates: list[dict] = []
    timeout = 20

    print(_hdr("2/5  Polymarket WebSocket"))
    if not token_id:
        return False, "No token_id available (Gamma API check must pass first)"

    print(f"     Connecting to {url} …")
    print(f"     Subscribing to token_id={token_id}")

    try:
        async with websockets.connect(url, ping_interval=10, open_timeout=10) as ws:
            await ws.send(json.dumps({"assets_ids": [token_id], "type": "Market"}))
            deadline = time.monotonic() + timeout
            while len(updates) < 3 and time.monotonic() < deadline:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=5)
                    payload = json.loads(raw)
                    events = payload if isinstance(payload, list) else [payload]
                    for ev in events:
                        et = ev.get("event_type") or ev.get("type", "")
                        if et in ("book", "best_bid_ask", "price_change"):
                            bid, ask = _extract_bid_ask(ev)
                            if bid and ask:
                                mid = (bid + ask) / 2
                                updates.append({"event_type": et, "bid": bid, "ask": ask, "mid": mid})
                                print(f"     update #{len(updates)}  type={et:<16}  "
                                      f"bid={bid:.4f}  ask={ask:.4f}  mid={mid:.4f}")
                except asyncio.TimeoutError:
                    continue
    except Exception as exc:
        return False, f"Connection error: {exc}"

    if len(updates) >= 3:
        return True, f"Received {len(updates)} price updates successfully"
    if updates:
        return True, f"Received {len(updates)}/3 updates (market may be quiet) — treating as OK"
    return False, f"No price updates received within {timeout}s"


def _extract_bid_ask(event: dict) -> tuple[Optional[float], Optional[float]]:
    if "best_bid" in event and "best_ask" in event:
        try:
            return float(event["best_bid"]), float(event["best_ask"])
        except (ValueError, TypeError):
            pass
    bids = event.get("bids", [])
    asks = event.get("asks", [])
    if bids and asks:
        try:
            return max(float(b["price"]) for b in bids), min(float(a["price"]) for a in asks)
        except (KeyError, ValueError, TypeError):
            pass
    return None, None


# ---------------------------------------------------------------------------
# 3. Discord webhooks
# ---------------------------------------------------------------------------

async def check_discord() -> tuple[bool, str]:
    print(_hdr("3/5  Discord Webhooks"))

    channels = {
        "RAW_TICKS":  config.DISCORD_WEBHOOK_RAW_TICKS,
        "LAG_EVENTS": config.DISCORD_WEBHOOK_LAG_EVENTS,
        "STATS":      config.DISCORD_WEBHOOK_STATS,
        "ALERTS":     config.DISCORD_WEBHOOK_ALERTS,
    }

    now_str = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%H:%M:%S UTC")
    all_ok = True
    failed: list[str] = []

    async with aiohttp.ClientSession() as session:
        for name, url in channels.items():
            if not url or url == "https://discord.com/api/webhooks/xxx/yyy":
                print(_fail(f"{name:<12} — not configured (placeholder value)"))
                all_ok = False
                failed.append(name)
                continue

            payload = {
                "embeds": [{
                    "title": f"🔧 Connection Test — {name}",
                    "description": f"crypto-lag-monitor pre-flight check at {now_str}",
                    "color": 0x5865F2,
                }]
            }
            try:
                async with session.post(
                    url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=5),
                ) as resp:
                    if resp.status in (200, 204):
                        print(_ok(f"{name:<12} — HTTP {resp.status}"))
                    elif resp.status == 429:
                        body = await resp.json()
                        print(_warn(f"{name:<12} — 429 rate-limited "
                                    f"(retry_after={body.get('retry_after')}s) — webhook is valid"))
                    else:
                        body = await resp.text()
                        print(_fail(f"{name:<12} — HTTP {resp.status}: {body[:100]}"))
                        all_ok = False
                        failed.append(name)
            except Exception as exc:
                print(_fail(f"{name:<12} — {exc}"))
                all_ok = False
                failed.append(name)

    if all_ok:
        return True, "All 4 webhooks reachable"
    return False, f"Failed webhooks: {', '.join(failed)}"


# ---------------------------------------------------------------------------
# 4. Gamma API — active BTC 5-min market
# ---------------------------------------------------------------------------

async def check_gamma_api() -> tuple[bool, str, Optional[str]]:
    """Returns (ok, message, token_id)."""
    print(_hdr("4/5  Polymarket Gamma API — Active 5-min BTC Market"))

    url = "https://gamma-api.polymarket.com/markets"
    params = {"tag": "crypto", "active": "true", "limit": "200"}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                resp.raise_for_status()
                markets = await resp.json()
    except Exception as exc:
        return False, f"Gamma API error: {exc}", None

    print(f"     Fetched {len(markets)} active markets — filtering for BTC 5-min…")

    now = datetime.datetime.now(tz=datetime.timezone.utc)
    candidates = []

    for m in markets:
        question: str = m.get("question") or m.get("title") or ""
        ql = question.lower()
        if "bitcoin" not in ql and "btc" not in ql:
            continue
        if not (("5" in ql and "minute" in ql) or "5-minute" in ql or "5min" in ql):
            continue

        end_str = m.get("endDate") or m.get("end_date") or ""
        if not end_str:
            continue
        try:
            end_dt = datetime.datetime.fromisoformat(end_str.replace("Z", "+00:00"))
        except ValueError:
            continue
        if end_dt <= now:
            continue

        clob_ids = m.get("clobTokenIds") or []
        token_id = clob_ids[0] if clob_ids else (m.get("conditionId") or m.get("id") or "")
        if not token_id:
            continue
        candidates.append((end_dt, token_id, question))

    if not candidates:
        return False, "No active BTC 5-min market found", None

    candidates.sort(key=lambda x: x[0])
    end_dt, token_id, question = candidates[0]

    delta = end_dt - now
    m_left, s_left = divmod(int(delta.total_seconds()), 60)
    expiry_str = end_dt.strftime("%H:%M:%S UTC")

    print(_ok(f"Question : {question}"))
    print(_ok(f"token_id : {token_id}"))
    print(_ok(f"Expires  : {expiry_str}  (in {m_left}m {s_left}s)"))

    if len(candidates) > 1:
        print(f"     ({len(candidates) - 1} other candidate(s) also available)")

    return True, f"Market found — expires in {m_left}m {s_left}s", token_id


# ---------------------------------------------------------------------------
# 5. Summary
# ---------------------------------------------------------------------------

def _print_summary(results: list[tuple[str, bool, str]]) -> bool:
    print(_hdr("=" * 56))
    print(_hdr("  SUMMARY"))
    print(_hdr("=" * 56))
    all_ok = True
    for name, ok, msg in results:
        if ok:
            print(_ok(f"{name:<28} {msg}"))
        else:
            print(_fail(f"{name:<28} {msg}"))
            all_ok = False
    print()
    if all_ok:
        print(f"{_GREEN}{_BOLD}  ✔  ALL CHECKS PASSED — GO{_RESET}")
    else:
        print(f"{_RED}{_BOLD}  ✘  SOME CHECKS FAILED — NO-GO{_RESET}")
        print(f"{_YELLOW}     Fix the issues above, then re-run test_connections.py{_RESET}")
    print()
    return all_ok


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    print(f"{_BOLD}crypto-lag-monitor — pre-flight connectivity check{_RESET}")
    print("=" * 56)

    results: list[tuple[str, bool, str]] = []

    # 4 first — we need the token_id for the Polymarket WS check
    ok4, msg4, token_id = await check_gamma_api()
    results_gamma = ("Gamma API (5-min market)", ok4, msg4)

    # 1. Binance
    ok1, msg1 = await check_binance()
    results.append(("Binance WebSocket", ok1, msg1))

    # 2. Polymarket WS
    ok2, msg2 = await check_polymarket(token_id or "")
    results.append(("Polymarket WebSocket", ok2, msg2))

    # 3. Discord
    ok3, msg3 = await check_discord()
    results.append(("Discord Webhooks (4 ch)", ok3, msg3))

    # Append Gamma result in display order
    results.append(results_gamma)

    _print_summary(results)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nAborted.")
