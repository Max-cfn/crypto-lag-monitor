"""
discord_logger.py — DiscordLogger

Four dedicated webhook channels, each with an internal async queue and a
sender coroutine capped at 5 messages / second (200 ms minimum inter-message
gap).  When the queue is non-empty at the rate-limit boundary the logger warns
once so operators know messages are being delayed, not dropped.
"""

import asyncio
import datetime
import logging
from typing import Optional

import aiohttp

import config
from lag_analyzer import LagMeasurement

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Rate-limit config
# ---------------------------------------------------------------------------
_RATE_LIMIT_PER_S = 5                         # Discord allows ~30/min safely
_MIN_INTERVAL_S = 1.0 / _RATE_LIMIT_PER_S    # 0.200 s between sends
_QUEUE_WARN_DEPTH = 20                        # warn once when backlog reaches this


# ---------------------------------------------------------------------------
# Internal per-webhook sender
# ---------------------------------------------------------------------------

class _WebhookChannel:
    """Async queue + sender loop for one Discord webhook endpoint."""

    def __init__(self, url: str, name: str) -> None:
        self.url = url
        self.name = name
        self._queue: asyncio.Queue[dict] = asyncio.Queue()
        self._task: Optional[asyncio.Task] = None
        self._warned_backlog = False

    def start(self) -> None:
        """Spawn the background sender task (call once the event loop is running)."""
        self._task = asyncio.create_task(self._sender(), name=f"discord-{self.name}")

    def enqueue(self, payload: dict) -> None:
        """Non-blocking: add a payload to the send queue."""
        depth = self._queue.qsize()
        if depth >= _QUEUE_WARN_DEPTH and not self._warned_backlog:
            log.warning(
                "[Discord/%s] Rate-limit backlog reached %d messages — "
                "messages are delayed, not dropped.",
                self.name, depth,
            )
            self._warned_backlog = True
        elif depth < _QUEUE_WARN_DEPTH // 2:
            self._warned_backlog = False
        self._queue.put_nowait(payload)

    async def _sender(self) -> None:
        async with aiohttp.ClientSession() as session:
            while True:
                payload = await self._queue.get()
                await self._post(session, payload)
                self._queue.task_done()
                await asyncio.sleep(_MIN_INTERVAL_S)

    async def _post(self, session: aiohttp.ClientSession, payload: dict) -> None:
        try:
            async with session.post(
                self.url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=5),
            ) as resp:
                if resp.status == 429:
                    body = await resp.json()
                    retry_after = body.get("retry_after", 1.0)
                    log.warning(
                        "[Discord/%s] 429 Too Many Requests — "
                        "backing off %.2fs (Discord-side rate limit).",
                        self.name, retry_after,
                    )
                    await asyncio.sleep(float(retry_after))
                    # Re-queue and try again
                    self._queue.put_nowait(payload)
                elif resp.status not in (200, 204):
                    body = await resp.text()
                    log.warning(
                        "[Discord/%s] Unexpected status %s: %s",
                        self.name, resp.status, body[:200],
                    )
        except Exception as exc:
            log.error("[Discord/%s] POST failed: %s", self.name, exc)


# ---------------------------------------------------------------------------
# Public class
# ---------------------------------------------------------------------------

class DiscordLogger:
    """Four-channel Discord logger with per-webhook rate limiting.

    Call ``start()`` once inside an async context before logging anything.

    Channels
    --------
    raw_ticks  : every significant Binance move
    lag_events : completed lag measurements
    stats      : periodic stats reports
    alerts     : plain-text operational alerts
    """

    def __init__(self) -> None:
        self._raw   = _WebhookChannel(config.DISCORD_WEBHOOK_RAW_TICKS,  "raw_ticks")
        self._lag   = _WebhookChannel(config.DISCORD_WEBHOOK_LAG_EVENTS, "lag_events")
        self._stats = _WebhookChannel(config.DISCORD_WEBHOOK_STATS,      "stats")
        self._alert = _WebhookChannel(config.DISCORD_WEBHOOK_ALERTS,     "alerts")
        self._channels = [self._raw, self._lag, self._stats, self._alert]

    def start(self) -> None:
        """Start all four sender tasks. Must be called from a running event loop."""
        for ch in self._channels:
            ch.start()

    # -----------------------------------------------------------------------
    # 1. RAW_TICKS — significant Binance move
    # -----------------------------------------------------------------------

    def log_binance_move(self, move: dict) -> None:
        """Enqueue a BTC move embed to the RAW_TICKS channel."""
        direction = move.get("direction", "?")
        delta_usd = move.get("delta_usd", 0.0)
        delta_pct = move.get("delta_pct", 0.0)
        price_before = move.get("price_before", 0.0)
        price_after = move.get("price_after", 0.0)
        ts_ms = move.get("timestamp_ms", 0)

        color = 0x2ECC71 if direction == "UP" else 0xE74C3C   # green / red
        time_str = _fmt_time(ts_ms)

        embed = {
            "title": "📊 BTC Move Detected",
            "color": color,
            "fields": [
                {"name": "Direction", "value": f"**{direction}**", "inline": True},
                {
                    "name": "Delta",
                    "value": f"`{'+' if delta_usd >= 0 else ''}{delta_usd:,.2f}` "
                             f"({'+' if delta_pct >= 0 else ''}{delta_pct:.3f}%)",
                    "inline": True,
                },
                {
                    "name": "Price",
                    "value": f"`${price_before:,.0f}` → `${price_after:,.0f}`",
                    "inline": False,
                },
                {"name": "Time", "value": f"`{time_str} UTC`", "inline": True},
            ],
        }
        self._raw.enqueue({"embeds": [embed]})

    # -----------------------------------------------------------------------
    # 2. LAG_EVENTS — completed lag measurement
    # -----------------------------------------------------------------------

    def log_lag_measurement(self, m: LagMeasurement) -> None:
        """Enqueue a lag measurement embed to the LAG_EVENTS channel."""
        move = m.binance_move

        if not m.repriced:
            title = "❓ No Reprice (30s)"
            color = 0x95A5A6   # grey
        else:
            title = "⏱️ Lag Measured"
            if m.lag_ms is not None and m.lag_ms < 1_000:
                color = 0x2ECC71   # green  < 1 s
            elif m.lag_ms is not None and m.lag_ms < 3_000:
                color = 0xF39C12   # yellow 1–3 s
            else:
                color = 0xE74C3C   # red    > 3 s

        lag_str = f"`{m.lag_ms:,} ms`" if m.lag_ms is not None else "`—`"
        exploitable = m.repriced and m.direction_match and (m.lag_ms or 0) > 3_000

        poly_after_str = (
            f"`{m.polymarket_price_after_reprice:.4f}`"
            if m.polymarket_price_after_reprice is not None
            else "`—`"
        )

        embed = {
            "title": title,
            "color": color,
            "fields": [
                {
                    "name": "Binance Move",
                    "value": (
                        f"{move.get('direction')} "
                        f"`${move.get('price_after', 0):,.0f}` "
                        f"(Δ`{'+' if move.get('delta_usd', 0) >= 0 else ''}"
                        f"{move.get('delta_usd', 0):,.2f}`)"
                    ),
                    "inline": False,
                },
                {"name": "Lag",               "value": lag_str,                                  "inline": True},
                {"name": "Poly Before",        "value": f"`{m.polymarket_price_at_move:.4f}`",   "inline": True},
                {"name": "Poly After",         "value": poly_after_str,                           "inline": True},
                {"name": "Direction Match",    "value": "✅" if m.direction_match else "❌",      "inline": True},
                {"name": "Exploitable",        "value": "✅" if exploitable else "—",             "inline": True},
                {"name": "ID",                 "value": f"`{m.id[:8]}`",                          "inline": True},
            ],
        }
        self._lag.enqueue({"embeds": [embed]})

    # -----------------------------------------------------------------------
    # 3. STATS — periodic report
    # -----------------------------------------------------------------------

    def log_stats_report(self, report: dict) -> None:
        """Enqueue a stats report embed to the STATS channel."""
        n = report.get("nb_moves_total", 0)
        if n == 0:
            embed = {
                "title": "📈 Lag Stats Report — 5min",
                "color": 0x95A5A6,
                "description": "No lag measurements yet.",
            }
            self._stats.enqueue({"embeds": [embed]})
            return

        repriced = report.get("nb_repriced", 0)
        no_reprice = report.get("nb_no_reprice", 0)
        median = report.get("median_lag")
        p25    = report.get("p25_lag")
        p75    = report.get("p75_lag")
        p95    = report.get("p95_lag")
        max_l  = report.get("max_lag")
        avg_move = report.get("avg_move_size_usd")

        # Edge label
        if median is None:
            edge_str = "—"
        elif median > 3_000:
            edge_str = "🔴 YES"
        elif median > 2_000:
            edge_str = "⚠️ BORDERLINE"
        else:
            edge_str = "🟢 NO"

        color = 0xE74C3C if (median or 0) > 3_000 else (
            0xF39C12 if (median or 0) > 2_000 else 0x2ECC71
        )

        embed = {
            "title": "📈 Lag Stats Report — 5min",
            "color": color,
            "fields": [
                {"name": "Moves detected",   "value": f"`{n}`",                              "inline": True},
                {"name": "Repriced",         "value": f"`{repriced}/{n}`",                   "inline": True},
                {"name": "No reprice",       "value": f"`{no_reprice}`",                     "inline": True},
                {"name": "Median lag",       "value": _fmt_ms(median),                       "inline": True},
                {"name": "P25 / P75",        "value": f"{_fmt_ms(p25)} / {_fmt_ms(p75)}",   "inline": True},
                {"name": "P95",              "value": _fmt_ms(p95),                          "inline": True},
                {"name": "Max lag",          "value": _fmt_ms(max_l),                        "inline": True},
                {"name": "Edge exploitable", "value": edge_str,                              "inline": True},
                {"name": "Avg move",         "value": f"`${avg_move:.1f}`" if avg_move else "—", "inline": True},
            ],
        }
        self._stats.enqueue({"embeds": [embed]})

    # -----------------------------------------------------------------------
    # 4. ALERTS — plain-text operational messages
    # -----------------------------------------------------------------------

    def log_alert(self, message: str) -> None:
        """Enqueue a plain-text alert to the ALERTS channel."""
        self._alert.enqueue({"content": message})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_ms(v: Optional[float]) -> str:
    return f"`{v:,.0f} ms`" if v is not None else "`—`"


def _fmt_time(ts_ms: int) -> str:
    """Format a millisecond timestamp as HH:MM:SS.mmm UTC."""
    if not ts_ms:
        return "—"
    dt = datetime.datetime.fromtimestamp(ts_ms / 1000, tz=datetime.timezone.utc)
    return dt.strftime("%H:%M:%S.") + f"{dt.microsecond // 1000:03d}"
