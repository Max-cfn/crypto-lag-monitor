import logging
from typing import Optional

import aiohttp

import config
from lag_analyzer import LagEvent

logger = logging.getLogger(__name__)


async def _post(webhook_url: str, payload: dict) -> None:
    """Fire-and-forget POST to a Discord webhook."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(webhook_url, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status not in (200, 204):
                    body = await resp.text()
                    logger.warning(
                        "Discord webhook returned %s: %s", resp.status, body[:200]
                    )
    except Exception as exc:
        logger.error("Failed to post to Discord webhook: %s", exc)


# ------------------------------------------------------------------
# Channel-specific helpers
# ------------------------------------------------------------------

async def log_raw_tick(source: str, data: dict) -> None:
    """Log every raw tick to the RAW_TICKS channel (debug/verbose)."""
    content = (
        f"**[{source}]** "
        + " | ".join(f"{k}: `{v}`" for k, v in data.items())
    )
    await _post(config.DISCORD_WEBHOOK_RAW_TICKS, {"content": content})


async def log_lag_event(event: LagEvent) -> None:
    """Log a detected lag event to the LAG_EVENTS channel."""
    lag_str = f"{event.lag_ms} ms" if event.lag_ms is not None else "pending"
    embed = {
        "title": "Lag Event Detected",
        "color": 0xFFA500,
        "fields": [
            {"name": "Binance price", "value": f"${event.binance_price:,.2f}", "inline": True},
            {"name": "Move (USD)", "value": f"${event.binance_move_usd:,.2f}", "inline": True},
            {"name": "Lag", "value": lag_str, "inline": True},
            {
                "name": "Polymarket (before → after)",
                "value": (
                    f"{event.polymarket_price_before:.4f} → "
                    f"{event.polymarket_price_after:.4f}"
                    if event.polymarket_price_after is not None
                    else f"{event.polymarket_price_before:.4f} → ?"
                ),
                "inline": False,
            },
        ],
    }
    await _post(config.DISCORD_WEBHOOK_LAG_EVENTS, {"embeds": [embed]})


async def log_stats(stats: dict) -> None:
    """Post rolling statistics to the STATS channel."""
    if stats["count"] == 0:
        content = "**Stats** — no lag measurements yet."
    else:
        content = (
            f"**Rolling lag stats** (n={stats['count']})\n"
            f"Mean: `{stats['mean_ms']:.0f} ms` | "
            f"Median: `{stats['median_ms']:.0f} ms` | "
            f"P95: `{stats['p95_ms']:.0f} ms` | "
            f"P99: `{stats['p99_ms']:.0f} ms` | "
            f"Max: `{stats['max_ms']:.0f} ms`"
        )
    await _post(config.DISCORD_WEBHOOK_STATS, {"content": content})


async def log_alert(message: str, color: int = 0xFF0000) -> None:
    """Post a high-priority alert to the ALERTS channel."""
    embed = {"title": "ALERT", "description": message, "color": color}
    await _post(config.DISCORD_WEBHOOK_ALERTS, {"embeds": [embed]})
