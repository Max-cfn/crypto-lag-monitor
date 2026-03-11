import logging
from typing import Optional

import aiohttp

import config
from lag_analyzer import LagMeasurement

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


async def log_lag_measurement(m: LagMeasurement) -> None:
    """Log a completed LagMeasurement to the LAG_EVENTS channel."""
    move = m.binance_move
    lag_str = f"{m.lag_ms} ms" if m.lag_ms is not None else "NO_REPRICE"
    color = 0x00FF00 if m.repriced and m.direction_match else (0xFFA500 if m.repriced else 0xFF0000)
    embed = {
        "title": "Lag Measurement",
        "color": color,
        "fields": [
            {"name": "Binance move", "value": f"{move.get('direction')} ${move.get('price_after', 0):,.2f} (Δ${move.get('delta_usd', 0):+.2f})", "inline": False},
            {"name": "Poly at move", "value": f"{m.polymarket_price_at_move:.4f}", "inline": True},
            {"name": "Poly after reprice", "value": f"{m.polymarket_price_after_reprice:.4f}" if m.polymarket_price_after_reprice is not None else "—", "inline": True},
            {"name": "Lag", "value": lag_str, "inline": True},
            {"name": "Direction match", "value": "✓" if m.direction_match else "✗", "inline": True},
            {"name": "ID", "value": f"`{m.id[:8]}`", "inline": True},
        ],
    }
    await _post(config.DISCORD_WEBHOOK_LAG_EVENTS, {"embeds": [embed]})


async def log_stats(report: dict) -> None:
    """Post rolling statistics (last hour) to the STATS channel."""
    n = report.get("nb_moves_total", 0)
    if n == 0:
        content = "**Stats (1h)** — no lag measurements yet."
    else:
        def _fmt(v: Optional[float]) -> str:
            return f"{v:.0f} ms" if v is not None else "—"

        content = (
            f"**Rolling lag stats (1h)** — {n} moves, "
            f"{report['nb_repriced']} repriced, {report['nb_no_reprice']} no-reprice\n"
            f"Median: `{_fmt(report['median_lag'])}` | "
            f"P25: `{_fmt(report['p25_lag'])}` | "
            f"P75: `{_fmt(report['p75_lag'])}` | "
            f"P95: `{_fmt(report['p95_lag'])}` | "
            f"Max: `{_fmt(report['max_lag'])}`\n"
            f"Avg Binance move: `${report['avg_move_size_usd']:.2f}`  |  "
            f"Avg Poly move: `{report['avg_polymarket_move_cents']:.2f} ¢`"
            if report.get("avg_move_size_usd") is not None else ""
        )
    await _post(config.DISCORD_WEBHOOK_STATS, {"content": content})


async def log_alert(message: str, color: int = 0xFF0000) -> None:
    """Post a high-priority alert to the ALERTS channel."""
    embed = {"title": "ALERT", "description": message, "color": color}
    await _post(config.DISCORD_WEBHOOK_ALERTS, {"embeds": [embed]})
