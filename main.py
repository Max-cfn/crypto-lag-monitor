"""
crypto-lag-monitor — entry point

Streams Binance trade ticks and Polymarket price updates concurrently,
detects lag between significant BTC price moves and Polymarket reactions,
and logs everything to Discord.
"""

import asyncio
import logging
import signal
import sys

import config
from lag_analyzer import LagAnalyzer, BinanceTick, PolymarketTick
from binance_ws import stream_binance_trades
from polymarket_ws import stream_polymarket_prices
import discord_logger

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# How often (seconds) to post rolling stats to Discord
STATS_INTERVAL_S = 300


async def run() -> None:
    config.validate_config()

    analyzer = LagAnalyzer()
    stop_event = asyncio.Event()

    # Graceful shutdown on SIGINT / SIGTERM
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    # ------------------------------------------------------------------
    # Callbacks
    # ------------------------------------------------------------------

    async def handle_binance_tick(tick: dict) -> None:
        bt = BinanceTick(
            price=tick["price"],
            timestamp_ms=tick["timestamp_ms"],
            received_ms=tick["received_ms"],
        )
        await discord_logger.log_raw_tick("Binance", tick)
        event = analyzer.on_binance_tick(bt)
        if event is not None:
            await discord_logger.log_lag_event(event)
            if analyzer.is_lag_event(event):
                await discord_logger.log_alert(
                    f"Lag threshold exceeded: {event.lag_ms} ms "
                    f"(threshold: {config.LAG_THRESHOLD_MS} ms)\n"
                    f"Binance move: ${event.binance_move_usd:.2f} → "
                    f"Polymarket {event.polymarket_price_before:.4f} → "
                    f"{event.polymarket_price_after}"
                )

    async def handle_polymarket_tick(tick: dict) -> None:
        pt = PolymarketTick(
            price=tick["price"],
            timestamp_ms=tick["timestamp_ms"],
            received_ms=tick["received_ms"],
            outcome=tick.get("outcome", "YES"),
        )
        await discord_logger.log_raw_tick("Polymarket", tick)
        event = analyzer.on_polymarket_tick(pt)
        if event is not None:
            await discord_logger.log_lag_event(event)
            if analyzer.is_lag_event(event):
                await discord_logger.log_alert(
                    f"Lag threshold exceeded: {event.lag_ms} ms "
                    f"(threshold: {config.LAG_THRESHOLD_MS} ms)\n"
                    f"Binance move: ${event.binance_move_usd:.2f} → "
                    f"Polymarket {event.polymarket_price_before:.4f} → "
                    f"{event.polymarket_price_after}"
                )

    async def stats_loop() -> None:
        while not stop_event.is_set():
            await asyncio.sleep(STATS_INTERVAL_S)
            if stop_event.is_set():
                break
            stats = analyzer.get_stats()
            await discord_logger.log_stats(stats)
            logger.info("Stats posted: %s", stats)

    # ------------------------------------------------------------------
    # Launch all coroutines
    # ------------------------------------------------------------------
    logger.info("crypto-lag-monitor starting…")
    await asyncio.gather(
        stream_binance_trades(handle_binance_tick, stop_event),
        stream_polymarket_prices(handle_polymarket_tick, stop_event),
        stats_loop(),
    )
    logger.info("crypto-lag-monitor stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except EnvironmentError as exc:
        logger.error("Configuration error: %s", exc)
        sys.exit(1)
