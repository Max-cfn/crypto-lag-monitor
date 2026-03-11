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
from lag_analyzer import LagAnalyzer, LagMeasurement
from binance_ws import BinanceTradeStream
from polymarket_ws import PolymarketPriceStream
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

    async def on_measurement(m: LagMeasurement) -> None:
        await discord_logger.log_lag_measurement(m)
        if analyzer.is_edge_exploitable():
            report = analyzer.get_stats_report()
            await discord_logger.log_alert(
                f"Edge may be exploitable — median lag "
                f"{report['median_lag']:.0f} ms > 3 000 ms",
                color=0x00FF00,
            )

    analyzer = LagAnalyzer(on_measurement=on_measurement)
    stop_event = asyncio.Event()

    # Graceful shutdown on SIGINT / SIGTERM
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    # ------------------------------------------------------------------
    # Streams (declared before callbacks so closures can reference them)
    # ------------------------------------------------------------------

    async def handle_significant_move(move: dict) -> None:
        await discord_logger.log_raw_tick("Binance move", move)
        analyzer.on_binance_move(move, poly_stream)

    async def handle_polymarket_update(update: dict) -> None:
        await discord_logger.log_raw_tick("Polymarket", update)

    async def handle_market_expired() -> None:
        logger.warning("Polymarket market expired — update POLYMARKET_MARKET_ID in .env")
        await discord_logger.log_alert(
            "Polymarket 5-min BTC market has expired.\n"
            "Update `POLYMARKET_MARKET_ID` in `.env` and restart.",
            color=0xFFA500,
        )

    async def stats_loop() -> None:
        while not stop_event.is_set():
            await asyncio.sleep(STATS_INTERVAL_S)
            if stop_event.is_set():
                break
            report = analyzer.get_stats_report()
            await discord_logger.log_stats(report)
            if analyzer.is_edge_exploitable():
                await discord_logger.log_alert(
                    f"Edge may be exploitable — median lag "
                    f"{report['median_lag']:.0f} ms > 3 000 ms",
                    color=0x00FF00,
                )
            logger.info("Stats posted: %s", report)

    # ------------------------------------------------------------------
    # Launch all coroutines
    # ------------------------------------------------------------------
    poly_stream = PolymarketPriceStream(
        token_id=config.POLYMARKET_MARKET_ID,
        on_price_update=handle_polymarket_update,
        on_market_expired=handle_market_expired,
        stop_event=stop_event,
    )
    binance_stream = BinanceTradeStream(
        on_significant_move=handle_significant_move,
        stop_event=stop_event,
    )

    logger.info("crypto-lag-monitor starting…")
    await asyncio.gather(
        binance_stream.run(),
        poly_stream.run(),
        stats_loop(),
    )
    logger.info("crypto-lag-monitor stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except EnvironmentError as exc:
        logger.error("Configuration error: %s", exc)
        sys.exit(1)
