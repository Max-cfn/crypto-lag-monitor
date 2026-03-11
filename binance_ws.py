import asyncio
import json
import time
import logging
from typing import Callable, Awaitable

import websockets

import config

logger = logging.getLogger(__name__)


async def stream_binance_trades(
    on_trade: Callable[[dict], Awaitable[None]],
    stop_event: asyncio.Event,
) -> None:
    """Connect to Binance trade stream and call on_trade for each tick.

    Each tick dict contains:
        symbol   : str   — trading pair (e.g. "BTCUSDT")
        price    : float — trade price in USD
        quantity : float — trade quantity
        timestamp_ms : int — server trade time in ms
        received_ms  : int — local reception time in ms
    """
    while not stop_event.is_set():
        try:
            async with websockets.connect(
                config.BINANCE_WS_URL,
                ping_interval=20,
                ping_timeout=10,
            ) as ws:
                logger.info("Binance WebSocket connected: %s", config.BINANCE_WS_URL)
                async for raw in ws:
                    if stop_event.is_set():
                        break
                    received_ms = int(time.time() * 1000)
                    try:
                        msg = json.loads(raw)
                        tick = {
                            "symbol": msg.get("s", config.BINANCE_SYMBOL),
                            "price": float(msg["p"]),
                            "quantity": float(msg["q"]),
                            "timestamp_ms": int(msg["T"]),
                            "received_ms": received_ms,
                        }
                        await on_trade(tick)
                    except (KeyError, ValueError) as exc:
                        logger.warning("Malformed Binance message: %s — %s", raw, exc)

        except websockets.ConnectionClosed as exc:
            logger.warning("Binance WS closed (%s), reconnecting in 3s…", exc)
            await asyncio.sleep(3)
        except Exception as exc:
            logger.error("Binance WS error: %s, reconnecting in 5s…", exc)
            await asyncio.sleep(5)

    logger.info("Binance stream stopped.")
