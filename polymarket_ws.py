import asyncio
import json
import time
import logging
from typing import Callable, Awaitable

import websockets

import config

logger = logging.getLogger(__name__)

# Subscription message sent right after connection
_SUBSCRIBE_MSG = json.dumps(
    {
        "type": "Market",
        "assets_ids": [],  # populated at runtime with the market's token IDs
    }
)


async def stream_polymarket_prices(
    on_price: Callable[[dict], Awaitable[None]],
    stop_event: asyncio.Event,
) -> None:
    """Connect to Polymarket CLOB WebSocket and call on_price for each price update.

    Each price dict contains:
        market_id    : str   — Polymarket condition/market ID
        outcome      : str   — outcome label (e.g. "YES" / "NO")
        price        : float — mid price 0–1
        timestamp_ms : int   — event time in ms
        received_ms  : int   — local reception time in ms
    """
    while not stop_event.is_set():
        try:
            async with websockets.connect(
                config.POLYMARKET_WS_URL,
                ping_interval=20,
                ping_timeout=10,
            ) as ws:
                logger.info(
                    "Polymarket WebSocket connected: %s", config.POLYMARKET_WS_URL
                )

                # Subscribe to the configured market
                subscribe = json.dumps(
                    {
                        "type": "Market",
                        "markets": [config.POLYMARKET_MARKET_ID],
                    }
                )
                await ws.send(subscribe)
                logger.info(
                    "Subscribed to Polymarket market: %s", config.POLYMARKET_MARKET_ID
                )

                async for raw in ws:
                    if stop_event.is_set():
                        break
                    received_ms = int(time.time() * 1000)
                    try:
                        messages = json.loads(raw)
                        # The API may return a list of events
                        if not isinstance(messages, list):
                            messages = [messages]
                        for msg in messages:
                            event_type = msg.get("event_type", "")
                            # Handle price_change and book events
                            if event_type in ("price_change", "book"):
                                for outcome_data in msg.get("market_slugs", []):
                                    price_obj = {
                                        "market_id": config.POLYMARKET_MARKET_ID,
                                        "outcome": outcome_data.get("outcome", ""),
                                        "price": float(outcome_data.get("price", 0)),
                                        "timestamp_ms": int(
                                            msg.get("timestamp", received_ms)
                                        ),
                                        "received_ms": received_ms,
                                    }
                                    await on_price(price_obj)
                    except (KeyError, ValueError, TypeError) as exc:
                        logger.warning(
                            "Malformed Polymarket message: %s — %s", raw, exc
                        )

        except websockets.ConnectionClosed as exc:
            logger.warning("Polymarket WS closed (%s), reconnecting in 3s…", exc)
            await asyncio.sleep(3)
        except Exception as exc:
            logger.error("Polymarket WS error: %s, reconnecting in 5s…", exc)
            await asyncio.sleep(5)

    logger.info("Polymarket stream stopped.")
