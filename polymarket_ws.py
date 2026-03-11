import asyncio
import json
import time
import logging
from collections import deque
from typing import Callable, Awaitable, Optional

import websockets

import config

logger = logging.getLogger(__name__)

_WS_URL = config.POLYMARKET_WS_URL

# Backoff (mirrors BinanceTradeStream)
_BACKOFF_INITIAL_S = 1
_BACKOFF_MAX_S = 30

# Minimum mid-price move (in dollars, i.e. cents on a 0–1 scale × 100) to
# count as a "significant reprice".  0.005 = 0.5 ¢ on a 0–1 prob market.
_REPRICE_THRESHOLD = 0.005

# How many price samples to keep in history
_HISTORY_MAXLEN = 100

# Polymarket CLOB sends these event types for price updates
_PRICE_EVENT_TYPES = {"book", "best_bid_ask", "price_change"}


class PolymarketPriceStream:
    """Streams order-book updates for a Polymarket CLOB token and tracks mid price.

    Parameters
    ----------
    token_id:
        The asset/token ID for the market leg to monitor (e.g. the YES token).
    on_price_update:
        Async callback called for every price update with a dict:
            {
                "event_type"       : str,
                "timestamp_ms"     : int,
                "best_bid"         : float,
                "best_ask"         : float,
                "mid_price"        : float,
            }
    on_market_expired:
        Async callback called when the market is detected as resolved/expired,
        so the caller can subscribe to the next 5-min BTC market.
    stop_event:
        Optional asyncio.Event; set it to stop the stream cleanly.
    """

    def __init__(
        self,
        token_id: str,
        on_price_update: Callable[[dict], Awaitable[None]],
        on_market_expired: Optional[Callable[[], Awaitable[None]]] = None,
        stop_event: Optional[asyncio.Event] = None,
    ) -> None:
        self.token_id = token_id
        self._on_price_update = on_price_update
        self._on_market_expired = on_market_expired
        self._stop_event = stop_event or asyncio.Event()

        # Public state
        self.price_history: deque[tuple[int, float]] = deque(maxlen=_HISTORY_MAXLEN)
        self.last_significant_reprice: Optional[int] = None  # timestamp_ms

        self._last_mid: Optional[float] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Start streaming. Blocks until stop_event is set."""
        backoff = _BACKOFF_INITIAL_S
        attempt = 0

        while not self._stop_event.is_set():
            try:
                await self._connect_and_stream()
                backoff = _BACKOFF_INITIAL_S
                attempt = 0
            except websockets.ConnectionClosed as exc:
                attempt += 1
                logger.warning(
                    "Polymarket WS closed (attempt %d): %s — reconnecting in %ds…",
                    attempt, exc, backoff,
                )
            except Exception as exc:
                attempt += 1
                logger.error(
                    "Polymarket WS error (attempt %d): %s — reconnecting in %ds…",
                    attempt, exc, backoff,
                )

            if self._stop_event.is_set():
                break

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, _BACKOFF_MAX_S)

        logger.info("PolymarketPriceStream stopped (token_id=%s).", self.token_id)

    def get_current_mid(self) -> Optional[float]:
        """Return the most recent mid price, or None if no data yet."""
        return self._last_mid

    def get_price_at(self, timestamp_ms: int) -> Optional[float]:
        """Return the mid price whose timestamp is closest to timestamp_ms.

        Scans price_history (up to _HISTORY_MAXLEN entries) — O(n) but n ≤ 100.
        Returns None if history is empty.
        """
        if not self.price_history:
            return None
        best_ts, best_px = min(
            self.price_history,
            key=lambda item: abs(item[0] - timestamp_ms),
        )
        return best_px

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _connect_and_stream(self) -> None:
        async with websockets.connect(_WS_URL, ping_interval=20, ping_timeout=10) as ws:
            logger.info(
                "Polymarket WebSocket connected: %s (token_id=%s)",
                _WS_URL, self.token_id,
            )
            await self._subscribe(ws)
            async for raw in ws:
                if self._stop_event.is_set():
                    return
                await self._handle_message(raw)

    async def _subscribe(self, ws: websockets.WebSocketClientProtocol) -> None:
        msg = json.dumps({"assets_ids": [self.token_id], "type": "Market"})
        await ws.send(msg)
        logger.info("Subscribed to Polymarket token_id=%s", self.token_id)

    async def _handle_message(self, raw: str) -> None:
        timestamp_received = time.time_ns() // 1_000_000  # ms
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.warning("JSON decode error: %s — %s", raw[:200], exc)
            return

        # Normalise: the API may send a single object or a list
        events = payload if isinstance(payload, list) else [payload]

        for event in events:
            event_type = event.get("event_type") or event.get("type", "")

            # --- Market expiry / resolution detection -------------------
            if event_type in ("market_closed", "MARKET_RESOLVED"):
                logger.info(
                    "Market expired/resolved (token_id=%s): %s", self.token_id, event
                )
                if self._on_market_expired is not None:
                    await self._on_market_expired()
                return

            if event_type not in _PRICE_EVENT_TYPES:
                continue

            # --- Extract bid / ask -------------------------------------
            bid, ask = self._extract_bid_ask(event)
            if bid is None or ask is None:
                continue
            if ask <= bid:
                # Crossed book — skip
                continue

            mid = (bid + ask) / 2

            # --- Update history & reprice tracking --------------------
            self.price_history.append((timestamp_received, mid))

            if self._last_mid is not None:
                move = abs(mid - self._last_mid)
                if move >= _REPRICE_THRESHOLD:
                    self.last_significant_reprice = timestamp_received
                    logger.debug(
                        "Significant reprice: %.4f → %.4f (Δ%.4f)",
                        self._last_mid, mid, mid - self._last_mid,
                    )

            self._last_mid = mid

            # --- Fire callback ----------------------------------------
            update = {
                "event_type": event_type,
                "timestamp_ms": timestamp_received,
                "best_bid": bid,
                "best_ask": ask,
                "mid_price": mid,
            }
            await self._on_price_update(update)

    @staticmethod
    def _extract_bid_ask(event: dict) -> tuple[Optional[float], Optional[float]]:
        """Parse best_bid / best_ask from various Polymarket event shapes.

        Polymarket CLOB WebSocket delivers three relevant shapes:

        1. best_bid_ask / price_change
           { "best_bid": "0.54", "best_ask": "0.56", ... }

        2. book  (full snapshot)
           { "bids": [{"price": "0.54", "size": "100"}, ...],
             "asks": [{"price": "0.56", "size": "80"},  ...], ... }
        """
        # Shape 1: flat fields
        if "best_bid" in event and "best_ask" in event:
            try:
                return float(event["best_bid"]), float(event["best_ask"])
            except (ValueError, TypeError):
                pass

        # Shape 2: bids/asks arrays — take best (highest bid, lowest ask)
        bids = event.get("bids", [])
        asks = event.get("asks", [])
        if bids and asks:
            try:
                best_bid = max(float(b["price"]) for b in bids)
                best_ask = min(float(a["price"]) for a in asks)
                return best_bid, best_ask
            except (KeyError, ValueError, TypeError):
                pass

        return None, None
