import asyncio
import json
import time
import logging
from collections import deque
from typing import Callable, Awaitable, Optional

import websockets

import config

logger = logging.getLogger(__name__)

# Backoff config
_BACKOFF_INITIAL_S = 1
_BACKOFF_MAX_S = 30

# Rolling price history: store (timestamp_ms, price) pairs
_HISTORY_WINDOW_S = 6  # keep a bit more than 5 s to always have a 5 s-ago value


class BinanceTradeStream:
    """Streams BTC/USDT trades from Binance and detects significant price moves.

    State exposed:
        last_price     : float | None  — most recent trade price
        price_1s_ago   : float | None  — price ~1 s ago (rolling)
        price_5s_ago   : float | None  — price ~5 s ago (rolling)
        moves          : list[dict]    — all significant moves detected so far
    """

    def __init__(
        self,
        on_significant_move: Callable[[dict], Awaitable[None]],
        stop_event: Optional[asyncio.Event] = None,
    ) -> None:
        self._on_significant_move = on_significant_move
        self._stop_event = stop_event or asyncio.Event()

        # Public state
        self.last_price: Optional[float] = None
        self.price_1s_ago: Optional[float] = None
        self.price_5s_ago: Optional[float] = None
        self.moves: list[dict] = []

        # Internal rolling history: deque of (timestamp_ms, price)
        self._history: deque[tuple[int, float]] = deque()

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
                # Clean exit — stop_event was set inside the loop
                backoff = _BACKOFF_INITIAL_S
                attempt = 0
            except websockets.ConnectionClosed as exc:
                attempt += 1
                logger.warning(
                    "Binance WS closed (attempt %d): %s — reconnecting in %ds…",
                    attempt, exc, backoff,
                )
            except Exception as exc:
                attempt += 1
                logger.error(
                    "Binance WS error (attempt %d): %s — reconnecting in %ds…",
                    attempt, exc, backoff,
                )

            if self._stop_event.is_set():
                break

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, _BACKOFF_MAX_S)

        logger.info("BinanceTradeStream stopped.")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _connect_and_stream(self) -> None:
        url = config.BINANCE_WS_URL
        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            logger.info("Binance WebSocket connected: %s", url)
            async for raw in ws:
                if self._stop_event.is_set():
                    return
                await self._handle_message(raw)

    async def _handle_message(self, raw: str) -> None:
        timestamp_received = time.time_ns() // 1_000_000  # ms
        try:
            msg = json.loads(raw)
            timestamp_exchange = int(msg["T"])
            price = float(msg["p"])
            quantity = float(msg["q"])
            is_buyer_maker = bool(msg["m"])
        except (KeyError, ValueError, TypeError) as exc:
            logger.warning("Malformed Binance message: %s — %s", raw, exc)
            return

        # Update rolling history and derived prices
        self._update_history(timestamp_received, price)

        # Detect significant move vs 5 s ago
        if self.price_5s_ago is not None:
            delta_usd = price - self.price_5s_ago
            if abs(delta_usd) > config.BINANCE_MOVE_THRESHOLD_USD:
                move_event = {
                    "timestamp_ms": timestamp_received,
                    "price_before": self.price_5s_ago,
                    "price_after": price,
                    "delta_usd": round(delta_usd, 2),
                    "delta_pct": round(delta_usd / self.price_5s_ago * 100, 4),
                    "direction": "UP" if delta_usd > 0 else "DOWN",
                }
                self.moves.append(move_event)
                logger.info(
                    "Significant move: %s $%.2f → $%.2f (Δ$%.2f / %.4f%%)",
                    move_event["direction"],
                    self.price_5s_ago,
                    price,
                    delta_usd,
                    move_event["delta_pct"],
                )
                await self._on_significant_move(move_event)

        self.last_price = price

    def _update_history(self, now_ms: int, price: float) -> None:
        """Append current price and evict entries older than the window."""
        self._history.append((now_ms, price))

        cutoff = now_ms - _HISTORY_WINDOW_S * 1000
        while self._history and self._history[0][0] < cutoff:
            self._history.popleft()

        # price_1s_ago: oldest entry within the last 1–2 s band
        self.price_1s_ago = self._closest_ago(now_ms, target_ms=1_000)
        # price_5s_ago: oldest entry within the last 5–6 s band
        self.price_5s_ago = self._closest_ago(now_ms, target_ms=5_000)

    def _closest_ago(self, now_ms: int, target_ms: int) -> Optional[float]:
        """Return the price whose timestamp is closest to (now - target_ms)."""
        target_ts = now_ms - target_ms
        best: Optional[tuple[int, float]] = None
        for ts, px in self._history:
            if best is None or abs(ts - target_ts) < abs(best[0] - target_ts):
                best = (ts, px)
        return best[1] if best is not None else None
