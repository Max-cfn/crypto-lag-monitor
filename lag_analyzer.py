import time
import logging
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

import numpy as np

import config

logger = logging.getLogger(__name__)

# Rolling window size for statistics
STATS_WINDOW = 200


@dataclass
class BinanceTick:
    price: float
    timestamp_ms: int
    received_ms: int


@dataclass
class PolymarketTick:
    price: float
    timestamp_ms: int
    received_ms: int
    outcome: str = "YES"


@dataclass
class LagEvent:
    binance_price: float
    binance_move_usd: float
    polymarket_price_before: float
    polymarket_price_after: Optional[float]
    lag_ms: Optional[int]          # None if Polymarket did not react yet
    binance_timestamp_ms: int
    polymarket_timestamp_ms: Optional[int]
    detected_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))


class LagAnalyzer:
    """Detects lag between significant Binance price moves and Polymarket reactions."""

    def __init__(self) -> None:
        self._last_binance: Optional[BinanceTick] = None
        self._last_polymarket: Optional[PolymarketTick] = None

        # Pending move waiting for a Polymarket reaction
        self._pending_move: Optional[BinanceTick] = None
        self._pending_poly_price: Optional[float] = None

        # Rolling lag stats (ms)
        self._lag_window: deque[float] = deque(maxlen=STATS_WINDOW)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def on_binance_tick(self, tick: BinanceTick) -> Optional[LagEvent]:
        """Process a new Binance tick.

        Returns a LagEvent if a significant price move is detected and Polymarket
        has already reacted (or if we're closing a previously pending move).
        """
        event = None

        # Close any pending move if Polymarket already has a newer tick
        if self._pending_move is not None and self._last_polymarket is not None:
            poly = self._last_polymarket
            if poly.timestamp_ms > self._pending_move.timestamp_ms:
                lag_ms = poly.received_ms - self._pending_move.received_ms
                event = LagEvent(
                    binance_price=self._pending_move.price,
                    binance_move_usd=abs(
                        self._pending_move.price
                        - (self._last_binance.price if self._last_binance else self._pending_move.price)
                    ),
                    polymarket_price_before=self._pending_poly_price or poly.price,
                    polymarket_price_after=poly.price,
                    lag_ms=lag_ms,
                    binance_timestamp_ms=self._pending_move.timestamp_ms,
                    polymarket_timestamp_ms=poly.timestamp_ms,
                )
                if lag_ms > 0:
                    self._lag_window.append(lag_ms)
                self._pending_move = None
                self._pending_poly_price = None

        # Detect a new significant move
        if self._last_binance is not None:
            move = abs(tick.price - self._last_binance.price)
            if move >= config.BINANCE_MOVE_THRESHOLD_USD:
                logger.info(
                    "Significant Binance move detected: $%.2f (Δ$%.2f)",
                    tick.price,
                    move,
                )
                self._pending_move = tick
                self._pending_poly_price = (
                    self._last_polymarket.price if self._last_polymarket else None
                )

        self._last_binance = tick
        return event

    def on_polymarket_tick(self, tick: PolymarketTick) -> Optional[LagEvent]:
        """Process a new Polymarket tick.

        If there is a pending Binance move, compute the lag and return a LagEvent.
        """
        self._last_polymarket = tick

        if self._pending_move is None:
            return None

        lag_ms = tick.received_ms - self._pending_move.received_ms
        binance_price_ref = self._last_binance.price if self._last_binance else self._pending_move.price
        move_usd = abs(self._pending_move.price - binance_price_ref)

        event = LagEvent(
            binance_price=self._pending_move.price,
            binance_move_usd=move_usd,
            polymarket_price_before=self._pending_poly_price or tick.price,
            polymarket_price_after=tick.price,
            lag_ms=lag_ms,
            binance_timestamp_ms=self._pending_move.timestamp_ms,
            polymarket_timestamp_ms=tick.timestamp_ms,
        )

        if lag_ms > 0:
            self._lag_window.append(lag_ms)

        self._pending_move = None
        self._pending_poly_price = None

        return event

    def is_lag_event(self, event: LagEvent) -> bool:
        """Return True if the lag exceeds the configured threshold."""
        return event.lag_ms is not None and event.lag_ms >= config.LAG_THRESHOLD_MS

    def get_stats(self) -> dict:
        """Return rolling statistics over the last STATS_WINDOW lag measurements."""
        if not self._lag_window:
            return {
                "count": 0,
                "mean_ms": None,
                "median_ms": None,
                "p95_ms": None,
                "p99_ms": None,
                "max_ms": None,
            }
        arr = np.array(list(self._lag_window))
        return {
            "count": len(arr),
            "mean_ms": float(np.mean(arr)),
            "median_ms": float(np.median(arr)),
            "p95_ms": float(np.percentile(arr, 95)),
            "p99_ms": float(np.percentile(arr, 99)),
            "max_ms": float(np.max(arr)),
        }
