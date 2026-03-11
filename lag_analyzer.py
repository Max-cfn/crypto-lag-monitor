import asyncio
import time
import logging
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional, TYPE_CHECKING

import numpy as np

import config

if TYPE_CHECKING:
    from polymarket_ws import PolymarketPriceStream

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Legacy dataclasses kept for main.py compatibility
# ------------------------------------------------------------------

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


# ------------------------------------------------------------------
# Core types
# ------------------------------------------------------------------

# Minimum Polymarket mid move (in prob-dollars, i.e. ¢/100) that counts
# as a "reprice" in response to a Binance move.
REPRICE_THRESHOLD = 0.005   # 0.5 ¢

# How often to poll Polymarket price during a lag measurement (ms)
POLL_INTERVAL_MS = 100

# Maximum time to wait for a reprice before giving up (ms)
MAX_WAIT_MS = 30_000

# Rolling window: keep measurements from the last hour
ROLLING_WINDOW_S = 3600


@dataclass
class LagMeasurement:
    id: str
    binance_move: dict                        # raw move_event from BinanceTradeStream
    polymarket_price_at_move: float
    polymarket_price_after_reprice: Optional[float]
    lag_ms: Optional[int]                    # None → no reprice observed
    repriced: bool
    direction_match: bool                    # Polymarket moved same direction as Binance
    timestamp_ms: int = field(default_factory=lambda: time.time_ns() // 1_000_000)


# ------------------------------------------------------------------
# Main class
# ------------------------------------------------------------------

class LagAnalyzer:
    """Measures the lag between significant Binance moves and Polymarket repricing.

    Usage
    -----
    Call ``on_binance_move(move_event, polymarket_stream)`` from the
    BinanceTradeStream callback.  It fires a background asyncio task that
    polls Polymarket for up to 30 s and records the result.

    Parameters
    ----------
    on_measurement:
        Optional async callback fired with each completed LagMeasurement.
        Use it to post results to Discord, persist to DB, etc.
    """

    def __init__(
        self,
        on_measurement: Optional[Callable[["LagMeasurement"], Awaitable[None]]] = None,
    ) -> None:
        self._on_measurement = on_measurement

        # All completed measurements (unbounded — for in-process inspection)
        self.measurements: list[LagMeasurement] = []

        # Rolling window: (completed_at_ms, measurement) for the last hour
        self._rolling: deque[tuple[int, LagMeasurement]] = deque()

        # Guard: avoid launching two measurements for the same move simultaneously
        self._in_flight: bool = False

    # ------------------------------------------------------------------
    # Primary entry point
    # ------------------------------------------------------------------

    def on_binance_move(
        self,
        move_event: dict,
        polymarket_stream: "PolymarketPriceStream",
    ) -> None:
        """Schedule a lag measurement for *move_event* without blocking.

        Safe to call from inside an async callback — it creates a background
        task.  Overlapping moves are silently dropped (one in-flight at a time).
        """
        if self._in_flight:
            logger.debug(
                "Skipping overlapping lag measurement (already in-flight)."
            )
            return
        self._in_flight = True
        asyncio.create_task(
            self._measure(move_event, polymarket_stream),
            name=f"lag-measure-{move_event.get('timestamp_ms')}",
        )

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def get_stats_report(self) -> dict:
        """Return rolling stats for the last hour."""
        self._evict_old()
        window = [m for _, m in self._rolling]

        if not window:
            return {
                "nb_moves_total": 0,
                "nb_repriced": 0,
                "nb_no_reprice": 0,
                "nb_direction_match": 0,
                "median_lag": None,
                "p25_lag": None,
                "p75_lag": None,
                "p95_lag": None,
                "max_lag": None,
                "avg_move_size_usd": None,
                "avg_polymarket_move_cents": None,
            }

        lags = [m.lag_ms for m in window if m.lag_ms is not None]
        lag_arr = np.array(lags) if lags else None

        move_sizes = [abs(m.binance_move.get("delta_usd", 0)) for m in window]
        poly_moves = [
            abs(
                (m.polymarket_price_after_reprice or m.polymarket_price_at_move)
                - m.polymarket_price_at_move
            )
            * 100  # convert to cents
            for m in window
        ]

        return {
            "nb_moves_total": len(window),
            "nb_repriced": sum(1 for m in window if m.repriced),
            "nb_no_reprice": sum(1 for m in window if not m.repriced),
            "nb_direction_match": sum(1 for m in window if m.direction_match),
            "median_lag": float(np.median(lag_arr)) if lag_arr is not None else None,
            "p25_lag": float(np.percentile(lag_arr, 25)) if lag_arr is not None else None,
            "p75_lag": float(np.percentile(lag_arr, 75)) if lag_arr is not None else None,
            "p95_lag": float(np.percentile(lag_arr, 95)) if lag_arr is not None else None,
            "max_lag": float(np.max(lag_arr)) if lag_arr is not None else None,
            "avg_move_size_usd": float(np.mean(move_sizes)) if move_sizes else None,
            "avg_polymarket_move_cents": float(np.mean(poly_moves)) if poly_moves else None,
        }

    def is_edge_exploitable(self) -> bool:
        """Return True when the median lag over the last hour exceeds 3 000 ms."""
        report = self.get_stats_report()
        median = report.get("median_lag")
        return median is not None and median > 3_000

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _measure(
        self,
        move_event: dict,
        polymarket_stream: "PolymarketPriceStream",
    ) -> None:
        """Background task: poll Polymarket until reprice or timeout."""
        meas_id = str(uuid.uuid4())
        t0_ms = move_event["timestamp_ms"]
        binance_direction = move_event["direction"]           # "UP" or "DOWN"

        price_at_move = polymarket_stream.get_current_mid()
        if price_at_move is None:
            logger.warning("[%s] No Polymarket price at move time — skipping.", meas_id)
            self._in_flight = False
            return

        logger.info(
            "[%s] Lag measurement started — Binance %s $%.2f, Poly mid=%.4f",
            meas_id, binance_direction, move_event["price_after"], price_at_move,
        )

        repriced = False
        price_after_reprice: Optional[float] = None
        lag_ms: Optional[int] = None
        direction_match = False

        deadline_ms = t0_ms + MAX_WAIT_MS

        while True:
            now_ms = time.time_ns() // 1_000_000
            if now_ms >= deadline_ms:
                break

            await asyncio.sleep(POLL_INTERVAL_MS / 1000)

            current_mid = polymarket_stream.get_current_mid()
            if current_mid is None:
                continue

            poly_delta = current_mid - price_at_move
            if abs(poly_delta) >= REPRICE_THRESHOLD:
                reprice_ts = time.time_ns() // 1_000_000
                lag_ms = reprice_ts - t0_ms
                repriced = True
                price_after_reprice = current_mid
                direction_match = (
                    (poly_delta > 0 and binance_direction == "UP")
                    or (poly_delta < 0 and binance_direction == "DOWN")
                )
                logger.info(
                    "[%s] Reprice detected — lag=%d ms, poly %.4f→%.4f, dir_match=%s",
                    meas_id, lag_ms, price_at_move, current_mid, direction_match,
                )
                break

        if not repriced:
            logger.info(
                "[%s] NO_REPRICE after %d ms — Binance %s $%.2f, Poly mid=%.4f",
                meas_id, MAX_WAIT_MS, binance_direction,
                move_event["price_after"], price_at_move,
            )

        measurement = LagMeasurement(
            id=meas_id,
            binance_move=move_event,
            polymarket_price_at_move=price_at_move,
            polymarket_price_after_reprice=price_after_reprice,
            lag_ms=lag_ms,
            repriced=repriced,
            direction_match=direction_match,
        )
        self.measurements.append(measurement)
        self._rolling.append((measurement.timestamp_ms, measurement))
        self._evict_old()
        self._in_flight = False

        if self._on_measurement is not None:
            await self._on_measurement(measurement)

    def _evict_old(self) -> None:
        """Remove measurements older than ROLLING_WINDOW_S from the rolling deque."""
        cutoff = time.time_ns() // 1_000_000 - ROLLING_WINDOW_S * 1000
        while self._rolling and self._rolling[0][0] < cutoff:
            self._rolling.popleft()
