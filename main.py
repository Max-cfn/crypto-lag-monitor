"""
crypto-lag-monitor — entry point

Orchestrates:
  • Binance BTC/USDT trade stream
  • Polymarket 5-min BTC market stream (auto-refreshes when market expires)
  • Lag measurement between Binance moves and Polymarket repricing
  • Discord reporting (raw ticks, lag events, stats, alerts)
"""

import asyncio
import datetime
import logging
import signal
import sys
from typing import Optional

import aiohttp

import config
from lag_analyzer import LagAnalyzer, LagMeasurement
from binance_ws import BinanceTradeStream
from polymarket_ws import PolymarketPriceStream
from discord_logger import DiscordLogger

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
GAMMA_API_URL   = "https://gamma-api.polymarket.com/markets"
STATS_INTERVAL_S   = 300   # 5 minutes
MARKET_CHECK_INTERVAL_S = 60

# How many times to retry the full run() loop on unexpected crash
MAX_RESTART_ATTEMPTS = 5
RESTART_BACKOFF_S    = 10


# ---------------------------------------------------------------------------
# Gamma API helpers
# ---------------------------------------------------------------------------

class MarketInfo:
    __slots__ = ("token_id", "question", "end_dt")

    def __init__(self, token_id: str, question: str, end_dt: datetime.datetime) -> None:
        self.token_id = token_id
        self.question = question
        self.end_dt   = end_dt

    def expires_in_str(self) -> str:
        delta = self.end_dt - datetime.datetime.now(tz=datetime.timezone.utc)
        total = int(delta.total_seconds())
        if total <= 0:
            return "expired"
        m, s = divmod(total, 60)
        return f"{m}m {s}s"


async def fetch_active_btc_5min_market() -> Optional[MarketInfo]:
    """Query the Polymarket Gamma API and return the soonest-expiring active
    BTC 5-minute market, or None if none found."""
    params = {"tag": "crypto", "active": "true", "limit": "200"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                GAMMA_API_URL,
                params=params,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                resp.raise_for_status()
                markets = await resp.json()
    except Exception as exc:
        logger.error("Gamma API fetch failed: %s", exc)
        return None

    candidates: list[MarketInfo] = []
    now = datetime.datetime.now(tz=datetime.timezone.utc)

    for m in markets:
        question: str = m.get("question") or m.get("title") or ""
        ql = question.lower()
        # Must mention Bitcoin and a 5-minute window
        if "bitcoin" not in ql and "btc" not in ql:
            continue
        if not (("5" in ql and "minute" in ql) or "5-minute" in ql or "5min" in ql):
            continue

        end_str: str = m.get("endDate") or m.get("end_date") or ""
        if not end_str:
            continue
        try:
            end_dt = datetime.datetime.fromisoformat(end_str.replace("Z", "+00:00"))
        except ValueError:
            continue

        if end_dt <= now:
            continue  # already expired

        # Resolve token_id: prefer clobTokenIds[0], else conditionId
        token_id: str = ""
        clob_ids = m.get("clobTokenIds") or []
        if clob_ids:
            token_id = clob_ids[0]
        if not token_id:
            token_id = m.get("conditionId") or m.get("id") or ""
        if not token_id:
            continue

        candidates.append(MarketInfo(token_id=token_id, question=question, end_dt=end_dt))

    if not candidates:
        return None

    # Pick the market expiring soonest
    return min(candidates, key=lambda x: x.end_dt)


# ---------------------------------------------------------------------------
# Core run loop
# ---------------------------------------------------------------------------

async def _run_once(discord: DiscordLogger) -> None:
    """Single run iteration.  Raises on unexpected errors so the caller can
    decide whether to restart."""

    # ------------------------------------------------------------------
    # 1. Startup: fetch active market
    # ------------------------------------------------------------------
    market = await fetch_active_btc_5min_market()

    if market is None:
        # Fall back to the token_id configured in .env (may be empty)
        token_id = config.POLYMARKET_MARKET_ID
        question = "(configured in .env)"
        expires_str = "unknown"
        logger.warning("No active BTC 5-min market found via Gamma API — using .env value")
    else:
        token_id = market.token_id
        question = market.question
        expires_str = market.end_dt.strftime("%H:%M:%S UTC")
        logger.info("Active market found: %s | token_id=%s | expires %s",
                    question, token_id, expires_str)

    discord.log_alert(
        f"🚀 Starting lag monitor | Market: {question} | Expires: {expires_str}"
    )

    # ------------------------------------------------------------------
    # 2. Build components
    # ------------------------------------------------------------------
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    async def on_measurement(m: LagMeasurement) -> None:
        discord.log_lag_measurement(m)
        if analyzer.is_edge_exploitable():
            report = analyzer.get_stats_report()
            discord.log_alert(
                f"⚠️ Edge may be exploitable — median lag "
                f"{report['median_lag']:.0f} ms > 3 000 ms"
            )

    analyzer = LagAnalyzer(on_measurement=on_measurement)

    async def handle_significant_move(move: dict) -> None:
        discord.log_binance_move(move)
        analyzer.on_binance_move(move, poly_stream)

    async def handle_polymarket_update(update: dict) -> None:
        pass  # poly_stream.get_current_mid() is polled by the analyzer

    async def handle_market_expired() -> None:
        logger.warning("Polymarket market expired (WS event) — will refresh next cycle")
        discord.log_alert("⚠️ Market expired, looking for new 5-min market...")

    poly_stream = PolymarketPriceStream(
        token_id=token_id,
        on_price_update=handle_polymarket_update,
        on_market_expired=handle_market_expired,
        stop_event=stop_event,
    )
    binance_stream = BinanceTradeStream(
        on_significant_move=handle_significant_move,
        stop_event=stop_event,
    )

    # ------------------------------------------------------------------
    # 3. Background coroutines
    # ------------------------------------------------------------------

    async def stats_reporter() -> None:
        """Post stats every 5 minutes, with a final verdict on shutdown."""
        while not stop_event.is_set():
            await asyncio.sleep(STATS_INTERVAL_S)
            if stop_event.is_set():
                break
            _post_stats(analyzer, discord)

        # Final summary on clean shutdown
        report = analyzer.get_stats_report()
        exploitable = analyzer.is_edge_exploitable()
        median_str = f"{report['median_lag']:.0f}ms" if report.get("median_lag") else "N/A"
        verdict = "EXPLOITABLE ✅" if exploitable else "NOT EXPLOITABLE ❌"
        discord.log_alert(
            f"🏁 Monitor stopped | VERDICT: Edge {verdict} | Median lag: {median_str}"
        )
        logger.info("Final verdict: %s | median=%s", verdict, median_str)

    async def market_refresher() -> None:
        """Every 60 s, check if the current market is expired or about to expire.
        If so, fetch the next active 5-min BTC market and hot-swap the stream."""
        nonlocal market
        while not stop_event.is_set():
            await asyncio.sleep(MARKET_CHECK_INTERVAL_S)
            if stop_event.is_set():
                break

            now = datetime.datetime.now(tz=datetime.timezone.utc)
            current_expired = (
                market is None
                or market.end_dt <= now
            )

            if not current_expired:
                logger.debug(
                    "Market check OK — expires in %s", market.expires_in_str()
                )
                continue

            logger.info("Market expired — fetching next 5-min BTC market…")
            discord.log_alert("⚠️ Market expired, looking for new 5-min market...")

            new_market = await fetch_active_btc_5min_market()
            if new_market is None:
                logger.warning("No replacement market found — retrying next cycle")
                discord.log_alert(
                    "🔴 No active 5-min BTC market found — retrying in 60s"
                )
                continue

            if market is not None and new_market.token_id == market.token_id:
                logger.info("Same market still active — no swap needed")
                market = new_market   # refresh end_dt
                continue

            market = new_market
            poly_stream.swap_token(new_market.token_id)
            expires_str = new_market.end_dt.strftime("%H:%M:%S UTC")
            discord.log_alert(
                f"🟢 New market subscribed | {new_market.question} | "
                f"Expires: {expires_str}"
            )
            logger.info(
                "Swapped to new market: %s | token=%s | expires %s",
                new_market.question, new_market.token_id, expires_str,
            )

    # ------------------------------------------------------------------
    # 4. Run everything
    # ------------------------------------------------------------------
    logger.info("Starting all streams…")
    await asyncio.gather(
        binance_stream.run(),
        poly_stream.run(),
        stats_reporter(),
        market_refresher(),
    )


def _post_stats(analyzer: LagAnalyzer, discord: DiscordLogger) -> None:
    report = analyzer.get_stats_report()
    discord.log_stats_report(report)
    exploitable = analyzer.is_edge_exploitable()
    median_str = f"{report['median_lag']:.0f}ms" if report.get("median_lag") else "N/A"
    verdict = "EXPLOITABLE ✅" if exploitable else "NOT EXPLOITABLE ❌"
    logger.info(
        "Stats | VERDICT: Edge %s | Median lag: %s | moves=%s",
        verdict, median_str, report.get("nb_moves_total"),
    )


# ---------------------------------------------------------------------------
# Entry point with restart loop
# ---------------------------------------------------------------------------

async def run() -> None:
    config.validate_config()

    discord = DiscordLogger()
    discord.start()

    for attempt in range(1, MAX_RESTART_ATTEMPTS + 1):
        try:
            await _run_once(discord)
            break   # clean exit (stop_event set)
        except (KeyboardInterrupt, SystemExit):
            break
        except Exception as exc:
            logger.exception("Unexpected error in run loop (attempt %d): %s", attempt, exc)
            discord.log_alert(
                f"🔴 Monitor crashed (attempt {attempt}/{MAX_RESTART_ATTEMPTS}): "
                f"{type(exc).__name__}: {exc}"
            )
            if attempt >= MAX_RESTART_ATTEMPTS:
                discord.log_alert(
                    f"🔴 Max restart attempts reached — giving up"
                )
                raise
            await asyncio.sleep(RESTART_BACKOFF_S * attempt)
            discord.log_alert(
                f"🟡 Restarting monitor (attempt {attempt + 1}/{MAX_RESTART_ATTEMPTS})…"
            )


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except EnvironmentError as exc:
        logger.error("Configuration error: %s", exc)
        sys.exit(1)
    except Exception:
        sys.exit(1)
