import os
from dotenv import load_dotenv

load_dotenv()

# Discord webhooks
DISCORD_WEBHOOK_RAW_TICKS = os.getenv("DISCORD_WEBHOOK_RAW_TICKS", "")
DISCORD_WEBHOOK_LAG_EVENTS = os.getenv("DISCORD_WEBHOOK_LAG_EVENTS", "")
DISCORD_WEBHOOK_STATS = os.getenv("DISCORD_WEBHOOK_STATS", "")
DISCORD_WEBHOOK_ALERTS = os.getenv("DISCORD_WEBHOOK_ALERTS", "")

# Polymarket
POLYMARKET_MARKET_ID = os.getenv("POLYMARKET_MARKET_ID", "")

# Thresholds
LAG_THRESHOLD_MS = int(os.getenv("LAG_THRESHOLD_MS", "500"))
BINANCE_MOVE_THRESHOLD_USD = float(os.getenv("BINANCE_MOVE_THRESHOLD_USD", "5"))

# Binance
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
BINANCE_SYMBOL = "BTCUSDT"

# Polymarket CLOB WebSocket
POLYMARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def validate_config() -> None:
    """Raise a clear error if any required Discord webhook is missing or placeholder."""
    required_webhooks = {
        "DISCORD_WEBHOOK_RAW_TICKS": DISCORD_WEBHOOK_RAW_TICKS,
        "DISCORD_WEBHOOK_LAG_EVENTS": DISCORD_WEBHOOK_LAG_EVENTS,
        "DISCORD_WEBHOOK_STATS": DISCORD_WEBHOOK_STATS,
        "DISCORD_WEBHOOK_ALERTS": DISCORD_WEBHOOK_ALERTS,
    }

    missing = [
        name
        for name, value in required_webhooks.items()
        if not value or value == "https://discord.com/api/webhooks/xxx/yyy"
    ]

    if missing:
        raise EnvironmentError(
            f"Missing or unconfigured Discord webhooks: {', '.join(missing)}\n"
            "Please fill in the values in your .env file (see .env.example)."
        )

    if not POLYMARKET_MARKET_ID:
        raise EnvironmentError(
            "POLYMARKET_MARKET_ID is not set in .env.\n"
            "Please provide a valid Polymarket market ID."
        )
