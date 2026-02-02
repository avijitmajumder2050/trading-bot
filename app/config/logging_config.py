import logging
import os

LOG_FILE = os.getenv("BOT_LOG_FILE", "/var/log/trading-bot.log")

logging.basicConfig(
    level=logging.INFO,   # 🔥 THIS is the key
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE)
    ]
)
