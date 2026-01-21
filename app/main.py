import asyncio
import logging
import os
from telegram.ext import ApplicationBuilder, MessageHandler, CommandHandler, filters

from app.bot.handlers import handle_message, scan_command
from app.bot.scheduler import (
    insidebar_daily_scheduler,
    insidebar_breakout_tracker,
    opposite_15m_scheduler,
    opposite_15m_breakout_tracker,
    terminate_at
)
from app.config.aws_ssm import get_param

# ───────────────────────────────
# Ensure logs directory exists
# ───────────────────────────────
os.makedirs("logs", exist_ok=True)

# ───────────────────────────────
# Logging (APP LEVEL)
# ───────────────────────────────
logging.basicConfig(
    filename="logs/bot.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# Silence noisy libraries
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("telegram.ext").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("s3transfer").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# ───────────────────────────────
# Load secrets from AWS SSM
# ───────────────────────────────
BOT_TOKEN = get_param("/trading-bot/telegram/BOT_TOKEN")
CHAT_ID = get_param("/trading-bot/telegram/CHAT_ID")

# ───────────────────────────────
# Async main function
# ───────────────────────────────
async def main():
    # Build Telegram app
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Add handlers
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CommandHandler("scan", scan_command))

    logging.info("🤖 Telegram bot started (polling enabled)")

    # Schedule background tasks
    tasks = [
        asyncio.create_task(insidebar_daily_scheduler()),
        asyncio.create_task(insidebar_breakout_tracker()),
        asyncio.create_task(opposite_15m_scheduler()),
        asyncio.create_task(opposite_15m_breakout_tracker()),
        asyncio.create_task(terminate_at(target_hour=10, target_minute=30))
    ]

    # Start polling
    try:
        await app.run_polling()
    finally:
        # Gracefully cancel all tasks on shutdown
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

# ───────────────────────────────
# Entry point
# ───────────────────────────────
if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If loop is already running (Jupyter/managed env), schedule main as a task
            asyncio.ensure_future(main())
        else:
            # Run normally
            loop.run_until_complete(main())
    except RuntimeError:
        # Fallback if loop cannot be retrieved
        asyncio.run(main())
