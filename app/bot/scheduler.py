# app/bot/scheduler.py
import asyncio
import logging
import boto3
from datetime import datetime, time
from app.config.settings import IST, INSIDEBAR_SCAN_TIME
from app.bot.telegram_sender import send_telegram_message
from app.scanners.inside_bar_15min_RS80 import run_inside_bar_algo_scan
from app.scanners.inside_bar_algo import track_insidebar_algo_breakouts_bot
from app.utils.get_instance_id import get_instance_id  # your existing function
from app.utils.ec2_launcher import check_csv_and_launch_ec2  # import your function
from app.scanners.nifty_15m_opposite_breakout_scan import (
    build_opposite_ranges,
    scan_nifty_stocks
)
import threading
import random

# --------------------------
# InsideBar 5-min scan state
# --------------------------
insidebar_done = None
insidebar_enabled = False
insidebar_alerted = set()
insidebar_alert_lock = threading.Lock()
insidebar_lock = asyncio.Lock()

# --------------------------
# 15-min Opposite Candle state
# --------------------------
opposite_done = None
opposite_enabled = False
opposite_alerted = set()
opposite_alert_lock = threading.Lock()
opposite_lock = asyncio.Lock()


# --------------------------
# InsideBar daily scan @ 9:31 IST
# --------------------------
async def insidebar_daily_scheduler():
    global insidebar_done, insidebar_enabled
    while True:
        try:
            now = datetime.now(IST)
            today = now.date()

            if insidebar_done != today and now.time() >= INSIDEBAR_SCAN_TIME:
                logging.info("📊 Running InsideBar 5-min daily scan")

                async with insidebar_lock:
                    run_inside_bar_algo_scan(interval=5)

                insidebar_done = today
                insidebar_enabled = True

                with insidebar_alert_lock:
                    insidebar_alerted.clear()

                await send_telegram_message(
                    "📊 <b>InsideBar Scan Completed</b>\n"
                    "⏱ Interval: 5 min\n"
                    "📡 Breakout tracking started"
                )

        except Exception as e:
            logging.error(f"❌ InsideBar scheduler error: {e}")

        await asyncio.sleep(20)


# --------------------------
# InsideBar 5-sec breakout tracker
# --------------------------
async def insidebar_breakout_tracker():
    global insidebar_enabled
    loop = asyncio.get_running_loop()

    while True:
        try:
            if not insidebar_enabled:
                await asyncio.sleep(5)
                continue

            async with insidebar_lock:
                breakouts = await loop.run_in_executor(None, track_insidebar_algo_breakouts_bot)

            if breakouts:
                for hit in breakouts:
                    key = (hit["Stock Name"], hit["Signal"])

                    with insidebar_alert_lock:
                        if key in insidebar_alerted:
                            continue
                        insidebar_alerted.add(key)

                    msg = (
                        f"📌 <b>InsideBar Breakout</b>\n\n"
                        f"📈 <b>{hit['Stock Name']}</b>\n"
                        f"💰 Price: {hit['Price']}\n"
                        f"🎯 Entry: {hit['Entry']}\n"
                        f"🛑 SL: {hit['SL']}\n"
                        f"📦 Qty: {hit['Quantity']}\n"
                        f"💸 Risk: ₹{hit['Expected Loss']}\n"
                        f"🕒 {datetime.now().strftime('%H:%M:%S')}"
                    )

                    await send_telegram_message(msg)

        except Exception as e:
            logging.error(f"❌ InsideBar breakout tracker error: {e}")

        await asyncio.sleep(5)


# --------------------------
# 15-min Opposite Candle scheduler @ 9:46 IST
# --------------------------
async def opposite_15m_scheduler():
    global opposite_done, opposite_enabled
    TARGET_TIME = time(9, 46)

    while True:
        try:
            now = datetime.now(IST)
            today = now.date()

            if opposite_done != today and now.time() >= TARGET_TIME:
                logging.info("📊 Building 15-min Opposite Candle ranges")

                async with opposite_lock:
                    build_opposite_ranges()

                opposite_done = today
                opposite_enabled = True

                with opposite_alert_lock:
                    opposite_alerted.clear()

                await send_telegram_message(
                    "📊 <b>15-Min Opposite Candle Scan Completed</b>\n"
                    "⏱ First 2 candles captured\n"
                    "📡 Live breakout tracking started"
                )

        except Exception as e:
            logging.error(f"❌ Opposite scheduler error: {e}")

        await asyncio.sleep(20)


# --------------------------
# 15-min Opposite Candle breakout tracker (5-sec)
# --------------------------
async def opposite_15m_breakout_tracker():
    global opposite_enabled
    loop = asyncio.get_running_loop()

    while True:
        try:
            if not opposite_enabled:
                await asyncio.sleep(5)
                continue

            async with opposite_lock:
                signals = await loop.run_in_executor(None, scan_nifty_stocks)

            if signals:
                for hit in signals:
                    key = (hit["Stock Name"], hit["Signal"])

                    with opposite_alert_lock:
                        if key in opposite_alerted:
                            continue
                        opposite_alerted.add(key)

                    msg = (
                        f"🔥 <b>15-Min Opposite Breakout</b>\n\n"
                        f"📈 <b>{hit['Stock Name']}</b>\n"
                        f"📊 Signal: {hit['Signal']}\n"
                        f"💰 Price: {hit['Price']}\n"
                        f"🎯 Entry: {hit['Entry']}\n"
                        f"🛑 SL: {hit['SL']}\n"
                        f"📦 Qty: {hit['Quantity']}\n"
                        f"💸 Risk: ₹{hit['Expected Loss']}\n"
                        f"💼 Exposure: ₹{round(hit['Exposure'], 2)}\n"
                        f"🕒 {datetime.now().strftime('%H:%M:%S')}"
                    )

                    await send_telegram_message(msg)

        except Exception as e:
            logging.error(f"❌ Opposite breakout tracker error: {e}")

        await asyncio.sleep(5)


# --------------------------
# EC2 Termination Scheduler
# --------------------------
def terminate_instance(instance_id, region="ap-south-1"):
    try:
        ec2 = boto3.client("ec2", region_name=region)
        ec2.terminate_instances(InstanceIds=[instance_id])
        logging.info(f"✅ Termination command sent for instance: {instance_id}")
    except Exception as e:
        logging.error(f"❌ Termination failed: {e}")

async def terminate_at(target_hour=10, target_minute=40):
    instance_id = get_instance_id()
    if not instance_id or instance_id == "UNKNOWN":
        logging.error("❌ Cannot terminate — instance ID not found")
        return

    while True:
        now = datetime.now()
        if now.hour == target_hour and now.minute == target_minute:
            logging.info(f"🕓 Time reached {target_hour}:{target_minute}, terminating instance...")
            terminate_instance(instance_id)
            break
        await asyncio.sleep(20)

async def terminate_after_delay(delay_minutes=3):
    """Terminate EC2 after X minutes from launch."""
    await asyncio.sleep(delay_minutes * 60)
    instance_id = get_instance_id()
    if not instance_id or instance_id == "UNKNOWN":
        logging.error("❌ Cannot terminate — instance ID not found")
        return
    logging.info(f"⏳ {delay_minutes} minutes elapsed. Terminating EC2 {instance_id}...")
    await send_telegram_message(f"⏳ {delay_minutes} minutes elapsed. Terminating EC2 {instance_id}...")
    terminate_instance(instance_id)
# --------------------------
# EC2 Launch Scheduler @ configurable time
# --------------------------

# --------------------------
# EC2 Launch Scheduler @ 9:55 IST
# --------------------------




async def ec2_launch_scheduler(launch_hour=9, launch_minute=55):
    """
    Runs once per day at given time.
    Launches EC2 only if CSV has >= 3 rows.
    """
    EC2_LAUNCH_TIME = time(launch_hour, launch_minute)

    last_run_date = None

    while True:
        try:
            now = datetime.now(IST)
            today = now.date()

            if last_run_date != today and now.time() >= EC2_LAUNCH_TIME:
                logging.info(
                    f"🚀 EC2 launch scheduler triggered at "
                    f"{EC2_LAUNCH_TIME.strftime('%H:%M')}"
                )

                result = check_csv_and_launch_ec2()
                logging.info(f"📊 EC2 launch result: {result}")
                 # 🔥 AUTO-TERMINATE IN 3 MINUTES
                delay = random.randint(2, 5)
                asyncio.create_task(terminate_after_delay(delay_minutes=delay))

                last_run_date = today

        except Exception as e:
            logging.error(f"❌ EC2 launch scheduler error: {e}")

        await asyncio.sleep(20)
