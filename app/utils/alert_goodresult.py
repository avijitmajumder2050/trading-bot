import pandas as pd
import time
import logging
import os
import sys
from datetime import datetime
from dhanhq import DhanContext, dhanhq
from logging.handlers import RotatingFileHandler
import boto3
from io import StringIO
from app.config.settings import S3_BUCKET, AWS_REGION, IST, MAP_FILE_KEY
from app.config.aws_ssm import get_param

# === Logging Setup ===
log_file = "logs/goodresult_alerts.log"
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,  # Change to INFO to reduce debug noise
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        RotatingFileHandler(log_file, maxBytes=5_000_000, backupCount=5, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

logging.info("🚀 Good Result Alerts module loaded")

# === AWS S3 Setup ===
s3 = boto3.client("s3", region_name=AWS_REGION)

# === Load credentials from SSM ===
DHAN_CLIENT_ID = get_param("/dhan/client_id")
DHAN_ACCESS_TOKEN = get_param("/dhan/access_token")

# === Dhan API Setup ===
dhan_context = DhanContext(DHAN_CLIENT_ID, DHAN_ACCESS_TOKEN)
dhan = dhanhq(dhan_context)

# === Utilities ===
def batch_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def fetch_live_data(instrument_ids):
    live_data = {}
    logging.info(f"📡 Fetching live data for {len(instrument_ids)} instruments...")
    for batch in batch_list(instrument_ids, 1000):
        try:
            response = dhan.quote_data(securities={"NSE_EQ": batch})
            if isinstance(response, dict) and "data" in response and "data" in response["data"]:
                batch_data = response["data"]["data"].get("NSE_EQ", {})
                live_data.update({int(k): v for k, v in batch_data.items()})
                logging.info(f"✅ Received {len(batch_data)} quotes in batch")
            else:
                logging.warning("⚠️ Unexpected response structure from Dhan API")
        except Exception as e:
            logging.warning(f"⚠️ Batch fetch error: {e}")
        time.sleep(0.5)
    logging.info(f"📊 Total live quotes fetched: {len(live_data)}")
    return live_data

# === Load CSV from S3 ===
def read_csv_from_s3(key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
    except Exception as e:
        logging.error(f"❌ Failed to read CSV from S3 ({key}): {e}")
        return pd.DataFrame()

# === Daily base from EOD (computed once per day — EOD files only change after close) ===
# Each scan cycle then only needs the live LTP: today's EMA is one step on top of
# yesterday's EMA, identical to re-running ewm(adjust=False) over the whole history.
_daily_base = {}
_daily_base_day = None


def _load_daily_base(instrument_id):
    """prev_high + yesterday's EMA10/20/50 for one stock, cached for the day (None if unavailable)."""
    global _daily_base_day
    today = datetime.now(IST).date()
    if _daily_base_day != today:
        _daily_base.clear()
        _daily_base_day = today
    if instrument_id in _daily_base:
        return _daily_base[instrument_id]

    base = None
    df = read_csv_from_s3(f"eod_data/{instrument_id}.csv")
    if not df.empty:
        df = df.rename(columns=str.lower)
        df = df[["date", "open", "high", "low", "close", "volume"]].dropna()
        df["date"] = pd.to_datetime(df["date"])
        df = df[df["date"].dt.date < today].sort_values("date")   # today's row comes from LTP
        if len(df) >= 9:                                          # 9 history + today = 10 bars
            prev = df.iloc[-1]
            if prev["close"] > prev["open"]:
                prev_high = prev["close"]
            elif prev["close"] < prev["open"]:
                prev_high = prev["open"]
            else:
                prev_high = prev["high"]
            closes = df["close"]
            base = {
                "prev_high": float(prev_high),
                "ema10": float(closes.ewm(span=10, adjust=False).mean().iloc[-1]),
                "ema20": float(closes.ewm(span=20, adjust=False).mean().iloc[-1]),
                "ema50": float(closes.ewm(span=50, adjust=False).mean().iloc[-1]),
            }
    _daily_base[instrument_id] = base
    return base


def load_today_data_with_ema(instrument_id, live):
    base = _load_daily_base(instrument_id)
    if base is None:
        logging.warning(f"⚠️ Missing / short EOD data for {instrument_id}")
        return None

    ltp = live["last_price"]
    step = lambda prev_ema, span: prev_ema + (2 / (span + 1)) * (ltp - prev_ema)
    return {
        "prev_high": base["prev_high"],
        "today_low": live["ohlc"]["low"],
        "ema10": step(base["ema10"], 10),
        "ema20": step(base["ema20"], 20),
        "ema50": step(base["ema50"], 50),
    }


def _load_setup_map():
    df_map = read_csv_from_s3(MAP_FILE_KEY)
    if df_map.empty:
        logging.info("ℹ️ Mapping CSV is empty")
        return df_map
    df_map = df_map[["Stock Name", "Instrument ID", "Market Cap", "Setup_Case"]].dropna()
    df_map["Instrument ID"] = df_map["Instrument ID"].astype(int)
    df_map = df_map[df_map["Setup_Case"].isin(["Case A", "Case B", "Case C"])]
    if df_map.empty:
        logging.info("ℹ️ No instruments with Setup_Case found.")
    return df_map


def _quarterly_breakouts(df_map, live_data):
    """All mapped stocks currently meeting the strong-quarterly breakout rule, best % change first."""
    breakout_rows = []
    for _, row in df_map.iterrows():
        iid = row["Instrument ID"]
        symbol = row["Stock Name"]
        live = live_data.get(iid)
        if not live:
            continue

        eod = load_today_data_with_ema(iid, live)
        if not eod:
            continue

        prev_high = eod["prev_high"]
        today_low = eod["today_low"]
        ema10, ema20, ema50 = round(eod["ema10"], 2), round(eod["ema20"], 2), round(eod["ema50"], 2)
        ltp = live.get("last_price", 0)

        if ltp > prev_high and ema20 > ema50 and (today_low < ema10 or today_low < ema20):
            change = round((ltp - prev_high) / prev_high * 100, 2)
            breakout_rows.append({
                "symbol": symbol,
                "ltp": ltp,
                "prev_high": prev_high,
                "change": change,
                "setup_case": row["Setup_Case"],
                "ema10": ema10,
                "ema20": ema20
            })

    breakout_rows.sort(key=lambda x: x["change"], reverse=True)
    return breakout_rows


def strong_quarterly_symbols(symbols, live_data):
    """
    Which of `symbols` meet the strong-quarterly rule right now — no top-15 cap.
    Uses the caller's live quotes ({security_id(int): dhan quote}), so no extra
    API call, and only evaluates the given symbols.
    """
    df_map = _load_setup_map()
    if df_map.empty:
        return set()
    df_map = df_map[df_map["Stock Name"].isin(symbols)]
    return {b["symbol"] for b in _quarterly_breakouts(df_map, live_data)}


# === Main Alert Function ===
def strong_quarterly_alert():
    df_map = _load_setup_map()
    if df_map.empty:
        return [], []

    instrument_ids = df_map["Instrument ID"].tolist()
    live_data = fetch_live_data(instrument_ids)

    breakout_rows = _quarterly_breakouts(df_map, live_data)
    top_15 = breakout_rows[:15]
    alerts = [f"🔔 {b['symbol']} LTP {b['ltp']} > Prev High {b['prev_high']} (+{b['change']}%)" for b in top_15]

    if alerts:
        logging.info(f"🔔 Top {len(alerts)} alerts prepared")
    else:
        logging.info("ℹ️ No breakout alerts today")

    return alerts, top_15

# === Run Script ===
if __name__ == "__main__":
    logging.info("🚀 Running strong quarterly alert check...")
    alerts, popups = strong_quarterly_alert()
    logging.info(f"Alerts: {alerts}")
