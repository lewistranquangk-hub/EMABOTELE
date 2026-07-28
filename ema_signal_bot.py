"""
EMA SIGNAL BOT — Quét Binance Spot, tìm coin thỏa điều kiện EMA20 (D&W đã đóng)
+ giá chạm/gần EMA200 (H4/H12/D/W), gửi tín hiệu qua Telegram.

CÀI ĐẶT:
    pip install ccxt pandas requests

CHẠY:
    export TELEGRAM_BOT_TOKEN="xxxx:yyyy"
    export TELEGRAM_CHAT_ID="123456789"
    python3 ema_signal_bot.py

Bot sẽ tự lặp quét mỗi 3 tiếng, ưu tiên chạy 20 phút trước khi nến H4 đóng
(H4 đóng lúc 00:00, 04:00, 08:00, 12:00, 16:00, 20:00 UTC).

LƯU Ý QUAN TRỌNG:
- Dữ liệu nến (klines) lấy công khai từ Binance, KHÔNG cần API key/secret.
- Tổng cung (total supply) lấy từ CoinGecko (miễn phí, có rate-limit ~10-30
  req/phút). Với thị trường lớn (~500+ cặp USDT), việc tra cứu supply cho
  từng coin sẽ chậm — script có cache để tránh gọi lại nhiều lần trong 1 lần
  quét, nhưng lần chạy đầu vẫn sẽ mất vài phút.
- Không bịa dữ liệu: nếu không lấy được total supply, trường đó sẽ ghi
  "Không xác định" thay vì số giả.
"""

import os
import time
import math
import logging
from datetime import datetime, timezone, timedelta

import ccxt
import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("ema_signal_bot")

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

QUOTE = "USDT"                 # chỉ quét các cặp .../USDT
EMA_FAST = 20
EMA_SLOW = 200
EMA200_PROXIMITY_PCT = 1.5      # % khoảng cách tối đa tới EMA200
TOTAL_SUPPLY_THRESHOLD = 2_000_000_000  # 2 tỷ

SCAN_TIMEFRAMES_FOR_EMA200 = ["4h", "12h", "1d", "1w"]
KLINE_LIMIT = 260               # đủ để tính EMA200 ổn định

SEND_NO_SIGNAL_MESSAGE = True   # gửi "không có coin thỏa mãn" khi quét xong không có kết quả
REQUEST_SLEEP = 0.25            # nghỉ giữa các request để tránh rate-limit Binance

_coingecko_symbol_map = None    # cache mapping SYMBOL -> coingecko id
_supply_cache = {}              # cache supply theo symbol trong 1 lần chạy


# ---------------------------------------------------------------------------
# EXCHANGE
# ---------------------------------------------------------------------------
def get_exchange():
    return ccxt.binance({
        "enableRateLimit": True,
    })


def get_usdt_spot_symbols(exchange):
    markets = exchange.load_markets()
    symbols = []
    for sym, m in markets.items():
        if not m.get("spot"):
            continue
        if not m.get("active", True):
            continue
        if m.get("quote") != QUOTE:
            continue
        symbols.append(sym)
    return sorted(symbols)


# ---------------------------------------------------------------------------
# KLINES / EMA
# ---------------------------------------------------------------------------
def timeframe_to_timedelta(tf):
    unit = tf[-1]
    n = int(tf[:-1])
    if unit == "m":
        return timedelta(minutes=n)
    if unit == "h":
        return timedelta(hours=n)
    if unit == "d":
        return timedelta(days=n)
    if unit == "w":
        return timedelta(weeks=n)
    raise ValueError(f"Unknown timeframe {tf}")


def fetch_closed_ohlcv(exchange, symbol, timeframe, limit=KLINE_LIMIT):
    """Lấy nến, LOẠI BỎ nến cuối nếu chưa đóng cửa hoàn toàn."""
    raw = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    if not raw:
        return None
    df = pd.DataFrame(raw, columns=["ts", "open", "high", "low", "close", "volume"])
    df["close_time"] = pd.to_datetime(df["ts"], unit="ms", utc=True) + timeframe_to_timedelta(timeframe)
    now = datetime.now(timezone.utc)
    if df.iloc[-1]["close_time"] > now:
        df = df.iloc[:-1]  # nến cuối chưa đóng -> bỏ
    if df.empty:
        return None
    return df


def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


# ---------------------------------------------------------------------------
# TOTAL SUPPLY (CoinGecko)
# ---------------------------------------------------------------------------
def load_coingecko_symbol_map():
    global _coingecko_symbol_map
    if _coingecko_symbol_map is not None:
        return _coingecko_symbol_map
    try:
        resp = requests.get("https://api.coingecko.com/api/v3/coins/list", timeout=15)
        resp.raise_for_status()
        data = resp.json()
        mapping = {}
        for coin in data:
            sym = coin["symbol"].upper()
            # nếu trùng symbol, giữ id đầu tiên gặp (không hoàn hảo nhưng đủ dùng)
            mapping.setdefault(sym, coin["id"])
        _coingecko_symbol_map = mapping
    except Exception as e:
        log.warning(f"Không lấy được danh sách CoinGecko: {e}")
        _coingecko_symbol_map = {}
    return _coingecko_symbol_map


def get_total_supply(base_symbol):
    """Trả về (total_supply hoặc None, is_under_threshold hoặc None)."""
    base_symbol = base_symbol.upper()
    if base_symbol in _supply_cache:
        return _supply_cache[base_symbol]

    mapping = load_coingecko_symbol_map()
    cg_id = mapping.get(base_symbol)
    if not cg_id:
        _supply_cache[base_symbol] = (None, None)
        return None, None

    try:
        resp = requests.get(
            f"https://api.coingecko.com/api/v3/coins/{cg_id}",
            params={
                "localization": "false", "tickers": "false", "market_data": "true",
                "community_data": "false", "developer_data": "false",
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        supply = data.get("market_data", {}).get("total_supply")
        if supply is None:
            _supply_cache[base_symbol] = (None, None)
            return None, None
        under = supply < TOTAL_SUPPLY_THRESHOLD
        _supply_cache[base_symbol] = (supply, under)
        return supply, under
    except Exception as e:
        log.warning(f"Lỗi lấy total supply cho {base_symbol}: {e}")
        _supply_cache[base_symbol] = (None, None)
        return None, None


# ---------------------------------------------------------------------------
# ĐIỀU KIỆN
# ---------------------------------------------------------------------------
def check_symbol(exchange, symbol):
    """Trả về dict tín hiệu nếu thỏa đủ điều kiện, ngược lại None."""
    try:
        df_d = fetch_closed_ohlcv(exchange, symbol, "1d")
        df_w = fetch_closed_ohlcv(exchange, symbol, "1w")
        if df_d is None or len(df_d) < EMA_SLOW or df_w is None or len(df_w) < EMA_FAST:
            return None

        # --- Điều kiện 1: EMA20 D & W (nến đã đóng) ---
        df_d["ema20"] = ema(df_d["close"], EMA_FAST)
        df_w["ema20"] = ema(df_w["close"], EMA_FAST)

        last_d_close = df_d.iloc[-1]["close"]
        last_d_ema20 = df_d.iloc[-1]["ema20"]
        last_w_close = df_w.iloc[-1]["close"]
        last_w_ema20 = df_w.iloc[-1]["ema20"]

        if not (last_d_close > last_d_ema20):
            return None
        if not (last_w_close > last_w_ema20):
            return None

        # --- Xu hướng tăng (đơn giản hoá): giá D trên EMA200 D và EMA20 D dốc lên ---
        df_d["ema200"] = ema(df_d["close"], EMA_SLOW)
        if len(df_d) < EMA_SLOW + 5:
            return None
        ema20_slope_up = df_d.iloc[-1]["ema20"] > df_d.iloc[-5]["ema20"]
        uptrend = (last_d_close > df_d.iloc[-1]["ema200"]) and ema20_slope_up
        if not uptrend:
            return None

        current_price = last_d_close  # giá gần nhất (nến D vừa đóng)

        # --- Điều kiện 2: giá chạm/gần EMA200 trên ít nhất 1 khung ---
        best_match = None  # (timeframe, ema200_value, distance_pct)
        for tf in SCAN_TIMEFRAMES_FOR_EMA200:
            df_tf = fetch_closed_ohlcv(exchange, symbol, tf)
            time.sleep(REQUEST_SLEEP)
            if df_tf is None or len(df_tf) < EMA_SLOW:
                continue
            df_tf["ema200"] = ema(df_tf["close"], EMA_SLOW)
            ema200_val = df_tf.iloc[-1]["ema200"]
            price_ref = df_tf.iloc[-1]["close"]
            if ema200_val <= 0:
                continue
            distance_pct = abs(price_ref - ema200_val) / ema200_val * 100
            if distance_pct <= EMA200_PROXIMITY_PCT:
                if best_match is None or distance_pct < best_match[2]:
                    best_match = (tf, ema200_val, distance_pct)

        if best_match is None:
            return None

        tf_match, ema200_val, distance_pct = best_match

        return {
            "symbol": symbol,
            "price": current_price,
            "ema20_d": last_d_ema20,
            "ema20_w": last_w_ema20,
            "ema200_tf": tf_match,
            "ema200_val": ema200_val,
            "distance_pct": distance_pct,
        }
    except Exception as e:
        log.debug(f"Bỏ qua {symbol}: {e}")
        return None


# ---------------------------------------------------------------------------
# FORMAT & GỬI TELEGRAM
# ---------------------------------------------------------------------------
def format_signal_message(sig, exchange_name="Binance"):
    base = sig["symbol"].split("/")[0]
    supply, under = get_total_supply(base)
    supply_str = f"{supply:,.0f}" if supply is not None else "Không xác định"
    under_str = "Có" if under else ("Không" if under is not None else "Không xác định")

    now_str = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")

    return f"""🔔 TÍN HIỆU MỚI – EMA SIGNAL BOT

Coin: {sig['symbol']}
Sàn: {exchange_name}

Giá hiện tại: ${sig['price']:.6g}
Xu hướng: Tăng

EMA20 Daily: ${sig['ema20_d']:.6g} → Nến D đã đóng trên EMA20
EMA20 Weekly: ${sig['ema20_w']:.6g} → Nến W đã đóng trên EMA20

EMA200 chạm tại khung: {sig['ema200_tf'].upper()}
Giá EMA200: ${sig['ema200_val']:.6g}
Khoảng cách đến EMA200: {sig['distance_pct']:.2f}%

Total Supply: {supply_str} (Dưới 2 tỷ: {under_str})

Thời gian quét: {now_str}

Nhận xét ngắn: Giá đang trong xu hướng tăng và test lại vùng EMA200 trên khung {sig['ema200_tf'].upper()}, đây là vùng hỗ trợ động cần theo dõi phản ứng giá."""


def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Chưa cấu hình TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID — chỉ in ra console.")
        print(text)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log.error(f"Lỗi gửi Telegram: {e}")


# ---------------------------------------------------------------------------
# VÒNG QUÉT CHÍNH
# ---------------------------------------------------------------------------
def run_scan():
    log.info("Bắt đầu quét thị trường...")
    exchange = get_exchange()
    symbols = get_usdt_spot_symbols(exchange)
    log.info(f"Tổng số cặp {QUOTE} cần quét: {len(symbols)}")

    signals = []
    for i, symbol in enumerate(symbols):
        sig = check_symbol(exchange, symbol)
        if sig:
            signals.append(sig)
            log.info(f"✅ Thỏa điều kiện: {symbol}")
        time.sleep(REQUEST_SLEEP)
        if (i + 1) % 50 == 0:
            log.info(f"Đã quét {i + 1}/{len(symbols)}")

    if not signals:
        log.info("Không có coin nào thỏa mãn.")
        if SEND_NO_SIGNAL_MESSAGE:
            send_telegram("Hiện tại không có coin nào thỏa mãn đầy đủ điều kiện.")
        return

    # ưu tiên coin có total supply thấp: xếp theo distance_pct trước, rồi gửi
    for sig in signals:
        msg = format_signal_message(sig)
        send_telegram(msg)
        time.sleep(1)

    log.info(f"Hoàn tất. Số tín hiệu gửi đi: {len(signals)}")


def seconds_until_next_run():
    """Tính thời gian chờ để lần chạy tiếp theo rơi vào ~20 phút trước khi
    nến H4 đóng (H4 đóng lúc 0,4,8,12,16,20h UTC)."""
    now = datetime.now(timezone.utc)
    h4_close_hours = [0, 4, 8, 12, 16, 20]
    candidates = []
    for h in h4_close_hours:
        close_time = now.replace(hour=h, minute=0, second=0, microsecond=0)
        target = close_time - timedelta(minutes=20)
        if target <= now:
            target += timedelta(hours=24) if h == h4_close_hours[-1] else timedelta(0)
        candidates.append(target)
    future = [t for t in candidates if t > now]
    if not future:
        # tất cả target hôm nay đã qua -> lấy target đầu tiên ngày mai
        next_target = candidates[0] + timedelta(days=1)
    else:
        next_target = min(future)
    return max(1, (next_target - now).total_seconds())


def main_loop():
    while True:
        try:
            run_scan()
        except Exception as e:
            log.error(f"Lỗi trong vòng quét: {e}")
        wait_s = seconds_until_next_run()
        log.info(f"Chờ {wait_s/60:.1f} phút tới lần quét tiếp theo...")
        time.sleep(wait_s)


if __name__ == "__main__":
    import sys
    if "--once" in sys.argv:
        # Dùng cho GitHub Actions: chạy 1 lần quét rồi thoát (workflow tự lặp theo cron)
        run_scan()
    else:
        # Dùng cho VPS: chạy 1 lần ngay khi khởi động, sau đó tự lặp theo lịch H4
        main_loop()
