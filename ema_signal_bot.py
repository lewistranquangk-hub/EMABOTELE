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
import json
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

VN_TZ = timezone(timedelta(hours=7))  # UTC+7, cố định — không phụ thuộc giờ hệ thống server

# File lưu lại các tín hiệu ĐÃ BÁO (coin + loại + khung + nến cụ thể), để
# không báo trùng. File này được workflow GitHub Actions tự commit ngược lại
# repo sau mỗi lần quét (xem ema-signal.yml) — nhờ đó "nhớ" xuyên suốt các
# lần chạy dù mỗi lần GitHub Actions là môi trường hoàn toàn mới.
STATE_FILE = os.environ.get("STATE_FILE", "sent_signals.json")
STATE_MAX_AGE_DAYS = 35  # tự dọn key cũ hơn số ngày này để file không phình to mãi

EXCHANGE_ID = os.environ.get("EXCHANGE_ID", "binance").lower()  # "binance" hoặc "coinbase"
QUOTE = "USD" if EXCHANGE_ID == "coinbase" else "USDT"  # Coinbase dùng cặp .../USD, Binance dùng .../USDT
EMA_FAST = 20
EMA_SLOW = 200
EMA200_PROXIMITY_PCT = 1.5      # % khoảng cách tối đa tới EMA200
TOTAL_SUPPLY_THRESHOLD = 2_000_000_000  # 2 tỷ (chỉ dùng để HIỂN THỊ, không lọc bỏ coin)

# Stablecoin / tiền pháp định — giá luôn neo quanh $1, tín hiệu EMA trên các
# cặp này là nhiễu, không có ý nghĩa giao dịch -> loại khỏi danh sách quét.
STABLECOIN_BASES = {
    "USDC", "USDT", "USDE", "FDUSD", "TUSD", "DAI", "BUSD", "USDP", "PYUSD",
    "GUSD", "USDD", "EURI", "EUR", "USD1", "AEUR", "USTC", "FRAX", "LUSD",
}

SCAN_TIMEFRAMES_FOR_EMA200 = ["4h", "12h", "1d", "1w"]
TF_LABELS = {"4h": "H4", "12h": "H12", "1d": "D", "1w": "W"}
KLINE_LIMIT = 260               # đủ để tính EMA200 ổn định

SEND_NO_SIGNAL_MESSAGE = True   # gửi "không có coin thỏa mãn" khi quét xong không có kết quả
REQUEST_SLEEP = 0.25            # nghỉ giữa các request để tránh rate-limit Binance

_coingecko_symbol_map = None    # cache mapping SYMBOL -> coingecko id
_supply_cache = {}              # cache supply theo symbol trong 1 lần chạy


# ---------------------------------------------------------------------------
# STATE (chống báo trùng)
# ---------------------------------------------------------------------------
def load_sent_state():
    if not os.path.exists(STATE_FILE):
        return {"sent": {}}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            data.setdefault("sent", {})
            return data
    except Exception as e:
        log.warning(f"Không đọc được {STATE_FILE}, tạo mới: {e}")
        return {"sent": {}}


def save_sent_state(state):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception as e:
        log.error(f"Lỗi lưu {STATE_FILE}: {e}")


def prune_sent_state(state, days=STATE_MAX_AGE_DAYS):
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    sent = state.get("sent", {})
    to_delete = []
    for key, ts_str in sent.items():
        try:
            ts = datetime.fromisoformat(ts_str)
        except Exception:
            to_delete.append(key)
            continue
        if ts < cutoff:
            to_delete.append(key)
    for key in to_delete:
        del sent[key]
    if to_delete:
        log.info(f"Đã dọn {len(to_delete)} tín hiệu cũ (>{days} ngày) khỏi state.")
    return state


# ---------------------------------------------------------------------------
# EXCHANGE
# ---------------------------------------------------------------------------
def get_exchange():
    if EXCHANGE_ID == "coinbase":
        exchange = ccxt.coinbase({
            "enableRateLimit": True,
        })
        return exchange

    # Mặc định: Binance
    exchange = ccxt.binance({
        "enableRateLimit": True,
        "options": {
            "defaultType": "spot",
            "fetchMarkets": ["spot"],  # KHÔNG gọi API Futures (fapi.binance.com) - cũng bị chặn 451 và không cần thiết
        },
    })
    # GitHub Actions (và một số VPS) chạy ở khu vực bị Binance chặn (lỗi 451
    # "restricted location"). data-api.binance.vision là mirror CHÍNH THỨC
    # của Binance dành riêng cho dữ liệu thị trường công khai (không giao
    # dịch), không bị áp dụng giới hạn địa lý này.
    exchange.urls["api"]["public"] = "https://data-api.binance.vision/api/v3"
    return exchange


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
        base = m.get("base", "")
        if base.upper() in STABLECOIN_BASES:
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
# ĐIỀU KIỆN — tách 2 tín hiệu độc lập
# ---------------------------------------------------------------------------
def analyze_symbol(exchange, symbol):
    """Trả về (ema20_signal, ema200_signal) — mỗi cái là dict hoặc None.
    2 tín hiệu này ĐỘC LẬP với nhau, không cần thỏa cả hai cùng lúc.

    - EMA20: kiểm tra D và W RIÊNG (OR), nếu khung nào đóng nến trên EMA20
      thì liệt kê khung đó vào danh sách "matches" của coin.
    - EMA200: kiểm tra từng khung H4/H12/D/W, khung nào gần chạm EMA200 thì
      liệt kê vào "matches" — 1 coin có thể chạm nhiều khung cùng lúc, gộp
      lại báo gọn 1 tin duy nhất cho coin đó.
    """
    ema20_signal = None
    ema200_signal = None
    try:
        df_d = fetch_closed_ohlcv(exchange, symbol, "1d")
        if df_d is None or len(df_d) < EMA_SLOW + 5:
            return None, None

        df_d["ema20"] = ema(df_d["close"], EMA_FAST)
        df_d["ema200"] = ema(df_d["close"], EMA_SLOW)
        last_d_close = df_d.iloc[-1]["close"]
        last_d_ema20 = df_d.iloc[-1]["ema20"]

        df_w = fetch_closed_ohlcv(exchange, symbol, "1w")
        last_w_close = last_w_ema20 = None
        if df_w is not None and len(df_w) >= EMA_FAST:
            df_w["ema20"] = ema(df_w["close"], EMA_FAST)
            last_w_close = df_w.iloc[-1]["close"]
            last_w_ema20 = df_w.iloc[-1]["ema20"]

        # =========================================================
        # TÍN HIỆU A — EMA20: báo riêng từng khung D / W (OR, không bắt buộc cả 2)
        # =========================================================
        ema20_matches = []  # [(tf_label, ema20_val, period_id), ...]
        if last_d_close > last_d_ema20:
            period_id_d = df_d.iloc[-1]["close_time"].strftime("%Y-%m-%d")
            ema20_matches.append(("D", last_d_ema20, period_id_d))
        if last_w_close is not None and last_w_close > last_w_ema20:
            period_id_w = df_w.iloc[-1]["close_time"].strftime("%Y-%m-%d")
            ema20_matches.append(("W", last_w_ema20, period_id_w))
        if ema20_matches:
            ema20_signal = {
                "symbol": symbol,
                "price": last_d_close,
                "matches": ema20_matches,
            }

        # =========================================================
        # TÍN HIỆU B — EMA200: gộp TẤT CẢ khung (H4/H12/D/W) đang chạm
        # =========================================================
        ema20_slope_up = df_d.iloc[-1]["ema20"] > df_d.iloc[-5]["ema20"]
        uptrend = (last_d_close > df_d.iloc[-1]["ema200"]) and ema20_slope_up

        if uptrend:
            ema200_matches = []  # [(tf_label, ema200_val, distance_pct), ...]
            for tf in SCAN_TIMEFRAMES_FOR_EMA200:
                if tf == "1d":
                    df_tf = df_d  # tái sử dụng, khỏi gọi lại API
                else:
                    df_tf = fetch_closed_ohlcv(exchange, symbol, tf)
                    time.sleep(REQUEST_SLEEP)
                if df_tf is None or len(df_tf) < EMA_SLOW:
                    continue
                if "ema200" not in df_tf.columns:
                    df_tf = df_tf.copy()
                    df_tf["ema200"] = ema(df_tf["close"], EMA_SLOW)
                ema200_val = df_tf.iloc[-1]["ema200"]
                price_ref = df_tf.iloc[-1]["close"]
                if ema200_val <= 0:
                    continue
                distance_pct = abs(price_ref - ema200_val) / ema200_val * 100
                if distance_pct <= EMA200_PROXIMITY_PCT:
                    period_id = df_tf.iloc[-1]["close_time"].strftime("%Y-%m-%dT%H:%M")
                    ema200_matches.append((TF_LABELS[tf], ema200_val, distance_pct, period_id))

            if ema200_matches:
                ema200_matches.sort(key=lambda m: m[2])  # gần nhất trước
                ema200_signal = {
                    "symbol": symbol,
                    "price": last_d_close,
                    "matches": ema200_matches,
                }

    except Exception as e:
        log.debug(f"Bỏ qua {symbol}: {e}")

    return ema20_signal, ema200_signal


# ---------------------------------------------------------------------------
# FORMAT & GỬI TELEGRAM
# ---------------------------------------------------------------------------
def _exchange_display_name():
    return "Coinbase" if EXCHANGE_ID == "coinbase" else "Binance"


def _supply_line(sig):
    """Dùng supply đã tra & gắn sẵn vào sig. Báo ngắn gọn: chỉ nói bé/lớn hơn 2 tỷ."""
    supply = sig.get("supply")
    if supply is None:
        return "Supply: N/A"
    return "Supply: bé hơn 2 tỷ" if supply < TOTAL_SUPPLY_THRESHOLD else "Supply: lớn hơn 2 tỷ"


def format_ema20_message(sig):
    now_str = datetime.now(VN_TZ).strftime("%d/%m %H:%M")
    detail = "  |  ".join(f"{tf}: ${val:.6g}" for tf, val, _pid in sig["matches"])
    return (
        f"📈 <b>EMA20 – {sig['symbol']}</b> ({_exchange_display_name()})\n"
        f"Giá: ${sig['price']:.6g}\n"
        f"Đóng trên EMA20: {detail}\n"
        f"{_supply_line(sig)}\n"
        f"⏱ {now_str}"
    )


def format_ema200_message(sig):
    now_str = datetime.now(VN_TZ).strftime("%d/%m %H:%M")
    detail = "  |  ".join(f"{tf} (cách {dist:.2f}%)" for tf, val, dist, _pid in sig["matches"])
    return (
        f"🎯 <b>Chạm EMA200 – {sig['symbol']}</b> ({_exchange_display_name()})\n"
        f"Giá: ${sig['price']:.6g}  |  Xu hướng: Tăng\n"
        f"Chạm tại: {detail}\n"
        f"{_supply_line(sig)}\n"
        f"⏱ {now_str}"
    )


def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Chưa cấu hình TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID — chỉ in ra console.")
        print(text)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=15,
        )
        r.raise_for_status()
    except Exception as e:
        log.error(f"Lỗi gửi Telegram: {e}")


# ---------------------------------------------------------------------------
# VÒNG QUÉT CHÍNH
# ---------------------------------------------------------------------------
def _make_key(symbol, kind, tf, period_id):
    return f"{symbol}|{kind}|{tf}|{period_id}"


def dedupe_signals(signals, kind, sent):
    """Với mỗi coin, chỉ giữ lại những khung (match) CHƯA từng báo cho đúng
    kỳ nến đó. Nếu tất cả khung của coin đã báo rồi -> bỏ hẳn coin đó.
    """
    kept = []
    for sig in signals:
        fresh_matches = []
        for m in sig["matches"]:
            tf, *_rest, period_id = m  # period_id luôn là phần tử cuối
            key = _make_key(sig["symbol"], kind, tf, period_id)
            if key in sent:
                continue  # đã báo đúng kỳ nến này rồi -> bỏ qua
            fresh_matches.append(m)
        if fresh_matches:
            sig = dict(sig)
            sig["matches"] = fresh_matches
            kept.append(sig)
    return kept


def keys_for_signals(signals, kind):
    """Sinh danh sách key 'đã báo' từ danh sách signal CUỐI CÙNG thực sự
    được gửi đi (sau khi đã lọc trùng + lọc supply...). Chỉ những coin thật
    sự gửi Telegram mới bị đánh dấu, tránh đánh dấu nhầm coin bị loại vì lý
    do khác (vd: supply lớn) — để lần sau vẫn được xét lại bình thường.
    """
    keys = []
    for sig in signals:
        for m in sig["matches"]:
            tf, *_rest, period_id = m
            keys.append(_make_key(sig["symbol"], kind, tf, period_id))
    return keys


def attach_supply_info(signals):
    """Tra Total Supply cho từng coin đã thỏa điều kiện, gắn vào sig để hiển thị.
    KHÔNG tự loại bỏ coin nào ở bước này — việc có lọc theo supply hay
    không tùy vào từng loại tín hiệu, xử lý riêng ở run_scan().
    """
    for sig in signals:
        base = sig["symbol"].split("/")[0]
        supply, under = get_total_supply(base)
        sig["supply"] = supply
        sig["supply_under"] = under
    return signals


def run_scan():
    log.info("Bắt đầu quét thị trường...")
    exchange = get_exchange()
    symbols = get_usdt_spot_symbols(exchange)
    log.info(f"Tổng số cặp {QUOTE} cần quét: {len(symbols)}")

    state = load_sent_state()
    state = prune_sent_state(state)
    sent = state["sent"]

    ema20_signals = []
    ema200_signals = []
    for i, symbol in enumerate(symbols):
        s_ema20, s_ema200 = analyze_symbol(exchange, symbol)
        if s_ema20:
            ema20_signals.append(s_ema20)
        if s_ema200:
            ema200_signals.append(s_ema200)
        time.sleep(REQUEST_SLEEP)
        if (i + 1) % 50 == 0:
            log.info(f"Đã quét {i + 1}/{len(symbols)}")

    log.info(f"Trước khi lọc trùng: EMA20={len(ema20_signals)} | EMA200={len(ema200_signals)}")

    # Bỏ qua những khung/coin đã báo đúng kỳ nến này rồi (vd: BOME EMA20 W
    # tuần này đã báo thì không báo lại, tới tuần sau có nến W mới mới báo tiếp)
    ema20_signals = dedupe_signals(ema20_signals, "EMA20", sent)
    ema200_signals = dedupe_signals(ema200_signals, "EMA200", sent)

    log.info(f"Sau khi lọc trùng: EMA20={len(ema20_signals)} | EMA200={len(ema200_signals)}")

    # Tra Total Supply để hiển thị (chỉ tra cho coin còn lại sau khi lọc trùng)
    ema20_signals = attach_supply_info(ema20_signals)
    ema200_signals = attach_supply_info(ema200_signals)

    # CHỈ RIÊNG tín hiệu EMA20: lọc thêm điều kiện Total Supply < 2 tỷ.
    # Biết chắc supply >= 2 tỷ -> loại. Không tra được dữ liệu (None) -> vẫn
    # giữ để báo, không vì thiếu dữ liệu mà bỏ sót tín hiệu.
    # (Tín hiệu EMA200 KHÔNG áp dụng bộ lọc này, vẫn giữ nguyên như cũ.)
    before_ema20 = len(ema20_signals)
    ema20_signals = [s for s in ema20_signals if s.get("supply_under") is not False]
    log.info(f"Lọc Total Supply <2 tỷ (chỉ EMA20): {before_ema20} -> {len(ema20_signals)}")

    if not ema20_signals and not ema200_signals:
        log.info("Không có tín hiệu MỚI nào (có thể đã báo hết trước đó).")
        return

    # Sắp EMA200 theo khoảng cách gần nhất trước (dựa vào khung khớp gần nhất)
    ema200_signals.sort(key=lambda s: s["matches"][0][2])

    now_iso = datetime.now(timezone.utc).isoformat()

    for sig in ema20_signals:
        send_telegram(format_ema20_message(sig))
        time.sleep(1)
    for sig in ema200_signals:
        send_telegram(format_ema200_message(sig))
        time.sleep(1)

    # Đánh dấu đã báo (chỉ những coin THỰC SỰ được gửi) + lưu lại state
    ema20_new_keys = keys_for_signals(ema20_signals, "EMA20")
    ema200_new_keys = keys_for_signals(ema200_signals, "EMA200")
    for key in ema20_new_keys + ema200_new_keys:
        sent[key] = now_iso
    save_sent_state(state)

    log.info(f"Hoàn tất. Đã gửi EMA20: {len(ema20_signals)} | EMA200: {len(ema200_signals)}")


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
