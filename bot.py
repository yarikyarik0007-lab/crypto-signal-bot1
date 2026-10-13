import asyncio
import datetime
import html
import json
import logging
import math
import os
import time

import aiohttp
import asyncpg
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiohttp import web

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
SIGNAL_CHANNEL_ID = os.getenv("SIGNAL_CHANNEL_ID", "").strip()
ADMIN_USER_ID = os.getenv("ADMIN_USER_ID", "").strip()
STRATEGY_VERSION = "v0.5.1-experimental"

COINS = ["BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "ADA", "AVAX", "LINK", "SUI", "ZEC"]

BINGX_BASE = "https://open-api.bingx.com"
BINGX_SYMBOL = {coin: f"{coin}-USDT" for coin in COINS}

STATUS_EMOJI = {
    "NONE": "⚪", "WATCH": "👀", "INTEREST_ZONE": "🔵",
    "WAIT": "🟡", "LONG": "🟢", "SHORT": "🔴",
}

DATA_NOTE = "📡 <i>Реальные данные BingX · вход и балл появятся позже</i>"

_MARKET_CACHE: dict[str, dict] = {}
_SESSION: aiohttp.ClientSession | None = None
_DB_POOL: asyncpg.Pool | None = None
_OUTBOX_READY = False
_BACKTEST_ALL_RUNNING = False
# C4.1: защита live-сигналов от повторного логирования.
_LIVE_SIGNALS_SENT: set[str] = set()
_LIVE_SIGNALS_ORDER: list[str] = []
LIVE_SIGNALS_MAX_HISTORY = 1000
LIVE_SIGNALS_INTERVAL_SECONDS = 15 * 60

# C4.3 Этап 4: параметры обработки transactional outbox.
OUTBOX_POLL_INTERVAL_SECONDS = 30
OUTBOX_CLAIM_LEASE_SECONDS = 300
OUTBOX_MAX_ATTEMPTS = 8
OUTBOX_MAX_BACKOFF_SECONDS = 1800




def is_candle_closed(candle_t_ms: int, interval_ms: int = 4 * 3600 * 1000) -> bool:
    """True, если свеча с timestamp открытия t_ms уже закрыта."""
    if not candle_t_ms:
        return False
    now_ms = int(time.time() * 1000)
    delta = now_ms - candle_t_ms
    if delta < 0:
        return False
    return delta >= interval_ms


async def _get_json(url: str, params: dict) -> dict | None:
    try:
        async with _SESSION.get(url, params=params, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            if resp.status != 200:
                return None
            return await resp.json(content_type=None)
    except Exception as e:
        logging.warning(f"BingX request failed ({url}, {params}): {e}")
        return None


def _parse_klines(payload: dict) -> list[dict]:
    data = payload.get("data") if payload else None
    if not data:
        return []
    candles = []
    for item in data:
        try:
            if isinstance(item, dict):
                o = float(item.get("open"))
                h = float(item.get("high"))
                l = float(item.get("low"))
                c = float(item.get("close"))
                v = float(item.get("volume", 0) or 0)
            elif isinstance(item, (list, tuple)) and len(item) >= 6:
                o, h, l, c, v = (float(item[1]), float(item[2]),
                                 float(item[3]), float(item[4]), float(item[5]))
            else:
                continue
            candles.append({"o": o, "h": h, "l": l, "c": c, "v": v})
        except (TypeError, ValueError):
            continue
    return candles


def _sma(values: list[float], length: int) -> float | None:
    if len(values) < length:
        return None
    return sum(values[-length:]) / length


def _ema_series(values: list[float], period: int) -> list[float]:
    if len(values) < period:
        return []
    series = [sum(values[:period]) / period]
    multiplier = 2 / (period + 1)
    for price in values[period:]:
        series.append((price - series[-1]) * multiplier + series[-1])
    return series


def _ema_last(values: list[float], period: int) -> float | None:
    series = _ema_series(values, period)
    return series[-1] if series else None


def compute_rsi(closes: list[float], period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        change = closes[i] - closes[i - 1]
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 1)


def compute_macd(closes: list[float], fast=12, slow=26, signal=9) -> dict | None:
    if len(closes) < slow + signal:
        return None
    ema_fast = _ema_series(closes, fast)
    ema_slow = _ema_series(closes, slow)
    offset = len(ema_fast) - len(ema_slow)
    macd_line = [ema_fast[i + offset] - ema_slow[i] for i in range(len(ema_slow))]
    if len(macd_line) < signal:
        return None
    signal_line = _ema_series(macd_line, signal)
    macd_val = macd_line[-1]
    signal_val = signal_line[-1]
    return {"macd": macd_val, "signal": signal_val, "hist": macd_val - signal_val}


def compute_atr(candles: list[dict], period: int = 14) -> float | None:
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h, l, prev_c = candles[i]["h"], candles[i]["l"], candles[i - 1]["c"]
        trs.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr


# ============================================================
# ШАГ 5 (исправлен): Определение рыночного режима
# ADX считается на ВСЕЙ истории до idx, а не на окне 100 свечей
# ============================================================

ADX_TREND_THRESHOLD = 20.0
ATR_VOLATILE_PCT = 4.0


def compute_adx(candles: list[dict], period: int = 14) -> float | None:
    """ADX по Уайлдеру. Возвращает последнее значение ADX."""
    if len(candles) < period * 2:
        return None

    trs, plus_dm, minus_dm = [], [], []
    for i in range(1, len(candles)):
        h, l = candles[i]["h"], candles[i]["l"]
        prev_h, prev_l, prev_c = candles[i - 1]["h"], candles[i - 1]["l"], candles[i - 1]["c"]

        tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
        up_move = h - prev_h
        down_move = prev_l - l

        plus_dm.append(up_move if (up_move > down_move and up_move > 0) else 0.0)
        minus_dm.append(down_move if (down_move > up_move and down_move > 0) else 0.0)
        trs.append(tr)

    def wilder(values: list[float], p: int) -> list[float]:
        if len(values) < p:
            return []
        result = [sum(values[:p])]
        for v in values[p:]:
            result.append(result[-1] - result[-1] / p + v)
        return result

    atr_series = wilder(trs, period)
    plus_di_series = wilder(plus_dm, period)
    minus_di_series = wilder(minus_dm, period)

    if not atr_series or not plus_di_series or not minus_di_series:
        return None

    n = min(len(atr_series), len(plus_di_series), len(minus_di_series))
    atr_series = atr_series[-n:]
    plus_di_series = plus_di_series[-n:]
    minus_di_series = minus_di_series[-n:]

    dx_series = []
    for p_di, m_di in zip(plus_di_series, minus_di_series):
        s = p_di + m_di
        dx_series.append(100 * abs(p_di - m_di) / s if s else 0.0)

    if len(dx_series) < period:
        return None
    # ADX — обычное усреднение Уайлдера над DX (0-100)
    adx = sum(dx_series[:period]) / period
    for dx in dx_series[period:]:
        adx = (adx * (period - 1) + dx) / period
    return adx

def classify_regime(candles: list[dict], idx: int) -> str:
    """TREND_UP / TREND_DOWN / RANGE / VOLATILE / UNKNOWN.
    ADX считается на всей истории до idx включительно (без утечки будущего)."""
    history = candles[:idx + 1]
    if len(history) < 30:
        return "UNKNOWN"

    adx = compute_adx(history)
    atr = compute_atr(history)
    price = history[-1]["c"]
    atr_pct = (atr / price * 100) if (atr and price) else 0.0

    if atr_pct > ATR_VOLATILE_PCT:
        return "VOLATILE"

    if adx is None:
        return "UNKNOWN"

    if adx < ADX_TREND_THRESHOLD:
        return "RANGE"

    closes = [c["c"] for c in history]
    sma20 = _sma(closes, 20)
    sma50 = _sma(closes, 50)

    if sma20 and sma50:
        if sma20 > sma50:
            return "TREND_UP"
        return "TREND_DOWN"

    return "UNKNOWN"


def describe_ema(price: float, ema20: float | None, ema50: float | None) -> str:
    if ema20 is None or ema50 is None:
        return "Недостаточно данных"
    if price > ema20 > ema50:
        return "Цена выше EMA20 и EMA50"
    if price < ema20 < ema50:
        return "Цена ниже EMA20 и EMA50"
    return "Цена между EMA20 и EMA50"


# MACD description helper; keep each return on its own line.
def describe_macd(macd: dict | None) -> str:
    if macd is None:
        return "Недостаточно данных"
    if macd["hist"] > 0:
        return "Бычье пересечение"
    if macd["hist"] < 0:
        return "Медвежье пересечение"
    return "Нейтрально"


def describe_volume(volumes: list[float]) -> str:
    if len(volumes) < 21:
        return "Недостаточно данных"
    avg = sum(volumes[-21:-1]) / 20
    last = volumes[-1]
    if avg == 0:
        return "Недостаточно данных"
    if last > avg * 1.2:
        return "Растёт"
    if last < avg * 0.8:
        return "Падает"
    return "Средний"


async def fetch_coin_data(coin: str) -> dict:
    symbol = BINGX_SYMBOL[coin]

    klines = await _get_json(
        f"{BINGX_BASE}/openApi/swap/v2/quote/klines",
        {"symbol": symbol, "interval": "4h", "limit": 100},
    )
    candles = _parse_klines(klines) if klines else []
    closes = [c["c"] for c in candles]
    volumes = [c["v"] for c in candles]

    price = closes[-1] if closes else None
    change_24h = None
    if len(closes) >= 7 and closes[-7]:
        change_24h = round((closes[-1] - closes[-7]) / closes[-7] * 100, 2)

    trend = "Недостаточно данных"
    sma20, sma50 = _sma(closes, 20), _sma(closes, 50)
    if sma20 and sma50:
        if sma20 > sma50 * 1.002:
            trend = "Восходящий"
        elif sma20 < sma50 * 0.998:
            trend = "Нисходящий"
        else:
            trend = "Боковой"

    if price is None:
        price_payload = await _get_json(
            f"{BINGX_BASE}/openApi/swap/v2/quote/price", {"symbol": symbol}
        )
        try:
            price = float(price_payload["data"]["price"])
        except (TypeError, KeyError, ValueError):
            price = None

    rsi = compute_rsi(closes)
    macd = compute_macd(closes)
    atr = compute_atr(candles)
    ema20 = _ema_last(closes, 20)
    ema50 = _ema_last(closes, 50)

    return {
        "coin": coin,
        "status": "NONE",
        "price": price,
        "change_24h": change_24h,
        "trend": trend,
        "rsi": rsi,
        "macd_desc": describe_macd(macd),
        "macd_hist": macd["hist"] if macd else None,
        "ema_desc": describe_ema(price, ema20, ema50) if price else "Недостаточно данных",
        "atr": atr,
        "atr_pct": round(atr / price * 100, 2) if atr and price else None,
        "volume_desc": describe_volume(volumes),
        "updated_at": time.time(),
        "ok": price is not None,
    }


async def fetch_historical_klines(symbol: str, interval: str = "4h", target: int = 1000) -> list[dict]:
    """Постранично скачивает длинную историю свечей у BingX."""
    all_candles: list[dict] = []
    end_time = None
    for _ in range(10):
        if len(all_candles) >= target:
            break
        params = {"symbol": symbol, "interval": interval, "limit": 500}
        if end_time:
            params["endTime"] = end_time
        payload = await _get_json(f"{BINGX_BASE}/openApi/swap/v2/quote/klines", params)
        raw = payload.get("data") if payload else None
        if not raw:
            break
        batch = []
        for item in raw:
            try:
                if isinstance(item, dict):
                    t = int(item.get("time"))
                    o, h, l, c = (float(item.get("open")), float(item.get("high")),
                                  float(item.get("low")), float(item.get("close")))
                    v = float(item.get("volume", 0) or 0)
                elif isinstance(item, (list, tuple)) and len(item) >= 6:
                    t = int(item[0])
                    o, h, l, c, v = (float(item[1]), float(item[2]),
                                      float(item[3]), float(item[4]), float(item[5]))
                else:
                    continue
                batch.append({"t": t, "o": o, "h": h, "l": l, "c": c, "v": v})
            except (TypeError, ValueError):
                continue
        if not batch:
            break
        batch.sort(key=lambda x: x["t"])
        if all_candles:
            existing_min_t = min(x["t"] for x in all_candles)
            batch = [x for x in batch if x["t"] < existing_min_t]
        if not batch:
            break
        all_candles = batch + all_candles
        end_time = batch[0]["t"] - 1
        if len(batch) < 10:
            break
    all_candles.sort(key=lambda x: x["t"])
    return all_candles[-target:] if len(all_candles) > target else all_candles


BT_ZONE_LOOKBACK = 40
BT_PIVOT_WINDOW = 2
BT_RR_TARGET = 2.0
BT_FEE_R = 0.05
BT_BREAKOUT_MARGIN = 0.005
BT_RETEST_TOLERANCE = 0.006
BT_RETEST_WINDOW = 8


def find_pivot_level(candles: list[dict], end_idx: int, lookback: int, side: str,
                      pivot_window: int = BT_PIVOT_WINDOW) -> float | None:
    """Самый значимый пивот в окне."""
    start = max(end_idx - lookback, pivot_window)
    pivots = []
    for idx in range(start, end_idx - 2):
        lo, hi = idx - pivot_window, idx + pivot_window + 1
        if lo < 0 or hi > len(candles):
            continue
        segment = candles[lo:hi]
        center = candles[idx]
        if side == "low":
            if center["l"] == min(c["l"] for c in segment):
                pivots.append(center["l"])
        else:
            if center["h"] == max(c["h"] for c in segment):
                pivots.append(center["h"])
    if not pivots:
        return None
    return max(pivots) if side == "high" else min(pivots)


def run_backtest(candles: list[dict], zone_lookback=BT_ZONE_LOOKBACK, rr_target=BT_RR_TARGET,
                  fee_r=BT_FEE_R, breakout_margin=BT_BREAKOUT_MARGIN,
                  retest_tolerance=BT_RETEST_TOLERANCE, retest_window=BT_RETEST_WINDOW) -> list[dict]:
    """Стратегия: пробой значимого уровня + ретест. Цель 2R."""
    n = len(candles)
    trades = []
    pending: list[dict] = []
    i = zone_lookback + BT_PIVOT_WINDOW + 2

    while i < n - 1:
        sig = candles[i]

        pending = [p for p in pending if i - p["breakout_i"] <= retest_window]

        triggered = None
        for p in pending:
            if p["dir"] == "LONG":
                retest_touch = sig["l"] <= p["level"] * (1 + retest_tolerance)
                holds = sig["c"] > p["level"]
                bullish = sig["c"] > sig["o"]
                if retest_touch and holds and bullish:
                    triggered = p
                    break
            else:
                retest_touch = sig["h"] >= p["level"] * (1 - retest_tolerance)
                holds = sig["c"] < p["level"]
                bearish = sig["c"] < sig["o"]
                if retest_touch and holds and bearish:
                    triggered = p
                    break

        if triggered:
            pending = [p for p in pending if p is not triggered]
            direction = triggered["dir"]
            level = triggered["level"]

            # Фильтр: торгуем только SHORT в TREND_DOWN
            regime = classify_regime(candles, i)
            if regime != "TREND_DOWN" or direction != "SHORT":
                pending = [p for p in pending if p is not triggered]
                i += 1
                continue

            if i + 1 >= n:
                break
            entry = candles[i + 1]["o"]

            if direction == "LONG":
                sl = min(level * (1 - 0.005), sig["l"] * 0.998)
                risk = entry - sl
            else:
                sl = max(level * (1 + 0.005), sig["h"] * 1.002)
                risk = sl - entry

            if risk <= 0:
                i += 1
                continue

            tp = entry + rr_target * risk if direction == "LONG" else entry - rr_target * risk

            exit_r, exit_i = None, None
            for k in range(i + 1, n):
                c = candles[k]
                if direction == "LONG":
                    hit_sl, hit_tp = c["l"] <= sl, c["h"] >= tp
                else:
                    hit_sl, hit_tp = c["h"] >= sl, c["l"] <= tp
                if hit_sl:
                    exit_r, exit_i = -1.0, k
                    break
                if hit_tp:
                    exit_r, exit_i = rr_target, k
                    break

            if exit_r is None:
                last = candles[-1]["c"]
                exit_r = (last - entry) / risk if direction == "LONG" else (entry - last) / risk
                exit_i = n - 1

            exit_r -= fee_r
            trades.append({
                "dir": direction,
                "entry_i": i + 1,
                "exit_i": exit_i,
                "r": round(exit_r, 2),
                "regime": regime,
                "entry_time": candles[i + 1].get("t"),
                "exit_time": candles[exit_i].get("t"),
            })
            pending = []
            i = exit_i + 1
            continue

        resistance = find_pivot_level(candles, i, zone_lookback, "high")
        support = find_pivot_level(candles, i, zone_lookback, "low")

        recent_vol = [c["v"] for c in candles[max(0, i - 10):i]]
        avg_vol = sum(recent_vol) / len(recent_vol) if recent_vol else 0
        vol_ok = avg_vol == 0 or sig["v"] >= avg_vol

        if resistance and sig["c"] > resistance * (1 + breakout_margin) and vol_ok:
            already_pending = any(p["dir"] == "LONG" and p["level"] == resistance for p in pending)
            if not already_pending:
                pending.append({"level": resistance, "dir": "LONG", "breakout_i": i})

        if support and sig["c"] < support * (1 - breakout_margin) and vol_ok:
            already_pending = any(p["dir"] == "SHORT" and p["level"] == support for p in pending)
            if not already_pending:
                pending.append({"level": support, "dir": "SHORT", "breakout_i": i})

        i += 1

    return trades


def evaluate_last_candle(candles: list[dict], diagnostics: dict | None = None) -> dict | None:
    """Checks the existing live SHORT strategy; optional counters explain rejected setups."""
    stats = diagnostics if diagnostics is not None else {}
    # Each evaluation must start with clean counters; callers may reuse a dict.
    stats["history"] = {}
    stats["latest_stats"] = {}
    stats.pop("latest", None)
    stats.pop("blocking_setup", None)
    diagnostic_index = -1

    def count(key: str) -> None:
        bucket = stats["latest_stats"] if diagnostic_index == len(candles) - 1 else stats["history"]
        bucket[key] = bucket.get(key, 0) + 1

    if not candles:
        stats["result"] = "no_candles"
        return None
    if len(candles) < 100:
        stats["result"] = "insufficient_candles"
        stats["closed_candles"] = len(candles)
        return None

    n = len(candles)
    last_idx = n - 1
    candle_time = candles[last_idx].get("t")
    if candle_time is None:
        stats["result"] = "missing_last_candle_timestamp"
        return None

    latest = candles[last_idx]
    latest_support = find_pivot_level(candles, last_idx, BT_ZONE_LOOKBACK, "low")
    latest_resistance = find_pivot_level(candles, last_idx, BT_ZONE_LOOKBACK, "high")
    recent_volumes = [c["v"] for c in candles[max(0, last_idx - 10):last_idx]]
    latest_avg_volume = sum(recent_volumes) / len(recent_volumes) if recent_volumes else 0
    latest_vol_ok = latest_avg_volume == 0 or latest["v"] >= latest_avg_volume
    latest_regime = classify_regime(candles, last_idx)

    def distance_pct(price: float, level: float | None) -> float | None:
        if level is None or level == 0:
            return None
        return (price - level) / level * 100

    latest_short_breakout = bool(latest_support and latest["c"] < latest_support * (1 - BT_BREAKOUT_MARGIN))
    latest_long_breakout = bool(latest_resistance and latest["c"] > latest_resistance * (1 + BT_BREAKOUT_MARGIN))
    stats["latest"] = {
        "candle_time": candle_time, "regime": latest_regime,
        "volume": latest.get("v"), "avg_volume_10": latest_avg_volume, "vol_ok": latest_vol_ok,
        "support": latest_support, "resistance": latest_resistance,
        "distance_to_support_pct": distance_pct(latest["c"], latest_support),
        "distance_to_resistance_pct": distance_pct(latest["c"], latest_resistance),
        "short_breakout": latest_short_breakout, "short_breakout_vol_ok": latest_short_breakout and latest_vol_ok,
        "long_breakout": latest_long_breakout, "long_breakout_vol_ok": latest_long_breakout and latest_vol_ok,
        "result": "evaluating_latest_candle",
    }

    pending: list[dict] = []
    i = BT_ZONE_LOOKBACK + BT_PIVOT_WINDOW + 2

    while i <= last_idx:
        diagnostic_index = i
        sig = candles[i]
        pending = [p for p in pending if i - p["breakout_i"] <= BT_RETEST_WINDOW]

        triggered = None
        for p in pending:
            if p["dir"] == "LONG":
                retest_touch = sig["l"] <= p["level"] * (1 + BT_RETEST_TOLERANCE)
                holds = sig["c"] > p["level"]
                bullish = sig["c"] > sig["o"]
                if retest_touch and holds and bullish:
                    triggered = p
                    break
            else:
                retest_touch = sig["h"] >= p["level"] * (1 - BT_RETEST_TOLERANCE)
                holds = sig["c"] < p["level"]
                bearish = sig["c"] < sig["o"]
                if retest_touch and holds and bearish:
                    triggered = p
                    break

        if triggered is not None:
            count("retest_triggered")
            pending = [p for p in pending if p is not triggered]
            direction = triggered["dir"]
            level = triggered["level"]
            regime = classify_regime(candles, i)

            if direction != "SHORT":
                count("retest_rejected_direction")
                i += 1
                continue
            if regime != "TREND_DOWN":
                count("retest_rejected_regime_" + str(regime))
                i += 1
                continue

            if i == last_idx:
                entry = sig["c"]
                sl = max(level * (1 + 0.005), sig["h"] * 1.002)
                risk = sl - entry
                if risk <= 0:
                    count("retest_rejected_invalid_risk")
                    stats["result"] = "invalid_risk_on_latest_candle"
                    stats["latest"]["result"] = "invalid_risk_on_latest_candle"
                    stats["latest"]["rejected_signal"] = {
                        "direction": "SHORT", "entry_price": entry,
                        "sl_price": sl, "risk": risk,
                    }
                    return None
                tp = entry - BT_RR_TARGET * risk
                stats["result"] = "signal_found"
                stats["latest"]["result"] = "signal_found"
                stats["latest"]["signal"] = {"direction": "SHORT", "entry_price": entry, "sl_price": sl, "tp_price": tp, "rr": BT_RR_TARGET}
                return {
                    "direction": "SHORT",
                    "level": level,
                    "entry_price": entry,
                    "sl_price": sl,
                    "tp_price": tp,
                    "rr": BT_RR_TARGET,
                    "regime": regime,
                    "candle_time": candle_time,
                    "reason": "TREND_DOWN + пробой уровня + ретест + объём",
                }

            if i + 1 > last_idx:
                stats["result"] = "no_next_candle"
                return None

            entry = candles[i + 1]["o"]
            sl = max(level * (1 + 0.005), sig["h"] * 1.002)
            risk = sl - entry
            if risk <= 0:
                count("historical_retest_invalid_risk")
                i += 1
                continue
            tp = entry - BT_RR_TARGET * risk

            pending = []
            exit_i = None
            for k in range(i + 1, last_idx + 1):
                candle = candles[k]
                if candle["h"] >= sl:
                    exit_i = k
                    break
                if candle["l"] <= tp:
                    exit_i = k
                    break

            if exit_i is None:
                count("historical_retest_unresolved")
                stats["result"] = "historical_setup_unresolved"
                stats["blocking_setup"] = {
                    "status": "unresolved", "direction": direction,
                    "trigger_candle_time": sig.get("t"), "age_candles": last_idx - i,
                    "entry_price": entry, "sl_price": sl, "tp_price": tp,
                }
                stats["latest"]["result"] = "blocked_by_historical_setup_unresolved"
                return None
            if exit_i >= last_idx:
                count("historical_retest_exited_on_latest_candle")
                stats["result"] = "historical_setup_exited_on_latest_candle"
                stats["blocking_setup"] = {
                    "status": "exited_on_latest_candle", "direction": direction,
                    "trigger_candle_time": sig.get("t"), "age_candles": last_idx - i,
                    "entry_price": entry, "sl_price": sl, "tp_price": tp,
                }
                stats["latest"]["result"] = "blocked_by_historical_setup_exit_on_latest"
                return None

            count("historical_retest_not_latest")
            i = exit_i + 1
            continue

        resistance = find_pivot_level(candles, i, BT_ZONE_LOOKBACK, "high")
        support = find_pivot_level(candles, i, BT_ZONE_LOOKBACK, "low")

        recent_vol = [c["v"] for c in candles[max(0, i - 10):i]]
        avg_vol = sum(recent_vol) / len(recent_vol) if recent_vol else 0
        vol_ok = avg_vol == 0 or sig["v"] >= avg_vol

        long_breakout = bool(resistance and sig["c"] > resistance * (1 + BT_BREAKOUT_MARGIN))
        short_breakout = bool(support and sig["c"] < support * (1 - BT_BREAKOUT_MARGIN))
        if long_breakout and not vol_ok:
            count("breakout_rejected_volume_long")
        if short_breakout and not vol_ok:
            count("breakout_rejected_volume_short")

        if long_breakout and vol_ok:
            count("breakout_detected_long")
            already_pending = any(
                p["dir"] == "LONG" and p["level"] == resistance for p in pending
            )
            if not already_pending:
                pending.append({"level": resistance, "dir": "LONG", "breakout_i": i})

        if short_breakout and vol_ok:
            count("breakout_detected_short")
            already_pending = any(
                p["dir"] == "SHORT" and p["level"] == support for p in pending
            )
            if not already_pending:
                pending.append({"level": support, "dir": "SHORT", "breakout_i": i})

        i += 1

    latest_stats = stats["latest_stats"]
    # Prefer the most actionable reason for the latest candle. Retest evaluation
    # happens before breakout detection, so it takes precedence when it triggered.
    if latest_stats.get("retest_triggered"):
        result = "latest_retest_rejected_or_not_signal"
    elif latest_stats.get("breakout_rejected_volume_short") or (
        latest_short_breakout and not latest_vol_ok
    ):
        result = "latest_short_breakout_rejected_by_volume"
    elif latest_stats.get("breakout_detected_short") or latest_short_breakout:
        result = "latest_short_breakout_seen_but_no_valid_retest"
    elif latest_long_breakout and not latest_vol_ok:
        result = "latest_long_breakout_rejected_by_volume"
    elif latest_stats.get("breakout_detected_long") or latest_long_breakout:
        result = "latest_only_long_breakout_but_strategy_is_short_only"
    else:
        result = "latest_no_qualifying_breakout"
    stats["latest"]["result"] = result
    stats["result"] = result
    return None



async def save_live_signal(sig: dict, source: str = "live") -> int:
    """Сохраняет сигнал с указанным source. Возвращает ID или -1 при дубле/ошибке."""
    if _DB_POOL is None:
        logging.error("save_live_signal skipped: PostgreSQL is not connected")
        return -1
    if source == "live" and not _OUTBOX_READY:
        logging.error(
            "save_live_signal skipped: Outbox schema is not ready; "
            "refusing to create a live signal without a durable delivery task"
        )
        return -1
    try:
        coin = str(sig.get("coin", "")).upper().strip()
        direction = str(sig.get("direction", "")).upper().strip()
        entry = float(sig.get("entry_price"))
        sl = float(sig.get("sl_price", sig.get("stop_loss")))
        tp = float(sig.get("tp_price", sig.get("take_profit")))
        rr = float(sig.get("rr"))
        regime = str(sig.get("regime", "UNKNOWN"))
        reason = str(sig.get("reason", sig.get("entry_reason", "")))
        source = str(source).strip().lower()
        if source not in ("live", "test"):
            raise ValueError("source must be live or test")
        if not coin or direction not in ("LONG", "SHORT"):
            raise ValueError("missing coin or invalid direction")
        if not all(math.isfinite(v) and v > 0 for v in (entry, sl, tp, rr)):
            raise ValueError("entry/SL/TP/RR must be finite positive numbers")
        if direction == "SHORT" and not (sl > entry > tp):
            raise ValueError("SHORT requires SL > entry > TP")
        if direction == "LONG" and not (tp > entry > sl):
            raise ValueError("LONG requires TP > entry > SL")
        async with _DB_POOL.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", coin)
                existing = await conn.fetchval("""
                    SELECT id FROM signals
                    WHERE coin = $1 AND source = $2 AND status = 'ACTIVE'
                    LIMIT 1
                """, coin, source)
                if existing is not None:
                    logging.info("Live signal skipped: active signal already exists for %s (id=%s)", coin, existing)
                    return -1
                cooling = await conn.fetchval("""
                    SELECT id FROM signals
                    WHERE coin = $1 AND source = $2
                      AND (status IN ('WIN', 'LOSS')
                           OR (status = 'CLOSED' AND result_r IS NOT NULL))
                      AND closed_at >= NOW() - INTERVAL '4 hours'
                    ORDER BY closed_at DESC LIMIT 1
                """, coin, source)
                if cooling is not None:
                    logging.info("Live signal skipped: 4h cooldown for %s after id=%s", coin, cooling)
                    return -1
                row = await conn.fetchrow("""
                    INSERT INTO signals
                        (coin, direction, entry_price, stop_loss, take_profit,
                         status, entry_reason, strategy_version, source, regime)
                    VALUES ($1, $2, $3, $4, $5, 'ACTIVE', $6, $7, $8, $9)
                    ON CONFLICT DO NOTHING
                    RETURNING id
                """, coin, direction, entry, sl, tp, reason, STRATEGY_VERSION, source, regime)
                if row is None:
                    logging.info("Live signal insert ignored by database constraint for %s", coin)
                    return -1
                signal_id = int(row["id"])
                payload = {
                    "coin": coin,
                    "direction": direction,
                    "entry_price": entry,
                    "sl_price": sl,
                    "tp_price": tp,
                    "rr": rr,
                    "regime": regime,
                    "reason": reason,
                }
                await conn.execute("""
                    INSERT INTO outbox (signal_id, kind, payload)
                    VALUES ($1, 'telegram_channel', $2)
                """, signal_id, json.dumps(payload))
                logging.info(
                    "Live signal and outbox task saved: id=%s coin=%s direction=%s",
                    signal_id, coin, direction,
                )
                return signal_id
    except Exception:
        logging.exception("save_live_signal failed")
        return -1


async def notify_channel(sig: dict) -> bool:
    """Публикует сигнал в канал; ошибки не прерывают live-цикл."""
    if not SIGNAL_CHANNEL_ID:
        logging.error("notify_channel skipped: SIGNAL_CHANNEL_ID is empty")
        return False
    try:
        coin = str(sig.get("coin", "UNKNOWN")).upper()
        direction = str(sig.get("direction", "")).upper()
        entry = float(sig.get("entry_price"))
        sl = float(sig.get("sl_price", sig.get("stop_loss")))
        tp = float(sig.get("tp_price", sig.get("take_profit")))
        rr = float(sig.get("rr"))
        regime = str(sig.get("regime", "UNKNOWN"))
        if direction not in ("LONG", "SHORT") or not all(
            math.isfinite(v) and v > 0 for v in (entry, sl, tp, rr)
        ):
            raise ValueError("invalid signal values")
        if direction == "SHORT" and not (sl > entry > tp):
            raise ValueError("invalid SHORT entry/SL/TP relationship")
        if direction == "LONG" and not (tp > entry > sl):
            raise ValueError("invalid LONG entry/SL/TP relationship")
        bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        try:
            test_prefix = (
                "🧪 <b>ТЕСТОВЫЙ СИГНАЛ (не торговать) 🧪</b>\n\n"
                if sig.get("reason") == "TESTSIGNAL" else ""
            )
            text = test_prefix + (
                f"📣 <b>CRYPTO SIGNAL · {coin}</b>\n\n"
                f"Направление: <b>{direction}</b>\n"
                f"Entry: <code>{entry:.8g}</code>\n"
                f"SL: <code>{sl:.8g}</code>\n"
                f"TP: <code>{tp:.8g}</code>\n"
                f"RR: <b>1:{rr:.2f}</b>\n"
                f"Режим: <b>{regime}</b>"
            )
            await bot.send_message(chat_id=SIGNAL_CHANNEL_ID, text=text)
        finally:
            await bot.session.close()
        logging.info("Live signal published: coin=%s direction=%s", coin, direction)
        return True
    except Exception:
        logging.exception("notify_channel failed")
        return False


async def outbox_worker() -> None:
    """Доставляет PENDING-задачи Outbox с ограниченными повторами и backoff."""
    while True:
        if _DB_POOL is None:
            await asyncio.sleep(OUTBOX_POLL_INTERVAL_SECONDS)
            continue

        task = None
        try:
            # Короткая транзакция атомарно забирает одну задачу.
            # Lease не даёт другому worker забрать её, пока выполняется отправка.
            async with _DB_POOL.acquire() as conn:
                async with conn.transaction():
                    task = await conn.fetchrow(
                        """
                        WITH candidate AS (
                            SELECT id
                            FROM outbox
                            WHERE status = 'PENDING'
                              AND kind = 'telegram_channel'
                              AND next_attempt_at <= NOW()
                            ORDER BY next_attempt_at, id
                            LIMIT 1
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE outbox AS o
                        SET attempts = o.attempts + 1,
                            next_attempt_at = NOW() + ($1::int * INTERVAL '1 second')
                        FROM candidate
                        WHERE o.id = candidate.id
                        RETURNING o.id, o.signal_id, o.payload, o.attempts
                        """,
                        OUTBOX_CLAIM_LEASE_SECONDS,
                    )
        except Exception:
            logging.exception("Outbox worker failed to claim a task")
            await asyncio.sleep(OUTBOX_POLL_INTERVAL_SECONDS)
            continue

        if task is None:
            await asyncio.sleep(OUTBOX_POLL_INTERVAL_SECONDS)
            continue

        outbox_id = int(task["id"])
        attempts = int(task["attempts"])
        error_text = None
        sent = False

        try:
            payload = task["payload"]
            # asyncpg обычно возвращает JSONB как str без пользовательского codec.
            if isinstance(payload, str):
                payload = json.loads(payload)
            if not isinstance(payload, dict):
                raise ValueError("Outbox payload must be a JSON object")
            sent = await notify_channel(payload)
            if not sent:
                raise RuntimeError("notify_channel returned False")
        except Exception as exc:
            error_text = str(exc)[:2000]
            logging.warning(
                "Outbox delivery failed: id=%s signal_id=%s attempt=%s error=%s",
                outbox_id, task["signal_id"], attempts, error_text,
            )

        try:
            async with _DB_POOL.acquire() as conn:
                if sent:
                    await conn.execute(
                        """
                        UPDATE outbox
                        SET status = 'SENT',
                            sent_at = NOW(),
                            last_error = NULL
                        WHERE id = $1 AND status = 'PENDING'
                        """,
                        outbox_id,
                    )
                    logging.info(
                        "Outbox delivery succeeded: id=%s signal_id=%s attempts=%s",
                        outbox_id, task["signal_id"], attempts,
                    )
                else:
                    if attempts >= OUTBOX_MAX_ATTEMPTS:
                        status = "FAILED"
                        delay_seconds = 0
                    else:
                        status = "PENDING"
                        delay_seconds = min(
                            30 * (2 ** max(0, attempts - 1)),
                            OUTBOX_MAX_BACKOFF_SECONDS,
                        )
                    await conn.execute(
                        """
                        UPDATE outbox
                        SET status = $2,
                            last_error = $3,
                            next_attempt_at = NOW() + ($4::int * INTERVAL '1 second')
                        WHERE id = $1 AND status = 'PENDING'
                        """,
                        outbox_id, status, error_text or "Unknown delivery error",
                        delay_seconds,
                    )
                    logging.info(
                        "Outbox task rescheduled: id=%s status=%s attempts=%s delay_seconds=%s",
                        outbox_id, status, attempts, delay_seconds,
                    )
        except Exception:
            # Если Telegram принял сообщение, а запись SENT не сохранилась,
            # задача может быть отправлена повторно после lease. Это at-least-once delivery.
            logging.exception("Outbox worker failed to persist delivery result: id=%s", outbox_id)


async def outbox_worker_supervisor() -> None:
    """Перезапускает outbox_worker при неожиданном завершении."""
    while True:
        try:
            await outbox_worker()
        except asyncio.CancelledError:
            logging.info("Outbox worker supervisor cancelled")
            raise
        except Exception:
            logging.exception("outbox_worker crashed, restarting in 10s")
            await asyncio.sleep(10)


async def check_live_signals() -> None:
    """C4.1: ищет live-сигналы по всем монетам; только логирование."""
    found_count = 0
    checked_count = 0

    for coin in COINS:
        checked_count += 1
        try:
            symbol = BINGX_SYMBOL[coin]
            candles = await fetch_historical_klines(symbol, interval="4h", target=200)

            if not candles:
                logging.warning("Live signals: %s — no candles received", coin)
                continue

            last_t = candles[-1].get("t")
            if not last_t:
                logging.warning(
                    "Live signals: %s — invalid candle timestamp",
                    coin
                )
                continue

            logging.info(
                "Live check: coin=%s last_candle_t=%s closed=%s",
                coin, last_t, is_candle_closed(last_t)
            )

            if not is_candle_closed(last_t):
                closed_candles = candles[:-1]
            else:
                closed_candles = candles

            if len(closed_candles) < 100:
                logging.warning(
                    "Live signals: %s — insufficient closed candles: %s",
                    coin, len(closed_candles)
                )
                continue

            diagnostics: dict = {}
            sig = await asyncio.to_thread(evaluate_last_candle, closed_candles, diagnostics)
            if sig is None:
                logging.info(
                    "Live signal diagnostics: coin=%s result=%s counters=%s",
                    coin, diagnostics.get("result", "no_signal"), diagnostics
                )
                continue

            if sig.get("direction") != "SHORT" or sig.get("regime") != "TREND_DOWN":
                continue
            sig["coin"] = coin

            candle_time = sig.get("candle_time")
            if candle_time is None:
                continue

            key = f"{coin}:{candle_time}"
            if key in _LIVE_SIGNALS_SENT:
                continue

            found_count += 1
            signal_id = await save_live_signal(sig)
            if signal_id > 0:
                # Mark the candle only after the database confirms the signal and outbox row were saved.
                _LIVE_SIGNALS_SENT.add(key)
                _LIVE_SIGNALS_ORDER.append(key)

                while len(_LIVE_SIGNALS_ORDER) > LIVE_SIGNALS_MAX_HISTORY:
                    oldest_key = _LIVE_SIGNALS_ORDER.pop(0)
                    _LIVE_SIGNALS_SENT.discard(oldest_key)

                logging.info(
                    "LIVE SIGNAL SAVED (outbox queued): id=%s coin=%s candle_time=%s",
                    signal_id, coin, candle_time,
                )
                logging.info(
                    "LIVE SIGNAL: coin=%s dir=%s entry=%.8f sl=%.8f tp=%.8f "
                    "regime=%s candle_time=%s rr=%.2f reason=%s",
                    coin, sig["direction"], sig["entry_price"], sig["sl_price"],
                    sig["tp_price"], sig["regime"], sig["candle_time"],
                    sig["rr"], sig["reason"]
                )
            else:
                logging.info(
                    "Live signal not saved (signal_id=%s; duplicate, cooldown, or DB error): coin=%s candle_time=%s",
                    signal_id, coin, candle_time,
                )

        except Exception:
            logging.exception("Live signals check failed for coin=%s", coin)

    logging.info(
        "Live signals check: checked %s coins, found %s signals",
        checked_count, found_count
    )


def run_backtest_baseline(candles: list[dict], rr_target: float = 2.0,
                          fee_r: float = BT_FEE_R) -> list[dict]:
    """Baseline: входим SHORT на каждой свече в TREND_DOWN, без логики пробоя.
    SL = 1% от цены, TP = rr_target * risk. Цель — сравнить с основной стратегией."""
    n = len(candles)
    trades = []
    i = 50  # пропускаем первые 50 свечей для расчёта ADX

    while i < n - 1:
        # Определяем режим только для свечей, где он валиден
        regime = classify_regime(candles, i)
        if regime != "TREND_DOWN":
            i += 1
            continue

        sig = candles[i]
        entry = candles[i + 1]["o"]
        risk = entry * 0.01  # 1% от цены
        if risk <= 0:
            i += 1
            continue

        sl = entry + risk
        tp = entry - rr_target * risk  # SHORT

        exit_r, exit_i = None, None
        for k in range(i + 1, n):
            c = candles[k]
            hit_sl = c["h"] >= sl
            hit_tp = c["l"] <= tp
            if hit_sl:
                exit_r, exit_i = -1.0, k
                break
            if hit_tp:
                exit_r, exit_i = rr_target, k
                break

        if exit_r is None:
            last = candles[-1]["c"]
            exit_r = (entry - last) / risk
            exit_i = n - 1

        exit_r -= fee_r
        trades.append({
            "dir": "SHORT",
            "entry_i": i + 1,
            "exit_i": exit_i,
            "r": round(exit_r, 2),
            "regime": "TREND_DOWN",
            "entry_time": candles[i + 1].get("t"),
            "exit_time": candles[exit_i].get("t"),
        })
        i = exit_i + 1  # одна сделка за раз, как в основной стратегии

    return trades


def summarize_trades(trades: list[dict]) -> dict | None:
    if not trades:
        return None
    wins = [t["r"] for t in trades if t["r"] > 0]
    losses = [t["r"] for t in trades if t["r"] <= 0]
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    max_win_streak = cur_win = 0
    max_loss_streak = cur_loss = 0
    for t in trades:
        if t["r"] > 0:
            cur_win += 1
            cur_loss = 0
        else:
            cur_loss += 1
            cur_win = 0
        max_win_streak = max(max_win_streak, cur_win)
        max_loss_streak = max(max_loss_streak, cur_loss)
    return {
        "count": len(trades),
        "win_rate": round(len(wins) / len(trades) * 100, 1),
        "avg_r": round(sum(t["r"] for t in trades) / len(trades), 2),
        "profit_factor": round(gross_win / gross_loss, 2) if gross_loss > 0 else None,
        "max_win_streak": max_win_streak,
        "max_loss_streak": max_loss_streak,
        "total_r": round(sum(t["r"] for t in trades), 2),
        "long_count": sum(1 for t in trades if t["dir"] == "LONG"),
        "short_count": sum(1 for t in trades if t["dir"] == "SHORT"),
    }


def summarize_by_regime(trades: list[dict]) -> dict:
    """Группирует сделки по режимам и считает статистику по каждому."""
    buckets: dict[str, list[dict]] = {}
    for t in trades:
        r = t.get("regime", "UNKNOWN")
        buckets.setdefault(r, []).append(t)
    return {regime: summarize_trades(ts) for regime, ts in buckets.items()}


def summarize_by_regime_and_dir(trades: list[dict]) -> dict:
    """Группирует сделки по комбинации (режим, направление)."""
    buckets: dict[str, list[dict]] = {}
    for t in trades:
        r = t.get("regime", "UNKNOWN")
        d = t.get("dir", "UNKNOWN")
        key = f"{r}|{d}"
        buckets.setdefault(key, []).append(t)
    return {key: summarize_trades(ts) for key, ts in buckets.items()}


def summarize_by_quarter(trades: list[dict]) -> dict:
    """Группирует сделки по кварталам (YYYY-QN)."""
    import datetime
    buckets: dict[str, list[dict]] = {}
    for t in trades:
        ts = t.get("entry_time")
        if ts is None:
            continue
        # BingX отдаёт timestamp в миллисекундах
        dt = datetime.datetime.utcfromtimestamp(ts / 1000)
        q = (dt.month - 1) // 3 + 1
        key = f"{dt.year}-Q{q}"
        buckets.setdefault(key, []).append(t)
    return {key: summarize_trades(ts) for key, ts in buckets.items()}


def summarize_by_month(trades: list[dict]) -> dict:
    """Группирует сделки по месяцам (YYYY-MM)."""
    import datetime
    buckets: dict[str, list[dict]] = {}
    for t in trades:
        ts = t.get("entry_time")
        if ts is None:
            continue
        dt = datetime.datetime.utcfromtimestamp(ts / 1000)
        key = f"{dt.year}-{dt.month:02d}"
        buckets.setdefault(key, []).append(t)
    return {key: summarize_trades(ts) for key, ts in buckets.items()}


def split_trades_by_time(trades: list[dict], in_sample_ratio: float = 0.75) -> tuple[list[dict], list[dict]]:
    """Делит сделки по временному диапазону: первые 75% истории и последние 25%."""
    dated = [t for t in trades if t.get("entry_time") is not None]
    if len(dated) < 4:
        return dated, []
    dated.sort(key=lambda t: t["entry_time"])
    first_ts = dated[0]["entry_time"]
    last_ts = dated[-1]["entry_time"]
    cutoff_ts = first_ts + (last_ts - first_ts) * in_sample_ratio
    is_trades = [t for t in dated if t["entry_time"] < cutoff_ts]
    oos_trades = [t for t in dated if t["entry_time"] >= cutoff_ts]
    if len(is_trades) < 2 or len(oos_trades) < 2:
        return dated, []
    return is_trades, oos_trades


async def refresh_all_market_data():
    results = await asyncio.gather(*(fetch_coin_data(c) for c in COINS), return_exceptions=True)
    for coin, result in zip(COINS, results):
        if isinstance(result, Exception):
            logging.warning(f"Failed to refresh {coin}: {result}")
            continue
        _MARKET_CACHE[coin] = result


async def market_data_loop():
    """Обновляет кэш каждые 5 минут. Проверяет live-сигналы каждые 15 минут."""
    loop = asyncio.get_running_loop()
    next_live_check = 0.0
    iteration = 0

    while True:
        iteration += 1
        try:
            await refresh_all_market_data()

            now = loop.time()
            if now >= next_live_check:
                next_live_check = loop.time() + LIVE_SIGNALS_INTERVAL_SECONDS
                await check_live_signals()

            logging.info(
                "Market loop heartbeat: iteration=%s, next_live_check_in=%ss",
                iteration, int(next_live_check - loop.time())
            )

        except Exception:
            logging.exception("Market data loop failed")

        await asyncio.sleep(300)


async def market_data_supervisor():
    """Перезапускает market_data_loop при неожиданном завершении."""
    while True:
        try:
            await market_data_loop()
        except asyncio.CancelledError:
            logging.info("Market data supervisor cancelled")
            raise
        except Exception:
            logging.exception("market_data_loop crashed, restarting in 10s")
            await asyncio.sleep(10)


def get_all_states() -> dict:
    return {c: _MARKET_CACHE.get(c, {"coin": c, "status": "NONE", "price": None,
                                      "change_24h": None, "trend": "н/д", "ok": False})
            for c in COINS}


def get_coin_state(coin: str) -> dict:
    return get_all_states()[coin]


def _group_lines(states: dict, statuses: tuple) -> list:
    result = []
    for st in statuses:
        coins = [c for c in COINS if states[c]["status"] == st]
        if coins:
            result.append(f"{STATUS_EMOJI[st]} " + " · ".join(coins))
    return result


def render_main_menu(states: dict) -> str:
    active = _group_lines(states, ("LONG", "SHORT"))
    forming = _group_lines(states, ("INTEREST_ZONE", "WAIT"))
    lines = ["🤖 <b>Crypto Signals</b>", DATA_NOTE, "", "🔥 <b>Активные:</b>"]
    lines += active if active else ["пока нет"]
    lines += ["", "👀 <b>Формируются:</b>"]
    lines += forming if forming else ["пока нет"]
    lines += ["", "Выберите монету 👇", "Что значат цвета — в «Памятке»"]
    return "\n".join(lines)


def render_coin_card(state: dict) -> str:
    coin = state["coin"]
    status = state["status"]
    lines = [f"🪙 <b>{coin}</b> · {STATUS_EMOJI[status]}", DATA_NOTE, ""]

    if not state.get("ok"):
        lines.append("⏳ Не удалось получить данные с BingX, попробуйте «Обновить».")
        return "\n".join(lines)

    change = state["change_24h"]
    change_str = f"{change:+.2f}%" if change is not None else "н/д"
    lines += [
        f"💰 ${state['price']:,.2f}".replace(",", " "),
        f"📊 24ч: {change_str}",
        f"📈 Тренд: {state['trend']}",
        "",
        "Подробный технический анализ — кнопка «Детали».",
        "",
        "Вход, стоп, тейк и балл появятся позже.",
    ]
    return "\n".join(lines)


def render_details(state: dict) -> str:
    coin = state["coin"]
    if not state.get("ok"):
        return f"📋 <b>Детали · {coin}</b>\n{DATA_NOTE}\n\nДанные недоступны, попробуйте «Обновить»."

    rsi = state["rsi"]
    rsi_str = f"{rsi}" if rsi is not None else "недостаточно данных"
    atr_str = (
        f"${state['atr']:.2f} ({state['atr_pct']}% от цены)"
        if state.get("atr") and state.get("atr_pct") is not None
        else "недостаточно данных"    )
    return "\n".join([
        f"📋 <b>Детали · {coin}</b>",
        DATA_NOTE,
        "",
        "📊 <b>Технический анализ</b>",
        f"RSI (14): {rsi_str}",
        f"MACD: {state['macd_desc']}",
        f"EMA: {state['ema_desc']}",
        f"Объём: {state['volume_desc']}",
        f"ATR (14): {atr_str}",
        "",
        "Таймфрейм: 4 часа · свечи с BingX Futures",
    ])


def render_legend() -> str:
    return "\n".join([
        "ℹ️ <b>Памятка</b>",
        "",
        "Сейчас бот показывает реальную цену, тренд и технические",
        "индикаторы с BingX. Цветные статусы появятся позже.",
        "",
        "<b>Индикаторы в «Деталях»</b>",
        "RSI — перекупленность/перепроданность (0–100)",
        "MACD — соотношение быстрой и медленной скользящих",
        "EMA — положение цены относительно средних за 20 и 50 свечей",
        "ATR — средняя волатильность (пригодится для стопа)",
    ])


def render_backtest(coin: str, candles_count: int, stats: dict | None) -> str:
    lines = [f"🧪 <b>Бэктест · {coin}</b>", DATA_NOTE, ""]
    lines.append(f"История: {candles_count} свечей по 4ч (~{candles_count // 6} дней)")
    lines.append("Стратегия: пробой уровня + ретест, цель 2R")
    lines.append("")

    if not stats:
        lines.append("За этот период сделок по правилам стратегии не было.")
        return "\n".join(lines)

    lines += [
        f"Сделок: {stats['count']} (LONG {stats['long_count']} · SHORT {stats['short_count']})",
        f"Win Rate: {stats['win_rate']}%",
        f"Средний результат: {stats['avg_r']:+.2f}R",
        f"Profit Factor: {stats['profit_factor'] if stats['profit_factor'] is not None else '∞'}",
        f"Суммарный результат: {stats['total_r']:+.2f}R",
        f"Макс. серия побед: {stats['max_win_streak']}",
        f"Макс. серия убытков: {stats['max_loss_streak']}",
        "",
    ]

    if stats["count"] < 20:
        lines.append("⚠️ Сделок мало — это ознакомительный прогон, а не статистика.")
    lines.append("Комиссия учтена приблизительно (−0.05R за сделку).")
    return "\n".join(lines)


def render_backtest_all(per_coin: list[dict], overall: dict | None, all_trades: list[dict] | None = None, baseline_stats: dict | None = None) -> str:
    lines = ["🧪 <b>Бэктест по всем монетам</b>", DATA_NOTE, ""]
    lines.append(f"Версия стратегии: {STRATEGY_VERSION}")
    lines.append("Стратегия: пробой уровня + ретест, цель 2R")
    lines.append("История: до 4000 свечей по 4ч на монету (~666 дней)")
    lines.append("")

    if not overall:
        lines.append("За этот период сделок по правилам стратегии не было.")
        return "\n".join(lines)

    lines += [
        f"📊 <b>Итого по всем монетам</b>",
        f"Сделок: {overall['count']} (LONG {overall['long_count']} · SHORT {overall['short_count']})",
        f"Win Rate: {overall['win_rate']}%",
        f"Средний результат: {overall['avg_r']:+.2f}R",
        f"Profit Factor: {overall['profit_factor'] if overall['profit_factor'] is not None else '∞'}",
        f"Суммарный результат: {overall['total_r']:+.2f}R",
        f"Макс. серия побед: {overall['max_win_streak']}",
        f"Макс. серия убытков: {overall['max_loss_streak']}",
        "",
        "📋 <b>По монетам</b>",
    ]

    for row in per_coin:
        if row["stats"] is None:
            lines.append(f"{row['coin']}: нет сделок")
        else:
            s = row["stats"]
            lines.append(
                f"{row['coin']}: {s['count']} сделок, {s['avg_r']:+.2f}R сред., "
                f"{s['total_r']:+.2f}R итого"
            )

    if all_trades:
        regime_stats = summarize_by_regime(all_trades)
        if regime_stats:
            lines += ["", "🎯 <b>По рыночным режимам</b>"]
            regime_order = ["TREND_UP", "TREND_DOWN", "RANGE", "VOLATILE", "UNKNOWN"]
            for reg in regime_order:
                s = regime_stats.get(reg)
                if not s:
                    continue
                lines.append(
                    f"{reg}: {s['count']} сделок, WR {s['win_rate']}%, "
                    f"avg {s['avg_r']:+.2f}R, PF {s['profit_factor'] if s['profit_factor'] is not None else '∞'}"
                )

    if all_trades:
        regime_dir_stats = summarize_by_regime_and_dir(all_trades)
        if regime_dir_stats:
            lines += ["", "🎯🎯 <b>По режимам + направлению</b>"]
            regime_order = ["TREND_UP", "TREND_DOWN", "RANGE", "VOLATILE", "UNKNOWN"]
            dir_order = ["LONG", "SHORT"]
            for reg in regime_order:
                for d in dir_order:
                    s = regime_dir_stats.get(f"{reg}|{d}")
                    if not s or s["count"] < 3:
                        continue
                    lines.append(
                        f"{reg} + {d}: {s['count']} сделок, "
                        f"WR {s['win_rate']}%, avg {s['avg_r']:+.2f}R"
                    )

    if all_trades:
        # Walk-forward: только TREND_DOWN + SHORT по кварталам
        td_shorts = [t for t in all_trades
                     if t.get("regime") == "TREND_DOWN" and t.get("dir") == "SHORT"]
        q_stats = summarize_by_quarter(td_shorts)
        if q_stats:
            lines += ["", "📅 <b>TREND_DOWN+SHORT по кварталам</b>"]
            for q in sorted(q_stats.keys()):
                s = q_stats[q]
                if not s:
                    continue
                lines.append(
                    f"{q}: {s['count']} сделок, WR {s['win_rate']}%, "
                    f"avg {s['avg_r']:+.2f}R, итого {s['total_r']:+.2f}R"
                )

    # Разбивка TREND_DOWN+SHORT по месяцам
    if all_trades:
        td_shorts = [t for t in all_trades
                     if t.get("regime") == "TREND_DOWN" and t.get("dir") == "SHORT"]
        m_stats = summarize_by_month(td_shorts)
        if m_stats:
            lines += ["", "📅 <b>TREND_DOWN+SHORT по месяцам</b>"]
            for m in sorted(m_stats.keys()):
                s = m_stats[m]
                if not s:
                    continue
                lines.append(
                    f"{m}: {s['count']} сделок, WR {s['win_rate']}%, "
                    f"avg {s['avg_r']:+.2f}R"
                )

    # Baseline
    if baseline_stats:
        lines += ["", "⚖️ <b>Baseline (шорт в TREND_DOWN без логики пробоя)</b>"]
        lines.append(
            f"Сделок: {baseline_stats['count']}, WR {baseline_stats['win_rate']}%, "
            f"avg {baseline_stats['avg_r']:+.2f}R, PF "
            f"{baseline_stats['profit_factor'] if baseline_stats['profit_factor'] is not None else '∞'}, "
            f"итого {baseline_stats['total_r']:+.2f}R"
        )

    # Walk-forward: in-sample vs out-of-sample
    if all_trades:
        is_trades, oos_trades = split_trades_by_time(all_trades, 0.75)
        if is_trades and oos_trades:
            is_stats = summarize_trades(is_trades)
            oos_stats = summarize_trades(oos_trades)
            lines += ["", "🔬 <b>Walk-forward (in-sample 75% / out-of-sample 25%)</b>"]
            if is_stats:
                lines.append(
                    f"IS: {is_stats['count']} сделок, WR {is_stats['win_rate']}%, "
                    f"avg {is_stats['avg_r']:+.2f}R, PF "
                    f"{is_stats['profit_factor'] if is_stats['profit_factor'] is not None else '∞'}, "
                    f"итого {is_stats['total_r']:+.2f}R"
                )
            if oos_stats:
                lines.append(
                    f"OOS: {oos_stats['count']} сделок, WR {oos_stats['win_rate']}%, "
                    f"avg {oos_stats['avg_r']:+.2f}R, PF "
                    f"{oos_stats['profit_factor'] if oos_stats['profit_factor'] is not None else '∞'}, "
                    f"итого {oos_stats['total_r']:+.2f}R"
                )
            if is_stats and oos_stats:
                is_avg = is_stats["avg_r"]
                oos_avg = oos_stats["avg_r"]
                if is_avg > 0.15 and oos_avg > 0.15:
                    lines.append("✅ Эдж подтверждён на OOS")
                elif is_avg > 0.15 and oos_avg > 0:
                    lines.append("⚠️ OOS слабее IS, но в плюсе — приемлемо")
                elif is_avg > 0.15 and oos_avg <= 0:
                    lines.append("❌ OOS в минусе — эдж может быть подогнан")
                else:
                    lines.append("⚠️ Эдж не подтверждён по порогу avg > +0.15R")

    if overall["count"] < 100:
        lines += ["", "⚠️ Суммарной выборки всё ещё немного — выводы предварительные."]
    lines.append("Комиссия учтена приблизительно (−0.05R за сделку).")
    return "\n".join(lines)


def render_news_placeholder() -> str:
    return "📰 <b>Новости</b>\n\nМодуль новостного анализа будет подключён позже."


async def get_stats(source: str) -> dict:
    """Возвращает статистику завершённых сделок выбранного источника."""
    if source not in ("backtest", "live"):
        raise ValueError("source должен быть backtest или live")
    if _DB_POOL is None:
        raise RuntimeError("PostgreSQL не подключён")

    async with _DB_POOL.acquire() as conn:
        if source == "backtest":
            summary = await conn.fetchrow("""
                SELECT
                    COUNT(*) AS total_signals,
                    COUNT(*) FILTER (
                        WHERE status = 'CLOSED' AND result_r IS NOT NULL
                    ) AS closed_count,
                    COUNT(*) FILTER (
                        WHERE status = 'CLOSED' AND result_r > 0
                    ) AS wins,
                    COUNT(*) FILTER (
                        WHERE status = 'CLOSED' AND result_r < 0
                    ) AS losses,
                    COUNT(*) FILTER (
                        WHERE status = 'CLOSED' AND result_r = 0
                    ) AS breakeven,
                    COUNT(*) FILTER (
                        WHERE status = 'CLOSED' AND direction = 'LONG'
                    ) AS long_count,
                    COUNT(*) FILTER (
                        WHERE status = 'CLOSED' AND direction = 'SHORT'
                    ) AS short_count,
                    COALESCE(SUM(result_r) FILTER (
                        WHERE status = 'CLOSED' AND result_r IS NOT NULL
                    ), 0) AS total_r,
                    COALESCE(SUM(result_r) FILTER (
                        WHERE status = 'CLOSED' AND result_r > 0
                    ), 0) AS gross_profit_r,
                    COALESCE(SUM(result_r) FILTER (
                        WHERE status = 'CLOSED' AND result_r < 0
                    ), 0) AS gross_loss_r
                FROM signals
                WHERE source = 'backtest' AND strategy_version = $1
            """, STRATEGY_VERSION)
            by_coin = await conn.fetch("""
                SELECT coin,
                       COUNT(*) FILTER (
                           WHERE status = 'CLOSED' AND result_r IS NOT NULL
                       ) AS trades,
                       COALESCE(SUM(result_r) FILTER (
                           WHERE status = 'CLOSED' AND result_r IS NOT NULL
                       ), 0) AS total_r
                FROM signals
                WHERE source = 'backtest' AND strategy_version = $1
                GROUP BY coin
                HAVING COUNT(*) FILTER (
                    WHERE status = 'CLOSED' AND result_r IS NOT NULL
                ) > 0
                ORDER BY total_r DESC, coin
            """, STRATEGY_VERSION)
        else:
            summary = await conn.fetchrow("""
                SELECT
                    COUNT(*) AS total_signals,
                    COUNT(*) FILTER (
                        WHERE status = 'CLOSED' AND result_r IS NOT NULL
                    ) AS closed_count,
                    COUNT(*) FILTER (
                        WHERE status = 'CLOSED' AND result_r > 0
                    ) AS wins,
                    COUNT(*) FILTER (
                        WHERE status = 'CLOSED' AND result_r < 0
                    ) AS losses,
                    COUNT(*) FILTER (
                        WHERE status = 'CLOSED' AND result_r = 0
                    ) AS breakeven,
                    COUNT(*) FILTER (
                        WHERE status = 'CLOSED' AND direction = 'LONG'
                    ) AS long_count,
                    COUNT(*) FILTER (
                        WHERE status = 'CLOSED' AND direction = 'SHORT'
                    ) AS short_count,
                    COUNT(*) FILTER (WHERE status <> 'CLOSED') AS open_count,
                    COALESCE(SUM(result_r) FILTER (
                        WHERE status = 'CLOSED' AND result_r IS NOT NULL
                    ), 0) AS total_r,
                    COALESCE(SUM(result_r) FILTER (
                        WHERE status = 'CLOSED' AND result_r > 0
                    ), 0) AS gross_profit_r,
                    COALESCE(SUM(result_r) FILTER (
                        WHERE status = 'CLOSED' AND result_r < 0
                    ), 0) AS gross_loss_r
                FROM signals
                WHERE source = 'live'
            """)
            by_coin = await conn.fetch("""
                SELECT coin,
                       COUNT(*) FILTER (
                           WHERE status = 'CLOSED' AND result_r IS NOT NULL
                       ) AS trades,
                       COALESCE(SUM(result_r) FILTER (
                           WHERE status = 'CLOSED' AND result_r IS NOT NULL
                       ), 0) AS total_r
                FROM signals
                WHERE source = 'live'
                GROUP BY coin
                HAVING COUNT(*) FILTER (
                    WHERE status = 'CLOSED' AND result_r IS NOT NULL
                ) > 0
                ORDER BY total_r DESC, coin
            """)

    data = dict(summary)
    data["by_coin"] = [dict(row) for row in by_coin]
    closed = int(data["closed_count"] or 0)
    wins = int(data["wins"] or 0)
    losses = int(data["losses"] or 0)
    data["win_rate"] = (wins / (wins + losses) * 100) if wins + losses else None
    data["avg_r"] = (float(data["total_r"] or 0) / closed) if closed else None
    gross_profit = float(data["gross_profit_r"] or 0)
    gross_loss = abs(float(data["gross_loss_r"] or 0))
    data["profit_factor"] = (gross_profit / gross_loss) if gross_loss > 0 else (float("inf") if gross_profit > 0 else None)
    data["total_r"] = float(data["total_r"] or 0)
    return data


async def get_recent_signals(limit: int = 5, source: str = "backtest") -> list[dict]:
    """Последние завершённые сделки; created_at — время записи в БД."""
    if source not in ("backtest", "live"):
        raise ValueError("source должен быть backtest или live")
    if _DB_POOL is None:
        raise RuntimeError("PostgreSQL не подключён")

    async with _DB_POOL.acquire() as conn:
        if source == "backtest":
            rows = await conn.fetch("""
                SELECT coin, direction, result_r, closed_at, created_at
                FROM signals
                WHERE source = 'backtest'
                  AND strategy_version = $1
                  AND status = 'CLOSED'
                  AND result_r IS NOT NULL
                ORDER BY COALESCE(closed_at, created_at) DESC, id DESC
                LIMIT $2
            """, STRATEGY_VERSION, max(1, min(int(limit), 20)))
        else:
            rows = await conn.fetch("""
                SELECT coin, direction, result_r, closed_at, created_at
                FROM signals
                WHERE source = 'live'
                  AND status = 'CLOSED'
                  AND result_r IS NOT NULL
                ORDER BY COALESCE(closed_at, created_at) DESC, id DESC
                LIMIT $1
            """, max(1, min(int(limit), 20)))
    return [dict(row) for row in rows]


def _format_r(value: float) -> str:
    return f"{value:+.2f}R".replace("-", "−")


def _render_stats_section(title: str, stats: dict, recent: list[dict], live: bool = False) -> list[str]:
    lines = [title]
    total_signals = int(stats.get("total_signals") or 0)
    closed = int(stats.get("closed_count") or 0)
    open_count = int(stats.get("open_count") or 0)
    lines.append(f"Сделок закрыто: <b>{closed}</b>")
    if live:
        lines.append(f"Активных сигналов: <b>{open_count}</b>")
    if closed == 0:
        lines.append("Пока нет завершённых сделок.")
        if live and total_signals == 0:
            lines.append("Ждём первого live-сигнала.")
        return lines

    win_rate = stats.get("win_rate")
    avg_r = stats.get("avg_r")
    pf = stats.get("profit_factor")
    lines.append(
        f"Win Rate: <b>{win_rate:.1f}%</b>" if win_rate is not None
        else "Win Rate: <b>н/д</b>"
    )
    lines.append(f"Средний результат: <b>{_format_r(avg_r)}</b>" if avg_r is not None else "Средний результат: <b>н/д</b>")
    if pf is None:
        pf_text = "н/д"
    elif pf == float("inf"):
        pf_text = "∞ (убытков нет)"
    else:
        pf_text = f"{pf:.2f}"
    lines.append(f"Profit Factor: <b>{pf_text}</b>")
    lines.append(f"Total R: <b>{_format_r(float(stats.get('total_r') or 0))}</b>")
    lines.append(
        f"LONG: {int(stats.get('long_count') or 0)} · "
        f"SHORT: {int(stats.get('short_count') or 0)}"
    )

    by_coin = stats.get("by_coin") or []
    if by_coin:
        best = by_coin[0]
        worst = min(by_coin, key=lambda row: float(row.get("total_r") or 0))
        lines.append(f"🏆 Лучшая монета: <b>{best['coin']} ({_format_r(float(best['total_r']))})</b>")
        lines.append(f"📉 Худшая монета: <b>{worst['coin']} ({_format_r(float(worst['total_r']))})</b>")

    if recent:
        lines.append("")
        lines.append("<b>Последние сделки:</b>")
        for trade in recent:
            stamp = trade.get("closed_at") or trade.get("created_at")
            date_text = stamp.strftime("%d.%m") if stamp else "дата н/д"
            lines.append(
                f"{trade['coin']} {trade['direction']}: "
                f"<b>{_format_r(float(trade['result_r']))}</b> · {date_text}"
            )
    return lines


async def render_stats() -> str:
    """Формирует раздельную статистику бэктеста и live по данным PostgreSQL."""
    if _DB_POOL is None:
        return "📊 <b>Статистика</b>\n\n❌ PostgreSQL не подключён."

    try:
        backtest = await get_stats("backtest")
        live = await get_stats("live")
        recent_backtest = await get_recent_signals(limit=5, source="backtest")
        recent_live = await get_recent_signals(limit=3, source="live")

        lines = [
            "📊 <b>Статистика стратегии</b>",
            f"Версия бэктеста: <code>{STRATEGY_VERSION}</code>",
            "",
        ]
        lines.extend(_render_stats_section("🧪 <b>Бэктест · тестирование</b>", backtest, recent_backtest))
        lines.extend(["", "🔴 <b>Live · реальные сделки</b>"])
        lines.extend(_render_stats_section("", live, recent_live, live=True))
        lines.extend([
            "",
            "ℹ️ Бэктест и live считаются отдельно.",
            "Время в списке — дата закрытия, если она записана; иначе дата создания записи в БД.",
        ])
        text = "\n".join(line for line in lines if line is not None)
        # Telegram ограничивает длину сообщения 4096 символами.
        return text[:4000]
    except Exception:
        logging.exception("render_stats failed")
        return "📊 <b>Статистика</b>\n\n❌ Не удалось прочитать данные. Проверь логи Render."


def main_menu_keyboard(states: dict) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for coin in COINS:
        emoji = STATUS_EMOJI[states[coin]["status"]]
        builder.button(text=f"{emoji} {coin}", callback_data=f"coin:{coin}")
    builder.adjust(3)
    builder.row(
        InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh"),
        InlineKeyboardButton(text="ℹ️ Памятка", callback_data="legend"),
    )
    builder.row(
        InlineKeyboardButton(text="📰 Новости", callback_data="news"),
        InlineKeyboardButton(text="📊 Статистика", callback_data="stats"),
    )
    builder.row(InlineKeyboardButton(text="🧪 Бэктест по всем монетам", callback_data="backtest_all"))
    return builder.as_markup()


def coin_card_keyboard(coin: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="📋 Детали", callback_data=f"details:{coin}"),
        InlineKeyboardButton(text="🧪 Бэктест", callback_data=f"backtest:{coin}"),
    )
    builder.row(
        InlineKeyboardButton(text="🔄 Обновить", callback_data=f"coin:{coin}"),
        InlineKeyboardButton(text="⬅️ Назад", callback_data="back"),
    )
    return builder.as_markup()


def back_keyboard(coin: str | None = None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if coin:
        builder.row(InlineKeyboardButton(text="⬅️ К монете", callback_data=f"coin:{coin}"))
    builder.row(InlineKeyboardButton(text="🏠 В меню", callback_data="back"))
    return builder.as_markup()

router = Router()


@router.message(CommandStart())
async def cmd_start(message: Message):
    if not _MARKET_CACHE:
        await refresh_all_market_data()
    states = get_all_states()
    await message.answer(render_main_menu(states), reply_markup=main_menu_keyboard(states))


@router.message(Command("test_channel"))
async def cmd_test_channel(message: Message):
    """Отправляет в канал одно техническое сообщение; доступно только администраторам канала."""
    if message.chat.type != "private":
        await message.answer("Открой личный чат с ботом и запусти /test_channel.")
        return

    if not message.from_user:
        await message.answer("Не удалось определить пользователя Telegram.")
        return

    if not SIGNAL_CHANNEL_ID:
        await message.answer(
            "❌ Не задан SIGNAL_CHANNEL_ID в Render Environment. "
            "Добавьте ID канала и повторите тест."
        )
        return

    try:
        admins = await message.bot.get_chat_administrators(chat_id=SIGNAL_CHANNEL_ID)
        is_channel_admin = any(admin.user.id == message.from_user.id for admin in admins)
        if not is_channel_admin:
            await message.answer("⛔ Тест доступен только администратору целевого канала.")
            return
    except Exception:
        logging.exception("Could not verify channel administrators for test")
        await message.answer(
            "❌ Не удалось проверить права канала. Убедитесь, что бот добавлен "
            "в канал администратором с правом публикации."
        )
        return

    try:
        await message.bot.send_message(
            chat_id=SIGNAL_CHANNEL_ID,
            text="🧪 ТЕСТ: бот может писать в канал. Это техническая проверка, не торговый сигнал.",
        )
        await message.answer("✅ Тестовое сообщение отправлено в канал. Проверьте публикацию.")
        logging.info("Test channel message sent by Telegram user %s", message.from_user.id)
    except Exception:
        logging.exception("Test channel message failed")
        await message.answer(
            "❌ Не получилось отправить сообщение. Проверьте ID канала и права бота."
        )



@router.message(Command("testsignal"))
async def cmd_testsignal(message: Message):
    """Создаёт тестовый live-сигнал BTC; доступен только ADMIN_USER_ID."""
    if not message.from_user or not ADMIN_USER_ID or str(message.from_user.id) != ADMIN_USER_ID:
        await message.answer("⛔ Команда доступна только администратору.")
        return
    try:
        price_payload = await _get_json(
            f"{BINGX_BASE}/openApi/swap/v2/quote/price",
            {"symbol": BINGX_SYMBOL["BTC"]},
        )
        try:
            entry = float(price_payload["data"]["price"])
        except (TypeError, KeyError, ValueError):
            await message.answer("❌ Не удалось получить текущую цену BTC с BingX.")
            return
        if not math.isfinite(entry) or entry <= 0:
            await message.answer("❌ Получена некорректная цена BTC.")
            return
        risk = entry * 0.005
        sig = {
            "coin": "BTC", "direction": "SHORT",
            "entry_price": entry, "sl_price": entry + risk,
            "tp_price": entry - risk * 2, "rr": 2.0,
            "regime": "TEST", "reason": "TESTSIGNAL",
            "entry_reason": "TESTSIGNAL",
        }
        signal_id = await save_live_signal(sig, source="test")
        if signal_id > 0:
            sent = await notify_channel(sig)
            await message.answer(
                f"✅ Тестовый сигнал сохранён: id={signal_id}. В канал: {sent}"
            )
        else:
            await message.answer(
                "⚠️ Тестовый сигнал не сохранён "
                "(активный BTC уже есть или cooldown 4ч)."
            )
    except Exception:
        logging.exception("/testsignal failed")
        await message.answer("❌ Ошибка /testsignal. Подробности — в логах Render.")


async def _send_long_message(message: Message, lines: list[str], limit: int = 3500):
    """Отправляет длинный отчёт частями по строкам, сохраняя HTML-теги."""
    chunks = []
    current = ""
    for line in lines:
        if len(current) + len(line) + 1 > limit:
            if current:
                chunks.append(current)
            current = line
        else:
            current = (current + "\n" + line) if current else line
    if current:
        chunks.append(current)

    for chunk in chunks:
        await message.answer(chunk)


@router.message(Command("dbtest"))
async def cmd_dbtest(message: Message):
    """Показывает состояние записей PostgreSQL по источникам."""
    if _DB_POOL is None:
        await message.answer("❌ PostgreSQL не подключён.")
        return

    try:
        async with _DB_POOL.acquire() as conn:
            summary = await conn.fetchrow("""
                SELECT
                    COUNT(*) AS total_count,

                    COUNT(*) FILTER (
                        WHERE source = 'backtest'
                    ) AS backtest_count,
                    COUNT(DISTINCT coin) FILTER (
                        WHERE source = 'backtest'
                    ) AS backtest_coins,
                    COALESCE(SUM(result_r) FILTER (
                        WHERE source = 'backtest'
                    ), 0) AS backtest_sum_r,

                    COUNT(*) FILTER (
                        WHERE source = 'live' AND status = 'ACTIVE'
                    ) AS live_active,
                    COUNT(*) FILTER (
                        WHERE source = 'live' AND status = 'WIN'
                    ) AS live_wins,
                    COUNT(*) FILTER (
                        WHERE source = 'live' AND status = 'LOSS'
                    ) AS live_losses,
                    COUNT(DISTINCT coin) FILTER (
                        WHERE source = 'live'
                    ) AS live_coins,

                    COUNT(*) FILTER (
                        WHERE source = 'test'
                    ) AS test_count,
                    COUNT(*) FILTER (
                        WHERE source = 'test' AND status = 'ACTIVE'
                    ) AS test_active,
                    COUNT(*) FILTER (
                        WHERE source = 'test' AND status = 'WIN'
                    ) AS test_wins,
                    COUNT(*) FILTER (
                        WHERE source = 'test' AND status = 'LOSS'
                    ) AS test_losses,
                    COUNT(*) FILTER (
                        WHERE source = 'test' AND status = 'CLOSED'
                    ) AS test_closed,
                    COUNT(DISTINCT coin) FILTER (
                        WHERE source = 'test'
                    ) AS test_coins,

                    COUNT(*) FILTER (
                        WHERE source IS NULL
                           OR source NOT IN ('backtest', 'live', 'test')
                    ) AS unknown_source
                FROM signals
            """)

            by_coin = await conn.fetch("""
                SELECT coin,
                       COUNT(*) AS trade_count,
                       COALESCE(SUM(result_r), 0) AS sum_r
                FROM signals
                WHERE source = 'backtest'
                GROUP BY coin
                ORDER BY coin
            """)

            live_by_coin = await conn.fetch("""
                SELECT coin,
                       COUNT(*) FILTER (
                           WHERE status = 'ACTIVE'
                       ) AS active_count,
                       COUNT(*) FILTER (
                           WHERE status = 'WIN'
                       ) AS wins,
                       COUNT(*) FILTER (
                           WHERE status = 'LOSS'
                       ) AS losses,
                       COUNT(*) AS total_count
                FROM signals
                WHERE source = 'live'
                GROUP BY coin
                ORDER BY coin
            """)

            test_by_coin = await conn.fetch("""
                SELECT coin,
                       COUNT(*) FILTER (
                           WHERE status = 'ACTIVE'
                       ) AS active_count,
                       COUNT(*) FILTER (
                           WHERE status = 'WIN'
                       ) AS wins,
                       COUNT(*) FILTER (
                           WHERE status = 'LOSS'
                       ) AS losses,
                       COUNT(*) FILTER (
                           WHERE status = 'CLOSED'
                       ) AS closed_count,
                       COUNT(*) AS total_count
                FROM signals
                WHERE source = 'test'
                GROUP BY coin
                ORDER BY coin
            """)

        lines = [
            "🗄 <b>Проверка PostgreSQL</b>",
            f"Всего строк: <b>{summary['total_count']}</b>",
            "",
            "📊 <b>Backtest</b>",
            f"Записей: <b>{summary['backtest_count']}</b>",
            f"Монет: <b>{summary['backtest_coins']}</b>",
            f"Сумма result_r: <b>{float(summary['backtest_sum_r']):+.2f}R</b>",
            "",
            "📡 <b>Live-сигналы</b>",
            f"Активных: <b>{summary['live_active']}</b>",
            f"WIN: <b>{summary['live_wins']}</b> · "
            f"LOSS: <b>{summary['live_losses']}</b>",
            f"Монет: <b>{summary['live_coins']}</b>",
            "",
            "🧪 <b>Test-сигналы</b>",
            f"Всего: <b>{summary['test_count']}</b>",
            f"Активных: <b>{summary['test_active']}</b>",
            f"WIN: <b>{summary['test_wins']}</b> · "
            f"LOSS: <b>{summary['test_losses']}</b>",
            f"CLOSED: <b>{summary['test_closed']}</b>",
            f"Монет: <b>{summary['test_coins']}</b>",
        ]

        if summary["unknown_source"] > 0:
            lines.extend([
                "",
                "⚠️ <b>Неизвестный source</b>",
                f"Записей: <b>{summary['unknown_source']}</b>",
            ])

        lines.extend(["", "<b>Backtest по монетам:</b>"])
        if by_coin:
            for row in by_coin:
                lines.append(
                    f"{row['coin']}: {row['trade_count']} сделок, "
                    f"{float(row['sum_r']):+.2f}R"
                )
        else:
            lines.append("Записей backtest пока нет.")

        lines.extend(["", "<b>Live по монетам:</b>"])
        if live_by_coin:
            for row in live_by_coin:
                lines.append(
                    f"{row['coin']}: {row['active_count']} активных, "
                    f"WIN {row['wins']}, LOSS {row['losses']} "
                    f"(всего {row['total_count']})"
                )
        else:
            lines.append("Live-записей пока нет.")

        lines.extend(["", "<b>Test по монетам:</b>"])
        if test_by_coin:
            for row in test_by_coin:
                lines.append(
                    f"{row['coin']}: {row['active_count']} активных, "
                    f"WIN {row['wins']}, LOSS {row['losses']}, "
                    f"CLOSED {row['closed_count']} "
                    f"(всего {row['total_count']})"
                )
        else:
            lines.append("Test-записей пока нет.")

        await _send_long_message(message, lines)

    except Exception:
        logging.exception("dbtest failed")
        await message.answer(
            "❌ Ошибка чтения PostgreSQL. Подробности — в логах Render."
        )


@router.callback_query(F.data == "back")
async def cb_back(callback: CallbackQuery):
    states = get_all_states()
    await callback.message.edit_text(render_main_menu(states), reply_markup=main_menu_keyboard(states))
    await callback.answer()


@router.callback_query(F.data == "refresh")
async def cb_refresh(callback: CallbackQuery):
    await callback.answer("Обновляю данные с BingX…")
    await refresh_all_market_data()
    states = get_all_states()
    await callback.message.edit_text(render_main_menu(states), reply_markup=main_menu_keyboard(states))


@router.callback_query(F.data == "legend")
async def cb_legend(callback: CallbackQuery):
    await callback.message.edit_text(render_legend(), reply_markup=back_keyboard())
    await callback.answer()


@router.callback_query(F.data.startswith("coin:"))
async def cb_coin_card(callback: CallbackQuery):
    coin = callback.data.split(":")[1]
    await callback.answer("Обновляю…")
    state = await fetch_coin_data(coin)
    _MARKET_CACHE[coin] = state
    await callback.message.edit_text(render_coin_card(state), reply_markup=coin_card_keyboard(coin))


@router.callback_query(F.data.startswith("backtest:"))
async def cb_backtest(callback: CallbackQuery):
    coin = callback.data.split(":")[1]
    await callback.answer("Скачиваю историю и считаю бэктест… это займёт немного времени")
    symbol = BINGX_SYMBOL[coin]
    candles = await fetch_historical_klines(symbol, interval="4h", target=4000)
    if len(candles) < 100:
        await callback.message.edit_text(
            f"🧪 <b>Бэктест · {coin}</b>\n{DATA_NOTE}\n\nНе удалось получить достаточно истории, попробуйте позже.",
            reply_markup=back_keyboard(coin),
        )
        return
    trades = run_backtest(candles)
    stats = summarize_trades(trades)
    await callback.message.edit_text(
        render_backtest(coin, len(candles), stats), reply_markup=back_keyboard(coin)
    )


async def _perform_backtest_all(message: Message):
    global _BACKTEST_ALL_RUNNING
    try:
        per_coin = []
        all_trades = []
        candles_by_coin = {}

        # Историю скачиваем один раз и используем для обеих стратегий.
        for index, coin in enumerate(COINS, start=1):
            candles = await fetch_historical_klines(
                BINGX_SYMBOL[coin], interval="4h", target=4000
            )
            candles_by_coin[coin] = candles
            if len(candles) < 100:
                per_coin.append({"coin": coin, "stats": None})
            else:
                # CPU-heavy calculations must not block Telegram's asyncio event loop.
                trades = await asyncio.to_thread(run_backtest, candles)
                stats = summarize_trades(trades)
                per_coin.append({"coin": coin, "stats": stats})
                all_trades.extend(trades)
                saved_count = await save_backtest_signals(
                    trades, coin, STRATEGY_VERSION
                )
                logging.info(
                    "Backtest progress: coin=%s trades=%s saved=%s",
                    coin, len(trades), saved_count,
                )

            if index % 3 == 0 or index == len(COINS):
                await message.edit_text(
                    f"🧪 <b>Бэктест по всем монетам</b>\n"
                    f"Загружено: {index}/{len(COINS)}. Считаю стратегии…"
                )
            await asyncio.sleep(0.2)

        overall = summarize_trades(all_trades)

        # Baseline использует уже загруженные свечи — без повторных запросов к BingX.
        # Показываем отдельный этап: baseline заметно тяжелее основной стратегии.
        await message.edit_text(
            "🧪 <b>Бэктест по всем монетам</b>\n"
            "Основная стратегия рассчитана. Считаю baseline: 0/11…"
        )
        baseline_trades = []
        eligible = [(coin, candles) for coin, candles in candles_by_coin.items() if len(candles) >= 100]
        for index, (coin, candles) in enumerate(eligible, start=1):
            baseline_trades.extend(await asyncio.to_thread(run_backtest_baseline, candles))
            await message.edit_text(
                "🧪 <b>Бэктест по всем монетам</b>\n"
                f"Основная стратегия готова. Baseline: {index}/{len(eligible)} ({coin})…"
            )

        baseline_stats = summarize_trades(baseline_trades)
        await message.edit_text(
            render_backtest_all(per_coin, overall, all_trades, baseline_stats),
            reply_markup=back_keyboard()
        )
    except Exception:
        logging.exception("Backtest-all failed")
        try:
            await message.edit_text(
                "⚠️ <b>Не удалось завершить бэктест.</b>\n"
                "Проверьте логи и попробуйте ещё раз позже.",
                reply_markup=back_keyboard()
            )
        except Exception:
            logging.exception("Could not show backtest error to user")
    finally:
        _BACKTEST_ALL_RUNNING = False


@router.callback_query(F.data == "backtest_all")
async def cb_backtest_all(callback: CallbackQuery):
    global _BACKTEST_ALL_RUNNING
    if _BACKTEST_ALL_RUNNING:
        await callback.answer("Бэктест уже выполняется, дождись результата.")
        return

    _BACKTEST_ALL_RUNNING = True
    await callback.answer("Запустил бэктест. Результат появится в этом сообщении.")
    try:
        await callback.message.edit_text(
            "🧪 <b>Бэктест по всем монетам</b>\n"
            "Подготовка данных BingX…",
            reply_markup=back_keyboard()
        )
    except Exception:
        _BACKTEST_ALL_RUNNING = False
        raise

    asyncio.create_task(_perform_backtest_all(callback.message))


@router.callback_query(F.data.startswith("details:"))
async def cb_details(callback: CallbackQuery):
    coin = callback.data.split(":")[1]
    state = get_coin_state(coin)
    await callback.message.edit_text(render_details(state), reply_markup=back_keyboard(coin))
    await callback.answer()


@router.callback_query(F.data == "news")
async def cb_news(callback: CallbackQuery):
    await callback.message.edit_text(render_news_placeholder(), reply_markup=back_keyboard())
    await callback.answer()


@router.callback_query(F.data == "stats")
async def cb_stats(callback: CallbackQuery):
    await callback.answer()
    text = await render_stats()
    await callback.message.edit_text(text, reply_markup=back_keyboard())


async def start_web_server():
    app = web.Application()
    app.router.add_get("/", lambda request: web.Response(text="Crypto Signals bot is running"))
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", 10000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logging.info(f"Web server started on port {port}")


async def init_database() -> None:
    """Подключается к PostgreSQL и создаёт таблицы signals и outbox."""
    global _DB_POOL, _OUTBOX_READY
    _OUTBOX_READY = False

    if not DATABASE_URL:
        logging.warning("DATABASE_URL не задан — PostgreSQL отключён.")
        return

    try:
        _DB_POOL = await asyncpg.create_pool(
            dsn=DATABASE_URL,
            min_size=1,
            max_size=5,
            command_timeout=10,
        )
        async with _DB_POOL.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id BIGSERIAL PRIMARY KEY,
                    coin TEXT NOT NULL,
                    direction TEXT NOT NULL CHECK (direction IN ('LONG', 'SHORT')),
                    entry_price DOUBLE PRECISION,
                    stop_loss DOUBLE PRECISION,
                    take_profit DOUBLE PRECISION,
                    status TEXT NOT NULL DEFAULT 'ACTIVE',
                    result_r DOUBLE PRECISION,
                    result_usd DOUBLE PRECISION,
                    result_pct DOUBLE PRECISION,
                    entry_reason TEXT,
                    strategy_version TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    closed_at TIMESTAMPTZ
                )
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_signals_created_at
                ON signals (created_at DESC)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_signals_status
                ON signals (status)
            """)
            await conn.execute("""
                ALTER TABLE signals
                ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'live'
            """)
            await conn.execute("""
                ALTER TABLE signals
                ADD COLUMN IF NOT EXISTS regime TEXT
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_signals_source
                ON signals (source)
            """)
            # Уникальный индекс — дополнительная защита от двух активных live-сигналов
            # на одну монету. Ошибка здесь не должна отключать весь PostgreSQL.
            try:
                duplicates = await conn.fetch("""
                    SELECT coin, COUNT(*) AS cnt
                    FROM signals
                    WHERE source = 'live' AND status = 'ACTIVE'
                    GROUP BY coin
                    HAVING COUNT(*) > 1
                    ORDER BY coin
                """)
                if duplicates:
                    duplicate_summary = [(row["coin"], row["cnt"]) for row in duplicates]
                    logging.warning(
                        "Duplicates found for active live signals: %s. "
                        "Skipping unique index creation; resolve duplicates before retrying.",
                        duplicate_summary,
                    )
                else:
                    await conn.execute("""
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_active_live_signal
                        ON signals (coin)
                        WHERE source = 'live' AND status = 'ACTIVE'
                    """)
                    logging.info("Unique index for live signals ensured.")
            except Exception:
                logging.exception(
                    "Failed to ensure unique index for active live signals. "
                    "PostgreSQL remains available, but duplicate live signals may be possible."
                )
            # Outbox is required for live signals: a signal and its delivery task
            # must be committed atomically. Keep the DB pool available for diagnostics,
            # but explicitly disable live signal writes if migration/validation fails.
            try:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS outbox (
                        id BIGSERIAL PRIMARY KEY,
                        signal_id BIGINT NOT NULL,
                        kind TEXT NOT NULL DEFAULT 'telegram_channel',
                        payload JSONB NOT NULL,
                        status TEXT NOT NULL DEFAULT 'PENDING'
                            CHECK (status IN ('PENDING', 'SENT', 'FAILED')),
                        attempts INT NOT NULL DEFAULT 0,
                        last_error TEXT,
                        next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        sent_at TIMESTAMPTZ,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_outbox_pending
                    ON outbox (next_attempt_at)
                    WHERE status = 'PENDING'
                """)
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_outbox_signal_id
                    ON outbox (signal_id)
                """)
                # CREATE TABLE IF NOT EXISTS does not repair an incompatible old table.
                await conn.fetchval("""
                    SELECT id, signal_id, kind, payload, status, attempts,
                           last_error, next_attempt_at, sent_at, created_at
                    FROM outbox
                    LIMIT 0
                """)
                _OUTBOX_READY = True
                logging.info("Outbox table and required columns are ready.")
            except Exception:
                _OUTBOX_READY = False
                logging.exception(
                    "CRITICAL: Outbox is not ready. Live signal writes are disabled "
                    "until the schema problem is fixed."
                )

            await conn.fetchval("SELECT 1")
        if _OUTBOX_READY:
            logging.info("PostgreSQL подключён; таблицы signals и outbox готовы.")
        else:
            logging.error(
                "PostgreSQL подключён, но Outbox не готов: сохранение live-сигналов отключено."
            )
    except Exception:
        _OUTBOX_READY = False
        logging.exception("Не удалось подключиться к PostgreSQL; бот продолжит работу без БД.")
        if _DB_POOL is not None:
            await _DB_POOL.close()
            _DB_POOL = None


def _ms_to_datetime(value: int | float | None) -> datetime.datetime | None:
    """Конвертирует timestamp свечи в миллисекундах в UTC datetime."""
    if value is None:
        return None
    try:
        return datetime.datetime.fromtimestamp(float(value) / 1000, tz=datetime.timezone.utc)
    except (TypeError, ValueError, OverflowError, OSError):
        return None


async def save_backtest_signals(
    trades: list[dict], coin: str,
    strategy_version: str,
) -> int:
    """Сохраняет сделки бэктеста в БД. Возвращает число сохранённых строк."""
    if _DB_POOL is None or not trades:
        return 0

    rows = []
    for trade in trades:
        direction = trade.get("dir")
        if direction not in ("LONG", "SHORT"):
            continue
        entry_dt = _ms_to_datetime(trade.get("entry_time"))
        exit_dt = _ms_to_datetime(trade.get("exit_time"))
        if entry_dt is None:
            entry_dt = datetime.datetime.now(datetime.timezone.utc)
        if exit_dt is None:
            exit_dt = entry_dt
        rows.append((
            coin,
            direction,
            float(trade.get("r", 0.0)),
            "CLOSED",
            strategy_version,
            trade.get("regime"),
            "backtest",
            entry_dt,
            exit_dt,
        ))

    if not rows:
        logging.warning("Backtest save skipped for %s: no valid trades", coin)
        return 0

    try:
        async with _DB_POOL.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    DELETE FROM signals
                    WHERE source = 'backtest'
                      AND coin = $1
                      AND strategy_version = $2
                    """,
                    coin, strategy_version,
                )
                await conn.executemany(
                    """
                    INSERT INTO signals
                        (coin, direction, result_r, status,
                         strategy_version, regime, source, created_at, closed_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    """,
                    rows,
                )
        logging.info(
            "Backtest signals saved: coin=%s strategy=%s count=%s",
            coin, strategy_version, len(rows),
        )
        return len(rows)
    except Exception:
        logging.exception("save_backtest_signals failed for %s", coin)
        return 0


@router.message(Command("dbindexes"))
async def cmd_dbindexes(message: Message):
    """Read-only: показывает реальные индексы signals; доступ только администратору."""
    if not message.from_user or not ADMIN_USER_ID or str(message.from_user.id) != ADMIN_USER_ID:
        await message.answer("⛔ Команда доступна только администратору.")
        return
    if _DB_POOL is None:
        await message.answer("⚠️ PostgreSQL не подключён. Проверь DATABASE_URL и логи Render.")
        return

    try:
        async with _DB_POOL.acquire() as conn:
            rows = await conn.fetch("""
                SELECT schemaname, indexname, indexdef
                FROM pg_indexes
                WHERE tablename = 'signals'
                  AND schemaname = current_schema()
                ORDER BY indexname
            """)
            database = await conn.fetchval("SELECT current_database()")
        lines = [f"🔎 <b>Индексы таблицы signals</b>", f"База: <code>{html.escape(str(database))}</code>", ""]
        if not rows:
            lines.append("Индексы не найдены в текущей схеме.")
        else:
            for row in rows:
                lines.append(f"<b>{html.escape(str(row['indexname']))}</b>")
                lines.append(f"<code>{html.escape(str(row['indexdef']))}</code>")
                lines.append("")
        output = "\n".join(lines)
        # Telegram ограничивает длину сообщения; отправляем несколькими частями при необходимости.
        while output:
            chunk = output[:3500]
            if len(output) > 3500 and "\n" in chunk:
                chunk = chunk.rsplit("\n", 1)[0]
            await message.answer(chunk)
            output = output[len(chunk):].lstrip("\n")
    except Exception:
        logging.exception("/dbindexes failed")
        await message.answer("❌ Не удалось прочитать индексы. Подробности — в логах Render.")


async def main():
    global _SESSION

    if not BOT_TOKEN:
        raise RuntimeError(
            "BOT_TOKEN не задан. Добавьте переменную окружения BOT_TOKEN "
            "со значением токена от @BotFather."
        )

    logging.basicConfig(level=logging.INFO)

    await init_database()
    _SESSION = aiohttp.ClientSession()

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)

    await bot.delete_webhook(drop_pending_updates=True)
    await asyncio.gather(
        dp.start_polling(bot),
        start_web_server(),
        market_data_supervisor(),
        outbox_worker_supervisor(),
    )


if __name__ == "__main__":
    asyncio.run(main())

