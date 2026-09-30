import asyncio
import logging
import os
import time

import aiohttp
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiohttp import web

BOT_TOKEN = os.getenv("BOT_TOKEN", "")

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


def describe_ema(price: float, ema20: float | None, ema50: float | None) -> str:
    if ema20 is None or ema50 is None:
        return "Недостаточно данных"
    if price > ema20 > ema50:
        return "Цена выше EMA20 и EMA50"
    if price < ema20 < ema50:
        return "Цена ниже EMA20 и EMA50"
    return "Цена между EMA20 и EMA50"


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
    for _ in range(6):
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


BT_SMA_FAST = 20
BT_SMA_SLOW = 50
BT_ZONE_LOOKBACK = 40
BT_PIVOT_WINDOW = 2
BT_RR_TARGET = 2.0
BT_FEE_R = 0.05
BT_TOUCH_TOLERANCE = 1.015


def find_pivot_level(candles: list[dict], end_idx: int, lookback: int, side: str,
                      pivot_window: int = BT_PIVOT_WINDOW) -> float | None:
    """Ищет ближайший подтверждённый локальный экстремум (пивот) назад от end_idx,
    не считая последних 3 свечей — уровень должен быть установлен заранее, а не сейчас."""
    start = max(end_idx - lookback, pivot_window)
    for idx in range(end_idx - 3, start, -1):
        lo, hi = idx - pivot_window, idx + pivot_window + 1
        if lo < 0 or hi > len(candles):
            continue
        segment = candles[lo:hi]
        center = candles[idx]
        if side == "low":
            if center["l"] == min(c["l"] for c in segment):
                return center["l"]
        else:
            if center["h"] == max(c["h"] for c in segment):
                return center["h"]
    return None


def run_backtest(candles: list[dict], sma_fast=BT_SMA_FAST, sma_slow=BT_SMA_SLOW,
                  zone_lookback=BT_ZONE_LOOKBACK, rr_target=BT_RR_TARGET, fee_r=BT_FEE_R) -> list[dict]:
    closes = [c["c"] for c in candles]
    n = len(candles)
    trades = []
    i = sma_slow + zone_lookback
    while i < n - 1:
        j = i
        closes_so_far = closes[:j + 1]
        sma_f = _sma(closes_so_far, sma_fast)
        sma_s = _sma(closes_so_far, sma_slow)
        if sma_f is None or sma_s is None:
            i += 1
            continue

        trend_up = sma_f > sma_s * 1.002
        trend_down = sma_f < sma_s * 0.998

        sig = candles[j]
        recent_vol = [c["v"] for c in candles[max(0, j - 10):j]]
        avg_vol = sum(recent_vol) / len(recent_vol) if recent_vol else 0
        volume_ok = avg_vol == 0 or sig["v"] >= avg_vol
        candle_range = sig["h"] - sig["l"]

        direction = None
        support = resistance = None

        if trend_up:
            support = find_pivot_level(candles, j, zone_lookback, "low")
            if support:
                near_support = sig["l"] <= support * BT_TOUCH_TOLERANCE
                bullish = sig["c"] > sig["o"] and sig["c"] > support
                upper_half = candle_range > 0 and (sig["c"] - sig["l"]) >= 0.5 * candle_range
                if near_support and bullish and upper_half and volume_ok:
                    direction = "LONG"

        if direction is None and trend_down:
            resistance = find_pivot_level(candles, j, zone_lookback, "high")
            if resistance:
                near_resistance = sig["h"] >= resistance / BT_TOUCH_TOLERANCE
                bearish = sig["c"] < sig["o"] and sig["c"] < resistance
                lower_half = candle_range > 0 and (sig["h"] - sig["c"]) >= 0.5 * candle_range
                if near_resistance and bearish and lower_half and volume_ok:
                    direction = "SHORT"

        if direction is None:
            i += 1
            continue

        if j + 1 >= n:
            break
        entry_candle = candles[j + 1]
        entry = entry_candle["o"]

        if direction == "LONG":
            sl = support * 0.995
            risk = entry - sl
        else:
            sl = resistance * 1.005
            risk = sl - entry

        if risk <= 0:
            i += 1
            continue

        tp = entry + rr_target * risk if direction == "LONG" else entry - rr_target * risk

        exit_r, exit_i = None, None
        for k in range(j + 1, n):
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

            sma_f_k = _sma(closes[:k + 1], sma_fast)
            sma_s_k = _sma(closes[:k + 1], sma_slow)
            if sma_f_k and sma_s_k:
                reversed_trend = (
                    (direction == "LONG" and sma_f_k < sma_s_k) or
                    (direction == "SHORT" and sma_f_k > sma_s_k)
                )
                if reversed_trend:
                    exit_price = c["c"]
                    exit_r = ((exit_price - entry) / risk if direction == "LONG"
                              else (entry - exit_price) / risk)
                    exit_i = k
                    break

        if exit_r is None:
            last = candles[-1]["c"]
            exit_r = (last - entry) / risk if direction == "LONG" else (entry - last) / risk
            exit_i = n - 1

        exit_r -= fee_r
        trades.append({"dir": direction, "entry_i": j + 1, "exit_i": exit_i, "r": round(exit_r, 2)})
        i = exit_i + 1

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


async def refresh_all_market_data():
    results = await asyncio.gather(*(fetch_coin_data(c) for c in COINS), return_exceptions=True)
    for coin, result in zip(COINS, results):
        if isinstance(result, Exception):
            logging.warning(f"Failed to refresh {coin}: {result}")
            continue
        _MARKET_CACHE[coin] = result


async def market_data_loop():
    while True:
        await refresh_all_market_data()
        await asyncio.sleep(300)


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
        "Вход, стоп, тейк и балл появятся, когда будет готова",
        "система оценки сигналов (этапы V0.5–V0.8).",
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
        else "недостаточно данных"
    )
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
        "индикаторы с BingX. Цветные статусы (Long/Short/Ожидание/",
        "Зона интереса) появятся, когда будет готова система оценки входов.",
        "",
        "<b>Пока используется</b>",
        "⚪ — по всем монетам, сигналов ещё нет",
        "",
        "<b>Индикаторы в «Деталях»</b>",
        "RSI — перекупленность/перепроданность (0–100)",
        "MACD — соотношение быстрой и медленной скользящих средних",
        "EMA — положение цены относительно средних за 20 и 50 свечей",
        "ATR — средняя волатильность (пригодится для стопа)",
    ])


def render_backtest(coin: str, candles_count: int, stats: dict | None) -> str:
    lines = [f"🧪 <b>Бэктест · {coin}</b>", DATA_NOTE, ""]
    lines.append(f"История: {candles_count} свечей по 4ч (~{candles_count // 6} дней)")
    lines.append("Стратегия: тренд SMA20/50 + отскок от пивот-уровня с объёмом, цель 2R")
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
    lines.append("Комиссия и проскальзывание учтены приблизительно (−0.05R за сделку).")
    lines.append("Тест на одном периоде истории, без разделения на обучающую/проверочную выборку.")
    return "\n".join(lines)


def render_news_placeholder() -> str:
    return "📰 <b>Новости</b>\n\nМодуль новостного анализа будет подключён на этапе V0.9."


def render_stats_placeholder() -> str:
    return "📊 <b>Статистика</b>\n\nМодуль статистики будет подключён на этапе V1.2."


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
    candles = await fetch_historical_klines(symbol, interval="4h", target=1000)
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
    await callback.message.edit_text(render_stats_placeholder(), reply_markup=back_keyboard())
    await callback.answer()


async def start_web_server():
    app = web.Application()
    app.router.add_get("/", lambda request: web.Response(text="Crypto Signals bot is running"))
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", 10000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logging.info(f"Web server started on port {port}")


async def main():
    global _SESSION

    if not BOT_TOKEN:
        raise RuntimeError(
            "BOT_TOKEN не задан. Добавьте переменную окружения BOT_TOKEN "
            "со значением токена от @BotFather."
        )

    logging.basicConfig(level=logging.INFO)

    _SESSION = aiohttp.ClientSession()

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)

    await bot.delete_webhook(drop_pending_updates=True)
    await asyncio.gather(
        dp.start_polling(bot),
        start_web_server(),
        market_data_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
