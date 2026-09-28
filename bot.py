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


def _closes_from_klines(payload: dict) -> list[float]:
    data = payload.get("data") if payload else None
    if not data:
        return []
    closes = []
    for candle in data:
        try:
            if isinstance(candle, dict):
                closes.append(float(candle.get("close")))
            elif isinstance(candle, (list, tuple)) and len(candle) >= 5:
                closes.append(float(candle[4]))
        except (TypeError, ValueError):
            continue
    return closes


def _sma(values: list[float], length: int) -> float | None:
    if len(values) < length:
        return None
    return sum(values[-length:]) / length


async def fetch_coin_data(coin: str) -> dict:
    symbol = BINGX_SYMBOL[coin]

    klines = await _get_json(
        f"{BINGX_BASE}/openApi/swap/v2/quote/klines",
        {"symbol": symbol, "interval": "4h", "limit": 60},
    )
    closes = _closes_from_klines(klines) if klines else []

    price = closes[-1] if closes else None
    change_24h = None
    if len(closes) >= 7:
        ref = closes[-7]
        if ref:
            change_24h = round((closes[-1] - ref) / ref * 100, 2)

    trend = "Недостаточно данных"
    if len(closes) >= 50:
        sma20 = _sma(closes, 20)
        sma50 = _sma(closes, 50)
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

    return {
        "coin": coin,
        "status": "NONE",
        "price": price,
        "change_24h": change_24h,
        "trend": trend,
        "updated_at": time.time(),
        "ok": price is not None,
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
        "Вход, стоп, тейк и балл появятся, когда будет готова",
        "система оценки сигналов (этапы V0.4–V0.8).",
    ]
    return "\n".join(lines)


def render_legend() -> str:
    return "\n".join([
        "ℹ️ <b>Памятка</b>",
        "",
        "Сейчас бот показывает только реальную цену и тренд с BingX.",
        "Цветные статусы (Long/Short/Ожидание/Зона интереса) появятся,",
        "когда будет готова система оценки входов.",
        "",
        "<b>Пока используется</b>",
        "⚪ — по всем монетам, сигналов ещё нет",
        "",
        "<b>Тренд считается так</b>",
        "Сравниваются средние цены за 20 и 50 последних 4-часовых свечей.",
    ])


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
        InlineKeyboardButton(text="🔄 Обновить", callback_data=f"coin:{coin}"),
        InlineKeyboardButton(text="⬅️ Назад", callback_data="back"),
    )
    return builder.as_markup()


def back_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
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
