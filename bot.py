import asyncio
import logging
import os
import random

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiohttp import web

BOT_TOKEN = os.getenv("BOT_TOKEN", "")

COINS = ["BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "ADA", "AVAX", "LINK", "SUI", "ZEC"]

STATUS_EMOJI = {
    "NONE": "⚪", "WATCH": "👀", "INTEREST_ZONE": "🔵",
    "WAIT": "🟡", "LONG": "🟢", "SHORT": "🔴",
}

STATUS_LABEL = {
    "NONE": "НЕТ СИГНАЛА", "WATCH": "НАБЛЮДЕНИЕ", "INTEREST_ZONE": "ЗОНА ИНТЕРЕСА",
    "WAIT": "ОЖИДАНИЕ", "LONG": "LONG", "SHORT": "SHORT",
}

TEST_NOTE = "⚠️ <i>Тестовые данные — не для торговли</i>"
_STATE_CACHE = {}


def _generate_coin_state(coin: str) -> dict:
    statuses = list(STATUS_EMOJI.keys())
    weights = [40, 20, 15, 10, 10, 5]
    status = random.choices(statuses, weights=weights, k=1)[0]
    price = round(random.uniform(0.5, 70000), 2)
    score = random.randint(30, 97)
    sign = -1 if status == "SHORT" else 1
    return {
        "coin": coin, "status": status, "price": price,
        "trend": random.choice(["ВОСХОДЯЩИЙ", "НИСХОДЯЩИЙ", "БОКОВОЙ"]),
        "score": score,
        "rsi": round(random.uniform(20, 80), 1),
        "macd": random.choice(["Бычье пересечение", "Медвежье пересечение", "Нейтрально"]),
        "ema": random.choice(["Цена выше EMA50/200", "Цена ниже EMA50/200", "Возле EMA"]),
        "volume": random.choice(["Растёт", "Падает", "Средний"]),
        "bid_ask": random.choice(["Перевес покупателей", "Перевес продавцов", "Баланс"]),
        "imbalance": f"{random.randint(-40, 40):+d}%",
        "candle_pattern": random.choice(["Bullish Engulfing", "Pin Bar", "Doji", "Не обнаружено"]),
        "zone_low": round(price * 0.985, 2),
        "zone_high": round(price * 1.005, 2),
        "entry": price,
        "stop_loss": round(price * (1 - 0.03 * sign), 2),
        "tp1": round(price * (1 + 0.02 * sign), 2),
        "tp2": round(price * (1 + 0.045 * sign), 2),
        "tp3": round(price * (1 + 0.08 * sign), 2),
        "leverage": random.choice([3, 5, 7, 10]),
        "news_impact": random.choice(["🟢 Положительное", "🟡 Нейтральное", "🔴 Отрицательное"]),
        "risk_reward": round(random.uniform(1.5, 4.0), 1),
    }


def get_all_states() -> dict:
    if not _STATE_CACHE:
        for coin in COINS:
            _STATE_CACHE[coin] = _generate_coin_state(coin)
    return _STATE_CACHE


def refresh_all_states() -> dict:
    for coin in COINS:
        _STATE_CACHE[coin] = _generate_coin_state(coin)
    return _STATE_CACHE


def get_coin_state(coin: str) -> dict:
    return get_all_states()[coin]


def score_verdict(score: int) -> str:
    if score >= 90:
        return "🟢 Сильный сетап"
    if score >= 80:
        return "🟢 Качественный сетап"
    if score >= 70:
        return "🟡 Нужно подтверждение"
    return "⚪ Нет сигнала"


def render_main_menu(states: dict) -> str:
    active = [(c, s) for c, s in states.items() if s["status"] in ("LONG", "SHORT")]
    forming = [(c, s) for c, s in states.items() if s["status"] in ("INTEREST_ZONE", "WAIT")]
    lines = ["🤖 <b>CRYPTO SIGNALS</b>", TEST_NOTE, ""]
    if active:
        items = " · ".join(f"{STATUS_EMOJI[s['status']]} {c} {s['status']}" for c, s in active)
        lines.append(f"🔥 <b>Активные:</b> {items}")
    else:
        lines.append("🔥 <b>Активные:</b> пока нет")
    if forming:
        items = " · ".join(f"{STATUS_EMOJI[s['status']]} {c}" for c, s in forming)
        lines.append(f"👀 <b>Формируются:</b> {items}")
    else:
        lines.append("👀 <b>Формируются:</b> пока нет")
    lines += ["", "Выберите монету 👇"]
    return "\n".join(lines)


def render_coin_card(state: dict) -> str:
    coin = state["coin"]
    status = state["status"]
    lines = [
        f"🪙 <b>{coin}</b> · {STATUS_EMOJI[status]} <b>{STATUS_LABEL[status]}</b>",
        TEST_NOTE,
        "",
        f"💰 ${state['price']}  ·  📈 {state['trend']}",
        f"🎯 Качество: <b>{state['score']}/100</b> — {score_verdict(state['score'])}",
        "",
    ]
    if status in ("LONG", "SHORT"):
        lines += [
            f"📍 Вход: ${state['entry']}",
            f"🛑 SL: ${state['stop_loss']}",
            f"🎯 TP: ${state['tp1']} / ${state['tp2']} / ${state['tp3']}",
            f"📊 R/R 1:{state['risk_reward']}  ·  ⚙️ {state['leverage']}x",
            "",
        ]
    elif status == "WAIT":
        lines += ["🟡 Балл достаточный, но не хватает подтверждения. Ждём.", ""]
    elif status == "INTEREST_ZONE":
        lines += ["🔵 Цена в зоне интереса, вход ещё не подтверждён.", ""]
    elif status == "WATCH":
        lines += ["👀 Ситуация под наблюдением.", ""]
    else:
        lines += ["⚪ Подходящего сетапа нет. Ждём более качественной ситуации.", ""]
    lines += [
        f"🔵 Зона интереса: ${state['zone_low']} — ${state['zone_high']}",
        f"📰 Новости: {state['news_impact']}",
    ]
    return "\n".join(lines)


def render_details(state: dict) -> str:
    return "\n".join([
        f"📋 <b>ДЕТАЛИ · {state['coin']}</b>",
        TEST_NOTE,
        "",
        "📊 <b>Технический анализ</b>",
        f"RSI: {state['rsi']}",
        f"MACD: {state['macd']}",
        f"EMA: {state['ema']}",
        f"Объём: {state['volume']}",
        "",
        "📖 <b>Стакан</b>",
        f"{state['bid_ask']} (дисбаланс {state['imbalance']})",
        "",
        "🕯 <b>Свечная формация</b>",
        f"{state['candle_pattern']}",
    ])


def render_why_entry(state: dict) -> str:
    coin = state["coin"]
    status = state["status"]
    if status not in ("LONG", "SHORT"):
        return (
            f"🎯 <b>ПОЧЕМУ НЕТ ВХОДА · {coin}</b>\n{TEST_NOTE}\n\n"
            f"Статус: {STATUS_EMOJI[status]} {STATUS_LABEL[status]}\n\n"
            "Недостаточно подтверждений для качественного входа. "
            "Как только условия совпадут, статус изменится."
        )
    if status == "LONG":
        cancel = f"Закрепление цены ниже ${state['zone_low']} отменяет LONG."
    else:
        cancel = f"Закрепление цены выше ${state['zone_high']} отменяет SHORT."
    return "\n".join([
        f"🎯 <b>ПОЧЕМУ {status} · {coin}</b>",
        TEST_NOTE,
        "",
        f"✅ Тренд: {state['trend']}",
        "✅ Цена в зоне интереса",
        f"✅ Стакан: {state['bid_ask']}",
        f"✅ Свеча: {state['candle_pattern']}",
        f"✅ Объём: {state['volume']}",
        f"✅ Новости: {state['news_impact']}",
        "",
        f"🎯 Балл: {state['score']}/100",
        f"📍 Вход ${state['entry']} · 🛑 SL ${state['stop_loss']}",
        "",
        f"❗ <b>Отмена сценария:</b> {cancel}",
    ])


def render_news_placeholder() -> str:
    return "📰 <b>НОВОСТИ</b>\n\nМодуль новостного анализа будет подключён на этапе V0.9."


def render_stats_placeholder() -> str:
    return "📊 <b>СТАТИСТИКА</b>\n\nМодуль статистики будет подключён на этапе V1.2."


def main_menu_keyboard(states: dict) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for coin in COINS:
        emoji = STATUS_EMOJI[states[coin]["status"]]
        builder.button(text=f"{emoji} {coin}", callback_data=f"coin:{coin}")
    builder.adjust(3)
    builder.row(InlineKeyboardButton(text="🔄 ОБНОВИТЬ", callback_data="refresh"))
    builder.row(
        InlineKeyboardButton(text="📰 НОВОСТИ", callback_data="news"),
        InlineKeyboardButton(text="📊 СТАТИСТИКА", callback_data="stats"),
    )
    return builder.as_markup()


def coin_card_keyboard(coin: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="📊 ГРАФИК", callback_data=f"chart:{coin}"),
        InlineKeyboardButton(text="🎯 ПОЧЕМУ?", callback_data=f"why:{coin}"),
    )
    builder.row(InlineKeyboardButton(text="📋 ДЕТАЛИ", callback_data=f"details:{coin}"))
    builder.row(
        InlineKeyboardButton(text="🔄 ОБНОВИТЬ", callback_data=f"coin:{coin}"),
        InlineKeyboardButton(text="⬅️ НАЗАД", callback_data="back"),
    )
    return builder.as_markup()


def back_keyboard(coin: str | None = None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if coin:
        builder.row(InlineKeyboardButton(text="⬅️ К МОНЕТЕ", callback_data=f"coin:{coin}"))
    builder.row(InlineKeyboardButton(text="🏠 В МЕНЮ", callback_data="back"))
    return builder.as_markup()


router = Router()


@router.message(CommandStart())
async def cmd_start(message: Message):
    states = get_all_states()
    await message.answer(render_main_menu(states), reply_markup=main_menu_keyboard(states))


@router.callback_query(F.data == "back")
async def cb_back(callback: CallbackQuery):
    states = get_all_states()
    await callback.message.edit_text(render_main_menu(states), reply_markup=main_menu_keyboard(states))
    await callback.answer()


@router.callback_query(F.data == "refresh")
async def cb_refresh(callback: CallbackQuery):
    states = refresh_all_states()
    await callback.message.edit_text(render_main_menu(states), reply_markup=main_menu_keyboard(states))
    await callback.answer("Обновлено")


@router.callback_query(F.data.startswith("coin:"))
async def cb_coin_card(callback: CallbackQuery):
    coin = callback.data.split(":")[1]
    state = get_coin_state(coin)
    await callback.message.edit_text(render_coin_card(state), reply_markup=coin_card_keyboard(coin))
    await callback.answer()


@router.callback_query(F.data.startswith("details:"))
async def cb_details(callback: CallbackQuery):
    coin = callback.data.split(":")[1]
    state = get_coin_state(coin)
    await callback.message.edit_text(render_details(state), reply_markup=back_keyboard(coin))
    await callback.answer()


@router.callback_query(F.data.startswith("why:"))
async def cb_why_entry(callback: CallbackQuery):
    coin = callback.data.split(":")[1]
    state = get_coin_state(coin)
    await callback.message.edit_text(render_why_entry(state), reply_markup=back_keyboard(coin))
    await callback.answer()


@router.callback_query(F.data.startswith("chart:"))
async def cb_chart(callback: CallbackQuery):
    coin = callback.data.split(":")[1]
    await callback.answer(f"📊 Графики для {coin} появятся на этапе V1.1", show_alert=True)


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
    if not BOT_TOKEN:
        raise RuntimeError(
            "BOT_TOKEN не задан. Добавьте переменную окружения BOT_TOKEN "
            "со значением токена от @BotFather."
        )
    logging.basicConfig(level=logging.INFO)
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)
    await bot.delete_webhook(drop_pending_updates=True)
    await asyncio.gather(dp.start_polling(bot), start_web_server())


if __name__ == "__main__":
    asyncio.run(main())
