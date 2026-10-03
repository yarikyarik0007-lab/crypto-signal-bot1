import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import bot

async def fetch_history(session, symbol, target=2000):
    all_candles = []
    end_time = None
    for _ in range(6):
        if len(all_candles) >= target:
            break
        params = {"symbol": symbol, "interval": "4h", "limit": 500}
        if end_time is not None:
            params["endTime"] = end_time
        try:
            async with session.get(
                f"{bot.BINGX_BASE}/openApi/swap/v2/quote/klines",
                params=params,
                timeout=20,
            ) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}")
                payload = await resp.json(content_type=None)
        except Exception as exc:
            raise RuntimeError(f"{symbol}: BingX request failed: {exc}") from exc

        raw = payload.get("data") if payload else None
        if not raw:
            break
        batch = []
        for item in raw:
            try:
                if isinstance(item, dict):
                    t = int(item["time"])
                    o = float(item["open"]); h = float(item["high"])
                    l = float(item["low"]); c = float(item["close"])
                    v = float(item.get("volume", 0) or 0)
                else:
                    t = int(item[0]); o = float(item[1]); h = float(item[2])
                    l = float(item[3]); c = float(item[4]); v = float(item[5])
                batch.append({"t": t, "o": o, "h": h, "l": l, "c": c, "v": v})
            except (TypeError, ValueError, KeyError, IndexError):
                continue
        if not batch:
            break
        batch.sort(key=lambda x: x["t"])
        if all_candles:
            oldest = min(x["t"] for x in all_candles)
            batch = [x for x in batch if x["t"] < oldest]
        if not batch:
            break
        all_candles = batch + all_candles
        end_time = batch[0]["t"] - 1
        if len(batch) < 10:
            break
    return all_candles[-target:] if len(all_candles) > target else all_candles

def stats_for(candles):
    trades = bot.run_backtest(candles)
    return bot.summarize_trades(trades) or {"count": 0}

def validate_split(candles, name):
    n = len(candles)
    a = candles[: n // 2]
    b = candles[n // 2:]
    return {
        "period": name,
        "all": stats_for(candles),
        "first_half": stats_for(a),
        "second_half": stats_for(b),
    }

async def main():
    import aiohttp
    results = {}
    async with aiohttp.ClientSession() as session:
        for coin in bot.COINS:
            candles = await fetch_history(session, bot.BINGX_SYMBOL[coin], 2000)
            if len(candles) < 200:
                results[coin] = {"error": f"only {len(candles)} candles"}
                continue
            results[coin] = {
                "candles": len(candles),
                "validation": validate_split(candles, "full_history"),
            }

    print(json.dumps(results, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    asyncio.run(main())
