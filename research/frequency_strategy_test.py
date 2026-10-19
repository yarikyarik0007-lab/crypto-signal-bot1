"""Research-only frequency/quality backtest. Never imported by production bot."""
import asyncio
import csv
import json
import os
from pathlib import Path

import aiohttp
import bot

COINS = list(bot.COINS)
TARGET = int(os.getenv("BT_CANDLES", "4000"))
TRAIN_FRACTION = 0.70
OUT = Path("frequency-results")
OUT.mkdir(exist_ok=True)

CONFIGS = [
    {"name": "A_4h_current_short_only", "interval": "4h", "margin": 0.005, "volume": 1.0,
     "tol": 0.006, "window": 8, "mode": "short"},
    {"name": "B_1h_current_short_only", "interval": "1h", "margin": 0.005, "volume": 1.0,
     "tol": 0.006, "window": 8, "mode": "short"},
    {"name": "C_1h_balanced_both_directions", "interval": "1h", "margin": 0.003, "volume": 0.8,
     "tol": 0.008, "window": 12, "mode": "trend_both"},
    {"name": "D_1h_more_opportunities", "interval": "1h", "margin": 0.0025, "volume": 0.8,
     "tol": 0.010, "window": 16, "mode": "trend_both"},
    {"name": "E_1h_volume_filter_removed", "interval": "1h", "margin": 0.003, "volume": 0.0,
     "tol": 0.008, "window": 12, "mode": "trend_both"},
]


def run(candles, cfg):
    trades, pending = [], []
    start = bot.BT_ZONE_LOOKBACK + bot.BT_PIVOT_WINDOW + 2
    i = start
    while i < len(candles) - 1:
        sig = candles[i]
        pending = [p for p in pending if i - p["breakout_i"] <= cfg["window"]]
        triggered = None
        for p in pending:
            if p["dir"] == "LONG":
                touch = sig["l"] <= p["level"] * (1 + cfg["tol"])
                valid = touch and sig["c"] > p["level"] and sig["c"] > sig["o"]
            else:
                touch = sig["h"] >= p["level"] * (1 - cfg["tol"])
                valid = touch and sig["c"] < p["level"] and sig["c"] < sig["o"]
            if valid:
                triggered = p
                break

        if triggered:
            pending = [p for p in pending if p is not triggered]
            direction, level = triggered["dir"], triggered["level"]
            regime = bot.classify_regime(candles, i)
            if cfg["mode"] == "short" and (direction != "SHORT" or regime != "TREND_DOWN"):
                i += 1
                continue
            if cfg["mode"] == "trend_both":
                if (direction == "LONG" and regime != "TREND_UP") or (
                    direction == "SHORT" and regime != "TREND_DOWN"
                ):
                    i += 1
                    continue

            entry = candles[i + 1]["o"]
            if direction == "LONG":
                sl = min(level * (1 - 0.005), sig["l"] * 0.998)
                risk = entry - sl
                tp = entry + bot.BT_RR_TARGET * risk
            else:
                sl = max(level * (1 + 0.005), sig["h"] * 1.002)
                risk = sl - entry
                tp = entry - bot.BT_RR_TARGET * risk
            if risk <= 0:
                i += 1
                continue

            exit_r, exit_i, exit_reason = None, None, None
            for k in range(i + 1, len(candles)):
                c = candles[k]
                if direction == "LONG":
                    hit_sl, hit_tp = c["l"] <= sl, c["h"] >= tp
                else:
                    hit_sl, hit_tp = c["h"] >= sl, c["l"] <= tp
                if hit_sl:
                    exit_r, exit_i, exit_reason = -1.0, k, "stop"
                    break
                if hit_tp:
                    exit_r, exit_i, exit_reason = bot.BT_RR_TARGET, k, "target"
                    break
            if exit_r is None:
                c = candles[-1]["c"]
                exit_r = (c - entry) / risk if direction == "LONG" else (entry - c) / risk
                exit_i, exit_reason = len(candles) - 1, "data_end"
            exit_r -= bot.BT_FEE_R
            trades.append({
                "direction": direction, "entry_i": i + 1, "exit_i": exit_i,
                "entry_time": candles[i + 1].get("t"), "exit_time": candles[exit_i].get("t"),
                "r": round(exit_r, 4), "exit_reason": exit_reason, "regime": regime,
            })
            pending = []
            i = exit_i + 1
            continue

        resistance = bot.find_pivot_level(candles, i, bot.BT_ZONE_LOOKBACK, "high")
        support = bot.find_pivot_level(candles, i, bot.BT_ZONE_LOOKBACK, "low")
        vols = [c["v"] for c in candles[max(0, i - 10):i]]
        avg_vol = sum(vols) / len(vols) if vols else 0
        vol_ok = avg_vol == 0 or sig["v"] >= avg_vol * cfg["volume"]
        if vol_ok and resistance and sig["c"] > resistance * (1 + cfg["margin"]):
            if not any(p["dir"] == "LONG" and p["level"] == resistance for p in pending):
                pending.append({"dir": "LONG", "level": resistance, "breakout_i": i})
        if vol_ok and support and sig["c"] < support * (1 - cfg["margin"]):
            if not any(p["dir"] == "SHORT" and p["level"] == support for p in pending):
                pending.append({"dir": "SHORT", "level": support, "breakout_i": i})
        i += 1
    return trades


def metrics(trades, days):
    rs = [float(t["r"]) for t in trades]
    wins = sum(x > 0 for x in rs)
    gw = sum(x for x in rs if x > 0)
    gl = -sum(x for x in rs if x < 0)
    equity = peak = max_dd = 0.0
    for x in rs:
        equity += x
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return {
        "trades": len(rs), "wins": wins,
        "win_rate_pct": round(100 * wins / len(rs), 2) if rs else 0,
        "profit_factor": round(gw / gl, 3) if gl else (999 if gw else 0),
        "net_R": round(sum(rs), 3), "avg_R": round(sum(rs) / len(rs), 4) if rs else 0,
        "max_drawdown_R": round(max_dd, 3),
        "signals_per_day": round(len(rs) / days, 3) if days > 0 else 0,
    }


async def load(session, coin, interval):
    bot._SESSION = session
    candles = await bot.fetch_historical_klines(f"{coin}-USDT", interval, TARGET)
    interval_ms = 60 * 60 * 1000 if interval == "1h" else 4 * 60 * 60 * 1000
    now_ms = int(__import__("time").time() * 1000)
    return [c for c in candles if c.get("t") and now_ms - c["t"] >= interval_ms]


async def main():
    errors, results, all_rows = [], [], []
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        cache = {}
        for cfg in CONFIGS:
            interval = cfg["interval"]
            for coin in COINS:
                try:
                    key = (coin, interval)
                    if key not in cache:
                        cache[key] = await load(session, coin, interval)
                    candles = cache[key]
                    if len(candles) < 500:
                        errors.append({"coin": coin, "interval": interval,
                                       "error": f"only {len(candles)} closed candles"})
                        continue
                    split = int(len(candles) * TRAIN_FRACTION)
                    split_time = candles[split]["t"]
                    train_trades = run(candles[:split], cfg)
                    test_all = run(candles[max(0, split - 200):], cfg)
                    test_trades = [t for t in test_all if (t.get("entry_time") or 0) >= split_time]
                    train_days = max(1, (candles[split - 1]["t"] - candles[0]["t"]) / 86400000)
                    test_days = max(1, (candles[-1]["t"] - split_time) / 86400000)
                    for period, trades, days in (
                        ("train", train_trades, train_days), ("holdout", test_trades, test_days)
                    ):
                        row = {"config": cfg["name"], "coin": coin, "period": period,
                               "candles": len(candles), **metrics(trades, days)}
                        all_rows.append(row)
                    print(f"{cfg['name']} {coin}: train={len(train_trades)}, holdout={len(test_trades)}")
                except Exception as exc:
                    errors.append({"coin": coin, "interval": interval, "error": repr(exc)})

    summaries = []
    for cfg in CONFIGS:
        for period in ("train", "holdout"):
            rows = [r for r in all_rows if r["config"] == cfg["name"] and r["period"] == period]
            total = sum(r["trades"] for r in rows)
            wins = sum(r["wins"] for r in rows)
            gw = sum(max(0, r["net_R"]) for r in [])  # gross values computed from trade rows below
            trades = []
            for coin in COINS:
                candles = cache.get((coin, cfg["interval"]), [])
                if not candles:
                    continue
                split = int(len(candles) * TRAIN_FRACTION)
                split_time = candles[split]["t"]
                if period == "train":
                    trades.extend(run(candles[:split], cfg))
                else:
                    trades.extend(t for t in run(candles[max(0, split - 200):], cfg)
                                  if (t.get("entry_time") or 0) >= split_time)
            trade_rs = [float(t["r"]) for t in trades]
            days = sum((max(1, (cache[(coin, cfg["interval"])][int(len(cache[(coin, cfg["interval"])])*TRAIN_FRACTION)-1]["t"] -
                                cache[(coin, cfg["interval"])][0]["t"]) / 86400000)
                        if period == "train" else
                        max(1, (cache[(coin, cfg["interval"])][-1]["t"] -
                                cache[(coin, cfg["interval"])][int(len(cache[(coin, cfg["interval"])])*TRAIN_FRACTION)]["t"]) / 86400000))
                       for coin in COINS if cache.get((coin, cfg["interval"])))
            days = days / len(COINS) if COINS else 1
            summary = {"config": cfg["name"], "period": period,
                       **metrics(trades, days)}
            summaries.append(summary)

    with (OUT / "per_coin.csv").open("w", newline="") as f:
        fields = list(all_rows[0].keys()) if all_rows else ["config", "coin", "period"]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(all_rows)
    report = {"purpose": "Compare signal frequency and trade quality on closed candles; research only.",
              "configs": CONFIGS, "summary": summaries, "per_coin": all_rows, "errors": errors,
              "warning": "A single historical split is exploratory. Signals/day is an average across the tested coin-universe, not a promise. No production settings were changed."}
    (OUT / "summary.json").write_text(json.dumps(report, indent=2))
    print(json.dumps({"summary": summaries, "errors": errors}, indent=2))
    if errors:
        raise SystemExit("Some coin histories were incomplete; do not treat run as fully validated.")


if __name__ == "__main__":
    asyncio.run(main())
