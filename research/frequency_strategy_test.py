"""Research-only frequency/quality backtest. Never imported by production bot."""
import asyncio
import csv
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

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
    {"name": "A2_4h_long_only", "interval": "4h", "margin": 0.005, "volume": 1.0,
     "tol": 0.006, "window": 8, "mode": "long"},
    {"name": "A3_4h_both_trend_aligned", "interval": "4h", "margin": 0.005, "volume": 1.0,
     "tol": 0.006, "window": 8, "mode": "trend_both"},
    {"name": "A4_4h_both_moderate", "interval": "4h", "margin": 0.003, "volume": 0.8,
     "tol": 0.008, "window": 12, "mode": "trend_both"},
    {"name": "B_1h_current_short_only", "interval": "1h", "margin": 0.005, "volume": 1.0,
     "tol": 0.006, "window": 8, "mode": "short"},
    {"name": "B1_1h_long_only", "interval": "1h", "margin": 0.005, "volume": 1.0,
     "tol": 0.006, "window": 8, "mode": "long"},
    {"name": "B2_2h_current_short_only", "interval": "2h", "margin": 0.005, "volume": 1.0,
     "tol": 0.006, "window": 8, "mode": "short"},
    {"name": "B3_2h_long_only", "interval": "2h", "margin": 0.005, "volume": 1.0,
     "tol": 0.006, "window": 8, "mode": "long"},
    {"name": "C2_2h_balanced_both_directions", "interval": "2h", "margin": 0.003, "volume": 0.8,
     "tol": 0.008, "window": 12, "mode": "trend_both"},
    {"name": "D2_2h_stricter_both_directions", "interval": "2h", "margin": 0.004, "volume": 1.0,
     "tol": 0.006, "window": 8, "mode": "trend_both"},
    {"name": "C_1h_balanced_both_directions", "interval": "1h", "margin": 0.003, "volume": 0.8,
     "tol": 0.008, "window": 12, "mode": "trend_both"},
    {"name": "D_1h_more_opportunities", "interval": "1h", "margin": 0.0025, "volume": 0.8,
     "tol": 0.010, "window": 16, "mode": "trend_both"},
    {"name": "E_1h_volume_filter_removed", "interval": "1h", "margin": 0.003, "volume": 0.0,
     "tol": 0.008, "window": 12, "mode": "trend_both"},
    {"name": "F_2h_long_confirmed_by_4h", "interval": "2h", "margin": 0.005, "volume": 1.0,
     "tol": 0.006, "window": 8, "mode": "long_htf", "htf": "4h"},
    {"name": "F2_1h_long_confirmed_by_4h", "interval": "1h", "margin": 0.005, "volume": 1.0,
     "tol": 0.006, "window": 8, "mode": "long_htf", "htf": "4h"},
    {"name": "F3_2h_long_confirmed_by_4h_strict", "interval": "2h", "margin": 0.006, "volume": 1.2,
     "tol": 0.005, "window": 6, "mode": "long_htf", "htf": "4h"},
    {"name": "F4_2h_long_confirmed_by_4h_balanced", "interval": "2h", "margin": 0.003, "volume": 0.8,
     "tol": 0.008, "window": 12, "mode": "long_htf", "htf": "4h"},
]


def run(candles, cfg, regimes=None, htf_regimes=None):
    trades, pending = [], []
    start = bot.BT_ZONE_LOOKBACK + bot.BT_PIVOT_WINDOW + 2
    i = start
    while i < len(candles) - 1:
        sig = candles[i]
        pending = [p for p in pending if i - p["breakout_i"] <= cfg["window"]]
        triggered = None
        regime = regimes[i] if regimes is not None else bot.classify_regime(candles, i)
        htf_regime = htf_regimes[i] if htf_regimes is not None else None
        for p in pending:
            # Filter direction/regime before selecting a retest. Otherwise an
            # ineligible pending setup can mask an eligible setup on the same candle.
            if cfg["mode"] == "short" and (p["dir"] != "SHORT" or regime != "TREND_DOWN"):
                continue
            if cfg["mode"] == "long" and (p["dir"] != "LONG" or regime != "TREND_UP"):
                continue
            if cfg["mode"] == "long_htf" and (p["dir"] != "LONG" or htf_regime != "TREND_UP"):
                continue
            if cfg["mode"] == "trend_both":
                if (p["dir"] == "LONG" and regime != "TREND_UP") or (
                    p["dir"] == "SHORT" and regime != "TREND_DOWN"
                ):
                    continue
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


def metrics(trades, days, extra_cost_r=0.0):
    rs = [float(t["r"]) - extra_cost_r for t in trades]
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
    interval_ms = {"1h": 60, "2h": 120, "4h": 240}[interval] * 60 * 1000
    now_ms = int(__import__("time").time() * 1000)
    return [c for c in candles if c.get("t") and now_ms - c["t"] >= interval_ms]



def fast_regimes(candles, period=14):
    """Linear-time equivalent of bot.classify_regime for research backtests."""
    n = len(candles)
    out = ["UNKNOWN"] * n
    if n < 2:
        return out

    trs, plus_dm, minus_dm = [], [], []
    for i in range(1, n):
        h, l = candles[i]["h"], candles[i]["l"]
        ph, pl, pc = candles[i - 1]["h"], candles[i - 1]["l"], candles[i - 1]["c"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        up, down = h - ph, pl - l
        plus_dm.append(up if (up > down and up > 0) else 0.0)
        minus_dm.append(down if (down > up and down > 0) else 0.0)

    atr = None
    adx = None
    atr_sum = plus_sum = minus_sum = None
    dx_values = []
    closes = []

    for idx, candle in enumerate(candles):
        closes.append(candle["c"])
        if idx == 0:
            continue
        j = idx - 1
        tr = trs[j]
        if j == period - 1:
            atr = sum(trs[:period]) / period
            atr_sum = sum(trs[:period])
            plus_sum = sum(plus_dm[:period])
            minus_sum = sum(minus_dm[:period])
        elif j >= period:
            atr = (atr * (period - 1) + tr) / period
            atr_sum = atr_sum - atr_sum / period + tr
            plus_sum = plus_sum - plus_sum / period + plus_dm[j]
            minus_sum = minus_sum - minus_sum / period + minus_dm[j]

        if j >= period - 1:
            denom = plus_sum + minus_sum
            dx_values.append(100 * abs(plus_sum - minus_sum) / denom if denom else 0.0)
            if len(dx_values) == period:
                adx = sum(dx_values[:period]) / period
            elif len(dx_values) > period:
                adx = (adx * (period - 1) + dx_values[-1]) / period

        if idx < 29:
            continue
        # bot.compute_adx returns None until 2*period candles.
        # Its DX series has period-valid values beginning at candle index 2*period.
        if idx < period * 2:
            adx_value = None
        else:
            adx_value = adx
        price = candle["c"]
        atr_pct = (atr / price * 100) if (atr and price) else 0.0
        if atr_pct > bot.ATR_VOLATILE_PCT:
            out[idx] = "VOLATILE"
        elif adx_value is None:
            out[idx] = "UNKNOWN"
        elif adx_value < bot.ADX_TREND_THRESHOLD:
            out[idx] = "RANGE"
        else:
            sma20 = sum(closes[-20:]) / 20 if len(closes) >= 20 else None
            sma50 = sum(closes[-50:]) / 50 if len(closes) >= 50 else None
            if sma20 and sma50:
                out[idx] = "TREND_UP" if sma20 > sma50 else "TREND_DOWN"
            else:
                out[idx] = "UNKNOWN"
    return out


def validated_fast_regimes(candles):
    """Cross-check the optimized regime calculation against production logic."""
    fast = fast_regimes(candles)
    if len(candles) < 30:
        return fast
    # Deterministic spread of checks, including early and late history.
    checks = sorted(set([29, 30, 55, len(candles) // 4, len(candles) // 2,
                         (3 * len(candles)) // 4, len(candles) - 1]))
    for idx in checks:
        if idx >= 0 and idx < len(candles):
            expected = bot.classify_regime(candles, idx)
            if fast[idx] != expected:
                raise RuntimeError(
                    f"fast regime mismatch at idx={idx}: {fast[idx]} != {expected}"
                )
    return fast


async def main():
    errors, results, all_rows = [], [], []
    regimes_cache = {}
    htf_cache = {}
    trades_cache = {}
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
                    regime_key = (coin, interval)
                    if regime_key not in regimes_cache:
                        regimes_cache[regime_key] = validated_fast_regimes(candles)
                    regimes = regimes_cache[regime_key]
                    htf_regimes = None
                    if cfg.get("htf"):
                        htf_key = (coin, cfg["htf"])
                        if htf_key not in cache:
                            cache[htf_key] = await load(session, coin, cfg["htf"])
                        high_candles = cache[htf_key]
                        if htf_key not in regimes_cache:
                            regimes_cache[htf_key] = validated_fast_regimes(high_candles)
                        high_regimes = regimes_cache[htf_key]
                        import bisect
                        high_times = [c["t"] for c in high_candles]
                        lower_ms = {"1h": 3600000, "2h": 7200000, "4h": 14400000}[interval]
                        high_ms = {"1h": 3600000, "2h": 7200000, "4h": 14400000}[cfg["htf"]]
                        htf_regimes = []
                        for candle in candles:
                            cutoff = candle["t"] + lower_ms - high_ms
                            hi = bisect.bisect_right(high_times, cutoff) - 1
                            htf_regimes.append(high_regimes[hi] if hi >= 0 else "UNKNOWN")
                        htf_cache[regime_key] = htf_regimes
                    train_trades = run(candles[:split], cfg, regimes[:split],
                                       htf_regimes[:split] if htf_regimes is not None else None)
                    test_start = max(0, split - 200)
                    test_all = run(candles[test_start:], cfg, regimes[test_start:],
                                   htf_regimes[test_start:] if htf_regimes is not None else None)
                    test_trades = [t for t in test_all if (t.get("entry_time") or 0) >= split_time]
                    trades_cache[(cfg["name"], coin, "train")] = train_trades
                    trades_cache[(cfg["name"], coin, "holdout")] = test_trades
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
    cost_stress = []
    for cfg in CONFIGS:
        for period in ("train", "holdout"):
            rows = [r for r in all_rows if r["config"] == cfg["name"] and r["period"] == period]
            total = sum(r["trades"] for r in rows)
            wins = sum(r["wins"] for r in rows)
            gw = sum(max(0, r["net_R"]) for r in [])  # gross values computed from trade rows below
            trades = []
            for coin in COINS:
                trades.extend(trades_cache.get((cfg["name"], coin, period), []))
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
            for extra_cost_r in (0.05, 0.10):
                cost_stress.append({
                    "config": cfg["name"], "period": period,
                    "extra_cost_R_per_trade": extra_cost_r,
                    **metrics(trades, days, extra_cost_r=extra_cost_r),
                })

    with (OUT / "per_coin.csv").open("w", newline="") as f:
        fields = list(all_rows[0].keys()) if all_rows else ["config", "coin", "period"]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(all_rows)
    report = {"purpose": "Compare signal frequency and trade quality on closed candles; research only.",
              "configs": CONFIGS, "summary": summaries, "cost_stress": cost_stress,
              "per_coin": all_rows, "errors": errors,
              "warning": "A single historical split is exploratory. Signals/day is an average across the tested coin-universe, not a promise. No production settings were changed."}
    (OUT / "summary.json").write_text(json.dumps(report, indent=2))
    print(json.dumps({"summary": summaries, "errors": errors}, indent=2))
    if errors:
        raise SystemExit("Some coin histories were incomplete; do not treat run as fully validated.")


if __name__ == "__main__":
    asyncio.run(main())
