"""Isolated strategy research harness. Never imported by the production bot."""
import asyncio
import csv
import json
import math
import os
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import aiohttp
import bot

COINS = ["BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "ADA", "AVAX", "LINK", "SUI", "ZEC"]
CANDLE_TARGET = int(os.getenv("BT_CANDLES", "1200"))
TRAIN_FRACTION = 0.70
OUT = Path("backtest-results")
OUT.mkdir(exist_ok=True)


def nearest_pivot(candles, end_idx, lookback, side, pivot_window=bot.BT_PIVOT_WINDOW):
    start = max(end_idx - lookback, pivot_window)
    pivots = []
    for idx in range(start, end_idx - pivot_window):
        lo, hi = idx - pivot_window, idx + pivot_window + 1
        if lo < 0 or hi > len(candles):
            continue
        seg = candles[lo:hi]
        center = candles[idx]
        if side == "low" and center["l"] == min(c["l"] for c in seg):
            pivots.append((idx, center["l"]))
        elif side != "low" and center["h"] == max(c["h"] for c in seg):
            pivots.append((idx, center["h"]))
    return pivots[-1][1] if pivots else None


def run_variant(candles, *, pivot="extreme", volume_multiple=1.0,
                retest_tolerance=bot.BT_RETEST_TOLERANCE,
                retest_window=bot.BT_RETEST_WINDOW,
                regime_history=None, entry_at_close=False):
    """Mirror current run_backtest; expose one controlled parameter at a time."""
    n, trades, pending = len(candles), [], []
    i = bot.BT_ZONE_LOOKBACK + bot.BT_PIVOT_WINDOW + 2
    pivot_fn = bot.find_pivot_level if pivot == "extreme" else nearest_pivot
    while i < n - 1:
        sig = candles[i]
        pending = [p for p in pending if i - p["breakout_i"] <= retest_window]
        triggered = None
        for p in pending:
            if p["dir"] == "LONG":
                touch = sig["l"] <= p["level"] * (1 + retest_tolerance)
                holds, candle_ok = sig["c"] > p["level"], sig["c"] > sig["o"]
            else:
                touch = sig["h"] >= p["level"] * (1 - retest_tolerance)
                holds, candle_ok = sig["c"] < p["level"], sig["c"] < sig["o"]
            if touch and holds and candle_ok:
                triggered = p
                break
        if triggered:
            pending = [p for p in pending if p is not triggered]
            direction, level = triggered["dir"], triggered["level"]
            if regime_history is None:
                regime = bot.classify_regime(candles, i)
            else:
                regime_start = max(0, i - regime_history + 1)
                regime_candles = candles[regime_start:i + 1]
                regime = bot.classify_regime(regime_candles, len(regime_candles) - 1)
            if regime != "TREND_DOWN" or direction != "SHORT":
                i += 1
                continue
            entry = sig["c"] if entry_at_close else candles[i + 1]["o"]
            sl = max(level * (1 + 0.005), sig["h"] * 1.002)
            risk = sl - entry
            if risk <= 0:
                i += 1
                continue
            tp = entry - bot.BT_RR_TARGET * risk
            exit_r, exit_i, exit_price, exit_reason = None, None, None, None
            for k in range(i + 1, n):
                c = candles[k]
                if c["h"] >= sl:
                    exit_r, exit_i, exit_price, exit_reason = -1.0, k, sl, "stop"
                    break
                if c["l"] <= tp:
                    exit_r, exit_i, exit_price, exit_reason = bot.BT_RR_TARGET, k, tp, "target"
                    break
            if exit_r is None:
                exit_i = n - 1
                exit_price = candles[-1]["c"]
                exit_r = (exit_price - entry) / risk
                exit_reason = "data_end"
            gross_r = exit_r
            exit_r -= bot.BT_FEE_R
            entry_i = i if entry_at_close else i + 1
            trades.append({"dir": direction, "entry_i": entry_i, "exit_i": exit_i,
                           "r": round(exit_r, 4), "gross_r": round(gross_r, 6),
                           "entry_price": entry, "exit_price": exit_price, "risk_price": risk,
                           "exit_reason": exit_reason, "regime": regime,
                           "entry_time": candles[entry_i].get("t"),
                           "exit_time": candles[exit_i].get("t")})
            pending = []
            i = exit_i + 1
            continue
        resistance = pivot_fn(candles, i, bot.BT_ZONE_LOOKBACK, "high")
        support = pivot_fn(candles, i, bot.BT_ZONE_LOOKBACK, "low")
        vols = [c["v"] for c in candles[max(0, i - 10):i]]
        avg_vol = sum(vols) / len(vols) if vols else 0
        vol_ok = avg_vol == 0 or sig["v"] >= avg_vol * volume_multiple
        if resistance and sig["c"] > resistance * (1 + bot.BT_BREAKOUT_MARGIN) and vol_ok:
            if not any(p["dir"] == "LONG" and p["level"] == resistance for p in pending):
                pending.append({"level": resistance, "dir": "LONG", "breakout_i": i})
        if support and sig["c"] < support * (1 - bot.BT_BREAKOUT_MARGIN) and vol_ok:
            if not any(p["dir"] == "SHORT" and p["level"] == support for p in pending):
                pending.append({"level": support, "dir": "SHORT", "breakout_i": i})
        i += 1
    return trades


def metrics(trades):
    rs = [float(t["r"]) for t in trades]
    wins = sum(r > 0 for r in rs)
    gross_win = sum(r for r in rs if r > 0)
    gross_loss = -sum(r for r in rs if r < 0)
    equity = peak = max_dd = 0.0
    for r in rs:
        equity += r
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return {
        "trades": len(rs),
        "wins": wins,
        "win_rate_pct": round(100 * wins / len(rs), 2) if rs else 0.0,
        "gross_win_R": round(gross_win, 3),
        "gross_loss_R": round(gross_loss, 3),
        "profit_factor": round(gross_win / gross_loss, 3) if gross_loss else (999.0 if gross_win else 0.0),
        "net_R": round(sum(rs), 3),
        "max_drawdown_R": round(max_dd, 3),
        "avg_R": round(statistics.mean(rs), 4) if rs else 0.0,
    }


async def load_candles(session, coin):
    bot._SESSION = session
    candles = await bot.fetch_historical_klines(f"{coin}-USDT", "4h", CANDLE_TARGET)
    now_ms = int(__import__("time").time() * 1000)
    interval_ms = 4 * 60 * 60 * 1000
    candles = [c for c in candles if c.get("t") and now_ms - c["t"] >= interval_ms]
    return candles


async def main():
    timeout = aiohttp.ClientTimeout(total=25)
    all_rows, fetch_errors = [], []
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for coin in COINS:
            try:
                candles = await load_candles(session, coin)
                if len(candles) < 250:
                    fetch_errors.append({"coin": coin, "error": f"only {len(candles)} closed candles"})
                    continue
                split = int(len(candles) * TRAIN_FRACTION)
                configs = [
                    ("A_current", dict(pivot="extreme", volume_multiple=1.0,
                                       retest_tolerance=bot.BT_RETEST_TOLERANCE,
                                       retest_window=bot.BT_RETEST_WINDOW)),
                    ("B_nearest_pivot", dict(pivot="nearest", volume_multiple=1.0,
                                             retest_tolerance=bot.BT_RETEST_TOLERANCE,
                                             retest_window=bot.BT_RETEST_WINDOW)),
                    ("C_volume_0.8x", dict(pivot="extreme", volume_multiple=0.8,
                                            retest_tolerance=bot.BT_RETEST_TOLERANCE,
                                            retest_window=bot.BT_RETEST_WINDOW)),
                    ("C2_no_volume_filter", dict(pivot="extreme", volume_multiple=0.0,
                                                 retest_tolerance=bot.BT_RETEST_TOLERANCE,
                                                 retest_window=bot.BT_RETEST_WINDOW)),
                    ("D_retest_wider", dict(pivot="extreme", volume_multiple=1.0,
                                            retest_tolerance=0.01, retest_window=12)),
                    ("E_live_like_200_close", dict(pivot="extreme", volume_multiple=1.0,
                                                   retest_tolerance=bot.BT_RETEST_TOLERANCE,
                                                   retest_window=bot.BT_RETEST_WINDOW,
                                                   regime_history=200, entry_at_close=True)),
                ]
                train = candles[:split]
                # Include a 200-candle warm-up, but only count entries after the split.
                test_start = max(0, split - 200)
                test = candles[test_start:]
                split_time = candles[split]["t"]
                for name, config in configs:
                    train_trades = run_variant(train, **config)
                    test_trades_all = run_variant(test, **config)
                    test_trades = [t for t in test_trades_all if t.get("entry_time", 0) >= split_time]
                    for period, trades in (("train", train_trades), ("holdout", test_trades)):
                        row = {"coin": coin, "variant": name, "period": period,
                               "candles": len(train) if period == "train" else len(test),
                               **metrics(trades)}
                        all_rows.append(row)
                # Sanity-check custom A against the production backtest implementation.
                base = bot.run_backtest(train)
                mirror = run_variant(train, pivot="extreme", volume_multiple=1.0,
                                      retest_tolerance=bot.BT_RETEST_TOLERANCE,
                                      retest_window=bot.BT_RETEST_WINDOW)
                if len(base) != len(mirror) or [t["r"] for t in base] != [t["r"] for t in mirror]:
                    fetch_errors.append({"coin": coin, "error": "A mirror mismatch vs bot.run_backtest",
                                         "production_trades": len(base), "mirror_trades": len(mirror)})
                print(f"{coin}: {len(candles)} closed candles processed")
            except Exception as exc:
                fetch_errors.append({"coin": coin, "error": repr(exc)})
                print(f"{coin}: ERROR {exc!r}")
    fields = ["coin", "variant", "period", "candles", "trades", "wins", "win_rate_pct",
              "gross_win_R", "gross_loss_R", "profit_factor", "net_R", "max_drawdown_R", "avg_R"]
    with (OUT / "metrics.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(all_rows)
    (OUT / "errors.json").write_text(json.dumps(fetch_errors, indent=2))
    summaries = []
    for variant in sorted({r["variant"] for r in all_rows}):
        for period in ("train", "holdout"):
            rows = [r for r in all_rows if r["variant"] == variant and r["period"] == period]
            total_trades = sum(r["trades"] for r in rows)
            total_r = sum(r["net_R"] for r in rows)
            total_dd = sum(r["max_drawdown_R"] for r in rows)
            wins_total = sum(r["wins"] for r in rows)
            gross_win = sum(r["gross_win_R"] for r in rows)
            gross_loss = sum(r["gross_loss_R"] for r in rows)
            summaries.append({"variant": variant, "period": period, "coins_tested": len(rows),
                              "trades": total_trades,
                              "weighted_win_rate_pct": round(100*wins_total/total_trades, 2) if total_trades else 0,
                              "profit_factor": round(gross_win/gross_loss, 3) if gross_loss else (999.0 if gross_win else 0.0),
                              "sum_net_R": round(total_r, 3),
                              "sum_coin_max_drawdowns_R": round(total_dd, 3)})
    (OUT / "summary.json").write_text(json.dumps({"summary": summaries, "errors": fetch_errors}, indent=2))
    print(json.dumps({"summary": summaries, "errors": fetch_errors}, indent=2))
    if fetch_errors:
        # Data fetch or parity failures mean results must not be treated as validated.
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
