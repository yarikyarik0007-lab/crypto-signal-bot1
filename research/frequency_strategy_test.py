"""Research-only frequency/quality backtest. Never imported by production bot."""
import asyncio
import csv
import json
import os
import sys
import bisect
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import aiohttp
import bot

_EXCLUDED_COINS = {coin.strip().upper() for coin in os.getenv("BT_EXCLUDE_COINS", "").split(",") if coin.strip()}
COINS = [coin for coin in list(bot.COINS) if coin.upper() not in _EXCLUDED_COINS]
if not COINS:
    raise RuntimeError("BT_EXCLUDE_COINS removed every coin from the research universe")
TARGET = int(os.getenv("BT_CANDLES", "4000"))
TRAIN_FRACTION = 0.70
OUT = Path(os.getenv("BT_OUT_DIR", "frequency-results"))
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
    {"name": "F5_1h_short_confirmed_by_4h", "interval": "1h", "margin": 0.005, "volume": 1.0,
     "tol": 0.006, "window": 8, "mode": "short_htf", "htf": "4h"},
    {"name": "F6_1h_both_directions_confirmed_by_4h", "interval": "1h", "margin": 0.005, "volume": 1.0,
     "tol": 0.006, "window": 8, "mode": "trend_both_htf", "htf": "4h"},
    {"name": "F7_2h_short_confirmed_by_4h", "interval": "2h", "margin": 0.005, "volume": 1.0,
     "tol": 0.006, "window": 8, "mode": "short_htf", "htf": "4h"},
    {"name": "F8_2h_long_confirmed_by_4h_rr_1_5", "interval": "2h", "margin": 0.005, "volume": 1.0,
     "tol": 0.006, "window": 8, "mode": "long_htf", "htf": "4h", "rr_target": 1.5},
    {"name": "F9_2h_long_confirmed_by_4h_rr_2_5", "interval": "2h", "margin": 0.005, "volume": 1.0,
     "tol": 0.006, "window": 8, "mode": "long_htf", "htf": "4h", "rr_target": 2.5},
    {"name": "F10_2h_long_confirmed_by_local_and_4h", "interval": "2h", "margin": 0.005, "volume": 1.0,
     "tol": 0.006, "window": 8, "mode": "long_dual_htf", "htf": "4h"},
    {"name": "F11_2h_short_confirmed_by_local_and_4h", "interval": "2h", "margin": 0.005, "volume": 1.0,
     "tol": 0.006, "window": 8, "mode": "short_dual_htf", "htf": "4h"},
    {"name": "F12_2h_long_dual_htf_strict", "interval": "2h", "margin": 0.006, "volume": 1.2,
     "tol": 0.005, "window": 6, "mode": "long_dual_htf", "htf": "4h"},
    {"name": "F13_2h_short_dual_htf_strict", "interval": "2h", "margin": 0.006, "volume": 1.2,
     "tol": 0.005, "window": 6, "mode": "short_dual_htf", "htf": "4h"},
    # Predeclared sensitivity checks for the 2h high-RR LONG and 4h-confirmed SHORT.
    {"name": "F14_2h_long_confirmed_by_4h_rr_2_5_strict", "interval": "2h", "margin": 0.006, "volume": 1.2,
     "tol": 0.005, "window": 6, "mode": "long_htf", "htf": "4h", "rr_target": 2.5},
    {"name": "F15_2h_long_dual_htf_rr_2_5", "interval": "2h", "margin": 0.005, "volume": 1.0,
     "tol": 0.006, "window": 8, "mode": "long_dual_htf", "htf": "4h", "rr_target": 2.5},
    {"name": "F16_2h_short_confirmed_by_4h_rr_2_5", "interval": "2h", "margin": 0.005, "volume": 1.0,
     "tol": 0.006, "window": 8, "mode": "short_htf", "htf": "4h", "rr_target": 2.5},
    # Cross-market regime hypotheses: require BTC's last closed 4h regime to align.
    {"name": "F17_2h_long_btc4h_rr_2_5_strict", "interval": "2h", "margin": 0.006, "volume": 1.2,
     "tol": 0.005, "window": 6, "mode": "long_market_htf", "market_htf": "4h", "rr_target": 2.5},
    {"name": "F18_2h_short_btc4h_rr_2_5_strict", "interval": "2h", "margin": 0.006, "volume": 1.2,
     "tol": 0.005, "window": 6, "mode": "short_market_htf", "market_htf": "4h", "rr_target": 2.5},
    {"name": "F19_2h_long_local_and_btc4h_rr_2_5_strict", "interval": "2h", "margin": 0.006, "volume": 1.2,
     "tol": 0.005, "window": 6, "mode": "long_dual_market_htf", "market_htf": "4h", "rr_target": 2.5},
    {"name": "F20_2h_short_local_and_btc4h_rr_2_5_strict", "interval": "2h", "margin": 0.006, "volume": 1.2,
     "tol": 0.005, "window": 6, "mode": "short_dual_market_htf", "market_htf": "4h", "rr_target": 2.5},
    # Predeclared RR=2.0 sensitivity checks for BTC-regime filters; no new thresholds.
    {"name": "F21_2h_long_btc4h_rr_2_0_strict", "interval": "2h", "margin": 0.006, "volume": 1.2,
     "tol": 0.005, "window": 6, "mode": "long_market_htf", "market_htf": "4h", "rr_target": 2.0},
    {"name": "F22_2h_long_local_and_btc4h_rr_2_0_strict", "interval": "2h", "margin": 0.006, "volume": 1.2,
     "tol": 0.005, "window": 6, "mode": "long_dual_market_htf", "market_htf": "4h", "rr_target": 2.0},
    {"name": "F23_2h_short_btc4h_rr_2_0_strict", "interval": "2h", "margin": 0.006, "volume": 1.2,
     "tol": 0.005, "window": 6, "mode": "short_market_htf", "market_htf": "4h", "rr_target": 2.0},
    {"name": "F24_2h_short_local_and_btc4h_rr_2_0_strict", "interval": "2h", "margin": 0.006, "volume": 1.2,
     "tol": 0.005, "window": 6, "mode": "short_dual_market_htf", "market_htf": "4h", "rr_target": 2.0},
    # Predeclared RR=1.5 sensitivity checks; all other thresholds match the strict BTC-regime variants.
    {"name": "F25_2h_long_btc4h_rr_1_5_strict", "interval": "2h", "margin": 0.006, "volume": 1.2,
     "tol": 0.005, "window": 6, "mode": "long_market_htf", "market_htf": "4h", "rr_target": 1.5},
    {"name": "F26_2h_long_local_and_btc4h_rr_1_5_strict", "interval": "2h", "margin": 0.006, "volume": 1.2,
     "tol": 0.005, "window": 6, "mode": "long_dual_market_htf", "market_htf": "4h", "rr_target": 1.5},
    {"name": "F27_2h_short_btc4h_rr_1_5_strict", "interval": "2h", "margin": 0.006, "volume": 1.2,
     "tol": 0.005, "window": 6, "mode": "short_market_htf", "market_htf": "4h", "rr_target": 1.5},
    {"name": "F28_2h_short_local_and_btc4h_rr_1_5_strict", "interval": "2h", "margin": 0.006, "volume": 1.2,
     "tol": 0.005, "window": 6, "mode": "short_dual_market_htf", "market_htf": "4h", "rr_target": 1.5},
]


def run_window(candles, cfg, regimes=None, htf_regimes=None, start_idx=0, end_idx=None, market_regimes=None):
    """Simulate only entries and exits inside [start_idx, end_idx).

    Earlier candles are available only for causal indicator/pivot warm-up. The
    pending-setup queue and position state are reset at each evaluation window.
    Trades still open at the window boundary are censored, not marked to market.
    """
    n = len(candles)
    end_idx = n if end_idx is None else min(n, max(0, end_idx))
    min_start = bot.BT_ZONE_LOOKBACK + bot.BT_PIVOT_WINDOW + 2
    i = max(min_start, max(0, start_idx))
    if end_idx <= i + 1:
        return []

    trades, pending = [], []
    rr_target = float(cfg.get("rr_target", bot.BT_RR_TARGET))
    while i < end_idx - 1:
        sig = candles[i]
        pending = [p for p in pending if i - p["breakout_i"] <= cfg["window"]]
        triggered = None
        regime = regimes[i] if regimes is not None else bot.classify_regime(candles, i)
        htf_regime = htf_regimes[i] if htf_regimes is not None else None
        market_regime = market_regimes[i] if market_regimes is not None else "UNKNOWN"
        for p in pending:
            # Filter direction/regime before selecting a retest. Otherwise an
            # ineligible pending setup can mask an eligible setup on the same candle.
            if cfg["mode"] == "short" and (p["dir"] != "SHORT" or regime != "TREND_DOWN"):
                continue
            if cfg["mode"] == "long" and (p["dir"] != "LONG" or regime != "TREND_UP"):
                continue
            if cfg["mode"] == "long_htf" and (p["dir"] != "LONG" or htf_regime != "TREND_UP"):
                continue
            if cfg["mode"] == "short_htf" and (p["dir"] != "SHORT" or htf_regime != "TREND_DOWN"):
                continue
            if cfg["mode"] == "long_market_htf" and (p["dir"] != "LONG" or market_regime != "TREND_UP"):
                continue
            if cfg["mode"] == "short_market_htf" and (p["dir"] != "SHORT" or market_regime != "TREND_DOWN"):
                continue
            if cfg["mode"] == "long_dual_market_htf" and (
                p["dir"] != "LONG" or regime != "TREND_UP" or market_regime != "TREND_UP"
            ):
                continue
            if cfg["mode"] == "short_dual_market_htf" and (
                p["dir"] != "SHORT" or regime != "TREND_DOWN" or market_regime != "TREND_DOWN"
            ):
                continue
            if cfg["mode"] == "long_dual_htf" and (
                p["dir"] != "LONG" or regime != "TREND_UP" or htf_regime != "TREND_UP"
            ):
                continue
            if cfg["mode"] == "short_dual_htf" and (
                p["dir"] != "SHORT" or regime != "TREND_DOWN" or htf_regime != "TREND_DOWN"
            ):
                continue
            if cfg["mode"] == "trend_both_htf":
                if (p["dir"] == "LONG" and htf_regime != "TREND_UP") or (
                    p["dir"] == "SHORT" and htf_regime != "TREND_DOWN"
                ):
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
                tp = entry + rr_target * risk
            else:
                sl = max(level * (1 + 0.005), sig["h"] * 1.002)
                risk = sl - entry
                tp = entry - rr_target * risk
            if risk <= 0:
                i += 1
                continue

            exit_r, exit_i, exit_reason = None, None, None
            # Crucially, never inspect candles beyond this evaluation window.
            for k in range(i + 1, end_idx):
                c = candles[k]
                if direction == "LONG":
                    hit_sl, hit_tp = c["l"] <= sl, c["h"] >= tp
                else:
                    hit_sl, hit_tp = c["h"] >= sl, c["l"] <= tp
                # Conservative tie-break: if both hit in one candle, assume stop first.
                if hit_sl:
                    exit_r, exit_i, exit_reason = -1.0, k, "stop"
                    break
                if hit_tp:
                    exit_r, exit_i, exit_reason = rr_target, k, "target"
                    break

            censored = exit_r is None
            if censored:
                exit_i, exit_reason = end_idx - 1, "censored_window_end"
                trade_r = None
            else:
                exit_r -= bot.BT_FEE_R
                trade_r = round(exit_r, 4)
            trades.append({
                "direction": direction, "entry_i": i + 1, "exit_i": exit_i,
                "entry_time": candles[i + 1].get("t"), "exit_time": candles[exit_i].get("t"),
                "r": trade_r, "exit_reason": exit_reason, "regime": regime,
                "censored": censored,
            })
            pending = []
            if censored:
                break
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
    # Only fully resolved trades belong in performance statistics.
    completed = [
        t for t in trades
        if t.get("r") is not None and not t.get("censored", False)
    ]
    rs = [(t, float(t["r"]) - extra_cost_r) for t in completed]
    wins = sum(r > 0 for _, r in rs)
    gross_wins = sum(r for _, r in rs if r > 0)
    gross_losses = -sum(r for _, r in rs if r < 0)

    # Realized equity changes at exit time, not entry time. Group simultaneous
    # exits to avoid arbitrary ordering between assets that close on one candle.
    by_exit_time = {}
    for trade, r in rs:
        key = trade.get("exit_time") or trade.get("entry_time") or 0
        by_exit_time[key] = by_exit_time.get(key, 0.0) + r
    equity = peak = max_dd = 0.0
    for key in sorted(by_exit_time):
        equity += by_exit_time[key]
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)

    return {
        "trades": len(rs),
        "censored_trades": sum(bool(t.get("censored")) for t in trades),
        "wins": wins,
        "win_rate_pct": round(100 * wins / len(rs), 2) if rs else 0,
        "profit_factor": round(gross_wins / gross_losses, 3) if gross_losses else (999 if gross_wins else 0),
        "net_R": round(sum(r for _, r in rs), 3),
        "gross_profit_R": round(gross_wins, 6),
        "gross_loss_R": round(gross_losses, 6),
        "avg_R": round(sum(r for _, r in rs) / len(rs), 4) if rs else 0,
        "max_drawdown_R": round(max_dd, 3),
        "signals_per_day": round(len(rs) / days, 3) if days > 0 else 0,
    }


def add_cost_stress(row, trades, days):
    for extra_cost_r in (0.05, 0.10, 0.15):
        stressed = metrics(trades, days, extra_cost_r=extra_cost_r)
        suffix = f"{extra_cost_r:.2f}R"
        row[f"net_R_after_extra_{suffix}"] = stressed["net_R"]
        row[f"PF_after_extra_{suffix}"] = stressed["profit_factor"]
        row[f"win_rate_pct_after_extra_{suffix}"] = stressed["win_rate_pct"]
        row[f"max_drawdown_R_after_extra_{suffix}"] = stressed["max_drawdown_R"]
        row[f"gross_profit_R_after_extra_{suffix}"] = stressed["gross_profit_R"]
        row[f"gross_loss_R_after_extra_{suffix}"] = stressed["gross_loss_R"]
    return row


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
    market_regimes_cache = {}
    trades_cache = {}
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        cache = {}
        intervals_needed = sorted(
            {cfg["interval"] for cfg in CONFIGS}
            | {cfg["htf"] for cfg in CONFIGS if cfg.get("htf")}
            | {cfg["market_htf"] for cfg in CONFIGS if cfg.get("market_htf")}
        )
        interval_ms_map = {"1h": 3600000, "2h": 7200000, "4h": 14400000}
        # Preload all histories first so every coin in an interval uses the same
        # UTC calendar boundaries rather than a coin-specific fraction of its data.
        for interval in intervals_needed:
            for coin in COINS:
                key = (coin, interval)
                if key not in cache:
                    try:
                        cache[key] = await load(session, coin, interval)
                    except Exception:
                        cache[key] = []

        interval_bounds = {}
        warmup_bars = bot.BT_ZONE_LOOKBACK + bot.BT_PIVOT_WINDOW + 2
        for interval in intervals_needed:
            histories = [cache.get((coin, interval), []) for coin in COINS]
            if not histories or any(len(history) < 500 for history in histories):
                continue
            common_start = max(history[0]["t"] for history in histories)
            common_end = min(history[-1]["t"] for history in histories)
            evaluation_start = common_start + warmup_bars * interval_ms_map[interval]
            if common_end <= evaluation_start:
                errors.append({
                    "interval": interval,
                    "error": "insufficient common UTC history after indicator warm-up",
                })
                continue
            interval_bounds[interval] = (common_start, evaluation_start, common_end)

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
                    if interval not in interval_bounds:
                        errors.append({"coin": coin, "interval": interval,
                                       "error": "common UTC calendar bounds unavailable"})
                        continue
                    common_start, evaluation_start, common_end = interval_bounds[interval]
                    candle_times = [c["t"] for c in candles]
                    evaluation_start_idx = bisect.bisect_left(candle_times, evaluation_start)
                    split_time = int(evaluation_start + (common_end - evaluation_start) * TRAIN_FRACTION)
                    split = bisect.bisect_left(candle_times, split_time)
                    evaluation_end_idx = bisect.bisect_right(candle_times, common_end)
                    if split <= evaluation_start_idx or evaluation_end_idx <= split:
                        errors.append({"coin": coin, "interval": interval,
                                       "error": "invalid train/holdout boundaries"})
                        continue
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
                        high_times = [c["t"] for c in high_candles]
                        lower_ms = {"1h": 3600000, "2h": 7200000, "4h": 14400000}[interval]
                        high_ms = {"1h": 3600000, "2h": 7200000, "4h": 14400000}[cfg["htf"]]
                        htf_regimes = []
                        for candle in candles:
                            cutoff = candle["t"] + lower_ms - high_ms
                            hi = bisect.bisect_right(high_times, cutoff) - 1
                            htf_regimes.append(high_regimes[hi] if hi >= 0 else "UNKNOWN")
                        htf_cache[regime_key] = htf_regimes
                    market_regimes = None
                    if cfg.get("market_htf"):
                        market_key = (coin, interval, cfg["market_htf"])
                        if market_key not in market_regimes_cache:
                            benchmark_key = ("BTC", cfg["market_htf"])
                            benchmark_candles = cache.get(benchmark_key, [])
                            if benchmark_candles:
                                if benchmark_key not in regimes_cache:
                                    regimes_cache[benchmark_key] = validated_fast_regimes(benchmark_candles)
                                benchmark_regimes = regimes_cache[benchmark_key]
                                benchmark_times = [c["t"] for c in benchmark_candles]
                                lower_ms = interval_ms_map[interval]
                                higher_ms = interval_ms_map[cfg["market_htf"]]
                                aligned = []
                                for candle in candles:
                                    cutoff = candle["t"] + lower_ms - higher_ms
                                    hi = bisect.bisect_right(benchmark_times, cutoff) - 1
                                    aligned.append(benchmark_regimes[hi] if hi >= 0 else "UNKNOWN")
                                market_regimes_cache[market_key] = aligned
                            else:
                                errors.append({"coin": "BTC", "interval": cfg["market_htf"],
                                               "error": "BTC benchmark history unavailable"})
                                market_regimes_cache[market_key] = ["UNKNOWN"] * len(candles)
                        market_regimes = market_regimes_cache[market_key]
                    # Train and holdout are independent evaluation windows. Historical
                    # candles remain available for causal indicator/pivot warm-up, but
                    # pending setups and open positions do not cross the boundary.
                    train_trades = run_window(
                        candles, cfg, regimes, htf_regimes,
                        start_idx=evaluation_start_idx, end_idx=split, market_regimes=market_regimes
                    )
                    test_trades = run_window(
                        candles, cfg, regimes, htf_regimes,
                        start_idx=split, end_idx=evaluation_end_idx, market_regimes=market_regimes
                    )
                    trades_cache[(cfg["name"], coin, "train")] = train_trades
                    trades_cache[(cfg["name"], coin, "holdout")] = test_trades
                    train_days = max(1, (split_time - evaluation_start) / 86400000)
                    test_days = max(1, (common_end - split_time) / 86400000)
                    for period, trades, days in (
                        ("train", train_trades, train_days), ("holdout", test_trades, test_days)
                    ):
                        row = {
                            "config": cfg["name"], "coin": coin, "period": period,
                            "candles": len(candles),
                            "evaluation_start_utc": evaluation_start,
                            "split_utc": split_time, "evaluation_end_utc": common_end,
                            **metrics(trades, days),
                        }
                        all_rows.append(row)
                    print(f"{cfg['name']} {coin}: train={len(train_trades)}, holdout={len(test_trades)}")
                except Exception as exc:
                    errors.append({"coin": coin, "interval": interval, "error": repr(exc)})

    summaries = []
    cost_stress = []
    for cfg in CONFIGS:
        for period in ("train", "holdout"):
            rows = [r for r in all_rows if r["config"] == cfg["name"] and r["period"] == period]
            trades = []
            for coin in COINS:
                trades.extend(trades_cache.get((cfg["name"], coin, period), []))
            _, evaluation_start, common_end = interval_bounds[cfg["interval"]]
            split_time = int(evaluation_start + (common_end - evaluation_start) * TRAIN_FRACTION)
            days = (
                max(1, (split_time - evaluation_start) / 86400000)
                if period == "train"
                else max(1, (common_end - split_time) / 86400000)
            )
            summary = {"config": cfg["name"], "period": period,
                       **metrics(trades, days)}
            summaries.append(summary)
            for extra_cost_r in (0.05, 0.10, 0.15):
                cost_stress.append({
                    "config": cfg["name"], "period": period,
                    "extra_cost_R_per_trade": extra_cost_r,
                    **metrics(trades, days, extra_cost_r=extra_cost_r),
                })

    # Five sequential out-of-sample windows. Each fold starts flat, uses
    # only past candles for warm-up, and censors positions that remain open at
    # the fold boundary. No fold can borrow a future exit from a later window.
    walk_forward = []
    shortlist = {
        "A_4h_current_short_only",
        "B_1h_current_short_only",
        "B1_1h_long_only",
        "C_1h_balanced_both_directions",
        "D_1h_more_opportunities",
        "B2_2h_current_short_only",
        "B3_2h_long_only",
        "C2_2h_balanced_both_directions",
        "F_2h_long_confirmed_by_4h",
        "F3_2h_long_confirmed_by_4h_strict",
        "F4_2h_long_confirmed_by_4h_balanced",
        "F5_1h_short_confirmed_by_4h",
        "F6_1h_both_directions_confirmed_by_4h",
        "F7_2h_short_confirmed_by_4h",
        "F8_2h_long_confirmed_by_4h_rr_1_5",
        "F9_2h_long_confirmed_by_4h_rr_2_5",
        "F10_2h_long_confirmed_by_local_and_4h",
        "F11_2h_short_confirmed_by_local_and_4h",
        "F12_2h_long_dual_htf_strict",
        "F13_2h_short_dual_htf_strict",
        "F14_2h_long_confirmed_by_4h_rr_2_5_strict",
        "F15_2h_long_dual_htf_rr_2_5",
        "F16_2h_short_confirmed_by_4h_rr_2_5",
        "F17_2h_long_btc4h_rr_2_5_strict",
        "F18_2h_short_btc4h_rr_2_5_strict",
        "F19_2h_long_local_and_btc4h_rr_2_5_strict",
        "F20_2h_short_local_and_btc4h_rr_2_5_strict",
        "F21_2h_long_btc4h_rr_2_0_strict",
        "F22_2h_long_local_and_btc4h_rr_2_0_strict",
        "F23_2h_short_btc4h_rr_2_0_strict",
        "F24_2h_short_local_and_btc4h_rr_2_0_strict",
        "F25_2h_long_btc4h_rr_1_5_strict",
        "F26_2h_long_local_and_btc4h_rr_1_5_strict",
        "F27_2h_short_btc4h_rr_1_5_strict",
        "F28_2h_short_local_and_btc4h_rr_1_5_strict",
    }
    folds = [(0.20, 0.36), (0.36, 0.52), (0.52, 0.68), (0.68, 0.84), (0.84, 1.00)]
    for cfg in CONFIGS:
        if cfg["name"] not in shortlist:
            continue
        bounds = interval_bounds.get(cfg["interval"])
        if not bounds:
            continue
        _, evaluation_start, common_end = bounds
        calendar_span = common_end - evaluation_start
        for fold_no, (start_frac, end_frac) in enumerate(folds, start=1):
            fold_start_utc = int(evaluation_start + calendar_span * start_frac)
            fold_end_utc = int(evaluation_start + calendar_span * end_frac)
            fold_trades = []
            fold_coin_rows = []
            for coin in COINS:
                candles = cache.get((coin, cfg["interval"]), [])
                if len(candles) < 500:
                    continue
                candle_times = [c["t"] for c in candles]
                start_idx = bisect.bisect_left(candle_times, fold_start_utc)
                end_idx = bisect.bisect_left(candle_times, fold_end_utc)
                if end_idx <= start_idx + 30:
                    continue
                regime_key = (coin, cfg["interval"])
                regimes = regimes_cache[regime_key]
                htf_regimes = htf_cache.get(regime_key)
                market_regimes = (
                    market_regimes_cache.get((coin, cfg["interval"], cfg["market_htf"]))
                    if cfg.get("market_htf") else None
                )
                selected = run_window(
                    candles, cfg, regimes, htf_regimes,
                    start_idx=start_idx, end_idx=end_idx, market_regimes=market_regimes,
                )
                fold_trades.extend(selected)
                days = max(1, (fold_end_utc - fold_start_utc) / 86400000)
                coin_row = {
                    "config": cfg["name"], "fold": fold_no, "coin": coin,
                    "start_fraction": start_frac, "end_fraction": end_frac,
                    "start_utc": fold_start_utc, "end_utc": fold_end_utc,
                    **metrics(selected, days),
                }
                fold_coin_rows.append(add_cost_stress(coin_row, selected, days))
            fold_days = max(1, (fold_end_utc - fold_start_utc) / 86400000)
            fold_row = {
                "config": cfg["name"], "fold": fold_no,
                "start_fraction": start_frac, "end_fraction": end_frac,
                "start_utc": fold_start_utc, "end_utc": fold_end_utc,
                **metrics(fold_trades, fold_days),
            }
            walk_forward.append(add_cost_stress(fold_row, fold_trades, fold_days))
            walk_forward.extend(fold_coin_rows)

    # Nested, chronological model-selection check. For each interval, choose a
    # predeclared candidate using only earlier walk-forward folds, then score it
    # on the next fold. This is stricter than selecting the best config after
    # inspecting all five folds. Extra 0.15R/trade is used for ranking.
    aggregate_wf = [r for r in walk_forward if "coin" not in r]
    nested_walk_forward = []

    def combine_stressed_fold_rows(rows):
        total_trades = sum(int(r.get("trades", 0)) for r in rows)
        net = sum(float(r.get("net_R_after_extra_0.15R", 0.0)) for r in rows)
        gross_win = sum(float(r.get("gross_profit_R_after_extra_0.15R", 0.0)) for r in rows)
        gross_loss = sum(float(r.get("gross_loss_R_after_extra_0.15R", 0.0)) for r in rows)
        combined_pf = (
            round(gross_win / gross_loss, 3) if gross_loss > 0
            else (999 if gross_win > 0 else 0)
        )
        return {
            "trades": total_trades,
            "net_R_after_extra_0.15R": round(net, 3),
            "PF_after_extra_0.15R": combined_pf,
            "max_fold_drawdown_R": round(max(
                (float(r.get("max_drawdown_R_after_extra_0.15R", 0.0)) for r in rows), default=0.0
            ), 3),
        }

    for interval in sorted({cfg["interval"] for cfg in CONFIGS}):
        candidates = [
            cfg for cfg in CONFIGS
            if cfg["interval"] == interval and cfg["name"] in shortlist
        ]
        interval_folds = sorted({
            int(r["fold"]) for r in aggregate_wf
            if r["config"] in {cfg["name"] for cfg in candidates}
        })
        for test_fold in interval_folds:
            prior_folds = [fold for fold in interval_folds if fold < test_fold]
            if len(prior_folds) < 2:
                nested_walk_forward.append({
                    "interval": interval,
                    "test_fold": test_fold,
                    "selection_folds": prior_folds,
                    "selected_config": None,
                    "status": "insufficient prior folds; require at least 2",
                })
                continue
            ranked = []
            for cfg in candidates:
                prior_rows = [
                    r for r in aggregate_wf
                    if r["config"] == cfg["name"] and int(r["fold"]) in prior_folds
                ]
                prior_by_fold = {int(r["fold"]): r for r in prior_rows}
                # Avoid selecting on one aggregate winner: every prior fold must
                # have at least 10 completed trades and remain profitable after
                # +0.15R/trade stress. This is deliberately conservative.
                if any(
                    fold not in prior_by_fold
                    or int(prior_by_fold[fold].get("trades", 0)) < 10
                    or float(prior_by_fold[fold].get("net_R_after_extra_0.15R", 0.0)) <= 0
                    or float(prior_by_fold[fold].get("PF_after_extra_0.15R", 0.0)) <= 1.0
                    for fold in prior_folds
                ):
                    continue
                train_stats = combine_stressed_fold_rows(prior_rows)
                if (
                    train_stats["trades"] >= 30
                    and train_stats["net_R_after_extra_0.15R"] > 0
                    and train_stats["PF_after_extra_0.15R"] > 1.0
                ):
                    ranked.append((train_stats, cfg))
            if not ranked:
                nested_walk_forward.append({
                    "interval": interval,
                    "test_fold": test_fold,
                    "selection_folds": prior_folds,
                    "selected_config": None,
                    "status": "no_candidate_met_prior-fold minimums",
                })
                continue
            train_stats, selected_cfg = max(
                ranked,
                key=lambda item: (
                    item[0]["net_R_after_extra_0.15R"],
                    item[0]["PF_after_extra_0.15R"],
                    -item[0]["max_fold_drawdown_R"],
                ),
            )
            test_rows = [
                r for r in aggregate_wf
                if r["config"] == selected_cfg["name"] and int(r["fold"]) == test_fold
            ]
            if not test_rows:
                nested_walk_forward.append({
                    "interval": interval,
                    "test_fold": test_fold,
                    "selection_folds": prior_folds,
                    "selected_config": selected_cfg["name"],
                    "status": "missing test-fold result",
                })
                continue
            test_row = test_rows[0]
            nested_walk_forward.append({
                "interval": interval,
                "test_fold": test_fold,
                "selection_folds": prior_folds,
                "selected_config": selected_cfg["name"],
                "selection_trades": train_stats["trades"],
                "selection_net_R_after_extra_0.15R": train_stats["net_R_after_extra_0.15R"],
                "selection_PF_after_extra_0.15R": train_stats["PF_after_extra_0.15R"],
                "test_trades": test_row["trades"],
                "test_win_rate_pct": test_row["win_rate_pct"],
                "test_profit_factor": test_row["profit_factor"],
                "test_net_R": test_row["net_R"],
                "test_net_R_after_extra_0.05R": test_row["net_R_after_extra_0.05R"],
                "test_PF_after_extra_0.05R": test_row["PF_after_extra_0.05R"],
                "test_net_R_after_extra_0.10R": test_row["net_R_after_extra_0.10R"],
                "test_PF_after_extra_0.10R": test_row["PF_after_extra_0.10R"],
                "test_net_R_after_extra_0.15R": test_row["net_R_after_extra_0.15R"],
                "test_PF_after_extra_0.15R": test_row["PF_after_extra_0.15R"],
                "test_max_drawdown_R": test_row["max_drawdown_R"],
                "status": "evaluated",
            })

    # Leave-one-coin-out robustness for the baseline and the most informative
    # LONG/SHORT candidates. This checks whether aggregate performance depends
    # on a single asset; all costs are applied trade-by-trade before PF/DD.
    robustness_names = {
        "A_4h_current_short_only",
        "F_2h_long_confirmed_by_4h",
        "F3_2h_long_confirmed_by_4h_strict",
        "F8_2h_long_confirmed_by_4h_rr_1_5",
        "F9_2h_long_confirmed_by_4h_rr_2_5",
        "F10_2h_long_confirmed_by_local_and_4h",
        "F11_2h_short_confirmed_by_local_and_4h",
        "F12_2h_long_dual_htf_strict",
        "F13_2h_short_dual_htf_strict",
        "F14_2h_long_confirmed_by_4h_rr_2_5_strict",
        "F15_2h_long_dual_htf_rr_2_5",
        "F16_2h_short_confirmed_by_4h_rr_2_5",
        "F17_2h_long_btc4h_rr_2_5_strict",
        "F18_2h_short_btc4h_rr_2_5_strict",
        "F19_2h_long_local_and_btc4h_rr_2_5_strict",
        "F20_2h_short_local_and_btc4h_rr_2_5_strict",
        "F21_2h_long_btc4h_rr_2_0_strict",
        "F22_2h_long_local_and_btc4h_rr_2_0_strict",
        "F23_2h_short_btc4h_rr_2_0_strict",
        "F24_2h_short_local_and_btc4h_rr_2_0_strict",
        "F25_2h_long_btc4h_rr_1_5_strict",
        "F26_2h_long_local_and_btc4h_rr_1_5_strict",
        "F27_2h_short_btc4h_rr_1_5_strict",
        "F28_2h_short_local_and_btc4h_rr_1_5_strict",
    }
    leave_one_coin_out = []
    for cfg in CONFIGS:
        if cfg["name"] not in robustness_names:
            continue
        bounds = interval_bounds.get(cfg["interval"])
        if not bounds:
            continue
        _, evaluation_start, common_end = bounds
        split_time = int(evaluation_start + (common_end - evaluation_start) * TRAIN_FRACTION)
        period_days = {
            "train": max(1, (split_time - evaluation_start) / 86400000),
            "holdout": max(1, (common_end - split_time) / 86400000),
        }
        for period in ("train", "holdout"):
            for excluded_coin in [None, *COINS]:
                combined = []
                for coin in COINS:
                    if excluded_coin is not None and coin == excluded_coin:
                        continue
                    combined.extend(trades_cache.get((cfg["name"], coin, period), []))
                base_stats = metrics(combined, period_days[period])
                stressed_010 = metrics(combined, period_days[period], extra_cost_r=0.10)
                stressed_015 = metrics(combined, period_days[period], extra_cost_r=0.15)
                leave_one_coin_out.append({
                    "config": cfg["name"],
                    "period": period,
                    "excluded_coin": excluded_coin or "NONE",
                    "trades": base_stats["trades"],
                    "win_rate_pct": base_stats["win_rate_pct"],
                    "profit_factor": base_stats["profit_factor"],
                    "net_R": base_stats["net_R"],
                    "max_drawdown_R": base_stats["max_drawdown_R"],
                    "net_R_after_extra_0.10R": stressed_010["net_R"],
                    "PF_after_extra_0.10R": stressed_010["profit_factor"],
                    "max_drawdown_R_after_extra_0.10R": stressed_010["max_drawdown_R"],
                    "net_R_after_extra_0.15R": stressed_015["net_R"],
                    "PF_after_extra_0.15R": stressed_015["profit_factor"],
                    "max_drawdown_R_after_extra_0.15R": stressed_015["max_drawdown_R"],
                })

    with (OUT / "per_coin.csv").open("w", newline="") as f:
        fields = list(all_rows[0].keys()) if all_rows else ["config", "coin", "period"]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(all_rows)
    calendar_windows = {}
    for interval, (common_start, evaluation_start, common_end) in interval_bounds.items():
        split_time = int(evaluation_start + (common_end - evaluation_start) * TRAIN_FRACTION)
        calendar_windows[interval] = {
            "common_history_start_utc": common_start,
            "evaluation_start_utc": evaluation_start,
            "train_holdout_split_utc": split_time,
            "evaluation_end_utc": common_end,
        }
    report = {
        "purpose": "Compare signal frequency and trade quality on closed candles; research only.",
        "coin_universe": COINS,
        "excluded_coins": sorted(_EXCLUDED_COINS),
        "configs": CONFIGS, "calendar_windows": calendar_windows,
        "summary": summaries, "cost_stress": cost_stress,
        "walk_forward": walk_forward, "nested_walk_forward": nested_walk_forward,
        "leave_one_coin_out": leave_one_coin_out, "per_coin": all_rows, "errors": errors,
        "warning": "Research only. Train/holdout and five walk-forward folds use common UTC calendar boundaries per interval. Fold positions are reset and unresolved trades are censored. Nested selection uses only prior folds and ranks candidates after +0.15R/trade stress using trade-level gross profit/loss aggregation. Additional +0.15R stress and leave-one-coin-out checks are included. Signals/day is across the tested coin-universe, not a promise. No production settings were changed.",
    }
    (OUT / "summary.json").write_text(json.dumps(report, indent=2))
    print(json.dumps({"summary": summaries, "errors": errors}, indent=2))
    if errors:
        raise SystemExit("Some coin histories were incomplete; do not treat run as fully validated.")


if __name__ == "__main__":
    asyncio.run(main())
