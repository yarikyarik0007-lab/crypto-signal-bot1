"""Research-only transaction-cost sensitivity; funding inputs are stress assumptions, not historical rates."""
import asyncio
import csv
import json
import math
from pathlib import Path

import aiohttp
import backtest_variants as bt

OUT = Path("backtest-results")
OUT.mkdir(exist_ok=True)
FUNDING_INTERVAL_MS = 8 * 60 * 60 * 1000
SCENARIOS = [
    ("baseline_existing_fee_only", 0, 0.0),
    ("mild_2bps_0.01pct_funding", 2, 0.0001),
    ("moderate_5bps_0.03pct_funding", 5, 0.0003),
    ("severe_10bps_0.05pct_funding", 10, 0.0005),
]


def apply_costs(trades, slippage_bps, funding_rate_per_8h):
    rows = []
    for trade in trades:
        risk = float(trade.get("risk_price") or 0)
        entry = float(trade.get("entry_price") or 0)
        exit_price = float(trade.get("exit_price") or 0)
        if risk <= 0 or entry <= 0 or exit_price <= 0:
            continue
        duration_ms = max(0, int(trade.get("exit_time") or 0) - int(trade.get("entry_time") or 0))
        funding_intervals = math.ceil(duration_ms / FUNDING_INTERVAL_MS)
        # Conservative adverse-cost proxy: slippage on both legs and funding paid
        # every 8h interval. This is not a reconstruction of actual exchange funding.
        slippage_r = ((entry + exit_price) * (slippage_bps / 10000.0)) / risk
        funding_r = (entry * funding_rate_per_8h * funding_intervals) / risk
        rows.append({**trade, "net_r_after_costs": float(trade["r"]) - slippage_r - funding_r,
                     "slippage_cost_R": slippage_r, "funding_cost_R": funding_r,
                     "funding_intervals_proxy": funding_intervals})
    return rows


def summarize(rows):
    rs = [float(row["net_r_after_costs"]) for row in rows]
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
        "win_rate_pct": round(100 * wins / len(rs), 2) if rs else 0.0,
        "profit_factor": round(gross_win / gross_loss, 3) if gross_loss else (999.0 if gross_win else 0.0),
        "net_R": round(sum(rs), 3),
        "avg_R": round(sum(rs) / len(rs), 4) if rs else 0.0,
        "max_drawdown_R": round(max_dd, 3),
        "total_slippage_cost_R": round(sum(row["slippage_cost_R"] for row in rows), 3),
        "total_funding_cost_R": round(sum(row["funding_cost_R"] for row in rows), 3),
    }


async def main():
    errors, raw_trades = [], {"train": [], "holdout": []}
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25)) as session:
        for coin in bt.COINS:
            try:
                candles = await bt.load_candles(session, coin)
                if len(candles) < 1000:
                    raise RuntimeError(f"only {len(candles)} closed candles")
                split = int(len(candles) * bt.TRAIN_FRACTION)
                train = candles[:split]
                holdout = candles[max(0, split - 200):]
                split_time = candles[split]["t"]
                train_trades = bt.run_variant(train)
                holdout_trades = [t for t in bt.run_variant(holdout)
                                  if (t.get("entry_time") or 0) >= split_time]
                for trade in train_trades:
                    raw_trades["train"].append({"coin": coin, **trade})
                for trade in holdout_trades:
                    raw_trades["holdout"].append({"coin": coin, **trade})
                print(f"{coin}: train={len(train_trades)}, holdout={len(holdout_trades)}")
            except Exception as exc:
                errors.append({"coin": coin, "error": repr(exc)})
    if errors:
        (OUT / "cost_stress.json").write_text(json.dumps({"errors": errors}, indent=2))
        raise SystemExit(f"data errors: {errors}")

    output = []
    for period, trades in raw_trades.items():
        for scenario, bps, funding_rate in SCENARIOS:
            costed = apply_costs(trades, bps, funding_rate)
            output.append({"period": period, "scenario": scenario, "slippage_bps_per_leg": bps,
                           "assumed_adverse_funding_rate_per_8h": funding_rate,
                           **summarize(costed)})
    with (OUT / "cost_stress.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(output[0].keys()) if output else ["period", "scenario"])
        writer.writeheader()
        writer.writerows(output)
    report = {
        "warning": "Funding is a conservative assumed adverse rate, not actual historical funding; settlement intervals are approximated from 4h candle timestamps. Slippage is a sensitivity assumption. Existing BT_FEE_R remains included in trade R.",
        "scenarios": output,
        "trade_counts": {period: len(trades) for period, trades in raw_trades.items()},
        "errors": errors,
    }
    (OUT / "cost_stress.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
