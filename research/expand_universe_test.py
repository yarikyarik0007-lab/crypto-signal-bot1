"""Research-only test of additional BingX USDT perpetual symbols; does not change bot.COINS."""
import asyncio
import csv
import json
from pathlib import Path

import aiohttp
import backtest_variants as bt

# Candidates only. Production's configured 11-coin universe remains untouched.
CANDIDATES = ["LTC", "TRX", "DOT", "ATOM", "BCH", "UNI", "ETC", "NEAR"]
OUT = Path("backtest-results")
OUT.mkdir(exist_ok=True)


async def main():
    rows, availability, errors = [], [], []
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25)) as session:
        for coin in CANDIDATES:
            try:
                candles = await bt.load_candles(session, coin)
                split = int(len(candles) * bt.TRAIN_FRACTION)
                if len(candles) < 1000 or split >= len(candles):
                    availability.append({"coin": coin, "status": "insufficient_history",
                                         "closed_candles": len(candles)})
                    continue
                gaps = sum(1 for a, b in zip(candles, candles[1:])
                           if b["t"] - a["t"] > 4 * 60 * 60 * 1000 * 1.5)
                train = candles[:split]
                holdout = candles[max(0, split - 200):]
                split_time = candles[split]["t"]
                train_trades = bt.run_variant(train)
                holdout_trades = [t for t in bt.run_variant(holdout)
                                  if (t.get("entry_time") or 0) >= split_time]
                availability.append({"coin": coin, "status": "ok",
                                     "closed_candles": len(candles),
                                     "first_candle_time": candles[0]["t"],
                                     "last_candle_time": candles[-1]["t"],
                                     "gap_count_over_6h": gaps})
                for period, trades, count in (
                    ("train", train_trades, len(train)),
                    ("holdout", holdout_trades, len(holdout)),
                ):
                    rows.append({"coin": coin, "period": period, "candles": count,
                                 **bt.metrics(trades)})
                print(f"{coin}: candles={len(candles)}, train={len(train_trades)}, holdout={len(holdout_trades)}")
            except Exception as exc:
                availability.append({"coin": coin, "status": "fetch_or_analysis_error",
                                     "error": repr(exc)})
                errors.append({"coin": coin, "error": repr(exc)})
    with (OUT / "expanded_universe.csv").open("w", newline="") as f:
        fields = ["coin", "period", "candles", "trades", "wins", "win_rate_pct",
                  "gross_win_R", "gross_loss_R", "profit_factor", "net_R",
                  "max_drawdown_R", "avg_R"]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    report = {
        "purpose": "Evaluate additional symbols on the same fixed strategy; no coin is automatically added to production.",
        "candidates": CANDIDATES,
        "availability_and_data_quality": availability,
        "metrics": rows,
        "errors": errors,
        "warning": "Small per-coin samples and one historical split are exploratory. Do not select or deploy coins solely from these results."
    }
    (OUT / "expanded_universe.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))
    # Missing symbols are recorded as unavailable; actual exceptions are visible in the artifact.
    # Do not fail the whole workflow merely because a candidate contract is unsupported.


if __name__ == "__main__":
    asyncio.run(main())
