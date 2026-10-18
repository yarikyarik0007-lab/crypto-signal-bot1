"""Walk-forward test of a pre-defined coin-universe filter; research only."""
import asyncio
import json
from pathlib import Path

import aiohttp
import backtest_variants as bt

OUT = Path("backtest-results")
OUT.mkdir(exist_ok=True)


def metrics(trades):
    trades = sorted(trades, key=lambda t: (t.get("entry_time") or 0, t.get("exit_time") or 0))
    rs = [float(t["r"]) for t in trades]
    wins = sum(r > 0 for r in rs)
    gross_win = sum(r for r in rs if r > 0)
    gross_loss = -sum(r for r in rs if r < 0)
    equity = peak = drawdown = 0.0
    for r in rs:
        equity += r
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    return {
        "trades": len(rs),
        "wins": wins,
        "win_rate_pct": round(100 * wins / len(rs), 2) if rs else 0.0,
        "profit_factor": round(gross_win / gross_loss, 3) if gross_loss else (999.0 if gross_win else 0.0),
        "net_R": round(sum(rs), 3),
        "max_drawdown_R": round(drawdown, 3),
    }


async def main():
    errors, per_coin, all_trades = [], {}, {}
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25)) as session:
        for coin in bt.COINS:
            try:
                candles = await bt.load_candles(session, coin)
                if len(candles) < 1000:
                    raise RuntimeError(f"only {len(candles)} closed candles")
                n = len(candles)
                cut1, cut2 = int(n * 0.60), int(n * 0.80)
                train = candles[:cut1]
                validation = candles[max(0, cut1 - 200):cut2]
                test = candles[max(0, cut2 - 200):]
                base_train = bt.run_variant(train)
                val_all = bt.run_variant(validation)
                test_all = bt.run_variant(test)
                val_all = [t for t in val_all if candles[cut1]["t"] <= (t.get("entry_time") or 0) < candles[cut2]["t"]]
                test_all = [t for t in test_all if (t.get("entry_time") or 0) >= candles[cut2]["t"]]
                train_stats = metrics(base_train)
                per_coin[coin] = {"train": train_stats, "validation_trades": len(val_all), "test_trades": len(test_all)}
                all_trades[coin] = {"validation": val_all, "test": test_all}
                print(f"{coin}: train={train_stats['trades']} trades, train_R={train_stats['net_R']}")
            except Exception as exc:
                errors.append({"coin": coin, "error": repr(exc)})
    if errors:
        (OUT / "walkforward_coin_filter.json").write_text(json.dumps({"errors": errors}, indent=2))
        raise SystemExit(f"data/parity errors: {errors}")
    # Rule fixed before looking at validation/test: at least 8 train trades, negative R and PF < 1.
    excluded = [coin for coin, row in per_coin.items()
                if row["train"]["trades"] >= 8 and row["train"]["net_R"] < 0
                and row["train"]["profit_factor"] < 1]
    results = {"selection_rule": "exclude coin only if first 60% has >=8 trades, net_R < 0 and PF < 1",
               "excluded_coins_selected_from_train_only": excluded,
               "per_coin_train": per_coin,
               "periods": {}}
    for period in ("validation", "test"):
        all_rows = [t for coin in bt.COINS for t in all_trades[coin][period]]
        filtered_rows = [t for coin in bt.COINS if coin not in excluded for t in all_trades[coin][period]]
        results["periods"][period] = {"all_11_coins": metrics(all_rows),
                                      "train_selected_coin_filter": metrics(filtered_rows)}
    (OUT / "walkforward_coin_filter.json").write_text(json.dumps(results, indent=2))
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
