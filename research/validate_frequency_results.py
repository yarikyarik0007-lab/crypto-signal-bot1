"""Conservative cross-universe acceptance gate for research-only strategy artifacts.

This script never changes trading code or production settings. It reads the three
completed research outputs and writes a reproducible candidate-gate report.
A zero-candidate result is a valid research outcome and exits successfully.
"""
import json
from datetime import datetime, timezone
from pathlib import Path

DATASETS = {
    "main_11": Path("frequency-results/summary.json"),
    "matched_10": Path("frequency-results-no-zec-matched/summary.json"),
    "extended_10": Path("frequency-results-no-zec-extended/summary.json"),
}
REQUIRED_COSTS = (0.15, 0.20)
MIN_PERIOD_TRADES = 30
MIN_FOLD_TRADES = 8
BOOTSTRAP_BLOCKS = (2, 4, 8)
BOOTSTRAP_MIN_POSITIVE_SHARE = 0.95
BOOTSTRAP_COST = 0.15
OUT = Path("frequency-gate")
OUT.mkdir(parents=True, exist_ok=True)


def close(a, b, tol=1e-9):
    return abs(float(a) - float(b)) <= tol


def cost_row(summary, config, period, cost):
    matches = [
        row for row in summary.get("cost_stress", [])
        if row.get("config") == config
        and row.get("period") == period
        and close(row.get("extra_cost_R_per_trade", -999), cost)
    ]
    return matches[0] if len(matches) == 1 else None


def aggregate_fold_rows(summary, field, config):
    return [
        row for row in summary.get(field, [])
        if row.get("config") == config and "coin" not in row
    ]


def diagnostic_coverage(config, loaded):
    """Separate absent diagnostics from a genuine strategy failure."""
    missing = {}
    for dataset_name, summary in loaded.items():
        issues = []
        available = {item.get("name") for item in summary.get("configs", [])}
        if config not in available:
            issues.append("configuration is absent")
        if summary.get("errors"):
            issues.append(f"artifact contains {len(summary['errors'])} data errors")

        for period in ("train", "holdout"):
            for cost in REQUIRED_COSTS:
                if cost_row(summary, config, period, cost) is None:
                    issues.append(f"missing unique cost-stress row: {period} +{cost:.2f}R")

        for field, expected in (("walk_forward", 5), ("walk_forward_dense", 7)):
            count = len(aggregate_fold_rows(summary, field, config))
            if count != expected:
                issues.append(f"{field}: expected {expected} portfolio folds, found {count}")

        boot_rows = [
            row for row in summary.get("bootstrap_uncertainty", [])
            if row.get("config") == config
            and close(row.get("extra_cost_R_per_trade", -999), BOOTSTRAP_COST)
            and row.get("period") in ("train", "holdout")
        ]
        observed_boot = {
            (row.get("period"), int(row.get("bootstrap_block_weeks", -1)))
            for row in boot_rows
        }
        expected_boot = {
            (period, block)
            for period in ("train", "holdout")
            for block in BOOTSTRAP_BLOCKS
        }
        if observed_boot != expected_boot or len(boot_rows) != len(expected_boot):
            issues.append("bootstrap coverage is incomplete at +0.15R for 2/4/8-week blocks")

        expected_coins = set(summary.get("coin_universe", []))
        exclusions = {
            row.get("excluded_coin")
            for row in summary.get("leave_one_coin_out", [])
            if row.get("config") == config
            and row.get("period") == "holdout"
            and row.get("excluded_coin") != "NONE"
        }
        if exclusions != expected_coins:
            issues.append(
                f"leave-one-coin-out holdout coverage: expected {len(expected_coins)} distinct exclusions, "
                f"found {len(exclusions)}"
            )

        if issues:
            missing[dataset_name] = issues
    return missing


def evaluate_config(config, loaded):
    failures = []
    checks = {
        "cross_dataset_train_holdout_cost_stress": True,
        "five_fold_walk_forward": True,
        "seven_fold_walk_forward": True,
        "weekly_block_bootstrap": True,
        "leave_one_coin_out_holdout": True,
    }
    observed = {"datasets": {}, "walk_forward": {}, "bootstrap": {}, "leave_one_coin_out": {}}

    for dataset_name, summary in loaded.items():
        dataset_result = {}
        if config not in {item.get("name") for item in summary.get("configs", [])}:
            failures.append(f"{dataset_name}: config missing")
            checks["cross_dataset_train_holdout_cost_stress"] = False
            continue

        if summary.get("errors"):
            failures.append(f"{dataset_name}: artifact contains {len(summary['errors'])} errors")
            checks["cross_dataset_train_holdout_cost_stress"] = False

        for period in ("train", "holdout"):
            period_result = {}
            for cost in REQUIRED_COSTS:
                row = cost_row(summary, config, period, cost)
                key = f"+{cost:.2f}R"
                if row is None:
                    failures.append(f"{dataset_name}/{period}/{key}: missing or duplicate cost row")
                    checks["cross_dataset_train_holdout_cost_stress"] = False
                    continue
                trades = int(row.get("trades", 0))
                net_r = float(row.get("net_R", 0.0))
                pf = float(row.get("profit_factor", 0.0))
                period_result[key] = {
                    "trades": trades,
                    "win_rate_pct": row.get("win_rate_pct"),
                    "profit_factor": pf,
                    "net_R": net_r,
                    "max_drawdown_R": row.get("max_drawdown_R"),
                }
                if trades < MIN_PERIOD_TRADES:
                    failures.append(
                        f"{dataset_name}/{period}/{key}: only {trades} trades; need {MIN_PERIOD_TRADES}"
                    )
                    checks["cross_dataset_train_holdout_cost_stress"] = False
                if net_r <= 0 or pf <= 1.0:
                    failures.append(
                        f"{dataset_name}/{period}/{key}: Net R {net_r:.3f}, PF {pf:.3f}"
                    )
                    checks["cross_dataset_train_holdout_cost_stress"] = False

            # Keep the harsher +0.25R stress visible even though it is not a hard gate.
            harsh = cost_row(summary, config, period, 0.25)
            if harsh:
                period_result["+0.25R_sensitivity"] = {
                    "trades": int(harsh.get("trades", 0)),
                    "win_rate_pct": harsh.get("win_rate_pct"),
                    "profit_factor": harsh.get("profit_factor"),
                    "net_R": harsh.get("net_R"),
                    "max_drawdown_R": harsh.get("max_drawdown_R"),
                }
            dataset_result[period] = period_result
        observed["datasets"][dataset_name] = dataset_result

        for field, expected, check_name in (
            ("walk_forward", 5, "five_fold_walk_forward"),
            ("walk_forward_dense", 7, "seven_fold_walk_forward"),
        ):
            rows = sorted(aggregate_fold_rows(summary, field, config), key=lambda row: int(row["fold"]))
            fold_details = []
            if len(rows) != expected:
                failures.append(f"{dataset_name}/{field}: expected {expected} portfolio folds, found {len(rows)}")
                checks[check_name] = False
            for row in rows:
                trades = int(row.get("trades", 0))
                net_r = float(row.get("net_R_after_extra_0.15R", 0.0))
                pf = float(row.get("PF_after_extra_0.15R", 0.0))
                fold_details.append({
                    "fold": int(row["fold"]),
                    "trades": trades,
                    "win_rate_pct": row.get("win_rate_pct_after_extra_0.15R", row.get("win_rate_pct")),
                    "profit_factor_after_0.15R": pf,
                    "net_R_after_0.15R": net_r,
                    "max_drawdown_R_after_0.15R": row.get("max_drawdown_R_after_extra_0.15R"),
                })
                if trades < MIN_FOLD_TRADES or net_r <= 0 or pf <= 1.0:
                    failures.append(
                        f"{dataset_name}/{field}/fold-{row['fold']}: "
                        f"trades={trades}, Net R={net_r:.3f}, PF={pf:.3f}"
                    )
                    checks[check_name] = False
            observed["walk_forward"][f"{dataset_name}/{field}"] = fold_details

        boot_rows = [
            row for row in summary.get("bootstrap_uncertainty", [])
            if row.get("config") == config
            and row.get("period") in ("train", "holdout")
            and close(row.get("extra_cost_R_per_trade", -999), BOOTSTRAP_COST)
        ]
        boot_details = []
        for period in ("train", "holdout"):
            for block in BOOTSTRAP_BLOCKS:
                matches = [
                    row for row in boot_rows
                    if row.get("period") == period and int(row.get("bootstrap_block_weeks", -1)) == block
                ]
                if len(matches) != 1:
                    failures.append(f"{dataset_name}/bootstrap/{period}/{block}w: missing or duplicate")
                    checks["weekly_block_bootstrap"] = False
                    continue
                row = matches[0]
                low = row.get("mean_R_ci95_low")
                share = row.get("bootstrap_probability_mean_R_positive")
                count = int(row.get("completed_trades", 0))
                boot_details.append({                    "period": period,
                    "block_weeks": block,
                    "completed_trades": count,
                    "mean_R_ci95_low": low,
                    "mean_R_ci95_high": row.get("mean_R_ci95_high"),
                    "positive_resample_share": share,
                })
                if count < MIN_PERIOD_TRADES or low is None or float(low) <= 0:
                    failures.append(
                        f"{dataset_name}/bootstrap/{period}/{block}w: "
                        f"insufficient trades or lower 95% bound not above zero"
                    )
                    checks["weekly_block_bootstrap"] = False
                if share is None or float(share) < BOOTSTRAP_MIN_POSITIVE_SHARE:
                    failures.append(
                        f"{dataset_name}/bootstrap/{period}/{block}w: "
                        f"positive resample share below {BOOTSTRAP_MIN_POSITIVE_SHARE:.0%}"
                    )
                    checks["weekly_block_bootstrap"] = False
        observed["bootstrap"][dataset_name] = boot_details

        loo_rows = [
            row for row in summary.get("leave_one_coin_out", [])
            if row.get("config") == config and row.get("period") == "holdout"
        ]
        loo_details = []
        exclusions = [row for row in loo_rows if row.get("excluded_coin") != "NONE"]
        if not exclusions:
            failures.append(f"{dataset_name}/leave-one-coin-out: no holdout exclusion results")
            checks["leave_one_coin_out_holdout"] = False
        for row in exclusions:
            trades = int(row.get("trades", 0))
            net_r = float(row.get("net_R_after_extra_0.15R", 0.0))
            pf = float(row.get("PF_after_extra_0.15R", 0.0))
            loo_details.append({
                "excluded_coin": row.get("excluded_coin"),
                "trades": trades,
                "net_R_after_0.15R": net_r,
                "profit_factor_after_0.15R": pf,
            })
            if trades < MIN_FOLD_TRADES or net_r <= 0 or pf <= 1.0:
                failures.append(
                    f"{dataset_name}/leave-one-coin-out/{row.get('excluded_coin')}: "
                    f"trades={trades}, Net R={net_r:.3f}, PF={pf:.3f}"
                )
                checks["leave_one_coin_out_holdout"] = False
        observed["leave_one_coin_out"][dataset_name] = loo_details

    return {
        "passed": all(checks.values()),
        "checks": checks,
        "failures": failures,
        "observed": observed,
    }


def evaluate_nested_selection(summary, field, minimum_evaluated_folds):
    """Evaluate the chronological model-selection test, not just fixed candidates."""
    rows = summary.get(field, [])
    evaluated = [row for row in rows if row.get("status") == "evaluated"]
    status_counts = {}
    for row in rows:
        status = row.get("status", "missing status")
        status_counts[status] = status_counts.get(status, 0) + 1

    failures = []
    observed = []
    for row in sorted(evaluated, key=lambda item: (item.get("interval", ""), int(item.get("test_fold", 0)))):
        trades = int(row.get("test_trades", 0))
        net_015 = float(row.get("test_net_R_after_extra_0.15R", 0.0))
        pf_015 = float(row.get("test_PF_after_extra_0.15R", 0.0))
        net_020 = float(row.get("test_net_R_after_extra_0.20R", 0.0))
        pf_020 = float(row.get("test_PF_after_extra_0.20R", 0.0))
        item = {
            "interval": row.get("interval"),
            "test_fold": row.get("test_fold"),
            "selected_config": row.get("selected_config"),
            "selection_folds": row.get("selection_folds", []),
            "test_trades": trades,
            "test_win_rate_pct": row.get("test_win_rate_pct"),
            "net_R_after_0.15R": net_015,
            "PF_after_0.15R": pf_015,
            "net_R_after_0.20R": net_020,
            "PF_after_0.20R": pf_020,
        }
        observed.append(item)
        if trades < MIN_FOLD_TRADES:
            failures.append(f"{item['interval']} fold {item['test_fold']}: only {trades} test trades")
        if net_015 <= 0 or pf_015 <= 1.0:
            failures.append(
                f"{item['interval']} fold {item['test_fold']} at +0.15R: "
                f"Net R {net_015:.3f}, PF {pf_015:.3f}"
            )
        if net_020 <= 0 or pf_020 <= 1.0:
            failures.append(
                f"{item['interval']} fold {item['test_fold']} at +0.20R: "
                f"Net R {net_020:.3f}, PF {pf_020:.3f}"
            )

    if len(evaluated) < minimum_evaluated_folds:
        state = "insufficient_coverage"
    elif failures:
        state = "failed"
    else:
        state = "passed"

    return {
        "status": state,
        "minimum_evaluated_folds": minimum_evaluated_folds,
        "evaluated_fold_count": len(evaluated),
        "status_counts": status_counts,
        "failures": failures,
        "observed": observed,
    }


def main():
    loaded = {}
    for name, path in DATASETS.items():
        if not path.exists():
            raise SystemExit(f"Missing required research artifact: {path}")
        summary = json.loads(path.read_text())
        if summary.get("errors"):
            raise SystemExit(f"{path} contains errors; refusing to evaluate candidates")
        loaded[name] = summary

    config_sets = [
        {item.get("name") for item in summary.get("configs", [])}
        for summary in loaded.values()
    ]
    common_configs = sorted(set.intersection(*config_sets))
    if not common_configs:
        raise SystemExit("No common predeclared configurations across the three datasets")

    coverage = {name: diagnostic_coverage(name, loaded) for name in common_configs}
    fully_diagnosed = sorted(name for name, missing in coverage.items() if not missing)
    not_fully_evaluated = {
        name: missing for name, missing in coverage.items() if missing
    }
    evaluations = {name: evaluate_config(name, loaded) for name in fully_diagnosed}
    nested_selection = {
        name: {
            "standard": evaluate_nested_selection(summary, "nested_walk_forward", 2),
            "dense": evaluate_nested_selection(summary, "nested_walk_forward_dense", 3),
        }
        for name, summary in loaded.items()
    }
    nested_selection_passed = all(
        result["standard"]["status"] == "passed"
        and result["dense"]["status"] == "passed"
        for result in nested_selection.values()
    )
    individually_passed = [
        name for name, result in evaluations.items() if result["passed"]
    ]
    # A candidate is not promoted from a fixed-split screen if chronological
    # model selection itself fails or lacks enough evaluated out-of-sample folds.
    passed = individually_passed if nested_selection_passed else []
    if passed:
        status = "candidate_passed_research_screen"
    elif fully_diagnosed:
        status = "no_candidate_passed_research_screen"
    else:
        status = "insufficient_diagnostic_coverage"
    report = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "purpose": "Conservative research-only cross-universe candidate screen; not a production approval.",
        "datasets": {
            name: {
                "path": str(DATASETS[name]),
                "coin_universe": summary.get("coin_universe", []),
                "evaluation_end_utc": {
                    interval: bounds.get("evaluation_end_utc")
                    for interval, bounds in summary.get("calendar_windows", {}).items()
                },
                "configuration_count": len(summary.get("configs", [])),
            }
            for name, summary in loaded.items()
        },
        "rules": {
            "minimum_trades_per_train_and_holdout_period": MIN_PERIOD_TRADES,
            "required_extra_costs_R_per_trade": list(REQUIRED_COSTS),
            "walk_forward_folds": {
                "standard": 5,
                "dense": 7,
                "minimum_trades_per_fold": MIN_FOLD_TRADES,
                "require_positive_net_R_and_PF_above_1_after_0.15R": True,
            },
            "bootstrap": {
                "cost_R_per_trade": BOOTSTRAP_COST,
                "block_lengths_weeks": list(BOOTSTRAP_BLOCKS),
                "minimum_positive_resample_share": BOOTSTRAP_MIN_POSITIVE_SHARE,
                "require_lower_95_percent_mean_R_bound_above_zero": True,
            },
            "leave_one_coin_out": "Every holdout exclusion must retain at least 8 trades, positive Net R and PF above 1 after +0.15R.",
            "nested_model_selection": {
                "standard_minimum_evaluated_test_folds_per_dataset": 2,
                "dense_minimum_evaluated_test_folds_per_dataset": 3,
                "minimum_test_trades": MIN_FOLD_TRADES,
                "require_positive_net_R_and_PF_above_1_after_costs": [0.15, 0.20],
                "all_evaluated_test_folds_must_pass": True,
            },
            "note": "These conservative screening rules reduce winner's-curse risk but do not prove future profitability. +0.25R is reported as sensitivity, not a hard gate.",
        },
        "common_configuration_count": len(common_configs),
        "fully_diagnosed_configuration_count": len(fully_diagnosed),
        "fully_diagnosed_configurations": fully_diagnosed,
        "not_fully_evaluated_configuration_count": len(not_fully_evaluated),
        "not_fully_evaluated_configs": not_fully_evaluated,
        "individually_passed_candidate_count": len(individually_passed),
        "individually_passed_candidates": individually_passed,
        "nested_model_selection": nested_selection,
        "nested_model_selection_passed": nested_selection_passed,
        "passed_candidate_count": len(passed),
        "passed_candidates": passed,
        "status": status,
        "coverage_note": (
            "Only configurations with complete cost-stress, 5/7 walk-forward, "
            "bootstrap and per-coin holdout diagnostics are evaluated as candidates. "
            "Chronological nested model-selection must also pass standard and dense "
            "out-of-sample folds across all datasets. Other configurations are marked "
            "not_fully_evaluated, not rejected."
        ),
        "evaluations": evaluations,
    }
    out_path = OUT / "candidate_gate.json"
    out_path.write_text(json.dumps(report, indent=2))
    print(json.dumps({
        "status": report["status"],
        "common_configuration_count": len(common_configs),
        "passed_candidate_count": len(passed),
        "passed_candidates": passed,
        "output": str(out_path),
    }, indent=2))


if __name__ == "__main__":
    main()