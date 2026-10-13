import unittest
from unittest.mock import patch

import bot


def candles(count=100):
    rows = []
    for i in range(count):
        rows.append({
            "t": i + 1,
            "o": 100.0,
            "h": 101.0,
            "l": 99.0,
            "c": 100.0,
            "v": 100.0,
        })
    return rows


class LiveSignalDiagnosticsTests(unittest.TestCase):
    def test_empty_and_insufficient_candles_keep_existing_guard_results(self):
        diagnostics = {}
        self.assertIsNone(bot.evaluate_last_candle([], diagnostics))
        self.assertEqual(diagnostics["result"], "no_candles")

        diagnostics = {}
        self.assertIsNone(bot.evaluate_last_candle(candles(99), diagnostics))
        self.assertEqual(diagnostics["result"], "insufficient_candles")

    def test_missing_latest_timestamp_is_reported(self):
        data = candles()
        data[-1]["t"] = None
        diagnostics = {}
        with patch.object(bot, "classify_regime", return_value="TREND_DOWN"):
            self.assertIsNone(bot.evaluate_last_candle(data, diagnostics))
        self.assertEqual(diagnostics["result"], "missing_last_candle_timestamp")

    def test_old_breakout_does_not_become_latest_candle_reason(self):
        data = candles()
        # An old short breakout; the final candle is not a breakout.
        data[70]["o"] = 100.0
        data[70]["c"] = 99.0
        data[70]["h"] = 100.0
        data[70]["l"] = 98.0
        diagnostics = {}

        def fixed_level(_candles, _index, _lookback, kind):
            return 100.0 if kind == "low" else 110.0

        with patch.object(bot, "find_pivot_level", side_effect=fixed_level), \
             patch.object(bot, "classify_regime", return_value="TREND_DOWN"):
            result = bot.evaluate_last_candle(data, diagnostics)

        self.assertIsNone(result)
        self.assertIn("breakout_detected_short", diagnostics["history"])
        self.assertNotIn("breakout_detected_short", diagnostics["latest_stats"])
        self.assertEqual(diagnostics["latest"]["candle_time"], data[-1]["t"])
        self.assertFalse(diagnostics["latest"]["short_breakout"])
        self.assertEqual(diagnostics["result"], "latest_no_qualifying_breakout")
        self.assertEqual(diagnostics["latest"]["result"], diagnostics["result"])

    def test_latest_short_breakout_rejected_by_volume_is_distinguished(self):
        data = candles()
        data[-1].update({"o": 100.0, "h": 100.2, "l": 98.8, "c": 99.0, "v": 1.0})
        diagnostics = {}

        def fixed_level(_candles, _index, _lookback, kind):
            return 100.0 if kind == "low" else 110.0

        with patch.object(bot, "find_pivot_level", side_effect=fixed_level), \
             patch.object(bot, "classify_regime", return_value="TREND_DOWN"):
            result = bot.evaluate_last_candle(data, diagnostics)

        self.assertIsNone(result)
        self.assertTrue(diagnostics["latest"]["short_breakout"])
        self.assertFalse(diagnostics["latest"]["vol_ok"])
        self.assertEqual(
            diagnostics["latest"]["result"],
            "latest_short_breakout_rejected_by_volume",
        )
        self.assertEqual(
            diagnostics["latest_stats"].get("breakout_rejected_volume_short"),
            1,
        )

    def test_diagnostics_counters_are_reset_when_dict_is_reused(self):
        data = candles()
        diagnostics = {
            "history": {"breakout_detected_short": 999},
            "latest_stats": {"breakout_detected_short": 999},
            "blocking_setup": {"status": "stale"},
        }

        with patch.object(bot, "find_pivot_level", return_value=None), \
             patch.object(bot, "classify_regime", return_value="TREND_DOWN"):
            result = bot.evaluate_last_candle(data, diagnostics)

        self.assertIsNone(result)
        self.assertEqual(diagnostics["history"], {})
        self.assertEqual(diagnostics["latest_stats"], {})
        self.assertNotIn("blocking_setup", diagnostics)
        self.assertEqual(diagnostics["latest"]["result"], "latest_no_qualifying_breakout")

    def test_latest_short_breakout_with_volume_is_not_confused_with_history(self):
        data = candles()
        data[70].update({"o": 100.0, "h": 100.2, "l": 98.8, "c": 99.0, "v": 100.0})
        data[-1].update({"o": 100.0, "h": 100.2, "l": 98.8, "c": 99.0, "v": 100.0})
        diagnostics = {}

        def fixed_level(_candles, _index, _lookback, kind):
            return 100.0 if kind == "low" else 110.0

        with patch.object(bot, "find_pivot_level", side_effect=fixed_level), \
             patch.object(bot, "classify_regime", return_value="TREND_DOWN"):
            result = bot.evaluate_last_candle(data, diagnostics)

        self.assertIsNone(result)
        self.assertEqual(diagnostics["latest"]["result"], "latest_short_breakout_seen_but_no_valid_retest")
        self.assertEqual(diagnostics["latest_stats"].get("breakout_detected_short"), 1)
        self.assertGreater(diagnostics["history"].get("breakout_detected_short", 0), 0)

    def test_latest_long_breakout_reports_short_only_strategy_rejection(self):
        data = candles()
        data[-1].update({"o": 100.0, "h": 112.0, "l": 99.8, "c": 111.0, "v": 100.0})
        diagnostics = {}

        def fixed_level(_candles, _index, _lookback, kind):
            return 90.0 if kind == "low" else 100.0

        with patch.object(bot, "find_pivot_level", side_effect=fixed_level), \
             patch.object(bot, "classify_regime", return_value="TREND_DOWN"):
            result = bot.evaluate_last_candle(data, diagnostics)

        self.assertIsNone(result)
        self.assertTrue(diagnostics["latest"]["long_breakout"])
        self.assertEqual(diagnostics["latest"]["result"], "latest_only_long_breakout_but_strategy_is_short_only")
        self.assertEqual(diagnostics["latest_stats"].get("breakout_detected_long"), 1)


    def test_latest_short_retest_in_downtrend_returns_signal_with_two_to_one_rr(self):
        data = candles()
        data[-2].update({"o": 100.0, "h": 100.2, "l": 98.8, "c": 99.0, "v": 100.0})
        data[-1].update({"o": 100.2, "h": 100.1, "l": 99.2, "c": 99.5, "v": 100.0})
        diagnostics = {}

        def fixed_level(_candles, _index, _lookback, kind):
            return 100.0 if kind == "low" else 110.0

        with patch.object(bot, "find_pivot_level", side_effect=fixed_level), \
             patch.object(bot, "classify_regime", return_value="TREND_DOWN"):
            result = bot.evaluate_last_candle(data, diagnostics)

        self.assertIsNotNone(result)
        self.assertEqual(result["direction"], "SHORT")
        self.assertEqual(result["regime"], "TREND_DOWN")
        self.assertGreater(result["sl_price"], result["entry_price"])
        self.assertLess(result["tp_price"], result["entry_price"])
        self.assertAlmostEqual(
            (result["entry_price"] - result["tp_price"]) /
            (result["sl_price"] - result["entry_price"]),
            2.0,
        )
        self.assertEqual(diagnostics["result"], "signal_found")

    def test_latest_short_retest_is_rejected_outside_downtrend(self):
        data = candles()
        data[-2].update({"o": 100.0, "h": 100.2, "l": 98.8, "c": 99.0, "v": 100.0})
        data[-1].update({"o": 100.2, "h": 100.1, "l": 99.2, "c": 99.5, "v": 100.0})
        diagnostics = {}

        def fixed_level(_candles, _index, _lookback, kind):
            return 100.0 if kind == "low" else 110.0

        with patch.object(bot, "find_pivot_level", side_effect=fixed_level), \
             patch.object(
                 bot, "classify_regime",
                 side_effect=["TREND_DOWN", "TREND_UP"],
             ):
            result = bot.evaluate_last_candle(data, diagnostics)

        self.assertIsNone(result)
        self.assertEqual(diagnostics["latest_stats"].get("retest_triggered"), 1)
        self.assertEqual(diagnostics["latest_stats"].get("retest_rejected_regime_TREND_UP"), 1)
        self.assertEqual(diagnostics["result"], "latest_retest_rejected_or_not_signal")

    def test_latest_long_retest_is_rejected_by_short_only_strategy(self):
        data = candles()
        data[-2].update({"o": 100.0, "h": 112.0, "l": 99.8, "c": 111.0, "v": 100.0})
        data[-1].update({"o": 110.0, "h": 110.2, "l": 99.8, "c": 110.5, "v": 100.0})
        diagnostics = {}

        def fixed_level(_candles, _index, _lookback, kind):
            return 90.0 if kind == "low" else 100.0

        with patch.object(bot, "find_pivot_level", side_effect=fixed_level), \
             patch.object(bot, "classify_regime", return_value="TREND_DOWN"):
            result = bot.evaluate_last_candle(data, diagnostics)

        self.assertIsNone(result)
        self.assertEqual(diagnostics["latest_stats"].get("retest_triggered"), 1)
        self.assertEqual(diagnostics["latest_stats"].get("retest_rejected_direction"), 1)
        self.assertEqual(diagnostics["result"], "latest_retest_rejected_or_not_signal")

    def test_unresolved_historical_retest_is_reported_as_blocking_latest_signal(self):
        data = candles()
        data[80].update({"o": 100.0, "h": 100.2, "l": 98.8, "c": 99.0, "v": 100.0})
        data[81].update({"o": 100.0, "h": 100.1, "l": 99.3, "c": 99.5, "v": 100.0})
        for candle in data[82:]:
            candle.update({"o": 100.0, "h": 100.2, "l": 99.8, "c": 100.0, "v": 100.0})
        diagnostics = {}

        def fixed_level(_candles, _index, _lookback, kind):
            return 100.0 if kind == "low" else 110.0

        with patch.object(bot, "find_pivot_level", side_effect=fixed_level), \
             patch.object(bot, "classify_regime", return_value="TREND_DOWN"):
            result = bot.evaluate_last_candle(data, diagnostics)

        self.assertIsNone(result)
        self.assertEqual(diagnostics["result"], "historical_setup_unresolved")
        self.assertEqual(diagnostics["latest"]["result"], "blocked_by_historical_setup_unresolved")
        self.assertEqual(diagnostics["blocking_setup"]["status"], "unresolved")
        self.assertEqual(diagnostics["history"].get("historical_retest_unresolved"), 1)


    def test_latest_long_breakout_rejected_by_volume_is_not_mislabeled_short_only(self):
        data = candles()
        data[-1].update({"o": 100.0, "h": 112.0, "l": 99.8, "c": 111.0, "v": 1.0})
        diagnostics = {}

        def fixed_level(_candles, _index, _lookback, kind):
            return 90.0 if kind == "low" else 100.0

        with patch.object(bot, "find_pivot_level", side_effect=fixed_level), \\
             patch.object(bot, "classify_regime", return_value="TREND_DOWN"):
            result = bot.evaluate_last_candle(data, diagnostics)

        self.assertIsNone(result)
        self.assertTrue(diagnostics["latest"]["long_breakout"])
        self.assertFalse(diagnostics["latest"]["vol_ok"])
        self.assertEqual(
            diagnostics["result"],
            "latest_long_breakout_rejected_by_volume",
        )
        self.assertEqual(
            diagnostics["latest_stats"].get("breakout_rejected_volume_long"),
            1,
        )


if __name__ == "__main__":
    unittest.main()
