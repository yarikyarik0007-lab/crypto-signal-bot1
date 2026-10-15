"""Unit tests for conservative live TP/SL outcome calculations."""
import unittest

from bot import _first_full_candle_ms, evaluate_signal_candle_outcome


class LiveSignalOutcomeTests(unittest.TestCase):
    def setUp(self):
        self.short = {
            "direction": "SHORT",
            "entry_price": 100.0,
            "stop_loss": 101.0,
            "take_profit": 98.0,
        }

    def test_short_take_profit_is_win(self):
        outcome = evaluate_signal_candle_outcome(
            self.short, {"h": 100.5, "l": 97.5}
        )
        self.assertEqual(outcome, {"status": "WIN", "result_r": 1.95})

    def test_short_stop_loss_is_loss(self):
        outcome = evaluate_signal_candle_outcome(
            self.short, {"h": 101.2, "l": 99.5}
        )
        self.assertEqual(outcome, {"status": "LOSS", "result_r": -1.05})

    def test_same_candle_hits_sl_and_tp_sl_wins_conservatively(self):
        outcome = evaluate_signal_candle_outcome(
            self.short, {"h": 101.2, "l": 97.5}
        )
        self.assertEqual(outcome, {"status": "LOSS", "result_r": -1.05})

    def test_no_level_hit_keeps_signal_active(self):
        self.assertIsNone(
            evaluate_signal_candle_outcome(
                self.short, {"h": 100.8, "l": 99.0}
            )
        )

    def test_long_take_profit_is_win(self):
        signal = {
            "direction": "LONG",
            "entry_price": 100.0,
            "stop_loss": 99.0,
            "take_profit": 102.0,
        }
        self.assertEqual(
            evaluate_signal_candle_outcome(signal, {"h": 102.1, "l": 99.5}),
            {"status": "WIN", "result_r": 1.95},
        )

    def test_first_full_candle_excludes_candle_already_open(self):
        from datetime import datetime, timezone

        created_at = datetime.fromtimestamp(12 * 3600 + 5 * 60, tz=timezone.utc)
        self.assertEqual(
            _first_full_candle_ms(created_at),
            16 * 3600 * 1000,
        )


if __name__ == "__main__":
    unittest.main()
