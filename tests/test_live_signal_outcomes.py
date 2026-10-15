"""Unit tests for conservative live TP/SL outcome calculations."""
import unittest
from unittest.mock import AsyncMock, patch

import bot
from bot import (
    _first_full_candle_ms,
    evaluate_signal_candle_outcome,
    fetch_historical_klines,
)


class LiveSignalOutcomeTests(unittest.IsolatedAsyncioTestCase):
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

    def test_rejects_stop_and_target_on_wrong_side(self):
        invalid_short = {
            "direction": "SHORT",
            "entry_price": 100.0,
            "stop_loss": 99.0,
            "take_profit": 102.0,
        }
        invalid_long = {
            "direction": "LONG",
            "entry_price": 100.0,
            "stop_loss": 101.0,
            "take_profit": 98.0,
        }
        candle = {"h": 103.0, "l": 97.0}
        self.assertIsNone(evaluate_signal_candle_outcome(invalid_short, candle))
        self.assertIsNone(evaluate_signal_candle_outcome(invalid_long, candle))

    def test_rejects_malformed_candle_range(self):
        self.assertIsNone(
            evaluate_signal_candle_outcome(self.short, {"h": 97.0, "l": 102.0})
        )

    def test_historical_klines_are_chronological_and_pagination_moves_backwards(self):
        newest = {
            "data": [
                {"time": "400", "open": "10", "high": "12", "low": "9", "close": "11", "volume": "5"},
                {"time": "300", "open": "9", "high": "11", "low": "8", "close": "10", "volume": "4"},
            ]
        }
        older = {
            "data": [
                {"time": "300", "open": "9", "high": "11", "low": "8", "close": "10", "volume": "4"},
                {"time": "200", "open": "8", "high": "10", "low": "7", "close": "9", "volume": "3"},
                {"time": "100", "open": "7", "high": "9", "low": "6", "close": "8", "volume": "2"},
            ]
        }
        request = AsyncMock(side_effect=[newest, older])
        with patch.object(bot, "_get_json", request):
            candles = await fetch_historical_klines("BTC-USDT", target=4)

        self.assertEqual([candle["t"] for candle in candles], [100, 200, 300, 400])
        self.assertEqual(request.await_count, 2)
        self.assertEqual(request.await_args_list[1].args[1]["endTime"], 299)

    def test_historical_klines_reject_invalid_ohlc_rows(self):
        payload = {
            "data": [
                {"time": "100", "open": "10", "high": "9", "low": "8", "close": "9", "volume": "1"},
                {"time": "200", "open": "10", "high": "12", "low": "8", "close": "11", "volume": "1"},
            ]
        }
        with patch.object(bot, "_get_json", AsyncMock(return_value=payload)):
            candles = await fetch_historical_klines("BTC-USDT", target=2)
        self.assertEqual([candle["t"] for candle in candles], [200])

    async def test_target_zero_returns_no_candles(self):
        with patch.object(bot, "_get_json", AsyncMock()) as request:
            self.assertEqual(await fetch_historical_klines("BTC-USDT", target=0), [])
        request.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
