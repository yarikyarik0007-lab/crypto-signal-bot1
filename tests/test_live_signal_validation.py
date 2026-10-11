import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import bot


class FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeConnection:
    def __init__(self, active_id=None, cooldown_id=None, inserted_id=123):
        self.active_id = active_id
        self.cooldown_id = cooldown_id
        self.inserted_id = inserted_id
        self.calls = []

    def transaction(self):
        return FakeTransaction()

    async def execute(self, query, *args):
        self.calls.append(("execute", query, args))
        return "SELECT 1"

    async def fetchval(self, query, *args):
        self.calls.append(("fetchval", query, args))
        if "status = 'ACTIVE'" in query:
            return self.active_id
        return self.cooldown_id

    async def fetchrow(self, query, *args):
        self.calls.append(("fetchrow", query, args))
        if "INSERT INTO signals" in query and self.inserted_id is not None:
            return {"id": self.inserted_id}
        return None


class FakeAcquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakePool:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return FakeAcquire(self.connection)


def valid_signal(direction="SHORT"):
    if direction == "SHORT":
        return {
            "coin": "BTC", "direction": "SHORT",
            "entry_price": 100.0, "sl_price": 101.0, "tp_price": 98.0,
            "rr": 2.0, "regime": "TEST", "reason": "unit test",
        }
    return {
        "coin": "ETH", "direction": "LONG",
        "entry_price": 100.0, "sl_price": 99.0, "tp_price": 102.0,
        "rr": 2.0, "regime": "TEST", "reason": "unit test",
    }


class LiveSignalValidationTests(unittest.TestCase):
    def test_valid_short_is_saved_and_source_is_passed(self):
        conn = FakeConnection(inserted_id=123)
        with patch.object(bot, "_DB_POOL", FakePool(conn)):
            result = asyncio.run(bot.save_live_signal(valid_signal(), source="test"))
        self.assertEqual(result, 123)
        insert = next(call for call in conn.calls if call[0] == "fetchrow")
        self.assertIn("INSERT INTO signals", insert[1])
        self.assertEqual(insert[2][7], "test")

    def test_valid_long_is_saved(self):
        conn = FakeConnection(inserted_id=456)
        with patch.object(bot, "_DB_POOL", FakePool(conn)):
            result = asyncio.run(bot.save_live_signal(valid_signal("LONG"), source="live"))
        self.assertEqual(result, 456)

    def test_active_duplicate_is_rejected(self):
        conn = FakeConnection(active_id=42)
        with patch.object(bot, "_DB_POOL", FakePool(conn)):
            result = asyncio.run(bot.save_live_signal(valid_signal(), source="live"))
        self.assertEqual(result, -1)
        self.assertFalse(any("INSERT INTO signals" in c[1] for c in conn.calls if c[0] == "fetchrow"))

    def test_four_hour_cooldown_is_rejected(self):
        conn = FakeConnection(cooldown_id=77)
        with patch.object(bot, "_DB_POOL", FakePool(conn)):
            result = asyncio.run(bot.save_live_signal(valid_signal(), source="live"))
        self.assertEqual(result, -1)

    def test_invalid_price_order_is_rejected_before_database_access(self):
        sig = valid_signal()
        sig["sl_price"] = 99.0
        conn = FakeConnection()
        with patch.object(bot, "_DB_POOL", FakePool(conn)):
            result = asyncio.run(bot.save_live_signal(sig, source="live"))
        self.assertEqual(result, -1)
        self.assertEqual(conn.calls, [])

    def test_non_finite_price_is_rejected_before_database_access(self):
        sig = valid_signal()
        sig["entry_price"] = float("nan")
        conn = FakeConnection()
        with patch.object(bot, "_DB_POOL", FakePool(conn)):
            result = asyncio.run(bot.save_live_signal(sig, source="live"))
        self.assertEqual(result, -1)
        self.assertEqual(conn.calls, [])

    def test_unknown_source_is_rejected_before_database_access(self):
        conn = FakeConnection()
        with patch.object(bot, "_DB_POOL", FakePool(conn)):
            result = asyncio.run(bot.save_live_signal(valid_signal(), source="production"))
        self.assertEqual(result, -1)
        self.assertEqual(conn.calls, [])

    def test_notify_channel_rejects_invalid_price_order(self):
        sig = valid_signal()
        sig["tp_price"] = 102.0
        with patch.object(bot, "SIGNAL_CHANNEL_ID", "@test_channel"), patch.object(
            bot, "Bot"
        ) as bot_constructor:
            result = asyncio.run(bot.notify_channel(sig))
        self.assertFalse(result)
        bot_constructor.assert_not_called()


if __name__ == "__main__":
    unittest.main()
