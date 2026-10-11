import unittest
from unittest.mock import patch

import bot


class FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeConnection:
    def __init__(self, existing=None, cooling=None, inserted_id=7):
        self.existing = existing
        self.cooling = cooling
        self.inserted_id = inserted_id
        self.fetchval_calls = []
        self.fetchrow_calls = []

    def transaction(self):
        return FakeTransaction()

    async def execute(self, query, *args):
        return "SELECT 1"

    async def fetchval(self, query, *args):
        self.fetchval_calls.append((query, args))
        if "pg_advisory_xact_lock" in query:
            return None
        if "status = 'ACTIVE'" in query:
            return self.existing
        if "INTERVAL '4 hours'" in query:
            return self.cooling
        raise AssertionError("Unexpected fetchval query")

    async def fetchrow(self, query, *args):
        self.fetchrow_calls.append((query, args))
        if self.inserted_id is None:
            return None
        return {"id": self.inserted_id}


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


VALID_SHORT = {
    "coin": "BTC",
    "direction": "SHORT",
    "entry_price": 100.0,
    "sl_price": 101.0,
    "tp_price": 98.0,
    "rr": 2.0,
    "regime": "TREND_DOWN",
    "reason": "unit-test",
}


class LiveSignalPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_valid_short_is_saved_and_returns_id(self):
        conn = FakeConnection(inserted_id=42)
        with patch.object(bot, "_DB_POOL", FakePool(conn)):
            result = await bot.save_live_signal(VALID_SHORT, source="live")
        self.assertEqual(result, 42)
        self.assertEqual(len(conn.fetchrow_calls), 1)
        self.assertEqual(conn.fetchrow_calls[0][1][0:2], ("BTC", "SHORT"))
        self.assertEqual(conn.fetchrow_calls[0][1][7], "live")

    async def test_invalid_price_order_is_rejected_before_insert(self):
        conn = FakeConnection()
        invalid = {**VALID_SHORT, "sl_price": 99.0}
        with patch.object(bot, "_DB_POOL", FakePool(conn)):
            result = await bot.save_live_signal(invalid, source="live")
        self.assertEqual(result, -1)
        self.assertEqual(conn.fetchrow_calls, [])

    async def test_active_duplicate_is_rejected(self):
        conn = FakeConnection(existing=123)
        with patch.object(bot, "_DB_POOL", FakePool(conn)):
            result = await bot.save_live_signal(VALID_SHORT, source="live")
        self.assertEqual(result, -1)
        self.assertEqual(conn.fetchrow_calls, [])

    async def test_test_source_is_kept_separate_from_live(self):
        conn = FakeConnection(inserted_id=9)
        with patch.object(bot, "_DB_POOL", FakePool(conn)):
            result = await bot.save_live_signal(VALID_SHORT, source="test")
        self.assertEqual(result, 9)
        active_query = next(call for call in conn.fetchval_calls if "status = 'ACTIVE'" in call[0])
        self.assertEqual(active_query[1], ("BTC", "test"))
        self.assertEqual(conn.fetchrow_calls[0][1][7], "test")

    async def test_unknown_source_is_rejected(self):
        conn = FakeConnection()
        with patch.object(bot, "_DB_POOL", FakePool(conn)):
            result = await bot.save_live_signal(VALID_SHORT, source="paper")
        self.assertEqual(result, -1)
        self.assertEqual(conn.fetchrow_calls, [])


if __name__ == "__main__":
    unittest.main()
