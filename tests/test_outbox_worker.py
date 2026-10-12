"""Mock-only unit tests for the transactional outbox worker.

These tests exercise application logic with fake connections. They do NOT prove
PostgreSQL transaction rollback, real FOR UPDATE SKIP LOCKED concurrency, or a
real process restart; see the test names and PR notes.
"""
import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import bot


class StopWorker(Exception):
    """Sentinel used to stop the worker after one item in tests."""


class FakeTransaction:
    def __init__(self, conn=None):
        self.conn = conn

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is not None and self.conn is not None:
            self.conn.rollback_observed = True
        return False


class FakeConnection:
    def __init__(self, task=None, fail_outbox_insert=False):
        self.task = task
        self.fetchrow_calls = []
        self.execute_calls = []
        self.rollback_observed = False
        self.fail_outbox_insert = fail_outbox_insert
        self.claimed = False

    def transaction(self):
        return FakeTransaction(self)

    async def fetchrow(self, query, *args):
        self.fetchrow_calls.append((query, args))
        if "INSERT INTO signals" in query:
            return {"id": 41}
        if "WITH candidate AS" in query:
            if self.task is not None and not self.claimed:
                self.claimed = True
                return dict(self.task)
            return None
        raise AssertionError("Unexpected fetchrow query")

    async def fetchval(self, query, *args):
        return None

    async def execute(self, query, *args):
        self.execute_calls.append((query, args))
        if "INSERT INTO outbox" in query and self.fail_outbox_insert:
            raise RuntimeError("simulated outbox insert failure")
        return "UPDATE 1"


class FakeAcquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakePool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return FakeAcquire(self.conn)


def task(attempts=1, payload=None):
    return {
        "id": 9,
        "signal_id": 41,
        "payload": payload or {
            "coin": "BTC", "direction": "SHORT", "entry_price": 100.0,
            "sl_price": 101.0, "tp_price": 98.0, "rr": 2.0,
            "regime": "TREND_DOWN", "reason": "unit-test",
        },
        "attempts": attempts,
    }


class OutboxWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def run_worker_until_one_attempt(self, conn, notify_result=True, notify_error=None):
        notify = AsyncMock()
        if notify_error is not None:
            notify.side_effect = notify_error
        else:
            notify.return_value = notify_result
        fake_asyncio = SimpleNamespace(sleep=AsyncMock(side_effect=StopWorker))
        with patch.object(bot, "_DB_POOL", FakePool(conn)), \
             patch.object(bot, "notify_channel", notify), \
             patch.object(bot, "asyncio", fake_asyncio):
            with self.assertRaises(StopWorker):
                await bot.outbox_worker()
        return notify

    async def test_rollback_on_outbox_insert_failure(self):
        conn = FakeConnection(fail_outbox_insert=True)
        valid = {
            "coin": "BTC", "direction": "SHORT", "entry_price": 100.0,
            "sl_price": 101.0, "tp_price": 98.0, "rr": 2.0,
            "regime": "TREND_DOWN", "reason": "unit-test",
        }
        with patch.object(bot, "_DB_POOL", FakePool(conn)), \\
             patch.object(bot, "_OUTBOX_READY", True):
            result = await bot.save_live_signal(valid, source="live")
        self.assertEqual(result, -1)
        self.assertTrue(any("INSERT INTO signals" in q for q, _ in conn.fetchrow_calls))
        self.assertTrue(any("INSERT INTO outbox" in q for q, _ in conn.execute_calls))
        self.assertTrue(conn.rollback_observed)

    async def test_live_signal_rejected_when_outbox_not_ready(self):
        conn = FakeConnection()
        valid = {
            "coin": "BTC", "direction": "SHORT", "entry_price": 100.0,
            "sl_price": 101.0, "tp_price": 98.0, "rr": 2.0,
            "regime": "TREND_DOWN", "reason": "unit-test",
        }
        with patch.object(bot, "_DB_POOL", FakePool(conn)), \\
             patch.object(bot, "_OUTBOX_READY", False):
            result = await bot.save_live_signal(valid, source="live")
        self.assertEqual(result, -1)
        self.assertEqual(conn.fetchrow_calls, [])
        self.assertEqual(conn.execute_calls, [])

    async def test_worker_marks_sent_on_success(self):
        conn = FakeConnection(task=task())
        notify = await self.run_worker_until_one_attempt(conn)
        notify.assert_awaited_once()
        updates = [(q, a) for q, a in conn.execute_calls if "SET status = 'SENT'" in q]
        self.assertEqual(len(updates), 1)
        self.assertIn("sent_at = NOW()", updates[0][0])

    async def test_worker_retry_on_failure(self):
        conn = FakeConnection(task=task(attempts=1))
        await self.run_worker_until_one_attempt(conn, notify_result=False)
        retry = [(q, a) for q, a in conn.execute_calls if "SET status = $2" in q]
        self.assertEqual(len(retry), 1)
        self.assertEqual(retry[0][1][1], "PENDING")
        self.assertEqual(retry[0][1][3], 30)

    async def test_worker_failed_after_max_attempts(self):
        conn = FakeConnection(task=task(attempts=bot.OUTBOX_MAX_ATTEMPTS))
        await self.run_worker_until_one_attempt(conn, notify_error=RuntimeError("telegram down"))
        updates = [(q, a) for q, a in conn.execute_calls if "SET status = $2" in q]
        self.assertEqual(updates[0][1][1], "FAILED")
        self.assertEqual(updates[0][1][3], 0)

    async def test_backoff_sequence(self):
        expected = [30, 60, 120, 240, 480, 960, 1800]
        for attempt, delay in enumerate(expected, start=1):
            with self.subTest(attempt=attempt):
                conn = FakeConnection(task=task(attempts=attempt))
                await self.run_worker_until_one_attempt(conn, notify_result=False)
                updates = [(q, a) for q, a in conn.execute_calls if "SET status = $2" in q]
                self.assertEqual(updates[0][1][3], delay)

    async def test_concurrent_workers_claim_one_task(self):
        conn = FakeConnection(task=task())
        await self.run_worker_until_one_attempt(conn)
        claim_query = conn.fetchrow_calls[0][0]
        self.assertIn("FOR UPDATE SKIP LOCKED", claim_query)
        self.assertIn("LIMIT 1", claim_query)
        self.assertIn("kind = 'telegram_channel'", claim_query)
        self.assertEqual(conn.fetchrow_calls[0][1], (300,))
        self.assertEqual(sum("WITH candidate AS" in q for q, _ in conn.fetchrow_calls), 2)
        # This is a single fake connection/worker simulation, not a real concurrency test.

    async def test_pending_task_recovered_after_restart(self):
        first = FakeConnection(task=task(attempts=1))
        await self.run_worker_until_one_attempt(first, notify_result=False)
        first_retry = next(a for q, a in first.execute_calls if "SET status = $2" in q)
        self.assertEqual(first_retry[1], "PENDING")

        # New fake connection/worker sees a persisted pending row after a simulated restart.
        recovered = FakeConnection(task=task(attempts=2))
        notify = await self.run_worker_until_one_attempt(recovered)
        notify.assert_awaited_once()
        sent = [(q, a) for q, a in recovered.execute_calls if "SET status = 'SENT'" in q]
        self.assertEqual(len(sent), 1)

    async def test_test_signal_preserves_marker(self):
        payload = {
            "coin": "BTC", "direction": "SHORT", "entry_price": 100.0,
            "sl_price": 101.0, "tp_price": 98.0, "rr": 2.0,
            "regime": "TEST", "reason": "TESTSIGNAL",
        }
        conn = FakeConnection(task=task(payload=payload))
        notify = await self.run_worker_until_one_attempt(conn)
        self.assertEqual(notify.await_args.args[0]["reason"], "TESTSIGNAL")
        self.assertIn("TESTSIGNAL", json.dumps(notify.await_args.args[0]))


if __name__ == "__main__":
    unittest.main()
