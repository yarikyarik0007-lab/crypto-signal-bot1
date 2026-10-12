"""Real PostgreSQL integration tests for transactional outbox.

Run with TEST_DATABASE_URL set to a disposable PostgreSQL database. These tests
use a dedicated temporary schema and never connect to production unless that URL
is explicitly supplied by the caller.
"""
import os
import uuid

import asyncpg
import pytest
import pytest_asyncio

import bot

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="TEST_DATABASE_URL is not configured; PostgreSQL integration tests skipped",
)


@pytest_asyncio.fixture
async def pg_env():
    admin = await asyncpg.connect(TEST_DATABASE_URL)
    schema = "outbox_test_" + uuid.uuid4().hex[:12]
    await admin.execute(f'CREATE SCHEMA "{schema}"')
    pool = await asyncpg.create_pool(
        TEST_DATABASE_URL,
        min_size=1,
        max_size=4,
        server_settings={"search_path": schema},
    )
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE signals (
                    id BIGSERIAL PRIMARY KEY,
                    coin TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    entry_price DOUBLE PRECISION,
                    stop_loss DOUBLE PRECISION,
                    take_profit DOUBLE PRECISION,
                    status TEXT NOT NULL DEFAULT 'ACTIVE',
                    result_r DOUBLE PRECISION,
                    entry_reason TEXT,
                    strategy_version TEXT,
                    source TEXT NOT NULL DEFAULT 'live',
                    regime TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    closed_at TIMESTAMPTZ
                )
            """)
            await conn.execute("""
                CREATE TABLE outbox (
                    id BIGSERIAL PRIMARY KEY,
                    signal_id BIGINT NOT NULL,
                    kind TEXT NOT NULL DEFAULT 'telegram_channel',
                    payload JSONB NOT NULL,
                    status TEXT NOT NULL DEFAULT 'PENDING'
                        CHECK (status IN ('PENDING', 'SENT', 'FAILED')),
                    attempts INT NOT NULL DEFAULT 0,
                    last_error TEXT,
                    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    sent_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
        yield pool, schema
    finally:
        await pool.close()
        await admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await admin.close()


def valid_signal():
    return {
        "coin": "BTC", "direction": "SHORT", "entry_price": 100.0,
        "sl_price": 101.0, "tp_price": 98.0, "rr": 2.0,
        "regime": "TREND_DOWN", "reason": "postgres-integration-test",
    }


@pytest.mark.asyncio
async def test_real_transaction_rolls_back_signal_if_outbox_insert_fails(pg_env):
    pool, schema = pg_env
    async with pool.acquire() as conn:
        await conn.execute(f"""
            CREATE FUNCTION "{schema}".reject_outbox_insert() RETURNS trigger
            LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'forced outbox failure'; END $$
        """)
        await conn.execute(f"""
            CREATE TRIGGER reject_outbox BEFORE INSERT ON outbox
            FOR EACH ROW EXECUTE FUNCTION "{schema}".reject_outbox_insert()
        """)

    previous_pool = bot._DB_POOL
    previous_outbox_ready = bot._OUTBOX_READY
    bot._DB_POOL = pool
    bot._OUTBOX_READY = True
    try:
        assert await bot.save_live_signal(valid_signal(), source="live") == -1
    finally:
        bot._DB_POOL = previous_pool
        bot._OUTBOX_READY = previous_outbox_ready

    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT COUNT(*) FROM signals") == 0
        assert await conn.fetchval("SELECT COUNT(*) FROM outbox") == 0


@pytest.mark.asyncio
async def test_two_real_connections_do_not_claim_same_locked_task(pg_env):
    pool, _ = pg_env
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO outbox (signal_id, payload) VALUES (1, $1::jsonb)",
            '{"coin":"BTC"}',
        )

    claim_sql = """
        WITH candidate AS (
            SELECT id FROM outbox
            WHERE status = 'PENDING' AND kind = 'telegram_channel'
              AND next_attempt_at <= NOW()
            ORDER BY next_attempt_at, id
            LIMIT 1 FOR UPDATE SKIP LOCKED
        )
        UPDATE outbox AS o
        SET attempts = o.attempts + 1,
            next_attempt_at = NOW() + ($1::int * INTERVAL '1 second')
        FROM candidate
        WHERE o.id = candidate.id
        RETURNING o.id
    """
    async with pool.acquire() as first, pool.acquire() as second:
        async with first.transaction():
            first_id = await first.fetchval(claim_sql, 300)
            assert first_id is not None
            # Keep the first row lock while a second independent connection claims.
            second_id = await second.fetchval(claim_sql, 300)
            assert second_id is None

    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT attempts FROM outbox WHERE id = $1", first_id) == 1


@pytest.mark.asyncio
async def test_pending_task_survives_pool_reconnect(pg_env):
    pool, schema = pg_env
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO outbox (signal_id, payload) VALUES (7, $1::jsonb)",
            '{"coin":"ETH","reason":"restart-check"}',
        )
    await pool.close()
    reopened = await asyncpg.create_pool(
        TEST_DATABASE_URL,
        min_size=1,
        max_size=2,
        server_settings={"search_path": schema},
    )
    try:
        async with reopened.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT signal_id, status, payload->>'reason' AS reason FROM outbox"
            )
            assert row["signal_id"] == 7
            assert row["status"] == "PENDING"
            assert row["reason"] == "restart-check"
    finally:
        await reopened.close()
