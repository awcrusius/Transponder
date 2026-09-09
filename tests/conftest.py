import contextlib
from datetime import datetime, timezone

import pytest

NOW = datetime(2026, 9, 8, 12, 0, 0, tzinfo=timezone.utc)


class FakeConn:
    """Records executemany/execute calls instead of talking to Postgres."""

    def __init__(self, fail_tables: set[str] | None = None):
        self.calls: list[tuple[str, list[tuple]]] = []
        self.fail_tables = fail_tables or set()

    async def executemany(self, sql: str, rows):
        rows = list(rows)
        for table in self.fail_tables:
            if f"INTO {table} " in sql:
                import asyncpg

                raise asyncpg.DataError(f"bad data for {table}")
        self.calls.append((sql, rows))

    async def execute(self, sql: str, *args):
        self.calls.append((sql, [args]))

    @contextlib.asynccontextmanager
    async def transaction(self):
        yield


class FakePool:
    def __init__(self, conn: FakeConn):
        self.conn = conn

    @contextlib.asynccontextmanager
    async def acquire(self):
        yield self.conn

    async def execute(self, sql, *args):
        await self.conn.execute(sql, *args)


@pytest.fixture
def fake_conn():
    return FakeConn()


@pytest.fixture
def fake_pool(fake_conn):
    return FakePool(fake_conn)
