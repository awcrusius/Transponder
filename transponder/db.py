"""asyncpg pool creation and a minimal SQL-file migration runner."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import asyncpg

from transponder.config import Config

log = logging.getLogger(__name__)


async def _init_connection(conn: asyncpg.Connection) -> None:
    # Let us pass plain Python objects for jsonb columns.
    await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")


async def create_pool(dsn: str, *, min_size: int, max_size: int) -> asyncpg.Pool:
    pool = await asyncpg.create_pool(dsn, min_size=min_size, max_size=max_size, init=_init_connection)
    assert pool is not None
    return pool


async def run_migrations(pool: asyncpg.Pool, migrations_dir: Path) -> list[str]:
    """Apply every migrations/*.sql not yet recorded in schema_migrations, in name order."""
    if not migrations_dir.is_dir():
        raise FileNotFoundError(f"migrations directory not found: {migrations_dir}")
    applied_now: list[str] = []
    async with pool.acquire() as conn:
        await conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            " version text PRIMARY KEY,"
            " applied_at timestamptz NOT NULL DEFAULT now())"
        )
        # Serialize concurrent runners (e.g. two containers starting at once).
        await conn.execute("SELECT pg_advisory_lock(7245206)")
        try:
            applied = {r["version"] for r in await conn.fetch("SELECT version FROM schema_migrations")}
            for path in sorted(migrations_dir.glob("*.sql")):
                if path.stem in applied:
                    continue
                log.info("applying migration %s", path.name)
                async with conn.transaction():
                    await conn.execute(path.read_text(encoding="utf-8"))
                    await conn.execute("INSERT INTO schema_migrations (version) VALUES ($1)", path.stem)
                applied_now.append(path.stem)
        finally:
            await conn.execute("SELECT pg_advisory_unlock(7245206)")
    return applied_now


async def sync_feeds(pool: asyncpg.Pool, config: Config) -> None:
    """Upsert the configured feeds into the feeds table (runs once at startup, before the writer)."""
    async with pool.acquire() as conn, conn.transaction():
        for feed in config.feeds:
            await conn.execute(
                """
                INSERT INTO feeds (feed_id, agency_name, static_url)
                VALUES ($1, $2, $3)
                ON CONFLICT (feed_id) DO UPDATE
                    SET agency_name = EXCLUDED.agency_name,
                        static_url = EXCLUDED.static_url,
                        updated_at = now()
                """,
                feed.feed_id,
                feed.agency,
                feed.static_url,
            )
