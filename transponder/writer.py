"""All database writes funnel through here.

Two lanes, each a single asyncio task, so hypertable inserts are never issued
concurrently and a long static load cannot starve realtime writes:

* rt lane     - drains a bounded queue of `Batch`es, coalesces them per table,
                and runs batched INSERT ... ON CONFLICT in one transaction.
* static lane - loads one GTFS static version at a time via COPY.
"""

from __future__ import annotations

import asyncio
import logging
from collections import Counter, defaultdict

import asyncpg

from transponder import static
from transponder.health import ErrorKind, Health
from transponder.rt import Batch
from transponder.tables import RT_TABLES

log = logging.getLogger(__name__)

_RETRYABLE = (asyncpg.PostgresConnectionError, asyncpg.InterfaceError, OSError, asyncio.TimeoutError)

StaticItem = static.StaticLoad | static.StaticUnchanged


class Writer:
    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        max_batch_rows: int = 5000,
        flush_interval: float = 1.0,
        queue_size: int = 2000,
        stats_interval: float = 60.0,
        health: Health | None = None,
    ) -> None:
        self._pool = pool
        self._max_batch_rows = max_batch_rows
        self._flush_interval = flush_interval
        self._stats_interval = stats_interval
        self._queue_size = queue_size
        self._health = health
        self._rt_queue: asyncio.Queue[Batch | None] = asyncio.Queue(maxsize=queue_size)
        self._static_queue: asyncio.Queue[StaticItem | None] = asyncio.Queue(maxsize=16)
        self.rows_written: Counter[str] = Counter()
        self.rows_dropped: Counter[str] = Counter()

    # --- producer API ---------------------------------------------------------

    async def submit(self, batch: Batch) -> None:
        """Queue realtime rows. Blocks (backpressure) if the writer is behind."""
        if batch.rows:
            await self._rt_queue.put(batch)

    async def submit_static(self, item: StaticItem) -> None:
        await self._static_queue.put(item)

    async def stop(self) -> None:
        """Ask both lanes to drain and exit. Await run() afterwards."""
        await self._rt_queue.put(None)
        await self._static_queue.put(None)

    @property
    def rt_backlog(self) -> int:
        return self._rt_queue.qsize()

    # --- lanes ------------------------------------------------------------------

    async def run(self) -> None:
        stats_task = asyncio.create_task(self._report_stats(), name="writer-stats")
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._rt_lane(), name="writer-rt")
                tg.create_task(self._static_lane(), name="writer-static")
        finally:
            stats_task.cancel()

    async def _rt_lane(self) -> None:
        while True:
            batches, done = await self._collect()
            if batches:
                await self._flush(batches)
            if done:
                log.info("rt writer lane drained and stopped")
                return

    async def _collect(self) -> tuple[list[Batch], bool]:
        """Block for the first batch, wait briefly for more, then drain what is queued."""
        first = await self._rt_queue.get()
        if first is None:
            return [], True
        batches = [first]
        total = len(first.rows)
        if total < self._max_batch_rows:
            await asyncio.sleep(self._flush_interval)
        while total < self._max_batch_rows:
            try:
                item = self._rt_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if item is None:
                return batches, True
            batches.append(item)
            total += len(item.rows)
        return batches, False

    async def _flush(self, batches: list[Batch]) -> None:
        grouped: dict[str, list[tuple]] = defaultdict(list)
        for b in batches:
            grouped[b.table].extend(b.rows)

        attempt = 0
        while True:
            try:
                async with self._pool.acquire() as conn, conn.transaction():
                    for table, rows in grouped.items():
                        await conn.executemany(RT_TABLES[table].insert_sql, rows)
                for table, rows in grouped.items():
                    self.rows_written[table] += len(rows)
                if attempt and self._health:
                    self._health.report_ok("db")
                return
            except _RETRYABLE as e:
                attempt += 1
                delay = min(30.0, 2.0 ** attempt)
                log.warning("db unavailable (%s: %s); retrying flush in %.0fs", type(e).__name__, e, delay)
                if self._health:
                    self._health.report_failure(
                        "db", ErrorKind.DB_UNAVAILABLE,
                        f"database writes failing ({type(e).__name__}: {e}); {self.rt_backlog} batches queued, retrying",
                    )
                await asyncio.sleep(delay)
            except asyncpg.PostgresError:
                # Data problem somewhere in this flush: isolate it per table so one bad
                # table does not discard everybody else's rows.
                await self._flush_per_table(grouped)
                return

    async def _flush_per_table(self, grouped: dict[str, list[tuple]]) -> None:
        for table, rows in grouped.items():
            try:
                async with self._pool.acquire() as conn, conn.transaction():
                    await conn.executemany(RT_TABLES[table].insert_sql, rows)
                self.rows_written[table] += len(rows)
            except asyncpg.PostgresError as e:
                self.rows_dropped[table] += len(rows)
                log.error("dropping %d rows for %s: %s: %s", len(rows), table, type(e).__name__, e)
                if self._health:
                    self._health.notice(
                        f"writer:{table}", ErrorKind.ROWS_DROPPED,
                        f"dropped {len(rows)} row(s) for {table} that Postgres rejected: {type(e).__name__}: {e}",
                    )

    async def _static_lane(self) -> None:
        while True:
            item = await self._static_queue.get()
            if item is None:
                log.info("static writer lane stopped")
                return
            scope = f"static-load:{item.feed_id}"
            try:
                await self._handle_static(item)
                if self._health:
                    self._health.report_ok(scope)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 - one bad feed must not kill the lane
                log.exception("static load failed for %s", item.feed_id)
                if self._health:
                    self._health.report_failure(
                        scope, ErrorKind.STATIC_LOAD_FAILED,
                        f"{item.feed_id}: GTFS static bundle could not be loaded, previous version stays current: {type(e).__name__}: {e}",
                    )
            finally:
                if isinstance(item, static.StaticLoad):
                    item.path.unlink(missing_ok=True)

    async def _handle_static(self, item: StaticItem) -> None:
        if isinstance(item, static.StaticUnchanged):
            await self._pool.execute(
                "UPDATE feeds SET static_last_checked_at = $2 WHERE feed_id = $1", item.feed_id, item.checked_at
            )
            log.info("%s: static unchanged (%s)", item.feed_id, item.reason)
            return
        version_id = await static.load_version(self._pool, item)
        log.info("%s: loaded static feed_version_id=%s sha256=%s", item.feed_id, version_id, item.sha256[:12])

    async def _report_stats(self) -> None:
        last: Counter[str] = Counter()
        while True:
            await asyncio.sleep(self._stats_interval)
            delta = self.rows_written - last
            last = Counter(self.rows_written)
            summary = ", ".join(f"{t}={n}" for t, n in sorted(delta.items())) or "none"
            backlog = self.rt_backlog
            log.info("rows flushed in last %.0fs (before ON CONFLICT dedupe): %s (backlog %d batches)", self._stats_interval, summary, backlog)
            if self._health:
                self._health.set_condition(
                    "writer", ErrorKind.WRITER_BACKLOG, backlog >= 0.8 * self._queue_size,
                    f"writer backlog is {backlog}/{self._queue_size} batches; pollers will block. "
                    "The database is not keeping up: check its load or lower the poll rate.",
                )
