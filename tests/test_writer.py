import asyncio

from transponder.rt import Batch
from transponder.writer import Writer
from tests.conftest import FakeConn, FakePool


async def run_writer(writer: Writer, batches: list[Batch]) -> None:
    task = asyncio.create_task(writer.run())
    for b in batches:
        await writer.submit(b)
    await writer.stop()
    await task


async def test_batches_are_coalesced_per_table(fake_pool, fake_conn):
    writer = Writer(fake_pool, flush_interval=0.01, stats_interval=1000)
    batches = [
        Batch("vehicle_positions", [("a",), ("b",)]),
        Batch("rt_fetches", [("f1",)]),
        Batch("vehicle_positions", [("c",)]),
        Batch("rt_fetches", [("f2",)]),
    ]
    await run_writer(writer, batches)

    by_table = {}
    for sql, rows in fake_conn.calls:
        by_table.setdefault(sql.split()[2], []).extend(rows)
    assert by_table["vehicle_positions"] == [("a",), ("b",), ("c",)]
    assert by_table["rt_fetches"] == [("f1",), ("f2",)]
    # One executemany per table, not per batch.
    assert len(fake_conn.calls) == 2
    assert writer.rows_written["vehicle_positions"] == 3
    assert writer.rows_written["rt_fetches"] == 2


async def test_empty_batches_are_ignored(fake_pool, fake_conn):
    writer = Writer(fake_pool, flush_interval=0.01, stats_interval=1000)
    await run_writer(writer, [Batch("vehicle_positions", [])])
    assert fake_conn.calls == []


async def test_bad_table_is_isolated():
    conn = FakeConn(fail_tables={"rt_fetches"})
    writer = Writer(FakePool(conn), flush_interval=0.01, stats_interval=1000)
    await run_writer(writer, [
        Batch("vehicle_positions", [("a",)]),
        Batch("rt_fetches", [("f1",)]),
    ])
    assert all("INTO vehicle_positions " in sql for sql, _ in conn.calls)
    assert writer.rows_written["vehicle_positions"] == 1
    assert writer.rows_dropped["rt_fetches"] == 1
    assert writer.rows_written["rt_fetches"] == 0


async def test_max_batch_rows_splits_flushes(fake_pool, fake_conn):
    writer = Writer(fake_pool, max_batch_rows=2, flush_interval=0.01, stats_interval=1000)
    await run_writer(writer, [Batch("rt_fetches", [(i,)]) for i in range(5)])
    sizes = [len(rows) for _, rows in fake_conn.calls]
    assert sum(sizes) == 5
    assert max(sizes) <= 2
