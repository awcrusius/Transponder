"""GTFS static: change detection, download, parsing, and versioned loading."""

from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import logging
import os
import posixpath
import tempfile
import zipfile
from collections.abc import AsyncIterator, Callable, Iterator
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import asyncpg
import httpx

from transponder.config import FeedConfig, ResolvedAuth
from transponder.health import ErrorKind, FetchError, Health
from transponder.keys import KeyRing, key_block_notifier, request_with_rotation

log = logging.getLogger(__name__)

STATIC_TIMEOUT = httpx.Timeout(30.0, read=600.0)
COPY_CHUNK_ROWS = 20_000


# --- work items handed to the writer -----------------------------------------

@dataclass(frozen=True)
class StaticUnchanged:
    feed_id: str
    checked_at: datetime
    reason: str  # "not-modified" (304) or "same-hash"


@dataclass(frozen=True)
class StaticLoad:
    feed_id: str
    path: Path
    sha256: str
    size_bytes: int
    etag: str | None
    last_modified: str | None
    fetched_at: datetime


# --- table specs -------------------------------------------------------------

def _text(v: str | None) -> str | None:
    v = v.strip() if v else None
    return v or None


def _text_default(default: str) -> Callable[[str | None], str]:
    return lambda v: _text(v) or default


def _int(v: str | None) -> int | None:
    v = _text(v)
    return int(v) if v is not None else None


def _float(v: str | None) -> float | None:
    v = _text(v)
    return float(v) if v is not None else None


def _date(v: str | None) -> date | None:
    v = _text(v)
    if v is None:
        return None
    return date(int(v[:4]), int(v[4:6]), int(v[6:8]))


def gtfs_time_to_secs(v: str | None) -> int | None:
    """'25:30:00' -> 91800. GTFS times may exceed 24:00:00."""
    v = _text(v)
    if v is None:
        return None
    parts = v.split(":")
    if len(parts) != 3:
        raise ValueError(f"bad GTFS time {v!r}")
    h, m, s = (int(p) for p in parts)
    return h * 3600 + m * 60 + s


@dataclass(frozen=True)
class Column:
    name: str
    parse: Callable[[str | None], Any] = _text
    source: str | None = None  # CSV column; defaults to `name`

    @property
    def csv_name(self) -> str:
        return self.source or self.name


@dataclass(frozen=True)
class StaticTable:
    file: str
    table: str
    required: bool
    columns: tuple[Column, ...]
    key: tuple[str, ...] = ()  # primary key columns; later duplicates in the file are skipped

    @property
    def copy_columns(self) -> list[str]:
        return ["feed_id", "feed_version_id", *(c.name for c in self.columns)]


STATIC_TABLES: tuple[StaticTable, ...] = (
    StaticTable("agency.txt", "gtfs_agency", True, (
        Column("agency_id", _text_default("")),
        Column("agency_name"), Column("agency_url"), Column("agency_timezone"),
        Column("agency_lang"), Column("agency_phone"), Column("agency_fare_url"), Column("agency_email"),
    ), key=("agency_id",)),
    StaticTable("routes.txt", "gtfs_routes", True, (
        Column("route_id"), Column("agency_id"), Column("route_short_name"), Column("route_long_name"),
        Column("route_desc"), Column("route_type", _int), Column("route_url"), Column("route_color"),
        Column("route_text_color"), Column("route_sort_order", _int),
    ), key=("route_id",)),
    StaticTable("stops.txt", "gtfs_stops", True, (
        Column("stop_id"), Column("stop_code"), Column("stop_name"), Column("stop_desc"),
        Column("stop_lat", _float), Column("stop_lon", _float), Column("zone_id"), Column("stop_url"),
        Column("location_type", _int), Column("parent_station"), Column("stop_timezone"),
        Column("wheelchair_boarding", _int), Column("platform_code"),
    ), key=("stop_id",)),
    StaticTable("trips.txt", "gtfs_trips", True, (
        Column("trip_id"), Column("route_id"), Column("service_id"), Column("trip_headsign"),
        Column("trip_short_name"), Column("direction_id", _int), Column("block_id"), Column("shape_id"),
        Column("wheelchair_accessible", _int), Column("bikes_allowed", _int),
    ), key=("trip_id",)),
    StaticTable("stop_times.txt", "gtfs_stop_times", True, (
        Column("trip_id"),
        Column("arrival_time"), Column("arrival_secs", gtfs_time_to_secs, source="arrival_time"),
        Column("departure_time"), Column("departure_secs", gtfs_time_to_secs, source="departure_time"),
        Column("stop_id"), Column("stop_sequence", _int), Column("stop_headsign"),
        Column("pickup_type", _int), Column("drop_off_type", _int),
        Column("shape_dist_traveled", _float), Column("timepoint", _int),
    )),
    StaticTable("calendar.txt", "gtfs_calendar", False, (
        Column("service_id"),
        Column("monday", _int), Column("tuesday", _int), Column("wednesday", _int), Column("thursday", _int),
        Column("friday", _int), Column("saturday", _int), Column("sunday", _int),
        Column("start_date", _date), Column("end_date", _date),
    ), key=("service_id",)),
    StaticTable("calendar_dates.txt", "gtfs_calendar_dates", False, (
        Column("service_id"), Column("date", _date), Column("exception_type", _int),
    ), key=("service_id", "date")),
)


# --- parsing -----------------------------------------------------------------

def zip_members(zf: zipfile.ZipFile) -> dict[str, str]:
    """Map GTFS file basename -> zip member name (handles feeds zipped inside a folder)."""
    members: dict[str, str] = {}
    for name in zf.namelist():
        base = posixpath.basename(name)
        if base.endswith(".txt") and base not in members:
            members[base] = name
    return members


def validate_members(members: dict[str, str]) -> None:
    missing = [t.file for t in STATIC_TABLES if t.required and t.file not in members]
    if missing:
        raise ValueError(f"GTFS zip is missing required files: {', '.join(missing)}")
    if "calendar.txt" not in members and "calendar_dates.txt" not in members:
        raise ValueError("GTFS zip has neither calendar.txt nor calendar_dates.txt")


def iter_rows(zf: zipfile.ZipFile, member: str, table: StaticTable, feed_id: str, version_id: int) -> Iterator[tuple]:
    """Yield COPY-ready tuples for one GTFS file. Synchronous; run off the event loop.

    Real feeds sometimes repeat a key (e.g. the same stop_id twice in stops.txt).
    For tables with a declared key the first occurrence wins and the rest are
    skipped, so one sloppy row does not abort the whole version.
    """
    names = table.copy_columns
    key_idx = [names.index(k) for k in table.key]
    seen: set[tuple] = set()
    duplicates = 0
    with zf.open(member) as raw, io.TextIOWrapper(raw, encoding="utf-8-sig", newline="") as text:
        reader = csv.DictReader(text)
        if reader.fieldnames:
            reader.fieldnames = [f.strip() for f in reader.fieldnames]
        for lineno, rec in enumerate(reader, start=2):
            try:
                row = (feed_id, version_id, *(c.parse(rec.get(c.csv_name)) for c in table.columns))
            except (ValueError, TypeError) as e:
                raise ValueError(f"{table.file} line {lineno}: {e}") from e
            if key_idx:
                key = tuple(row[i] for i in key_idx)
                if key in seen:
                    duplicates += 1
                    continue
                seen.add(key)
            yield row
    if duplicates:
        log.warning("%s: %s: skipped %d row(s) with a duplicate %s", feed_id, table.file, duplicates, "/".join(table.key))


def _chunks(it: Iterator[tuple], size: int) -> Iterator[list[tuple]]:
    chunk: list[tuple] = []
    for row in it:
        chunk.append(row)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


async def _async_records(zf: zipfile.ZipFile, member: str, table: StaticTable, feed_id: str, version_id: int) -> AsyncIterator[tuple]:
    """Parse CSV in a worker thread, one chunk at a time, yielding rows to asyncpg's COPY."""
    chunks = _chunks(iter_rows(zf, member, table, feed_id, version_id), COPY_CHUNK_ROWS)
    while True:
        chunk = await asyncio.to_thread(next, chunks, None)
        if chunk is None:
            return
        for row in chunk:
            yield row


# --- change detection + download (runs in the scheduler, network only) --------

async def check_for_update(
    feed: FeedConfig,
    keyring: KeyRing,
    client: httpx.AsyncClient,
    pool: asyncpg.Pool,
    tmp_dir: Path | None = None,
    *,
    health: Health | None = None,
) -> StaticLoad | StaticUnchanged:
    """Conditional GET against the current version's validators, then hash comparison.

    Returns a StaticLoad pointing at a temp file when the feed changed. The caller owns
    the temp file from then on. Uses the feed's key ring like the realtime pollers.

    "Changed" means the zip bytes changed. Servers that build the zip on request
    produce a new hash every time (zip headers carry timestamps) even when the
    timetable is identical, so such feeds yield one new version per check; the
    README's storage section shows how to prune superseded versions.
    """
    current = await pool.fetchrow(
        "SELECT sha256, etag, last_modified FROM feed_versions WHERE feed_id = $1 AND is_current",
        feed.feed_id,
    )
    conditional: dict[str, str] = {}
    if current:
        if current["etag"]:
            conditional["If-None-Match"] = current["etag"]
        if current["last_modified"]:
            conditional["If-Modified-Since"] = current["last_modified"]

    async def send(auth: ResolvedAuth) -> httpx.Response:
        req = client.build_request(
            "GET", feed.static_url, headers={**conditional, **auth.headers}, params=auth.params, timeout=STATIC_TIMEOUT
        )
        return await client.send(req, stream=True)

    checked_at = datetime.now(tz=timezone.utc)
    resp, _key = await request_with_rotation(keyring, send, on_block=key_block_notifier(health, keyring, "static"))
    try:
        if resp.status_code == 304:
            return StaticUnchanged(feed.feed_id, checked_at, "not-modified")
        if resp.status_code >= 400:
            snippet = (await resp.aread())[:200].decode("utf-8", errors="replace").strip()
            raise FetchError(
                ErrorKind.HTTP_ERROR,
                f"HTTP {resp.status_code} downloading {feed.static_url}: {snippet or resp.reason_phrase}",
                status=resp.status_code,
            )
        fd, name = tempfile.mkstemp(prefix=f"{feed.feed_id}-", suffix=".zip", dir=tmp_dir)
        path = Path(name)
        digest = hashlib.sha256()
        size = 0
        try:
            with os.fdopen(fd, "wb") as out:
                async for chunk in resp.aiter_bytes():
                    out.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
        except BaseException:
            path.unlink(missing_ok=True)
            raise
        etag = resp.headers.get("etag")
        last_modified = resp.headers.get("last-modified")
    finally:
        await resp.aclose()

    sha256 = digest.hexdigest()
    if current and current["sha256"] == sha256:
        path.unlink(missing_ok=True)
        return StaticUnchanged(feed.feed_id, checked_at, "same-hash")
    return StaticLoad(feed.feed_id, path, sha256, size, etag, last_modified, checked_at)


# --- loading (runs in the writer's static lane) -------------------------------

async def load_version(pool: asyncpg.Pool, item: StaticLoad) -> int:
    """Load a downloaded GTFS zip as a brand-new feed version, atomically.

    A new feed_versions row is created and every table is COPYed under that
    feed_version_id inside one transaction. Existing versions are never modified;
    on success the new version becomes current. On failure the transaction rolls
    back and no trace of the version remains.
    """
    with zipfile.ZipFile(item.path) as zf:
        members = zip_members(zf)
        validate_members(members)
        async with pool.acquire() as conn, conn.transaction():
            version_id: int = await conn.fetchval(
                """
                INSERT INTO feed_versions (feed_id, sha256, size_bytes, etag, last_modified, fetched_at)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING feed_version_id
                """,
                item.feed_id, item.sha256, item.size_bytes, item.etag, item.last_modified, item.fetched_at,
            )
            for table in STATIC_TABLES:
                member = members.get(table.file)
                if member is None:
                    continue
                status = await conn.copy_records_to_table(
                    table.table,
                    records=_async_records(zf, member, table, item.feed_id, version_id),
                    columns=table.copy_columns,
                )
                log.info("%s: %s -> %s (%s)", item.feed_id, table.file, table.table, status)
            await conn.execute(
                "UPDATE feed_versions SET is_current = false WHERE feed_id = $1 AND is_current", item.feed_id
            )
            await conn.execute(
                "UPDATE feed_versions SET is_current = true, loaded_at = now() WHERE feed_version_id = $1",
                version_id,
            )
            await conn.execute(
                "UPDATE feeds SET static_last_checked_at = $2 WHERE feed_id = $1", item.feed_id, item.fetched_at
            )
    return version_id
