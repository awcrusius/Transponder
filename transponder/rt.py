"""GTFS-Realtime: fetch with key rotation, decode with gtfs-realtime-bindings, detect stale feeds.

Each Poller owns one (feed, endpoint) pair. A poll fetches the protobuf, decodes
it in a worker thread into flat row tuples in the column order declared in
`tables.py`, drops rows that merely repeat what was last written (see
`dedupe.py`), hands the rest to the Writer, and records the attempt in
rt_fetches.

Row timestamps: `time` is the entity's own timestamp when the feed provides one
(vehicle.timestamp, trip_update.timestamp), otherwise the FeedMessage header
timestamp, otherwise the fetch time. `feed_timestamp` and `fetched_at` are
stored alongside so the three can always be told apart in queries.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any

import httpx
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2 as pb

from transponder import tables
from transponder.config import FeedConfig, ResolvedAuth
from transponder.dedupe import Deduper
from transponder.health import ErrorKind, FetchError, Health, classify_exception
from transponder.keys import KeyRing, key_block_notifier, request_with_rotation

if TYPE_CHECKING:
    from transponder.writer import Writer

log = logging.getLogger(__name__)

RT_TIMEOUT = httpx.Timeout(10.0, read=30.0)


@dataclass(frozen=True)
class Batch:
    """Rows destined for one realtime table, in that table's column order."""

    table: str
    rows: list[tuple]


@dataclass
class Decoded:
    batches: list[Batch] = field(default_factory=list)
    entity_count: int = 0
    feed_timestamp: datetime | None = None
    header_has_timestamp: bool = False


# --- protobuf helpers --------------------------------------------------------

def _opt(msg: Any, name: str) -> Any:
    """Value of an optional proto2 field, or None when unset."""
    return getattr(msg, name) if msg.HasField(name) else None


def _enum(wrapper: Any, msg: Any, name: str) -> str | None:
    return wrapper.Name(getattr(msg, name)) if msg.HasField(name) else None


def _ts(seconds: int | None) -> datetime | None:
    return datetime.fromtimestamp(seconds, tz=timezone.utc) if seconds else None


def _date(yyyymmdd: str | None) -> date | None:
    if not yyyymmdd or len(yyyymmdd) != 8:
        return None
    try:
        return date(int(yyyymmdd[:4]), int(yyyymmdd[4:6]), int(yyyymmdd[6:8]))
    except ValueError:
        return None


def _translation(ts: Any, lang: str = "en") -> str | None:
    if ts is None or not ts.translation:
        return None
    for t in ts.translation:
        if t.language and t.language.lower().startswith(lang):
            return t.text
    return ts.translation[0].text


def _trip_fields(trip: Any) -> tuple:
    """(trip_id, route_id, direction_id, start_date, start_time, schedule_relationship)."""
    if trip is None:
        return (None, None, None, None, None, None)
    return (
        _opt(trip, "trip_id"),
        _opt(trip, "route_id"),
        _opt(trip, "direction_id"),
        _date(_opt(trip, "start_date")),
        _opt(trip, "start_time"),
        _enum(pb.TripDescriptor.ScheduleRelationship, trip, "schedule_relationship"),
    )


# --- decoders ----------------------------------------------------------------

def decode(feed_id: str, kind: str, payload: bytes, fetched_at: datetime) -> Decoded:
    """Decode a FeedMessage into row batches. Pure function; safe to run in a thread."""
    msg = pb.FeedMessage()
    msg.ParseFromString(payload)
    header_ts = _ts(_opt(msg.header, "timestamp"))
    out = Decoded(entity_count=len(msg.entity), feed_timestamp=header_ts or fetched_at, header_has_timestamp=header_ts is not None)
    header_ts = out.feed_timestamp
    assert header_ts is not None

    if kind == "vehicle_positions":
        out.batches.append(Batch(tables.VEHICLE_POSITIONS.name, _vehicle_rows(feed_id, msg, header_ts, fetched_at)))
    elif kind == "trip_updates":
        tu_rows, stu_rows = _trip_update_rows(feed_id, msg, header_ts, fetched_at)
        out.batches.append(Batch(tables.TRIP_UPDATES.name, tu_rows))
        out.batches.append(Batch(tables.STOP_TIME_UPDATES.name, stu_rows))
    elif kind == "service_alerts":
        out.batches.append(Batch(tables.SERVICE_ALERTS.name, _alert_rows(feed_id, msg, header_ts)))
    else:
        raise ValueError(f"unknown realtime kind {kind!r}")
    return out


def _vehicle_rows(feed_id: str, msg: Any, header_ts: datetime, fetched_at: datetime) -> list[tuple]:
    rows: list[tuple] = []
    for ent in msg.entity:
        if not ent.HasField("vehicle"):
            continue
        v = ent.vehicle
        t = _ts(_opt(v, "timestamp")) or header_ts
        veh = v.vehicle if v.HasField("vehicle") else None
        pos = v.position if v.HasField("position") else None
        trip_id, route_id, direction_id, start_date, start_time, sched_rel = _trip_fields(
            v.trip if v.HasField("trip") else None
        )
        vehicle_id = (_opt(veh, "id") if veh else None) or ent.id
        rows.append((
            t, feed_id, vehicle_id, ent.id,
            _opt(veh, "label") if veh else None,
            _opt(veh, "license_plate") if veh else None,
            trip_id, route_id, direction_id, start_date, start_time, sched_rel,
            pos.latitude if pos else None,
            pos.longitude if pos else None,
            _opt(pos, "bearing") if pos else None,
            _opt(pos, "odometer") if pos else None,
            _opt(pos, "speed") if pos else None,
            _opt(v, "current_stop_sequence"),
            _opt(v, "stop_id"),
            _enum(pb.VehiclePosition.VehicleStopStatus, v, "current_status"),
            _enum(pb.VehiclePosition.CongestionLevel, v, "congestion_level"),
            _enum(pb.VehiclePosition.OccupancyStatus, v, "occupancy_status"),
            _opt(v, "occupancy_percentage"),
            header_ts, fetched_at,
        ))
    return rows


def _stop_time_event(ev: Any) -> tuple:
    """(delay, time, uncertainty) for a StopTimeEvent, or Nones."""
    if ev is None:
        return (None, None, None)
    return (_opt(ev, "delay"), _ts(_opt(ev, "time")), _opt(ev, "uncertainty"))


def _trip_update_rows(feed_id: str, msg: Any, header_ts: datetime, fetched_at: datetime) -> tuple[list[tuple], list[tuple]]:
    tu_rows: list[tuple] = []
    stu_rows: list[tuple] = []
    stu_rel = pb.TripUpdate.StopTimeUpdate.ScheduleRelationship
    for ent in msg.entity:
        if not ent.HasField("trip_update"):
            continue
        tu = ent.trip_update
        t = _ts(_opt(tu, "timestamp")) or header_ts
        trip_id, route_id, direction_id, start_date, start_time, sched_rel = _trip_fields(tu.trip)
        veh = tu.vehicle if tu.HasField("vehicle") else None
        tu_rows.append((
            t, feed_id, ent.id, trip_id, start_date, start_time, route_id, direction_id, sched_rel,
            _opt(veh, "id") if veh else None,
            _opt(veh, "label") if veh else None,
            _opt(tu, "delay"), len(tu.stop_time_update), header_ts, fetched_at,
        ))
        for stu in tu.stop_time_update:
            arr = _stop_time_event(stu.arrival if stu.HasField("arrival") else None)
            dep = _stop_time_event(stu.departure if stu.HasField("departure") else None)
            stu_rows.append((
                t, feed_id, ent.id, trip_id, start_date,
                _opt(stu, "stop_sequence"), _opt(stu, "stop_id"),
                *arr, *dep,
                _enum(stu_rel, stu, "schedule_relationship"),
                fetched_at,
            ))
    return tu_rows, stu_rows


def _alert_rows(feed_id: str, msg: Any, header_ts: datetime) -> list[tuple]:
    rows: list[tuple] = []
    for ent in msg.entity:
        if not ent.HasField("alert"):
            continue
        a = ent.alert
        alert_hash = hashlib.sha1(a.SerializeToString(deterministic=True)).hexdigest()
        active_periods = [
            {"start": _opt(p, "start"), "end": _opt(p, "end")} for p in a.active_period
        ]
        informed = [MessageToDict(e, preserving_proto_field_name=True) for e in a.informed_entity]
        rows.append((
            feed_id, ent.id, alert_hash, header_ts, header_ts,
            _enum(pb.Alert.Cause, a, "cause"),
            _enum(pb.Alert.Effect, a, "effect"),
            _enum(pb.Alert.SeverityLevel, a, "severity_level"),
            _translation(a.header_text if a.HasField("header_text") else None),
            _translation(a.description_text if a.HasField("description_text") else None),
            _translation(a.url if a.HasField("url") else None),
            active_periods, informed,
        ))
    return rows


# --- polling ---------------------------------------------------------------------

class Poller:
    """One realtime endpoint of one feed. `poll()` is the periodic job.

    Every attempt, successful or not, is recorded in rt_fetches with the key used
    and the error kind. Failures are raised as FetchError so the scheduler can
    back off and Health can alert.
    """

    def __init__(
        self,
        feed: FeedConfig,
        kind: str,
        url: str,
        keyring: KeyRing,
        client: httpx.AsyncClient,
        writer: "Writer",
        health: Health | None = None,
        stale_after: float = 600.0,
        dedupe_window: float = 3600.0,
    ) -> None:
        self.feed_id = feed.feed_id
        self.kind = kind
        self.url = url
        self.scope = f"rt:{feed.feed_id}:{kind}"
        self.keys_scope = f"keys:{feed.feed_id}"
        self._keyring = keyring
        self._client = client
        self._writer = writer
        self._health = health
        self._stale_after = stale_after
        self._on_block = key_block_notifier(health, keyring, kind)
        self._last_signature: Any = None
        self._last_change: float | None = None
        self._deduper = Deduper(dedupe_window)
        self._rows_out = 0

    def _decode_and_dedupe(self, payload: bytes, fetched_at: datetime) -> Decoded:
        """Runs in a worker thread; one call at a time per poller, so the deduper needs no lock."""
        decoded = decode(self.feed_id, self.kind, payload, fetched_at)
        now = fetched_at.timestamp()
        decoded.batches = [self._deduper.filter(b, now) for b in decoded.batches]
        return decoded

    async def _send(self, auth: ResolvedAuth) -> httpx.Response:
        return await self._client.get(self.url, headers=auth.headers, params=auth.params, timeout=RT_TIMEOUT)

    async def poll(self) -> None:
        fetched_at = datetime.now(tz=timezone.utc)
        started = time.perf_counter()
        status: int | None = None
        size: int | None = None
        key_label: str | None = None
        try:
            resp, key = await request_with_rotation(self._keyring, self._send, on_block=self._on_block)
            key_label = key.label
            status = resp.status_code
            payload = resp.content
            size = len(payload)
            if status >= 400:
                snippet = payload[:200].decode("utf-8", errors="replace").strip()
                raise FetchError(ErrorKind.HTTP_ERROR, f"HTTP {status} from {self.url}: {snippet or resp.reason_phrase}", status=status)
            try:
                decoded = await asyncio.to_thread(self._decode_and_dedupe, payload, fetched_at)
            except DecodeError as e:
                ctype = resp.headers.get("content-type", "?")
                raise FetchError(
                    ErrorKind.DECODE_FAILED,
                    f"{self.kind} body is not a GTFS-RT FeedMessage ({size} bytes, content-type {ctype}): {e}",
                    status=status,
                ) from e
        except FetchError as e:
            await self._log_fetch(fetched_at, started, status or e.status, None, size, key_label, str(e), e.kind)
            raise
        except Exception as e:
            kind = classify_exception(e)
            await self._log_fetch(fetched_at, started, status, None, size, key_label, f"{type(e).__name__}: {e}", kind)
            raise

        for batch in decoded.batches:
            if batch.rows:
                await self._writer.submit(batch)
        await self._log_fetch(fetched_at, started, status, decoded.entity_count, size, key_label, None, None)
        if self._health:
            self._health.report_ok(self.keys_scope)
            self._check_staleness(decoded, payload, fetched_at)
        kept = sum(len(b.rows) for b in decoded.batches)
        log.debug(
            "%s: %d entities, %d rows written (%d unchanged suppressed) in %.0f ms via %s",
            self.scope, decoded.entity_count, kept, sum(self._deduper.suppressed.values()) - self._rows_out,
            (time.perf_counter() - started) * 1000, key_label,
        )
        self._rows_out = sum(self._deduper.suppressed.values())

    async def _log_fetch(
        self,
        fetched_at: datetime,
        started: float,
        status: int | None,
        entity_count: int | None,
        size: int | None,
        key_label: str | None,
        error: str | None,
        kind: ErrorKind | None,
    ) -> None:
        duration_ms = int((time.perf_counter() - started) * 1000)
        await self._writer.submit(Batch(tables.RT_FETCHES.name, [(
            fetched_at, self.feed_id, self.kind, status, entity_count, size, duration_ms,
            error[:500] if error else None, kind.value if kind else None, key_label,
        )]))

    def _check_staleness(self, decoded: Decoded, payload: bytes, fetched_at: datetime) -> None:
        """Flag a feed that answers 200 but has stopped producing new data."""
        assert self._health is not None
        now = time.monotonic()
        signature: Any = decoded.feed_timestamp if decoded.header_has_timestamp else hashlib.sha1(payload).hexdigest()
        if signature != self._last_signature:
            self._last_signature = signature
            self._last_change = now
        assert self._last_change is not None
        unchanged_for = now - self._last_change
        lag = 0.0
        if decoded.header_has_timestamp and decoded.feed_timestamp is not None:
            lag = (fetched_at - decoded.feed_timestamp).total_seconds()

        if lag > self._stale_after:
            reason = f"feed timestamp {decoded.feed_timestamp:%Y-%m-%d %H:%M:%S} UTC is {lag / 60:.0f} min old"
        elif unchanged_for > self._stale_after:
            reason = f"payload unchanged for {unchanged_for / 60:.0f} min"
        else:
            self._health.set_condition(self.scope, ErrorKind.FEED_STALE, False)
            return
        self._health.set_condition(
            self.scope, ErrorKind.FEED_STALE, True,
            f"{self.feed_id} {self.kind}: no new data from the agency ({reason}, {decoded.entity_count} entities). "
            f"Requests still succeed, so the problem is on the agency's side.",
        )
