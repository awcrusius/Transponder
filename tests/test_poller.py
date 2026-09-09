"""Poller behaviour end to end with a mock HTTP transport: rotation, error classes, staleness."""

import asyncio
from datetime import datetime, timezone

import httpx
import pytest
from google.transit import gtfs_realtime_pb2 as pb

from transponder.config import FeedConfig, ResolvedAuth
from transponder.health import ErrorKind, FetchError, Health
from transponder.keys import ApiKey, KeyRing
from transponder.rt import Poller


class FakeWriter:
    def __init__(self):
        self.batches = []

    async def submit(self, batch):
        self.batches.append(batch)

    def fetch_rows(self):
        return [r for b in self.batches if b.table == "rt_fetches" for r in b.rows]


def now_ts() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp())


def payload(ts: int | None, n: int = 1) -> bytes:
    m = pb.FeedMessage()
    m.header.gtfs_realtime_version = "2.0"
    if ts is not None:
        m.header.timestamp = ts
    for i in range(n):
        e = m.entity.add()
        e.id = str(i)
        e.vehicle.vehicle.id = f"bus-{i}"
        e.vehicle.position.latitude = 1.0
        e.vehicle.position.longitude = 2.0
    return m.SerializeToString()


FEED = FeedConfig(feed_id="alpha", agency="A", static_url="https://x.example/g.zip",
                  realtime={"vehicle_positions": "https://x.example/vp"})


def make_poller(handler, keys=2, health=None, stale_after=600.0):
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    ring = KeyRing("alpha", [ApiKey(f"K{i}", ResolvedAuth(params={"apikey": f"k{i}"})) for i in range(1, keys + 1)])
    writer = FakeWriter()
    poller = Poller(FEED, "vehicle_positions", "https://x.example/vp", ring, client, writer, health, stale_after)
    return poller, writer, ring


async def test_success_logs_key_and_writes_rows():
    def handler(request):
        return httpx.Response(200, content=payload(now_ts(), n=3))

    poller, writer, _ = make_poller(handler)
    await poller.poll()
    (vp,) = [b for b in writer.batches if b.table == "vehicle_positions"]
    assert len(vp.rows) == 3
    (fetch,) = writer.fetch_rows()
    assert fetch[3] == 200 and fetch[4] == 3 and fetch[7] is None and fetch[8] is None and fetch[9] == "K1"


async def test_quota_rotates_and_logs_second_key():
    def handler(request):
        if request.url.params["apikey"] == "k1":
            return httpx.Response(429, text="Rate limit exceeded")
        return httpx.Response(200, content=payload(now_ts()))

    health = Health([], hostname="h")
    poller, writer, ring = make_poller(handler, health=health)
    await poller.poll()
    assert [k.label for k in ring.blocked()] == ["K1"]
    assert writer.fetch_rows()[0][9] == "K2"
    assert health.active_issues() == []


async def test_all_keys_exhausted_is_logged_and_raised():
    def handler(request):
        return httpx.Response(429)

    poller, writer, _ = make_poller(handler, keys=2)
    with pytest.raises(FetchError) as info:
        await poller.poll()
    assert info.value.kind == ErrorKind.ALL_KEYS_EXHAUSTED
    (fetch,) = writer.fetch_rows()
    assert fetch[8] == "all_keys_exhausted" and fetch[3] is None


async def test_http_error_and_decode_error_classified():
    responses = iter([httpx.Response(503, text="upstream down"), httpx.Response(200, content=b"<html>not protobuf</html>")])

    def handler(request):
        return next(responses)

    poller, writer, _ = make_poller(handler)
    with pytest.raises(FetchError) as info:
        await poller.poll()
    assert info.value.kind == ErrorKind.HTTP_ERROR and "upstream down" in str(info.value)
    with pytest.raises(FetchError) as info:
        await poller.poll()
    assert info.value.kind == ErrorKind.DECODE_FAILED
    assert [r[8] for r in writer.fetch_rows()] == ["http_error", "decode_failed"]


async def test_network_error_classified():
    def handler(request):
        raise httpx.ConnectError("dns failure")

    poller, writer, _ = make_poller(handler)
    with pytest.raises(httpx.ConnectError):
        await poller.poll()
    assert writer.fetch_rows()[0][8] == "network"


async def test_stale_feed_without_header_timestamp_uses_payload_hash():
    body = {"n": 1}

    def handler(request):
        return httpx.Response(200, content=payload(None, n=body["n"]))

    health = Health([], hostname="h")
    poller, writer, _ = make_poller(handler, health=health, stale_after=0.3)
    await poller.poll()
    assert health.active_issues() == []
    await asyncio.sleep(0.4)
    await poller.poll()  # identical bytes for longer than stale_after
    (issue,) = health.active_issues()
    assert issue.kind == ErrorKind.FEED_STALE and "payload unchanged" in issue.message
    body["n"] = 2
    await poller.poll()  # different payload: cleared
    assert health.active_issues() == []


async def test_stale_feed_by_old_header_timestamp():
    ts = {"v": now_ts() - 3600}

    def handler(request):
        return httpx.Response(200, content=payload(ts["v"]))

    health = Health([], hostname="h")
    poller, writer, _ = make_poller(handler, health=health, stale_after=600)
    await poller.poll()
    (issue,) = health.active_issues()
    assert issue.kind == ErrorKind.FEED_STALE and "60 min old" in issue.message
    ts["v"] = now_ts()
    await poller.poll()
    assert health.active_issues() == []
