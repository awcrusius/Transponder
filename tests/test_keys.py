import httpx
import pytest

from transponder.config import FeedConfig, ResolvedAuth
from transponder.health import ErrorKind, FetchError
from transponder.keys import ApiKey, KeyRing, classify_response, parse_retry_after, quota_advice, request_with_rotation


def ring(n=3, **kw) -> KeyRing:
    return KeyRing("alpha", [ApiKey(f"K{i}", ResolvedAuth(params={"apikey": f"k{i}"})) for i in range(1, n + 1)], **kw)


def make_client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def test_round_robin_skips_benched_keys():
    r = ring(3)
    k1, k2, k3 = r.keys
    assert [r.acquire() for _ in range(4)] == [k1, k2, k3, k1]
    r.block(k3, ErrorKind.QUOTA_EXHAUSTED)
    assert [r.acquire() for _ in range(4)] == [k2, k1, k2, k1]
    k3.blocked_until = 0
    assert [r.acquire() for _ in range(3)] == [k2, k3, k1]


def test_block_and_recover(monkeypatch):
    r = ring(2, exhausted_cooldown=100)
    k1, k2 = r.keys
    assert r.acquire() is k1
    assert r.block(k1, ErrorKind.QUOTA_EXHAUSTED) == 100
    assert r.acquire() is k2
    assert "1/2 key(s) blocked: K1 (quota exhausted)" in r.status()
    assert r.block(k2, ErrorKind.AUTH_FAILED, retry_after=5) == 5
    assert r.acquire() is None
    assert 0 < r.seconds_until_available() <= 5

    # Time passes: the shortest cooldown expires first.
    for k in r.keys:
        k.blocked_until -= 10
    assert r.acquire() is k2
    assert k2.blocked_for is None


async def test_rotation_on_429_then_success():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        key = request.url.params["apikey"]
        calls.append(key)
        if key == "k1":
            return httpx.Response(429, headers={"retry-after": "120"}, text="Rate limit exceeded")
        return httpx.Response(200, content=b"ok")

    r = ring(2)
    blocked = []
    async with make_client(handler) as client:
        async def send(auth):
            return await client.get("https://x.example/rt", params=auth.params)

        resp, key = await request_with_rotation(r, send, on_block=lambda k, kind, status, cd: blocked.append((k.label, kind, status, cd)))
    assert resp.status_code == 200
    assert key.label == "K2"
    assert calls == ["k1", "k2"]
    assert blocked == [("K1", ErrorKind.QUOTA_EXHAUSTED, 429, 120.0)]

    # The benched key is skipped on the next request without a network call.
    calls.clear()
    async with make_client(handler) as client:
        async def send(auth):
            return await client.get("https://x.example/rt", params=auth.params)
        resp, key = await request_with_rotation(r, send)
    assert calls == ["k2"]


async def test_all_keys_exhausted_raises_with_retry_after():
    def handler(request):
        return httpx.Response(429)

    r = ring(2, exhausted_cooldown=600, advice="ADVICE")
    async with make_client(handler) as client:
        async def send(auth):
            return await client.get("https://x.example/rt", params=auth.params)
        with pytest.raises(FetchError) as info:
            await request_with_rotation(r, send)
    e = info.value
    assert e.kind == ErrorKind.ALL_KEYS_EXHAUSTED
    assert e.scope == "keys:alpha"
    assert 590 < e.retry_after <= 600
    assert "ADVICE" in str(e)
    assert "all 2 API key(s) exhausted" in str(e)


async def test_all_keys_invalid_is_auth_failed():
    def handler(request):
        return httpx.Response(401)

    r = ring(1)
    async with make_client(handler) as client:
        async def send(auth):
            return await client.get("https://x.example/rt", params=auth.params)
        with pytest.raises(FetchError) as info:
            await request_with_rotation(r, send)
    assert info.value.kind == ErrorKind.AUTH_FAILED
    assert "authentication failed" in str(info.value)


async def test_other_http_errors_are_returned_not_rotated():
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(503)

    r = ring(3)
    async with make_client(handler) as client:
        async def send(auth):
            return await client.get("https://x.example/rt", params=auth.params)
        resp, key = await request_with_rotation(r, send)
    assert resp.status_code == 503 and key.label == "K1" and len(calls) == 1
    assert not r.blocked()


async def test_classify_403_by_body():
    assert await classify_response(httpx.Response(403, text="Daily quota exceeded")) == ErrorKind.QUOTA_EXHAUSTED
    assert await classify_response(httpx.Response(403, text="Forbidden: invalid key")) == ErrorKind.AUTH_FAILED
    assert await classify_response(httpx.Response(429)) == ErrorKind.QUOTA_EXHAUSTED
    assert await classify_response(httpx.Response(401)) == ErrorKind.AUTH_FAILED
    assert await classify_response(httpx.Response(500)) == ErrorKind.HTTP_ERROR
    assert await classify_response(httpx.Response(200)) is None
    assert await classify_response(httpx.Response(304)) is None


def test_parse_retry_after():
    assert parse_retry_after("30") == 30.0
    assert parse_retry_after(None) is None
    assert parse_retry_after("garbage") is None
    assert parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT") == 0.0  # in the past -> clamp


def test_quota_advice_numbers():
    feed = FeedConfig(
        feed_id="alpha", agency="A", static_url="https://x.example/g.zip",
        realtime={"vehicle_positions": "https://x.example/vp", "trip_updates": "https://x.example/tu"},
        auth={"type": "query_param", "key": "apikey", "env": "K", "daily_request_limit": 1000},
        rt_poll_interval_seconds=30,
    )
    msg = quota_advice(feed, key_count=2)
    # 2 endpoints * 2880/day = 5760/day, 2880 per key; limit 1000 -> need 6 keys or >= 87s interval.
    assert "~5,760 requests/day" in msg
    assert "~2,880 per key" in msg
    assert "at least 87s" in msg
    assert "at least 6 key(s)" in msg

    feed.auth.daily_request_limit = None
    assert "Increase rt_poll_interval_seconds" in quota_advice(feed, key_count=1)
