"""API key pools with rotation on quota and authentication failures.

A feed has one KeyRing shared by all of its endpoints (quotas are per key, not
per endpoint). Keys are used round-robin: every request takes the next usable
key, so the load is spread evenly and each key's quota lasts the whole day.
A key that is benched (quota or auth failure) is skipped until its cooldown ends.
"""

from __future__ import annotations

import email.utils
import logging
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx

from transponder.config import FeedConfig, ResolvedAuth
from transponder.health import ErrorKind, FetchError, Health

log = logging.getLogger(__name__)

INVALID_KEY_COOLDOWN = 6 * 3600.0
QUOTA_WORDS = ("rate limit", "ratelimit", "rate-limit", "quota", "too many", "exceeded", "throttl")

SendFn = Callable[[ResolvedAuth], Awaitable[httpx.Response]]
BlockCallback = Callable[["ApiKey", ErrorKind, int, float], None]


@dataclass
class ApiKey:
    label: str
    auth: ResolvedAuth
    blocked_until: float = 0.0  # monotonic clock
    blocked_for: ErrorKind | None = None

    def usable(self, now: float) -> bool:
        return now >= self.blocked_until


class KeyRing:
    def __init__(
        self,
        feed_id: str,
        keys: list[ApiKey],
        *,
        exhausted_cooldown: float = 3600.0,
        invalid_cooldown: float = INVALID_KEY_COOLDOWN,
        advice: str = "",
    ) -> None:
        if not keys:
            raise ValueError("a KeyRing needs at least one key")
        self.feed_id = feed_id
        self.keys = keys
        self.advice = advice
        self._exhausted_cooldown = exhausted_cooldown
        self._invalid_cooldown = invalid_cooldown
        self._next = 0

    @property
    def size(self) -> int:
        return len(self.keys)

    def acquire(self) -> ApiKey | None:
        """Next usable key in round-robin order, or None if every key is benched."""
        now = time.monotonic()
        for offset in range(self.size):
            idx = (self._next + offset) % self.size
            key = self.keys[idx]
            if key.usable(now):
                if key.blocked_for is not None:
                    log.info("%s: API key %s back in service", self.feed_id, key.label)
                    key.blocked_for = None
                self._next = (idx + 1) % self.size
                return key
        return None

    def block(self, key: ApiKey, kind: ErrorKind, retry_after: float | None = None) -> float:
        """Bench a key. Returns the cooldown applied, in seconds."""
        default = self._exhausted_cooldown if kind == ErrorKind.QUOTA_EXHAUSTED else self._invalid_cooldown
        cooldown = retry_after if retry_after and retry_after > 0 else default
        key.blocked_until = time.monotonic() + cooldown
        key.blocked_for = kind
        return cooldown

    def seconds_until_available(self) -> float:
        now = time.monotonic()
        return max(0.0, min(k.blocked_until for k in self.keys) - now)

    def blocked(self) -> list[ApiKey]:
        now = time.monotonic()
        return [k for k in self.keys if not k.usable(now)]

    def status(self) -> str:
        blocked = self.blocked()
        if not blocked:
            return f"all {self.size} key(s) usable"
        parts = ", ".join(f"{k.label} ({k.blocked_for.label if k.blocked_for else 'blocked'})" for k in blocked)
        return f"{len(blocked)}/{self.size} key(s) blocked: {parts}"


def build_keyring(feed: FeedConfig) -> KeyRing:
    keys = [ApiKey(label, auth) for label, auth in feed.auth.load_keys()]
    return KeyRing(
        feed.feed_id,
        keys,
        exhausted_cooldown=feed.auth.exhausted_cooldown_seconds,
        advice=quota_advice(feed, len(keys)),
    )


def quota_advice(feed: FeedConfig, key_count: int) -> str:
    """Human-readable sizing hint for the 'API maxed out' alert."""
    endpoints = feed.realtime.endpoints()
    if not endpoints:
        return ""
    per_day = sum(86400 / feed.rt_interval(ep) for _, ep in endpoints)
    per_key = per_day / key_count
    msg = (
        f"Current polling makes ~{per_day:,.0f} requests/day across {len(endpoints)} endpoint(s), "
        f"~{per_key:,.0f} per key with {key_count} key(s)."
    )
    limit = feed.auth.daily_request_limit
    if limit:
        needed_keys = math.ceil(per_day / limit)
        interval = math.ceil(len(endpoints) * 86400 / (key_count * limit))
        msg += (
            f" With a limit of {limit:,}/key/day: raise every poll interval to at least {interval}s, "
            f"or configure at least {needed_keys} key(s) at the current intervals."
        )
    else:
        msg += " Increase rt_poll_interval_seconds (or per-endpoint poll_interval_seconds), or add keys under auth.env."
    return msg


def parse_retry_after(value: str | None) -> float | None:
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, (when - datetime.now(tz=timezone.utc)).total_seconds())


async def classify_response(resp: httpx.Response) -> ErrorKind | None:
    """None for a usable response, else the failure kind. Reads the body for 403s."""
    status = resp.status_code
    if status < 400:
        return None
    if status == 429:
        return ErrorKind.QUOTA_EXHAUSTED
    if status == 401:
        return ErrorKind.AUTH_FAILED
    if status == 403:
        body = (await resp.aread())[:2048].decode("utf-8", errors="replace").lower()
        return ErrorKind.QUOTA_EXHAUSTED if any(w in body for w in QUOTA_WORDS) else ErrorKind.AUTH_FAILED
    return ErrorKind.HTTP_ERROR


async def request_with_rotation(
    keyring: KeyRing,
    send: SendFn,
    *,
    on_block: BlockCallback | None = None,
) -> tuple[httpx.Response, ApiKey]:
    """Call `send(auth)` with the first usable key, moving to the next key on quota/auth failure.

    Returns the response (which the caller owns and must close if streamed) and
    the key that produced it. Non-auth HTTP errors are returned, not raised.
    Raises FetchError(ALL_KEYS_EXHAUSTED or AUTH_FAILED) when no key is usable.
    """
    attempts = 0
    while attempts < keyring.size and (key := keyring.acquire()) is not None:
        attempts += 1
        resp = await send(key.auth)
        kind = await classify_response(resp)
        if kind not in (ErrorKind.QUOTA_EXHAUSTED, ErrorKind.AUTH_FAILED):
            return resp, key
        retry_after = parse_retry_after(resp.headers.get("retry-after"))
        status = resp.status_code
        await resp.aclose()
        cooldown = keyring.block(key, kind, retry_after)
        log.warning("%s: key %s blocked for %.0fs after HTTP %d (%s)", keyring.feed_id, key.label, cooldown, status, kind.label)
        if on_block:
            on_block(key, kind, status, cooldown)

    wait = keyring.seconds_until_available()
    reasons = {k.blocked_for for k in keyring.keys if k.blocked_for}
    if ErrorKind.QUOTA_EXHAUSTED in reasons:
        kind = ErrorKind.ALL_KEYS_EXHAUSTED
        message = f"{keyring.feed_id}: all {keyring.size} API key(s) exhausted; next key usable in {wait / 60:.0f} min. {keyring.advice}"
    else:
        kind = ErrorKind.AUTH_FAILED
        message = f"{keyring.feed_id}: authentication failed for all {keyring.size} API key(s); retrying in {wait / 60:.0f} min. Check the key(s) and auth.type/key in feeds.yaml."
    raise FetchError(kind, message.strip(), retry_after=wait, scope=f"keys:{keyring.feed_id}")


def key_block_notifier(health: Health | None, keyring: KeyRing, endpoint: str) -> BlockCallback | None:
    """Callback for request_with_rotation that raises a notice whenever a key is benched.

    Single-key rings get no per-key notice: the all-keys failure that follows
    immediately says the same thing with the right priority.
    """
    if health is None or keyring.size < 2:
        return None

    def on_block(key: ApiKey, kind: ErrorKind, status: int, cooldown: float) -> None:
        msg = (
            f"{keyring.feed_id}: API key {key.label} benched for {cooldown / 60:.0f} min after HTTP {status} "
            f"({kind.label}) on {endpoint}. Now {keyring.status()}."
        )
        if kind == ErrorKind.QUOTA_EXHAUSTED and keyring.advice:
            msg += " " + keyring.advice
        health.notice(f"keys:{keyring.feed_id}:{key.label}", kind, msg, priority=0)

    return on_block
