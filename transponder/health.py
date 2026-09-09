"""Failure classification, alert de-duplication, and notification delivery.

Anything that can go wrong reports here with a *scope* (what is failing, e.g.
"rt:translink:vehicle_positions") and an ErrorKind. Health decides whether the
event is worth a notification: transient kinds must fail several times in a row,
ongoing problems are repeated at most every `repeat_after` seconds, and a scope
that reports ok again triggers a recovery message.

Notifications are queued and delivered by a single sender task so that a slow
Pushover call never blocks a poller.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import time
from collections import Counter
from dataclasses import dataclass
from enum import Enum
from typing import Protocol

import asyncpg
import httpx

log = logging.getLogger(__name__)


class ErrorKind(str, Enum):
    QUOTA_EXHAUSTED = "quota_exhausted"        # one API key hit its rate/daily limit
    ALL_KEYS_EXHAUSTED = "all_keys_exhausted"  # every configured key is blocked
    AUTH_FAILED = "auth_failed"                # 401/403 not attributable to quota
    HTTP_ERROR = "http_error"                  # any other 4xx/5xx
    NETWORK = "network"                        # DNS, connect, TLS, timeout
    DECODE_FAILED = "decode_failed"            # body is not a GTFS-RT FeedMessage
    FEED_STALE = "feed_stale"                  # HTTP 200 but no new data from the agency
    STATIC_LOAD_FAILED = "static_load_failed"
    DB_UNAVAILABLE = "db_unavailable"
    ROWS_DROPPED = "rows_dropped"
    WRITER_BACKLOG = "writer_backlog"
    CRASH = "crash"
    OTHER = "other"

    @property
    def label(self) -> str:
        return self.value.replace("_", " ")


# Kinds that only fire after `min_consecutive_failures` in a row.
TRANSIENT = frozenset({ErrorKind.NETWORK, ErrorKind.HTTP_ERROR, ErrorKind.DECODE_FAILED, ErrorKind.OTHER})
# Kinds delivered with Pushover priority 1 (bypasses quiet hours).
HIGH_PRIORITY = frozenset({ErrorKind.ALL_KEYS_EXHAUSTED, ErrorKind.AUTH_FAILED, ErrorKind.DB_UNAVAILABLE, ErrorKind.CRASH})


class FetchError(Exception):
    """A classified fetch failure. `retry_after` asks the scheduler to wait at least that long."""

    def __init__(
        self,
        kind: ErrorKind,
        message: str,
        *,
        status: int | None = None,
        retry_after: float | None = None,
        scope: str | None = None,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.status = status
        self.retry_after = retry_after
        self.scope = scope


def classify_exception(e: BaseException) -> ErrorKind:
    if isinstance(e, FetchError):
        return e.kind
    if isinstance(e, httpx.HTTPStatusError):
        return ErrorKind.HTTP_ERROR
    if isinstance(e, (httpx.TransportError, OSError, asyncio.TimeoutError)):
        return ErrorKind.NETWORK
    if isinstance(e, (asyncpg.PostgresConnectionError, asyncpg.InterfaceError)):
        return ErrorKind.DB_UNAVAILABLE
    return ErrorKind.OTHER


# --- notifiers -----------------------------------------------------------------

@dataclass(frozen=True)
class Notification:
    title: str
    message: str
    priority: int = 0  # Pushover scale: -1 quiet, 0 normal, 1 high


class Notifier(Protocol):
    name: str

    async def send(self, client: httpx.AsyncClient, n: Notification) -> None: ...


class PushoverNotifier:
    name = "pushover"
    URL = "https://api.pushover.net/1/messages.json"

    def __init__(self, token: str, user: str, device: str | None = None) -> None:
        self._token = token
        self._user = user
        self._device = device

    async def send(self, client: httpx.AsyncClient, n: Notification) -> None:
        data = {
            "token": self._token,
            "user": self._user,
            "title": n.title[:250],
            "message": n.message[:1024],
            "priority": n.priority,
        }
        if self._device:
            data["device"] = self._device
        resp = await client.post(self.URL, data=data)
        resp.raise_for_status()


class WebhookNotifier:
    """POSTs {"title", "message", "priority"} as JSON. Works with most chat-ops relays."""

    name = "webhook"

    def __init__(self, url: str) -> None:
        self._url = url

    async def send(self, client: httpx.AsyncClient, n: Notification) -> None:
        resp = await client.post(self._url, json={"title": n.title, "message": n.message, "priority": n.priority})
        resp.raise_for_status()


# --- health tracker -----------------------------------------------------------

@dataclass
class Issue:
    scope: str
    kind: ErrorKind
    message: str
    first_at: float
    count: int = 0
    notified_at: float | None = None
    condition: bool = False  # set via set_condition(); cleared only by set_condition(False)


class Health:
    def __init__(
        self,
        notifiers: list[Notifier] | None = None,
        *,
        repeat_after: float = 3600.0,
        min_consecutive_failures: int = 3,
        hostname: str | None = None,
    ) -> None:
        self._notifiers = list(notifiers or [])
        self._repeat_after = repeat_after
        self._min_failures = max(1, min_consecutive_failures)
        self._prefix = f"[{hostname or socket.gethostname()}] "
        self._issues: dict[tuple[str, ErrorKind], Issue] = {}
        self._notices: dict[tuple[str, ErrorKind], float] = {}
        self._queue: asyncio.Queue[Notification | None] = asyncio.Queue()
        self.sent: Counter[str] = Counter()

    @property
    def notifier_names(self) -> list[str]:
        return [n.name for n in self._notifiers]

    def active_issues(self) -> list[Issue]:
        return list(self._issues.values())

    # --- reporting API (sync, callable from any coroutine) ----------------------

    def report_failure(self, scope: str, kind: ErrorKind, message: str) -> None:
        """A failure of `scope`. Cleared by report_ok(scope)."""
        self._touch(scope, kind, message, condition=False)

    def report_ok(self, scope: str) -> None:
        """`scope` succeeded: clear its failures and announce recovery for any that were notified."""
        for key in [k for k, v in self._issues.items() if k[0] == scope and not v.condition]:
            self._clear(key)

    def set_condition(self, scope: str, kind: ErrorKind, active: bool, message: str = "") -> None:
        """A state that is either active or not (stale feed, backlog). Unaffected by report_ok."""
        if active:
            self._touch(scope, kind, message, condition=True)
        else:
            self._clear((scope, kind))

    def notice(self, scope: str, kind: ErrorKind, message: str, *, priority: int | None = None) -> None:
        """A one-off event with no recovery, throttled to one per repeat_after per (scope, kind)."""
        now = time.monotonic()
        last = self._notices.get((scope, kind))
        log.warning("%s: %s: %s", scope, kind.label, message)
        if last is not None and now - last < self._repeat_after:
            return
        self._notices[(scope, kind)] = now
        self._enqueue(Notification(f"{self._prefix}{scope}: {kind.label}", message, self._priority(kind, priority)))

    def notify(self, title: str, message: str, *, priority: int = 0) -> None:
        """Send directly, no de-duplication (startup, crash, test)."""
        self._enqueue(Notification(f"{self._prefix}{title}", message, priority))

    # --- internals ----------------------------------------------------------------

    def _touch(self, scope: str, kind: ErrorKind, message: str, *, condition: bool) -> None:
        now = time.monotonic()
        key = (scope, kind)
        issue = self._issues.get(key)
        if issue is None:
            issue = self._issues[key] = Issue(scope, kind, message, first_at=now, condition=condition)
        issue.count += 1
        issue.message = message
        threshold = self._min_failures if kind in TRANSIENT else 1
        if issue.notified_at is None:
            if issue.count >= threshold:
                self._send_issue(issue, now, repeat=False)
            else:
                log.warning("%s: %s (%d/%d before alerting): %s", scope, kind.label, issue.count, threshold, message)
        elif now - issue.notified_at >= self._repeat_after:
            self._send_issue(issue, now, repeat=True)
        else:
            log.warning("%s: %s (ongoing, %d occurrences): %s", scope, kind.label, issue.count, message)

    def _clear(self, key: tuple[str, ErrorKind]) -> None:
        issue = self._issues.pop(key, None)
        if issue is None:
            return
        duration = time.monotonic() - issue.first_at
        if issue.notified_at is None:
            log.info("%s: %s cleared after %d occurrence(s)", issue.scope, issue.kind.label, issue.count)
            return
        log.info("%s: recovered from %s after %.0f min", issue.scope, issue.kind.label, duration / 60)
        self._enqueue(Notification(
            f"{self._prefix}{issue.scope}: recovered from {issue.kind.label}",
            f"OK again after {_fmt_duration(duration)} and {issue.count} failure(s). Last error: {issue.message}",
            -1,
        ))

    def _send_issue(self, issue: Issue, now: float, *, repeat: bool) -> None:
        issue.notified_at = now
        title = f"{self._prefix}{issue.scope}: {issue.kind.label}" + (" (still)" if repeat else "")
        body = issue.message
        if repeat:
            body += f"\n\nOngoing for {_fmt_duration(now - issue.first_at)}, {issue.count} occurrence(s)."
        log.error("%s: %s: %s", issue.scope, issue.kind.label, issue.message)
        self._enqueue(Notification(title, body, self._priority(issue.kind)))

    @staticmethod
    def _priority(kind: ErrorKind, override: int | None = None) -> int:
        if override is not None:
            return override
        return 1 if kind in HIGH_PRIORITY else 0

    def _enqueue(self, n: Notification) -> None:
        if not self._notifiers:
            return
        self._queue.put_nowait(n)

    # --- delivery -------------------------------------------------------------------

    async def run(self) -> None:
        """Deliver queued notifications until stop() is called."""
        async with httpx.AsyncClient(timeout=15.0) as client:
            while True:
                n = await self._queue.get()
                if n is None:
                    return
                for notifier in self._notifiers:
                    try:
                        await notifier.send(client, n)
                        self.sent[notifier.name] += 1
                    except Exception as e:  # noqa: BLE001
                        log.error("%s delivery failed for %r: %s: %s", notifier.name, n.title, type(e).__name__, e)

    async def stop(self) -> None:
        await self._queue.put(None)


def _fmt_duration(seconds: float) -> str:
    if seconds < 90:
        return f"{seconds:.0f}s"
    if seconds < 5400:
        return f"{seconds / 60:.0f} min"
    return f"{seconds / 3600:.1f} h"
