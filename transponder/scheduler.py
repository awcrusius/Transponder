"""A tiny asyncio scheduler: one coroutine per periodic job, no threads."""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable

from transponder.health import Health, classify_exception

log = logging.getLogger(__name__)


async def run_periodic(
    name: str,
    interval: float,
    fn: Callable[[], Awaitable[None]],
    *,
    health: Health | None = None,
    initial_delay: float = 0.0,
    max_backoff: float = 600.0,
) -> None:
    """Run `fn` every `interval` seconds (measured start-to-start).

    Failures are logged, reported to Health, and trigger exponential backoff
    capped at max(interval, max_backoff). An exception carrying `retry_after`
    (e.g. every API key exhausted) extends the wait to at least that long.
    """
    await asyncio.sleep(initial_delay)
    failures = 0
    loop = asyncio.get_running_loop()
    while True:
        started = loop.time()
        retry_after: float | None = None
        try:
            await fn()
            failures = 0
            if health:
                health.report_ok(name)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            failures += 1
            kind = classify_exception(e)
            retry_after = getattr(e, "retry_after", None)
            log.warning("%s failed (%d in a row, %s): %s: %s", name, failures, kind.label, type(e).__name__, e)
            if health:
                scope = getattr(e, "scope", None)
                # Key-level failures carry their own scope and already name the feed.
                health.report_failure(scope or name, kind, str(e) if scope else f"{name}: {e}")
        if failures:
            delay = min(interval * (2 ** failures), max(interval, max_backoff))
        else:
            delay = interval
        if retry_after:
            delay = max(delay, retry_after)
        elapsed = loop.time() - started
        await asyncio.sleep(max(0.0, delay - elapsed))


def stagger(interval: float, cap: float = 15.0) -> float:
    """Random initial delay so many jobs with the same interval do not fire together."""
    return random.uniform(0.0, min(interval, cap))
