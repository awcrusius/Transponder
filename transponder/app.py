"""Wire everything together and run until SIGINT/SIGTERM."""

from __future__ import annotations

import asyncio
import functools
import logging
import os
import signal
import socket

import httpx

from transponder import __version__, static
from transponder.config import Config, ConfigError, FeedConfig, Settings, load_config
from transponder.db import create_pool, run_migrations, sync_feeds
from transponder.health import ErrorKind, Health, Notifier, PushoverNotifier, WebhookNotifier
from transponder.keys import KeyRing, build_keyring
from transponder.rt import Poller
from transponder.scheduler import run_periodic, stagger
from transponder.writer import Writer

log = logging.getLogger(__name__)


def make_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers={"User-Agent": f"transponder/{__version__}"},
        follow_redirects=True,
        limits=httpx.Limits(max_connections=64, max_keepalive_connections=16),
    )


def build_keyrings(config: Config) -> dict[str, KeyRing]:
    """Load every feed's keys up front so a missing env var fails at startup."""
    return {feed.feed_id: build_keyring(feed) for feed in config.feeds}


def _required_env(name: str, what: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ConfigError(f"environment variable {name!r} ({what}) is not set")
    return value


def build_health(config: Config) -> Health:
    a = config.alerting
    notifiers: list[Notifier] = []
    if a.pushover:
        notifiers.append(PushoverNotifier(
            _required_env(a.pushover.token_env, "Pushover application token"),
            _required_env(a.pushover.user_env, "Pushover user key"),
            a.pushover.device,
        ))
    if a.webhook:
        notifiers.append(WebhookNotifier(_required_env(a.webhook.url_env, "alert webhook URL")))
    return Health(
        notifiers,
        repeat_after=a.repeat_after_seconds,
        min_consecutive_failures=a.min_consecutive_failures,
        hostname=socket.gethostname(),
    )


async def _static_job(feed: FeedConfig, keyring: KeyRing, client: httpx.AsyncClient, pool, writer: Writer, health: Health, settings: Settings) -> None:
    result = await static.check_for_update(feed, keyring, client, pool, settings.tmp_dir, health=health)
    await writer.submit_static(result)


def build_jobs(
    config: Config,
    keyrings: dict[str, KeyRing],
    client: httpx.AsyncClient,
    pool,
    writer: Writer,
    health: Health,
    settings: Settings,
) -> list[asyncio.Task]:
    tasks: list[asyncio.Task] = []
    for feed in config.feeds:
        keyring = keyrings[feed.feed_id]
        for kind, endpoint in feed.realtime.endpoints():
            interval = feed.rt_interval(endpoint)
            poller = Poller(
                feed, kind, endpoint.url, keyring, client, writer, health, config.stale_after(feed),
                dedupe_window=settings.rt_dedupe_seconds,
            )
            tasks.append(asyncio.create_task(
                run_periodic(poller.scope, interval, poller.poll, health=health, initial_delay=stagger(interval)),
                name=poller.scope,
            ))
        name = f"static-check:{feed.feed_id}"
        fn = functools.partial(_static_job, feed, keyring, client, pool, writer, health, settings)
        tasks.append(asyncio.create_task(
            run_periodic(name, feed.static_check_interval_hours * 3600, fn, health=health, initial_delay=stagger(30.0, 5.0)),
            name=name,
        ))
    return tasks


async def run(settings: Settings) -> None:
    config = load_config(settings.feeds_config)
    keyrings = build_keyrings(config)
    health = build_health(config)
    log.info("loaded %d feed(s) from %s; notifiers: %s", len(config.feeds), settings.feeds_config, ", ".join(health.notifier_names) or "log only")
    health_task = asyncio.create_task(health.run(), name="health")
    try:
        if config.alerting.notify_on_start:
            health.notify("transponder started", f"ingesting {len(config.feeds)} feed(s): {', '.join(f.feed_id for f in config.feeds)}", priority=-1)
        await _run(settings, config, keyrings, health)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.exception("transponder crashed")
        health.notify(f"crash: {ErrorKind.CRASH.label}", f"transponder exited with {type(e).__name__}: {e}", priority=1)
        raise
    finally:
        await health.stop()
        try:
            await asyncio.wait_for(health_task, timeout=20)
        except (asyncio.TimeoutError, Exception) as e:  # noqa: BLE001
            log.warning("notification delivery did not finish cleanly: %s", e)


async def _run(settings: Settings, config: Config, keyrings: dict[str, KeyRing], health: Health) -> None:
    pool = await create_pool(settings.database_url, min_size=settings.db_pool_min, max_size=settings.db_pool_max)
    try:
        applied = await run_migrations(pool, settings.migrations_dir)
        if applied:
            log.info("applied migrations: %s", ", ".join(applied))
        await sync_feeds(pool, config)

        writer = Writer(
            pool,
            max_batch_rows=settings.writer_batch_rows,
            flush_interval=settings.writer_flush_seconds,
            queue_size=settings.writer_queue_size,
            health=health,
        )
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)

        async with make_client() as client:
            writer_task = asyncio.create_task(writer.run(), name="writer")
            jobs = build_jobs(config, keyrings, client, pool, writer, health, settings)
            log.info("scheduled %d job(s); running", len(jobs))
            await stop.wait()

            log.info("shutting down: cancelling jobs")
            for job in jobs:
                job.cancel()
            await asyncio.gather(*jobs, return_exceptions=True)
            log.info("flushing writer (%d batches queued)", writer.rt_backlog)
            await writer.stop()
            await writer_task
    finally:
        await pool.close()
    log.info("stopped")


async def migrate(settings: Settings) -> list[str]:
    pool = await create_pool(settings.database_url, min_size=1, max_size=1)
    try:
        return await run_migrations(pool, settings.migrations_dir)
    finally:
        await pool.close()


async def test_alert(config: Config) -> list[str]:
    """Send one test notification through every configured notifier."""
    health = build_health(config)
    if not health.notifier_names:
        raise ConfigError("no notifiers configured: add an `alerting:` block with pushover and/or webhook to feeds.yaml")
    task = asyncio.create_task(health.run())
    health.notify("test notification", f"Alerting works. Feeds: {', '.join(f.feed_id for f in config.feeds)}", priority=0)
    await health.stop()
    await task
    return [f"{name}: {'sent' if health.sent[name] else 'FAILED (see log)'}" for name in health.notifier_names]
