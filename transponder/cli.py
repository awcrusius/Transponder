"""Command-line entry point: `transponder run | migrate | check-config | test-alert`."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys

from dotenv import load_dotenv

from transponder import app
from transponder.config import ConfigError, Settings, load_config


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        stream=sys.stdout,
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)


def main(argv: list[str] | None = None) -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(prog="transponder", description="Multi-agency GTFS ingestion into TimescaleDB")
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="run migrations, then ingest all configured feeds until stopped")
    p_run.add_argument("--config", help="path to feeds.yaml (default: $FEEDS_CONFIG or ./feeds.yaml)")

    sub.add_parser("migrate", help="apply pending migrations and exit")

    p_check = sub.add_parser("check-config", help="validate feeds.yaml, its secrets, and the alerting setup")
    p_check.add_argument("--config", help="path to feeds.yaml")

    p_alert = sub.add_parser("test-alert", help="send a test notification through the configured notifiers")
    p_alert.add_argument("--config", help="path to feeds.yaml")

    args = parser.parse_args(argv)

    try:
        config_path = getattr(args, "config", None) or os.environ.get("FEEDS_CONFIG", "feeds.yaml")
        if args.command == "check-config":
            _check_config(config_path)
            return
        if args.command == "test-alert":
            _configure_logging(os.environ.get("LOG_LEVEL", "INFO").upper())
            for line in asyncio.run(app.test_alert(load_config(config_path))):
                print(line)
            return
        settings = Settings.from_env(config_path=getattr(args, "config", None))
        _configure_logging(settings.log_level)
        if args.command == "migrate":
            applied = asyncio.run(app.migrate(settings))
            print("applied: " + (", ".join(applied) if applied else "nothing (up to date)"))
        elif args.command == "run":
            asyncio.run(app.run(settings))
    except ConfigError as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(2)


def _check_config(config_path: str) -> None:
    config = load_config(config_path)
    problems: list[str] = []
    for feed in config.feeds:
        try:
            keys = feed.auth.load_keys()
            auth_status = "none" if feed.auth.type == "none" else f"ok, {len(keys)} key(s): {', '.join(k for k, _ in keys)}"
        except ConfigError as e:
            auth_status = f"MISSING ({e})"
            problems.append(feed.feed_id)
        print(f"{feed.feed_id}: {feed.agency}")
        print(f"  static: {feed.static_url} every {feed.static_check_interval_hours}h")
        for kind, ep in feed.realtime.endpoints():
            print(f"  {kind}: {ep.url} every {feed.rt_interval(ep):g}s")
        print(f"  auth: {feed.auth.type} -> {auth_status}")
        print(f"  stale after: {config.stale_after(feed):g}s")

    a = config.alerting
    print("alerting:")
    for name, cfg, envs in (
        ("pushover", a.pushover, lambda c: [c.token_env, c.user_env]),
        ("webhook", a.webhook, lambda c: [c.url_env]),
    ):
        if cfg is None:
            print(f"  {name}: not configured")
            continue
        missing = [e for e in envs(cfg) if not os.environ.get(e)]
        print(f"  {name}: " + (f"MISSING {', '.join(missing)}" if missing else "ok"))
        if missing:
            problems.append(name)
    print(f"  transient failures before alert: {a.min_consecutive_failures}, repeat after {a.repeat_after_seconds:g}s")
    if problems:
        raise ConfigError(f"missing secrets for: {', '.join(problems)}")
    print(f"ok: {len(config.feeds)} feed(s)")
