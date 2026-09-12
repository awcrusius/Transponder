"""Feed configuration (feeds.yaml) and process settings (environment)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

RT_KINDS: tuple[str, ...] = ("vehicle_positions", "trip_updates", "service_alerts")


class ConfigError(Exception):
    """Raised for invalid configuration or missing secrets."""


@dataclass(frozen=True)
class ResolvedAuth:
    """Auth material ready to attach to an HTTP request."""

    headers: dict[str, str] = field(default_factory=dict)
    params: dict[str, str] = field(default_factory=dict)


class AuthConfig(BaseModel):
    type: Literal["none", "header", "query_param"] = "none"
    key: str | None = None
    # One env var name or a list of them. Each variable may hold several keys
    # separated by commas. Keys are used in this order and rotated on failure.
    env: str | list[str] | None = None
    # Per-key daily request limit, if the agency publishes one. Only used to
    # word the "quota exhausted" alert with a concrete suggested interval.
    daily_request_limit: int | None = Field(default=None, ge=1)
    # How long a key sits out after a quota failure (unless Retry-After says otherwise).
    exhausted_cooldown_seconds: float = Field(default=3600, ge=60)

    @model_validator(mode="after")
    def _require_key_and_env(self) -> "AuthConfig":
        if self.type != "none" and not (self.key and self.env_names):
            raise ValueError("auth.key and auth.env are required unless auth.type is 'none'")
        return self

    @property
    def env_names(self) -> list[str]:
        if self.env is None:
            return []
        return [self.env] if isinstance(self.env, str) else [e for e in self.env if e]

    def load_keys(self) -> list[tuple[str, ResolvedAuth]]:
        """Read every configured secret from the environment as (label, auth). Fails fast if any is missing."""
        if self.type == "none":
            return [("anonymous", ResolvedAuth())]
        assert self.key
        keys: list[tuple[str, ResolvedAuth]] = []
        for env in self.env_names:
            secrets = [s.strip() for s in os.environ.get(env, "").split(",") if s.strip()]
            if not secrets:
                raise ConfigError(f"environment variable {env!r} (auth for {self.key!r}) is not set")
            for i, secret in enumerate(secrets, start=1):
                label = env if len(secrets) == 1 else f"{env}[{i}]"
                auth = ResolvedAuth(headers={self.key: secret}) if self.type == "header" else ResolvedAuth(params={self.key: secret})
                keys.append((label, auth))
        return keys


def _http_url(v: str) -> str:
    if not v.startswith(("http://", "https://")):
        raise ValueError(f"not an http(s) URL: {v!r}")
    return v


class RealtimeEndpoint(BaseModel):
    url: str
    poll_interval_seconds: float | None = Field(default=None, ge=1)

    _url = field_validator("url")(_http_url)


class RealtimeConfig(BaseModel):
    vehicle_positions: RealtimeEndpoint | None = None
    trip_updates: RealtimeEndpoint | None = None
    service_alerts: RealtimeEndpoint | None = None

    @model_validator(mode="before")
    @classmethod
    def _coerce_bare_urls(cls, data: Any) -> Any:
        if isinstance(data, dict):
            return {k: ({"url": v} if isinstance(v, str) else v) for k, v in data.items()}
        return data

    def endpoints(self) -> list[tuple[str, RealtimeEndpoint]]:
        return [(kind, ep) for kind in RT_KINDS if (ep := getattr(self, kind)) is not None]


class FeedConfig(BaseModel):
    feed_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]*$", max_length=64)
    agency: str = Field(min_length=1)
    static_url: str
    realtime: RealtimeConfig = Field(default_factory=RealtimeConfig)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    rt_poll_interval_seconds: float = Field(default=30, ge=1)
    static_check_interval_hours: float = Field(default=24, gt=0)
    # Override alerting.stale_after_seconds for this feed.
    stale_after_seconds: float | None = Field(default=None, ge=30)

    _url = field_validator("static_url")(_http_url)

    def rt_interval(self, endpoint: RealtimeEndpoint) -> float:
        return endpoint.poll_interval_seconds or self.rt_poll_interval_seconds


class PushoverConfig(BaseModel):
    token_env: str = "PUSHOVER_TOKEN"
    user_env: str = "PUSHOVER_USER"
    device: str | None = None


class WebhookConfig(BaseModel):
    url_env: str = "ALERT_WEBHOOK_URL"


class AlertingConfig(BaseModel):
    pushover: PushoverConfig | None = None
    webhook: WebhookConfig | None = None
    # A realtime endpoint whose data has not changed for this long is reported stale.
    stale_after_seconds: float = Field(default=600, ge=30)
    # Re-notify an ongoing problem at most this often.
    repeat_after_seconds: float = Field(default=3600, ge=60)
    # Network/HTTP/decode blips must happen this many times in a row before alerting.
    min_consecutive_failures: int = Field(default=3, ge=1)
    notify_on_start: bool = False


class Config(BaseModel):
    feeds: list[FeedConfig] = Field(min_length=1)
    alerting: AlertingConfig = Field(default_factory=AlertingConfig)

    @model_validator(mode="after")
    def _unique_feed_ids(self) -> "Config":
        seen: set[str] = set()
        for feed in self.feeds:
            if feed.feed_id in seen:
                raise ValueError(f"duplicate feed_id {feed.feed_id!r}")
            seen.add(feed.feed_id)
        return self

    def stale_after(self, feed: FeedConfig) -> float:
        return feed.stale_after_seconds or self.alerting.stale_after_seconds


def load_config(path: str | Path) -> Config:
    path = Path(path)
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError as e:
        raise ConfigError(f"config file not found: {path}") from e
    except yaml.YAMLError as e:
        raise ConfigError(f"invalid YAML in {path}: {e}") from e
    if not isinstance(raw, dict):
        raise ConfigError(f"{path}: top level must be a mapping with a 'feeds' key")
    try:
        return Config.model_validate(raw)
    except ValueError as e:
        raise ConfigError(f"{path}: {e}") from e


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


@dataclass(frozen=True)
class Settings:
    """Process-level settings read from the environment."""

    database_url: str
    feeds_config: Path
    migrations_dir: Path
    log_level: str = "INFO"
    db_pool_min: int = 1
    db_pool_max: int = 4
    writer_batch_rows: int = 5000
    writer_flush_seconds: float = 1.0
    writer_queue_size: int = 2000
    # A realtime row identical to one written in the last this-many seconds is
    # not written again (see transponder.dedupe). 0 disables.
    rt_dedupe_seconds: float = 3600.0
    tmp_dir: Path | None = None

    @classmethod
    def from_env(cls, *, config_path: str | None = None, require_db: bool = True) -> "Settings":
        dsn = os.environ.get("DATABASE_URL", "")
        if require_db and not dsn:
            raise ConfigError("DATABASE_URL is not set")
        default_migrations = Path(__file__).resolve().parents[1] / "migrations"
        tmp = os.environ.get("TRANSPONDER_TMP_DIR")
        return cls(
            database_url=dsn,
            feeds_config=Path(config_path or os.environ.get("FEEDS_CONFIG", "feeds.yaml")),
            migrations_dir=Path(os.environ.get("MIGRATIONS_DIR", default_migrations)),
            log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
            db_pool_min=_env_int("DB_POOL_MIN", 1),
            db_pool_max=_env_int("DB_POOL_MAX", 4),
            writer_batch_rows=_env_int("WRITER_BATCH_ROWS", 5000),
            writer_flush_seconds=_env_float("WRITER_FLUSH_SECONDS", 1.0),
            writer_queue_size=_env_int("WRITER_QUEUE_SIZE", 2000),
            rt_dedupe_seconds=_env_float("RT_DEDUPE_SECONDS", 3600.0),
            tmp_dir=Path(tmp) if tmp else None,
        )
