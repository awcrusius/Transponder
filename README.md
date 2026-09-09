# Transponder

A config-driven, multi-agency GTFS ingestion pipeline that writes to TimescaleDB.

Point it at any number of GTFS feeds in `feeds.yaml` and it will:

- poll each agency's GTFS-Realtime endpoints (vehicle positions, trip updates,
  service alerts) on their own intervals and store every update as time-series rows;
- periodically check each agency's GTFS static bundle for changes (conditional GET,
  then content hash) and load new versions side by side, never overwriting old ones;
- scope every row by `feed_id` (plus `feed_version_id` for static data), because
  GTFS identifiers are only unique within one feed.

It also watches itself: every failure mode is classified, de-duplicated, and
pushed to Pushover or a webhook, and each feed can carry several API keys that
rotate automatically when one runs out of quota.

There is no web UI or API layer yet. The deliverable is the ingestion service, the
schema, and a Docker Compose setup for local development.

## Background

Transponder succeeds an earlier single-agency prototype,
[TransLink-data-analysis](https://github.com/awcrusius/TransLink-data-analysis),
which collected TransLink (Metro Vancouver) realtime data with a fixed set of
scripts and a schema keyed only on GTFS IDs. That worked for one agency but
could not hold data from several without ID collisions, had no notion of static
feed versions, and polled each endpoint in its own process. Transponder keeps the
same goal (a durable archive of realtime transit data for analysis) and rebuilds
the collection layer so adding an agency is a YAML entry rather than a fork.

## How it works

```
feeds.yaml ──► scheduler (asyncio, one coroutine per feed endpoint)
                  │
                  ├─ rt.poll ──── fetch + decode (gtfs-realtime-bindings) ──┐
                  │                                                         │  Batch(table, rows)
                  └─ static.check_for_update ── conditional GET / hash ──┐  │
                                                                         ▼  ▼
                                                     Writer  (bounded asyncio queues)
                                                       ├─ rt lane: coalesce per table, batched INSERT ... ON CONFLICT
                                                       └─ static lane: one version at a time, COPY inside a transaction
                                                                         │
                                                                         ▼
                                                               TimescaleDB (asyncpg pool)
```

**Scheduling.** A plain asyncio loop. Every `(feed, endpoint)` pair and every feed's
static check is one coroutine driven by `run_periodic`, so hundreds of feeds poll
concurrently on a single thread. Start times are jittered so identical intervals do
not fire in lockstep, and a failing endpoint backs off exponentially without
affecting the others.

**Writes.** Fetching and decoding run concurrently, but nothing writes to the
database directly. Rows go onto a bounded queue consumed by the `Writer`, which
runs exactly two tasks: an *rt lane* that drains the queue, groups rows by table,
and issues one batched `INSERT ... ON CONFLICT` transaction per flush; and a
*static lane* that loads one GTFS version at a time with `COPY`. Hypertables are
therefore never written concurrently, and a multi-minute static load cannot stall
realtime writes. If the queue fills, pollers block on `submit()`, which is the
intended backpressure.

**Static versioning.** Each check sends `If-None-Match` / `If-Modified-Since`
from the current version. A 304 ends the check. On 200 the zip is streamed to a
temp file while its SHA-256 is computed; an unchanged hash also ends the check
(some servers ignore conditional headers). Otherwise a new `feed_versions` row is
created and every table is COPYed under that `feed_version_id` in one transaction.
On success the new version becomes `is_current`; on failure the transaction rolls
back and nothing of the version remains. Old versions are kept. Real feeds are
not always clean (BC Transit ships a duplicated `stop_id`, for example), so for
keyed tables the first row per key wins and the duplicates are counted in a
warning rather than failing the load.

**Deduplication.** Realtime rows use the entity's own timestamp when the feed
provides one (falling back to the header timestamp), with a unique index that
includes it. Re-polling an unchanged vehicle or trip is then a no-op rather than a
duplicate row. Service alerts are slowly changing, so they are stored as state:
one row per distinct alert content with `first_seen` / `last_seen`.

## Layout

```
feeds.yaml               feed configuration (see below)
migrations/0001_initial.sql   schema; applied automatically on startup
transponder/
  config.py              feeds.yaml models + environment settings
  scheduler.py           run_periodic: asyncio periodic jobs with backoff
  rt.py                  GTFS-RT Poller: fetch, decode, staleness detection
  keys.py                API key ring with rotation on 429/401/403
  health.py              failure classification, alert de-dup, Pushover/webhook delivery
  static.py              GTFS static change detection, parsing, versioned COPY load
  writer.py              queue + rt lane + static lane
  tables.py              realtime table column specs / INSERT statements
  db.py                  asyncpg pool, migration runner, feeds upsert
  app.py                 wiring, signals, graceful shutdown
  cli.py                 `transponder run | migrate | check-config | test-alert`
tests/                   unit tests (no database needed)
docker-compose.yml       TimescaleDB + the service
```

## Quick start (Docker)

```sh
cp .env.example .env            # put feed API keys here (e.g. TRANSLINK_API_KEY)
docker compose up --build
```

TimescaleDB listens on `localhost:5432` (user/password/db all `transponder`).
The service applies migrations, upserts the configured feeds, and starts polling.
Logs show a per-minute summary of rows written per table.

```sh
psql postgresql://transponder:transponder@localhost:5432/transponder
```

## Running locally without Docker

```sh
uv venv && uv pip install -e '.[dev]'        # or: python -m venv .venv && pip install -e '.[dev]'
docker compose up -d timescaledb             # or any Postgres with the timescaledb extension
cp .env.example .env
transponder check-config                     # validates feeds.yaml, its secrets, and alerting
transponder test-alert                       # pushes one test notification
transponder run
pytest
```

`transponder migrate` applies pending migrations and exits. Migrations are plain
SQL files in `migrations/`, applied in name order and recorded in `schema_migrations`.

## Configuration

`feeds.yaml`:

```yaml
feeds:
  - feed_id: translink                      # unique, [a-z0-9_-]; scopes every row
    agency: TransLink (Metro Vancouver)
    static_url: https://gtfs-static.translink.ca/gtfs/google_transit.zip
    realtime:                               # any subset; bare URL or {url, poll_interval_seconds}
      vehicle_positions: https://gtfsrt.api.translink.ca/v3/gtfsposition
      trip_updates: https://gtfsrt.api.translink.ca/v3/gtfsrealtime
      service_alerts:
        url: https://gtfsrt.api.translink.ca/v3/gtfsalerts
        poll_interval_seconds: 120
    auth:
      type: query_param                     # none | header | query_param
      key: apikey                           # header name or query parameter name
      env: TRANSLINK_API_KEYS               # env var(s) holding the secret(s), see below
      daily_request_limit: 1000             # optional, per key; used to word quota alerts
    rt_poll_interval_seconds: 30            # default for realtime endpoints
    static_check_interval_hours: 24
    stale_after_seconds: 600                # optional per-feed override

alerting:                                   # optional, see "Alerting" below
  pushover:
    token_env: PUSHOVER_TOKEN
    user_env: PUSHOVER_USER
```

Secrets never live in the YAML. Every referenced environment variable is
resolved at startup, so a missing key fails immediately rather than at the first
poll. The same auth is applied to the static download.

### API key rotation

`auth.env` accepts one variable name or a list of them, and each variable may
hold several keys separated by commas:

```yaml
auth:
  type: query_param
  key: apikey
  env: [TRANSLINK_API_KEYS, TRANSLINK_SPARE_KEY]   # TRANSLINK_API_KEYS=k1,k2
```

All endpoints of a feed share one key ring. Keys are used in order: the first
usable key serves every request until the agency answers 429 (or 403 with
rate-limit wording), at which point it is benched for `Retry-After` if given,
else `exhausted_cooldown_seconds` (default one hour), and the next key takes
over in the same request. A 401, or a 403 without rate-limit wording, benches
the key for six hours as invalid. Every benching sends a notice naming the key.
Using keys in order rather than round-robin means you see keys run out one by
one and get warned before the last one goes.

When no key is usable the endpoint raises `all keys exhausted` (or
`auth failed` if none were quota-related), backs off until the earliest key
returns, and the alert says how many requests per day the current intervals
generate and, if `daily_request_limit` is set, the minimum poll interval or
number of keys that would fit. `rt_fetches.api_key` records which key served
each fetch.

## Alerting

Health tracking is always on. Configure `alerting.pushover` and/or
`alerting.webhook` in `feeds.yaml` (secrets via env vars) to receive pushes;
`transponder test-alert` confirms delivery. Everything is also logged.

| Kind | Trigger | Priority |
| --- | --- | --- |
| `quota exhausted` | one key benched after 429 / 403 rate-limit | normal |
| `all keys exhausted` | every key of a feed is benched; message includes the sizing advice | high |
| `auth failed` | 401, or 403 without rate-limit wording, on every key | high |
| `http error` | any other 4xx/5xx | normal, after 3 in a row |
| `network` | DNS, connect, TLS, timeout | normal, after 3 in a row |
| `decode failed` | body is not a GTFS-RT FeedMessage (HTML error page, empty) | normal, after 3 in a row |
| `feed stale` | HTTP 200 but the header timestamp / payload has not changed for `stale_after_seconds`, or the header timestamp is that far in the past | normal |
| `static load failed` | a new bundle could not be parsed or loaded; previous version stays current | normal |
| `db unavailable` | writer cannot reach Postgres; rows are held and retried | high |
| `rows dropped` | Postgres rejected rows on data grounds | normal |
| `writer backlog` | queue over 80% full; pollers are blocking | normal |
| `crash` | the process is exiting on an unhandled error | high |

Rules: a problem is notified once, repeated at most every
`repeat_after_seconds` with its duration and count, and followed by a low
priority "recovered" message when the scope succeeds again. Transient kinds
(network, HTTP, decode) must occur `min_consecutive_failures` times in a row
first. Set `notify_on_start: true` to get a message on every start.

`feed stale` is the "agency stopped publishing" case: requests succeed, so it is
tracked as a condition per endpoint rather than a fetch failure, and clears
itself when new data appears. Overnight gaps with an unchanging header
timestamp will trigger it; raise `stale_after_seconds` for such feeds.

Process settings (environment, see `.env.example`):

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATABASE_URL` | required | asyncpg DSN |
| `FEEDS_CONFIG` | `feeds.yaml` | path to the feed configuration |
| `LOG_LEVEL` | `INFO` | Python logging level |
| `DB_POOL_MIN` / `DB_POOL_MAX` | `1` / `4` | asyncpg pool size |
| `WRITER_BATCH_ROWS` | `5000` | max rows per flush |
| `WRITER_FLUSH_SECONDS` | `1.0` | how long the writer waits to coalesce a flush |
| `WRITER_QUEUE_SIZE` | `2000` | queued batches before pollers block |
| `TRANSPONDER_TMP_DIR` | system temp | where static zips are staged |

## Data model

All realtime tables are TimescaleDB hypertables partitioned on `time`.

| Table | Key | Notes |
| --- | --- | --- |
| `feeds` | `feed_id` | upserted from `feeds.yaml` on startup |
| `feed_versions` | `feed_version_id` | one row per loaded static bundle; `is_current` marks the live one |
| `gtfs_agency`, `gtfs_routes`, `gtfs_stops`, `gtfs_trips`, `gtfs_stop_times`, `gtfs_calendar`, `gtfs_calendar_dates` | `(feed_id, feed_version_id, …)` | standard GTFS columns; `gtfs_stop_times` adds `arrival_secs` / `departure_secs` for times past 24:00 |
| `vehicle_positions` | `(feed_id, vehicle_id, time)` | one row per vehicle report |
| `trip_updates` | `(feed_id, trip_id, trip_start_date, time)` | one row per trip update; `entity_id` stands in for a missing `trip_id` |
| `stop_time_updates` | `(…, stop_sequence, stop_id, time)` | one row per predicted stop, flattened for analysis |
| `service_alerts` | `(feed_id, alert_id, alert_hash)` | state, not time series; `first_seen` / `last_seen` |
| `rt_fetches` | none | one row per fetch attempt: status, entity count, latency, error, error kind, API key used |

Enum fields (`current_status`, `schedule_relationship`, `cause`, …) are stored as
their GTFS-RT names, not integers.

### Example queries

Current schedule for a feed:

```sql
SELECT r.route_short_name, count(*) AS trips
FROM gtfs_trips t
JOIN gtfs_routes r USING (feed_id, feed_version_id, route_id)
JOIN feed_versions v USING (feed_version_id)
WHERE v.feed_id = 'translink' AND v.is_current
GROUP BY 1 ORDER BY 2 DESC;
```

Vehicles reporting in the last five minutes, per feed:

```sql
SELECT feed_id, count(DISTINCT vehicle_id)
FROM vehicle_positions
WHERE time > now() - interval '5 minutes'
GROUP BY 1;
```

Feed health over the last hour, including which key served the requests:

```sql
SELECT feed_id, endpoint, api_key,
       count(*) FILTER (WHERE error IS NULL) AS ok,
       count(*) FILTER (WHERE error IS NOT NULL) AS failed,
       string_agg(DISTINCT error_kind, ',') AS error_kinds,
       avg(duration_ms)::int AS avg_ms
FROM rt_fetches
WHERE time > now() - interval '1 hour'
GROUP BY 1, 2, 3 ORDER BY 1, 2, 3;
```

## Operational notes

- Old static versions accumulate by design. To prune, delete the `gtfs_*` rows
  and the `feed_versions` row for versions that are not `is_current`.
- Retention and compression policies are not set. Add
  `add_retention_policy` / `add_compression_policy` in a follow-up migration once
  you know how much history you want to keep.
- A feed whose fetch fails is logged in `rt_fetches` with the error text and kind,
  and backs off exponentially (capped at 10 minutes, or until a key is usable
  again) until it recovers.
- Shutdown on SIGINT/SIGTERM cancels the pollers, flushes whatever is queued, and
  then closes the pool.

## Roadmap

- Read API / dashboard on top of the hypertables.
- Retention and compression policies.
- GTFS `shapes.txt` and fare files.
