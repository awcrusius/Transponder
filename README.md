# Transponder

A config-driven, multi-agency GTFS ingestion service that archives realtime
transit data in TimescaleDB.

Point it at any number of GTFS feeds in `feeds.yaml` and it will:

- poll each agency's GTFS-Realtime endpoints (vehicle positions, trip updates,
  service alerts) on their own intervals and store every *change* as a
  time-series row;
- periodically check each agency's GTFS static bundle for changes and load new
  versions side by side, never overwriting old ones;
- scope every row by `feed_id` (plus `feed_version_id` for static data), because
  GTFS identifiers are only unique within one feed;
- keep the archive bounded: unchanged observations are suppressed at ingest,
  older data is compressed, and the oldest is dropped on a schedule you choose.

It also watches itself. Every failure mode is classified, de-duplicated and
pushed to Pushover or a webhook, and each feed can spread its requests
round-robin over several API keys, benching any that run out of quota.

There is no web UI or API layer. The deliverable is the ingestion service, the
schema, and a Docker Compose setup. Query the database with anything that
speaks Postgres.

## Contents

- [Quick start](#quick-start)
- [Adding a feed](#adding-a-feed)
- [How it works](#how-it-works)
- [Configuration](#configuration)
- [Alerting](#alerting)
- [Data model](#data-model)
- [Storage, compression and retention](#storage-compression-and-retention)
- [Deploying and operating](#deploying-and-operating)
- [Development](#development)

## Quick start

Requirements: Docker with Compose, or Python 3.11+ and a Postgres with the
TimescaleDB extension.

```sh
git clone <this repository> transponder && cd transponder
cp .env.example .env      # fill in API keys for the feeds you keep in feeds.yaml
docker compose up --build
```

That starts TimescaleDB on `localhost:5432` (user, password and database are
all `transponder`) and the service. On start the service applies any pending
migrations from `migrations/`, upserts the configured feeds, downloads each
feed's static bundle, and begins polling. The log prints a per-minute summary
of rows written per table.

```sh
psql postgresql://transponder:transponder@localhost:5432/transponder
```

The shipped `feeds.yaml` lists a few real, publicly documented feeds from
British Columbia as worked examples. Replace them with your own agencies; the
only one that needs a key is TransLink (`TRANSLINK_API_KEYS`). Remove any feed
whose secret you do not have, or `transponder run` refuses to start.

### Running without Docker

```sh
python -m venv .venv && . .venv/bin/activate
pip install -e '.[dev]'
docker compose up -d timescaledb        # or point DATABASE_URL at your own Postgres + TimescaleDB
cp .env.example .env
transponder check-config                # validates feeds.yaml, its secrets, and alerting
transponder test-alert                  # sends one test notification
transponder run
```

Commands:

| Command | Purpose |
| --- | --- |
| `transponder run [--config feeds.yaml]` | apply migrations, then ingest until SIGINT/SIGTERM |
| `transponder migrate` | apply pending migrations and exit |
| `transponder check-config` | print the resolved configuration and fail on missing secrets |
| `transponder test-alert` | push one notification through every configured notifier |

## Adding a feed

1. Find the agency's GTFS static zip URL and its GTFS-Realtime endpoint URLs.
   Most agencies list them on a developer page or on
   [Mobility Database](https://mobilitydatabase.org/) / [transit.land](https://www.transit.land/).
2. Add an entry to `feeds.yaml`. `feed_id` is a short slug you choose; it
   labels every row this feed ever writes, so pick it once and keep it.

   ```yaml
   feeds:
     - feed_id: metro_example
       agency: Example Metro Transit
       static_url: https://example.org/gtfs/google_transit.zip
       realtime:
         vehicle_positions: https://example.org/gtfs-rt/vehicles.pb
         trip_updates: https://example.org/gtfs-rt/trips.pb
         service_alerts:
           url: https://example.org/gtfs-rt/alerts.pb
           poll_interval_seconds: 120
       auth:
         type: header                 # none | header | query_param
         key: X-API-Key               # header or query parameter name
         env: EXAMPLE_METRO_KEYS      # environment variable holding the secret(s)
       rt_poll_interval_seconds: 30
       static_check_interval_hours: 24
   ```

3. Put the secret in `.env` (`EXAMPLE_METRO_KEYS=abc123`). Several keys can
   be comma-separated; see [API key rotation](#api-key-rotation).
4. `transponder check-config`, then start or restart the service. A feed with
   no realtime endpoints is fine; its static bundle is still versioned.

Feeds are independent. A misconfigured or failing feed backs off and alerts
without affecting the others.

## How it works

```
feeds.yaml ──► scheduler (asyncio, one coroutine per feed endpoint)
                  │
                  ├─ rt.Poller ── fetch + decode (gtfs-realtime-bindings) ── Deduper ──┐
                  │                                                                    │  Batch(table, rows)
                  └─ static.check_for_update ── conditional GET / hash ──┐             │
                                                                         ▼             ▼
                                                     Writer  (bounded asyncio queues)
                                                       ├─ rt lane: coalesce per table, batched INSERT ... ON CONFLICT
                                                       └─ static lane: one version at a time, COPY inside a transaction
                                                                         │
                                                                         ▼
                                                               TimescaleDB (asyncpg pool)
```

**Scheduling.** A plain asyncio loop. Every `(feed, endpoint)` pair and every
feed's static check is one coroutine driven by `run_periodic`, so hundreds of
feeds poll concurrently on a single thread. Start times are jittered so
identical intervals do not fire in lockstep, and a failing endpoint backs off
exponentially without affecting the others.

**Decoding.** A GTFS-RT `FeedMessage` is decoded in a worker thread into flat
rows for `vehicle_positions`, `trip_updates` and `stop_time_updates` (one row
per predicted stop). A row's `time` is the entity's own timestamp when the feed
provides one, otherwise the feed header's timestamp, otherwise the fetch time.
`feed_timestamp` and `fetched_at` are stored alongside so the three can always
be told apart.

**Deduplication.** Agencies republish their whole snapshot on every poll, and
typically 80-90 % of trip and stop-time rows repeat the previous poll byte for
byte. Each poller therefore keeps a small in-memory `Deduper` (`dedupe.py`).
Per observed thing (a vehicle, a trip, or a trip+stop; see `TableSpec.identity`
in `tables.py`) it remembers a hash of the last row written and when. A row
identical to that one within the last `RT_DEDUPE_SECONDS` (default one hour) is
dropped before it reaches the writer; a changed row, a new thing, or an
unchanged one not written for a full window goes through. A stored realtime row
therefore means *"observed like this at `time`, and unchanged since the previous
row for the same thing"*. See [Reconstructing state](#reconstructing-state) for
how to query that. A unique index that includes `time` additionally makes a
second copy of the same observation a database no-op, and service alerts are
stored as state (one row per distinct alert content with `first_seen` /
`last_seen`) rather than as a time series.

**Writes.** Nothing writes to the database directly. Rows go onto a bounded
queue consumed by the `Writer`, which runs exactly two tasks: an *rt lane* that
drains the queue, groups rows by table and issues one batched
`INSERT ... ON CONFLICT DO NOTHING` transaction per flush; and a *static lane*
that loads one GTFS version at a time with `COPY`. Hypertables are therefore
never written concurrently, and a multi-minute static load cannot stall
realtime writes. If the queue fills, pollers block on `submit()`; that is the
intended backpressure, and it raises a `writer backlog` alert.

**Static versioning.** Each check sends `If-None-Match` / `If-Modified-Since`
from the current version. A 304 ends the check. On 200 the zip is streamed to
a temp file while its SHA-256 is computed; an unchanged hash also ends the check
(some servers ignore conditional headers). Otherwise a new `feed_versions` row
is created and every table is `COPY`ed under that `feed_version_id` in one
transaction. On success the new version becomes `is_current`; on failure the
transaction rolls back and nothing of the version remains. Old versions are
kept so that historical realtime rows can be joined to the schedule they were
generated against. Real feeds are not always clean (duplicated `stop_id`s are
common), so for keyed tables the first row per key wins and duplicates are
counted in a warning rather than failing the load.

## Configuration

Two sources: `feeds.yaml` for what to ingest and how to alert, and environment
variables (or a `.env` file) for secrets and process tuning. Secrets never live
in the YAML; every referenced environment variable is resolved at startup, so a
missing key fails immediately rather than at the first poll.

### feeds.yaml

```yaml
feeds:
  - feed_id: translink                      # unique, [a-z0-9_-]; scopes every row this feed writes
    agency: TransLink (Metro Vancouver)     # display name, stored in the feeds table
    static_url: https://gtfs-static.translink.ca/gtfs/google_transit.zip
    realtime:                               # any subset; bare URL or {url, poll_interval_seconds}
      vehicle_positions: https://gtfsapi.translink.ca/v3/gtfsposition
      trip_updates: https://gtfsapi.translink.ca/v3/gtfsrealtime
      service_alerts:
        url: https://gtfsapi.translink.ca/v3/gtfsalerts
        poll_interval_seconds: 120
    auth:
      type: query_param                     # none | header | query_param
      key: apikey                           # header name or query parameter name
      env: TRANSLINK_API_KEYS               # env var(s) holding the secret(s), see below
      daily_request_limit: 1000             # optional, per key; used to word quota alerts
      exhausted_cooldown_seconds: 3600      # optional; how long a quota-blocked key sits out
    rt_poll_interval_seconds: 30            # default for realtime endpoints without their own
    static_check_interval_hours: 24
    stale_after_seconds: 600                # optional per-feed override of alerting.stale_after_seconds

alerting:                                   # optional, see "Alerting" below
  pushover:
    token_env: PUSHOVER_TOKEN
    user_env: PUSHOVER_USER
    # device: my-phone
  # webhook:
  #   url_env: ALERT_WEBHOOK_URL
  stale_after_seconds: 600
  repeat_after_seconds: 3600
  min_consecutive_failures: 3
  notify_on_start: false
```

The same auth is applied to the static download. Choose poll intervals with
the agency's terms and quotas in mind; `transponder check-config` prints the
resolved intervals per endpoint.

### API key rotation

`auth.env` accepts one variable name or a list of them, and each variable may
hold several keys separated by commas:

```yaml
auth:
  type: query_param
  key: apikey
  env: [TRANSLINK_API_KEYS, TRANSLINK_SPARE_KEY]   # TRANSLINK_API_KEYS=k1,k2
```

All endpoints of a feed share one key ring, and every request takes the next
key round-robin, so the load is spread evenly and each key's daily quota lasts
as long as possible. When the agency answers 429 (or 403 with rate-limit
wording) the key is benched for `Retry-After` if given, else
`exhausted_cooldown_seconds`, and the request is retried at once with the next
usable key. A 401, or a 403 without rate-limit wording, benches the key for six
hours as invalid. Every benching sends a notice naming the key and how many
remain, so a feed with several keys warns you before the last one goes.

When no key is usable the endpoint raises `all keys exhausted` (or
`auth failed` if none were quota-related), backs off until the earliest key
returns, and the alert says how many requests per day the current intervals
generate and, if `daily_request_limit` is set, the minimum poll interval or
number of keys that would fit. `rt_fetches.api_key` records which key served
each fetch.

### Environment variables

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATABASE_URL` | required | asyncpg DSN, e.g. `postgresql://user:pw@host:5432/db` |
| `FEEDS_CONFIG` | `feeds.yaml` | path to the feed configuration |
| `MIGRATIONS_DIR` | `migrations/` next to the package | where `*.sql` migrations are read from |
| `LOG_LEVEL` | `INFO` | Python logging level |
| `DB_POOL_MIN` / `DB_POOL_MAX` | `1` / `4` | asyncpg pool size |
| `WRITER_BATCH_ROWS` | `5000` | max rows per flush |
| `WRITER_FLUSH_SECONDS` | `1.0` | how long the writer waits to coalesce a flush |
| `WRITER_QUEUE_SIZE` | `2000` | queued batches before pollers block |
| `RT_DEDUPE_SECONDS` | `3600` | suppress realtime rows identical to one written this recently; `0` disables |
| `TRANSPONDER_TMP_DIR` | system temp | where static zips are staged while loading |

Feed secrets (`auth.env`) and notifier secrets (`alerting.*.token_env` etc.)
are whatever variable names you configure in `feeds.yaml`.

## Alerting

Health tracking is always on. Configure `alerting.pushover` and/or
`alerting.webhook` in `feeds.yaml` (secrets via env vars) to receive pushes;
`transponder test-alert` confirms delivery. Without a notifier everything is
still classified and logged. The webhook receives
`{"title", "message", "priority"}` as JSON, which most chat-ops relays accept.

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
| `writer backlog` | queue over 80 % full; pollers are blocking | normal |
| `crash` | the process is exiting on an unhandled error | high |

Rules: a problem is notified once, repeated at most every
`repeat_after_seconds` with its duration and count, and followed by a low
priority "recovered" message when the scope succeeds again. Transient kinds
(network, HTTP, decode) must occur `min_consecutive_failures` times in a row
first. Set `notify_on_start: true` to get a message on every start.

`feed stale` is the "agency stopped publishing" case: requests succeed, so it
is tracked as a condition per endpoint rather than a fetch failure, and clears
itself when new data appears. Agencies that freeze their header timestamp
overnight will trigger it; raise `stale_after_seconds` for such feeds.

## Data model

All realtime tables are TimescaleDB hypertables partitioned on `time`. Enum
fields (`current_status`, `schedule_relationship`, `cause`, …) are stored as
their GTFS-RT names, not integers. The schema lives in `migrations/`.

| Table | Key | Notes |
| --- | --- | --- |
| `feeds` | `feed_id` | upserted from `feeds.yaml` on startup |
| `feed_versions` | `feed_version_id` | one row per loaded static bundle; `is_current` marks the live one |
| `gtfs_agency`, `gtfs_routes`, `gtfs_stops`, `gtfs_trips`, `gtfs_stop_times`, `gtfs_calendar`, `gtfs_calendar_dates` | `(feed_id, feed_version_id, …)` | standard GTFS columns; `gtfs_stop_times` adds `arrival_secs` / `departure_secs` (seconds since midnight, valid past 24:00) |
| `vehicle_positions` | `(feed_id, vehicle_id, time)` | one row per changed vehicle report |
| `trip_updates` | `(feed_id, trip_id, trip_start_date, time)` | one row per changed trip update; `entity_id` stands in for a missing `trip_id` |
| `stop_time_updates` | `(…, stop_sequence, stop_id, time)` | one row per changed stop prediction, flattened for analysis |
| `service_alerts` | `(feed_id, alert_id, alert_hash)` | state, not time series; `first_seen` / `last_seen` |
| `rt_fetches` | none | one row per fetch attempt: status, entity count, bytes, latency, error, error kind, API key used |

Joining realtime rows to the schedule: `gtfs_*` rows are keyed by
`feed_version_id`, realtime rows are not. Use the version that was current at
the row's `time` (`feed_versions.loaded_at` ordering), or simply the current
one for recent data.

### Reconstructing state

Because unchanged observations are suppressed, the state of anything at
instant `T` is its latest row at or before `T`, and an observation is
guaranteed to be re-asserted at least once per `RT_DEDUPE_SECONDS` while it is
still being reported. The idiom is `DISTINCT ON`:

```sql
-- Every stop prediction for one trip as it stood at 08:15
SELECT DISTINCT ON (stop_sequence) stop_sequence, stop_id, arrival_time, arrival_delay, time AS as_of
FROM stop_time_updates
WHERE feed_id = 'translink' AND trip_id = '15440320' AND trip_start_date = '2026-09-11'
  AND time BETWEEN '2026-09-11 07:15+00' AND '2026-09-11 08:15+00'
ORDER BY stop_sequence, time DESC;
```

Look back at least one dedupe window (one hour by default) so the last
re-assertion is inside the range. A thing that has not been re-asserted for
longer than the window is no longer in the feed.

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

Delay history of one trip (one row per change):

```sql
SELECT time, delay, vehicle_id
FROM trip_updates
WHERE feed_id = 'translink' AND trip_id = '15440320' AND trip_start_date = current_date
ORDER BY time;
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

## Storage, compression and retention

`stop_time_updates` is where the bytes go: one row per predicted stop per
change. Three mechanisms keep it bounded, all on by default.

**Ingest dedupe** (above) removes the 80-90 % of rows that repeat the previous
poll, typically a 5-7x reduction in what is written.

**Compression.** `migrations/0003_compression_and_retention.sql` enables
TimescaleDB compression on `stop_time_updates`, `trip_updates` and
`vehicle_positions` (segment by `feed_id`, order by `time DESC`). A chunk is
compressed one day after its time range closes, by a job that runs hourly.
Compression is roughly 8-15x on these tables and, more importantly, drops the
per-chunk indexes, which are larger than the data itself. `stop_time_updates`
uses 6-hour chunks so that at most ~30 hours of it is ever uncompressed.
Compressed chunks are still queryable; inserts only go to the current chunk.

**Retention.** The same migration drops chunks older than:

| Table | Kept for | Rationale |
| --- | --- | --- |
| `stop_time_updates` | 14 days | ~90 % of the volume, and per-stop predictions are largely derivable from `trip_updates` + the schedule |
| `trip_updates` | 90 days | small once compressed |
| `vehicle_positions` | 180 days | smallest, and the hardest to reconstruct |

Change a window without a migration (new settings apply from the next run):

```sql
SELECT remove_retention_policy('stop_time_updates');
SELECT add_retention_policy('stop_time_updates', drop_after => INTERVAL '30 days');
```

Inspect the jobs and results:

```sql
SELECT job_id, hypertable_name, proc_name, schedule_interval, config FROM timescaledb_information.jobs;
SELECT * FROM hypertable_compression_stats('stop_time_updates');
SELECT hypertable_name, pg_size_pretty(total_bytes) FROM timescaledb_information.hypertables h,
       LATERAL hypertable_detailed_size(format('%I.%I', hypertable_schema, hypertable_name));
```

**Sizing.** As a rule of thumb from a deployment ingesting two mid-size
agencies (about 2,000 concurrent trips, 30-second polling): before dedupe,
`stop_time_updates` received ~120 M rows and ~45 GB per day (half of it
indexes); after dedupe ~24 M rows and ~8 GB per day uncompressed, or under
1 GB per day compressed. With the default windows the realtime tables settle
at roughly 50 GB. `trip_updates` and `vehicle_positions` are an order of
magnitude smaller. Scale linearly with concurrent trips and inversely with the
poll interval.

**Static versions are the unbounded part.** Each new bundle costs roughly the
size of its `stop_times.txt` in the database (hundreds of MB for a large
agency), and old versions are never deleted automatically. Some servers build
the zip on request, so its hash changes on every check even when the timetable
did not, and you get one new version per check. To prune, delete everything
belonging to versions that are no longer current and older than you care to
keep:

```sql
WITH old AS (
  SELECT feed_version_id FROM feed_versions
  WHERE NOT is_current AND fetched_at < now() - interval '30 days'
)
DELETE FROM gtfs_stop_times     WHERE feed_version_id IN (SELECT feed_version_id FROM old);
-- repeat for gtfs_trips, gtfs_stops, gtfs_routes, gtfs_calendar, gtfs_calendar_dates, gtfs_agency
DELETE FROM feed_versions       WHERE feed_version_id IN (SELECT feed_version_id FROM old);
```

`rt_fetches` and `service_alerts` have no retention policy; they grow by a few
MB per day.

## Deploying and operating

Transponder is a single long-running process plus a database, and the
Compose file runs both on one host. It has been run on hardware as small as a
Raspberry Pi with an NVMe drive.

- **Upgrading.** Pull or copy the new source, then `docker compose up -d --build`.
  The new container applies any new migrations before it starts polling, so an
  upgrade costs a restart (seconds of missed polls) unless a migration rebuilds
  a large index, in which case its header comment says so.
- **Migrations** are plain SQL files in `migrations/`, applied in name order
  inside one transaction each and recorded in `schema_migrations`. Concurrent
  starters serialise on an advisory lock. Never edit an applied migration; add
  a new one.
- **Restarts** are safe. Realtime dedupe state is in memory, so the first poll
  after a restart writes one full snapshot; the database's unique indexes make
  any genuine duplicates a no-op.
- **Static checks run on every start** as well as on their interval. A server
  that regenerates its zip per request will therefore store a new version on
  every restart; see the pruning note above.
- **Shutdown** on SIGINT/SIGTERM cancels the pollers, flushes whatever is
  queued, and closes the pool.
- **Backups.** The database is the only state. Back up the Compose volume or
  use `pg_dump`; the service can be re-pointed at a restored database without
  any other change.
- **Exposure.** The Compose file publishes Postgres on port 5432 with a default
  password for local convenience. Change the credentials or remove the `ports`
  mapping before running anywhere reachable.
- **Where things fail.** A fetch that fails is logged in `rt_fetches` with the
  error text and kind, and backs off exponentially (capped at 10 minutes, or
  until a key is usable again) until it recovers. Rows Postgres rejects are
  dropped per table, counted, and alerted; they never block other tables.

## Development

```
feeds.yaml                 example feed configuration
.env.example               every environment variable, with defaults
migrations/                schema (0001), fetch diagnostics (0002), compression + retention (0003)
transponder/
  config.py                feeds.yaml models + environment settings
  scheduler.py             run_periodic: asyncio periodic jobs with backoff
  rt.py                    GTFS-RT Poller: fetch, decode, dedupe, staleness detection
  dedupe.py                per-poller suppression of unchanged observations
  keys.py                  API key ring with rotation on 429/401/403
  health.py                failure classification, alert de-dup, Pushover/webhook delivery
  static.py                GTFS static change detection, parsing, versioned COPY load
  writer.py                queue + rt lane + static lane
  tables.py                realtime table column specs, identities, INSERT statements
  db.py                    asyncpg pool, migration runner, feeds upsert
  app.py                   wiring, signals, graceful shutdown
  cli.py                   `transponder run | migrate | check-config | test-alert`
tests/                     unit tests; no database or network needed
docker-compose.yml         TimescaleDB + the service
```

`pytest` runs in under a second and needs no database: the writer is tested
against a fake pool, the poller against an httpx mock transport.

Adding a column to a realtime table takes three edits: the migration that adds
it, the column list in `tables.py`, and the row tuple in `rt.py` (the tests in
`tests/test_rt.py` assert the tuple shape). If the column describes *what* was
observed it participates in dedupe automatically; if it only describes *when*,
add it to that table's `observed_at`. Adding a static file is one entry in
`static.STATIC_TABLES` plus its `CREATE TABLE`.

## Background

Transponder replaces an earlier single-agency collector,
[TransLink-data-analysis](https://github.com/awcrusius/TransLink-data-analysis),
whose schema was keyed only on GTFS IDs and so could not hold several agencies
without collisions, had no notion of static versions, and ran one process per
endpoint. The goal is unchanged: a durable, queryable archive of realtime
transit data, where adding an agency is a YAML entry rather than a fork.

## Roadmap

- Read API / dashboard on top of the hypertables.
- Automatic pruning of superseded static versions.
- GTFS `shapes.txt` and fare files.
