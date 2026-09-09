-- Transponder initial schema.
-- Every row is scoped by feed_id because GTFS identifiers are only unique
-- within one feed. Static tables are additionally scoped by feed_version_id;
-- a version is written once and never modified.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------------------
-- Feeds and static versions
-- ---------------------------------------------------------------------------

CREATE TABLE feeds (
    feed_id                 text PRIMARY KEY,
    agency_name             text NOT NULL,
    static_url              text NOT NULL,
    static_last_checked_at  timestamptz,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE feed_versions (
    feed_version_id bigserial PRIMARY KEY,
    feed_id         text NOT NULL REFERENCES feeds (feed_id),
    sha256          text NOT NULL,
    size_bytes      bigint,
    etag            text,
    last_modified   text,
    fetched_at      timestamptz NOT NULL,
    loaded_at       timestamptz,
    is_current      boolean NOT NULL DEFAULT false
);
CREATE UNIQUE INDEX feed_versions_current_idx ON feed_versions (feed_id) WHERE is_current;
CREATE INDEX feed_versions_feed_fetched_idx ON feed_versions (feed_id, fetched_at DESC);

-- ---------------------------------------------------------------------------
-- GTFS static
-- ---------------------------------------------------------------------------

CREATE TABLE gtfs_agency (
    feed_id         text NOT NULL,
    feed_version_id bigint NOT NULL REFERENCES feed_versions (feed_version_id),
    agency_id       text NOT NULL,
    agency_name     text,
    agency_url      text,
    agency_timezone text,
    agency_lang     text,
    agency_phone    text,
    agency_fare_url text,
    agency_email    text,
    PRIMARY KEY (feed_id, feed_version_id, agency_id)
);

CREATE TABLE gtfs_routes (
    feed_id          text NOT NULL,
    feed_version_id  bigint NOT NULL REFERENCES feed_versions (feed_version_id),
    route_id         text NOT NULL,
    agency_id        text,
    route_short_name text,
    route_long_name  text,
    route_desc       text,
    route_type       integer,
    route_url        text,
    route_color      text,
    route_text_color text,
    route_sort_order integer,
    PRIMARY KEY (feed_id, feed_version_id, route_id)
);

CREATE TABLE gtfs_stops (
    feed_id             text NOT NULL,
    feed_version_id     bigint NOT NULL REFERENCES feed_versions (feed_version_id),
    stop_id             text NOT NULL,
    stop_code           text,
    stop_name           text,
    stop_desc           text,
    stop_lat            double precision,
    stop_lon            double precision,
    zone_id             text,
    stop_url            text,
    location_type       integer,
    parent_station      text,
    stop_timezone       text,
    wheelchair_boarding integer,
    platform_code       text,
    PRIMARY KEY (feed_id, feed_version_id, stop_id)
);

CREATE TABLE gtfs_trips (
    feed_id               text NOT NULL,
    feed_version_id       bigint NOT NULL REFERENCES feed_versions (feed_version_id),
    trip_id               text NOT NULL,
    route_id              text,
    service_id            text,
    trip_headsign         text,
    trip_short_name       text,
    direction_id          integer,
    block_id              text,
    shape_id              text,
    wheelchair_accessible integer,
    bikes_allowed         integer,
    PRIMARY KEY (feed_id, feed_version_id, trip_id)
);
CREATE INDEX gtfs_trips_route_idx ON gtfs_trips (feed_id, feed_version_id, route_id);

-- Largest table by far; loaded with COPY. Indexed rather than PK-constrained so a
-- feed with a duplicated (trip_id, stop_sequence) row still loads.
CREATE TABLE gtfs_stop_times (
    feed_id             text NOT NULL,
    feed_version_id     bigint NOT NULL REFERENCES feed_versions (feed_version_id),
    trip_id             text NOT NULL,
    arrival_time        text,
    arrival_secs        integer,
    departure_time      text,
    departure_secs      integer,
    stop_id             text,
    stop_sequence       integer,
    stop_headsign       text,
    pickup_type         integer,
    drop_off_type       integer,
    shape_dist_traveled double precision,
    timepoint           integer
);
CREATE INDEX gtfs_stop_times_trip_idx ON gtfs_stop_times (feed_id, feed_version_id, trip_id, stop_sequence);
CREATE INDEX gtfs_stop_times_stop_idx ON gtfs_stop_times (feed_id, feed_version_id, stop_id);

CREATE TABLE gtfs_calendar (
    feed_id         text NOT NULL,
    feed_version_id bigint NOT NULL REFERENCES feed_versions (feed_version_id),
    service_id      text NOT NULL,
    monday          integer,
    tuesday         integer,
    wednesday       integer,
    thursday        integer,
    friday          integer,
    saturday        integer,
    sunday          integer,
    start_date      date,
    end_date        date,
    PRIMARY KEY (feed_id, feed_version_id, service_id)
);

CREATE TABLE gtfs_calendar_dates (
    feed_id         text NOT NULL,
    feed_version_id bigint NOT NULL REFERENCES feed_versions (feed_version_id),
    service_id      text NOT NULL,
    date            date NOT NULL,
    exception_type  integer,
    PRIMARY KEY (feed_id, feed_version_id, service_id, date)
);

-- ---------------------------------------------------------------------------
-- GTFS-Realtime (hypertables)
-- ---------------------------------------------------------------------------

-- `time` is the entity's own timestamp when the feed provides one, else the
-- feed header timestamp. The unique indexes make re-polling an unchanged
-- entity a no-op (INSERT ... ON CONFLICT DO NOTHING). Entity ids are not part
-- of the keys because some producers number entities positionally per message;
-- they only stand in for a trip_id when an ADDED trip has none.
CREATE TABLE vehicle_positions (
    time                  timestamptz NOT NULL,
    feed_id               text NOT NULL,
    vehicle_id            text NOT NULL,
    entity_id             text,
    vehicle_label         text,
    license_plate         text,
    trip_id               text,
    route_id              text,
    direction_id          integer,
    trip_start_date       date,
    trip_start_time       text,
    schedule_relationship text,
    latitude              double precision,
    longitude             double precision,
    bearing               real,
    odometer              double precision,
    speed                 real,
    current_stop_sequence integer,
    stop_id               text,
    current_status        text,
    congestion_level      text,
    occupancy_status      text,
    occupancy_percentage  integer,
    feed_timestamp        timestamptz,
    fetched_at            timestamptz NOT NULL
);
SELECT create_hypertable('vehicle_positions', 'time', chunk_time_interval => INTERVAL '1 day');
CREATE UNIQUE INDEX vehicle_positions_uniq ON vehicle_positions (feed_id, vehicle_id, time);
CREATE INDEX vehicle_positions_trip_idx ON vehicle_positions (feed_id, trip_id, time DESC);
CREATE INDEX vehicle_positions_route_idx ON vehicle_positions (feed_id, route_id, time DESC);

CREATE TABLE trip_updates (
    time                    timestamptz NOT NULL,
    feed_id                 text NOT NULL,
    entity_id               text NOT NULL,
    trip_id                 text,
    trip_start_date         date,
    trip_start_time         text,
    route_id                text,
    direction_id            integer,
    schedule_relationship   text,
    vehicle_id              text,
    vehicle_label           text,
    delay                   integer,
    stop_time_update_count  integer,
    feed_timestamp          timestamptz,
    fetched_at              timestamptz NOT NULL
);
SELECT create_hypertable('trip_updates', 'time', chunk_time_interval => INTERVAL '1 day');
CREATE UNIQUE INDEX trip_updates_uniq
    ON trip_updates (feed_id, (coalesce(trip_id, entity_id)), trip_start_date, time)
    NULLS NOT DISTINCT;
CREATE INDEX trip_updates_trip_idx ON trip_updates (feed_id, trip_id, time DESC);

CREATE TABLE stop_time_updates (
    time                  timestamptz NOT NULL,
    feed_id               text NOT NULL,
    entity_id             text NOT NULL,
    trip_id               text,
    trip_start_date       date,
    stop_sequence         integer,
    stop_id               text,
    arrival_delay         integer,
    arrival_time          timestamptz,
    arrival_uncertainty   integer,
    departure_delay       integer,
    departure_time        timestamptz,
    departure_uncertainty integer,
    schedule_relationship text,
    fetched_at            timestamptz NOT NULL
);
SELECT create_hypertable('stop_time_updates', 'time', chunk_time_interval => INTERVAL '1 day');
CREATE UNIQUE INDEX stop_time_updates_uniq
    ON stop_time_updates (feed_id, (coalesce(trip_id, entity_id)), trip_start_date, stop_sequence, stop_id, time)
    NULLS NOT DISTINCT;
CREATE INDEX stop_time_updates_stop_idx ON stop_time_updates (feed_id, stop_id, time DESC);
CREATE INDEX stop_time_updates_trip_idx ON stop_time_updates (feed_id, trip_id, time DESC);

-- Alerts are slowly changing, so they are kept as state rather than as a time
-- series: one row per distinct alert content, with first/last seen bounds.
CREATE TABLE service_alerts (
    feed_id           text NOT NULL,
    alert_id          text NOT NULL,
    alert_hash        text NOT NULL,
    first_seen        timestamptz NOT NULL,
    last_seen         timestamptz NOT NULL,
    cause             text,
    effect            text,
    severity_level    text,
    header_text       text,
    description_text  text,
    url               text,
    active_periods    jsonb,
    informed_entities jsonb,
    PRIMARY KEY (feed_id, alert_id, alert_hash)
);
CREATE INDEX service_alerts_last_seen_idx ON service_alerts (feed_id, last_seen DESC);

-- One row per RT fetch attempt, for monitoring feed health without a UI.
CREATE TABLE rt_fetches (
    time         timestamptz NOT NULL,
    feed_id      text NOT NULL,
    endpoint     text NOT NULL,
    http_status  integer,
    entity_count integer,
    bytes        integer,
    duration_ms  integer,
    error        text
);
SELECT create_hypertable('rt_fetches', 'time', chunk_time_interval => INTERVAL '7 days');
CREATE INDEX rt_fetches_feed_idx ON rt_fetches (feed_id, endpoint, time DESC);
