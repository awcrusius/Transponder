"""Column specs and INSERT statements for the realtime tables the writer handles."""

from __future__ import annotations

from dataclasses import dataclass

# One entry of an identity: a column name, or a tuple of names of which the
# first non-null value is used (the coalesce in the table's unique index).
IdentityColumn = str | tuple[str, ...]


@dataclass(frozen=True)
class TableSpec:
    name: str
    columns: tuple[str, ...]
    on_conflict: str = "DO NOTHING"
    # Columns that say *what* was observed. Rows with the same identity and the
    # same remaining columns (other than `observed_at`) are re-observations of one
    # thing and are deduplicated by `transponder.dedupe` before they are written.
    # Empty means the table is never deduplicated.
    identity: tuple[IdentityColumn, ...] = ()
    # Columns that only say *when* it was observed; ignored when comparing rows.
    observed_at: tuple[str, ...] = ()

    @property
    def insert_sql(self) -> str:
        cols = ", ".join(self.columns)
        params = ", ".join(f"${i}" for i in range(1, len(self.columns) + 1))
        return f"INSERT INTO {self.name} ({cols}) VALUES ({params}) ON CONFLICT {self.on_conflict}"


VEHICLE_POSITIONS = TableSpec(
    "vehicle_positions",
    (
        "time", "feed_id", "vehicle_id", "entity_id", "vehicle_label", "license_plate",
        "trip_id", "route_id", "direction_id", "trip_start_date", "trip_start_time",
        "schedule_relationship", "latitude", "longitude", "bearing", "odometer", "speed",
        "current_stop_sequence", "stop_id", "current_status", "congestion_level",
        "occupancy_status", "occupancy_percentage", "feed_timestamp", "fetched_at",
    ),
    identity=("feed_id", "vehicle_id"),
    observed_at=("time", "feed_timestamp", "fetched_at"),
)

TRIP_UPDATES = TableSpec(
    "trip_updates",
    (
        "time", "feed_id", "entity_id", "trip_id", "trip_start_date", "trip_start_time",
        "route_id", "direction_id", "schedule_relationship", "vehicle_id", "vehicle_label",
        "delay", "stop_time_update_count", "feed_timestamp", "fetched_at",
    ),
    identity=("feed_id", ("trip_id", "entity_id"), "trip_start_date"),
    observed_at=("time", "feed_timestamp", "fetched_at"),
)

STOP_TIME_UPDATES = TableSpec(
    "stop_time_updates",
    (
        "time", "feed_id", "entity_id", "trip_id", "trip_start_date", "stop_sequence", "stop_id",
        "arrival_delay", "arrival_time", "arrival_uncertainty",
        "departure_delay", "departure_time", "departure_uncertainty",
        "schedule_relationship", "fetched_at",
    ),
    identity=("feed_id", ("trip_id", "entity_id"), "trip_start_date", "stop_sequence", "stop_id"),
    observed_at=("time", "fetched_at"),
)

SERVICE_ALERTS = TableSpec(
    "service_alerts",
    (
        "feed_id", "alert_id", "alert_hash", "first_seen", "last_seen",
        "cause", "effect", "severity_level", "header_text", "description_text", "url",
        "active_periods", "informed_entities",
    ),
    on_conflict=(
        "(feed_id, alert_id, alert_hash) DO UPDATE"
        " SET last_seen = GREATEST(service_alerts.last_seen, EXCLUDED.last_seen)"
    ),
)

RT_FETCHES = TableSpec(
    "rt_fetches",
    ("time", "feed_id", "endpoint", "http_status", "entity_count", "bytes", "duration_ms", "error", "error_kind", "api_key"),
)

RT_TABLES: dict[str, TableSpec] = {
    spec.name: spec for spec in (VEHICLE_POSITIONS, TRIP_UPDATES, STOP_TIME_UPDATES, SERVICE_ALERTS, RT_FETCHES)
}
