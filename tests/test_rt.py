from datetime import date, datetime, timezone

from google.transit import gtfs_realtime_pb2 as pb

from transponder import tables
from transponder.rt import decode
from tests.conftest import NOW


def _message(ts: int = 1_757_332_800) -> pb.FeedMessage:
    msg = pb.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = ts
    return msg


def _rowdict(spec: tables.TableSpec, row: tuple) -> dict:
    return dict(zip(spec.columns, row))


def test_vehicle_positions():
    msg = _message()
    e = msg.entity.add()
    e.id = "1"
    v = e.vehicle
    v.trip.trip_id = "T1"
    v.trip.route_id = "R1"
    v.trip.start_date = "20260908"
    v.trip.direction_id = 1
    v.vehicle.id = "bus-42"
    v.vehicle.label = "42"
    v.position.latitude = 49.28
    v.position.longitude = -123.12
    v.position.bearing = 90.0
    v.timestamp = msg.header.timestamp - 5
    v.current_status = pb.VehiclePosition.IN_TRANSIT_TO
    v.occupancy_status = pb.VehiclePosition.MANY_SEATS_AVAILABLE
    # A vehicle with no vehicle descriptor falls back to the entity id.
    e2 = msg.entity.add()
    e2.id = "2"
    e2.vehicle.position.latitude = 1.0
    e2.vehicle.position.longitude = 2.0

    out = decode("alpha", "vehicle_positions", msg.SerializeToString(), NOW)
    assert out.entity_count == 2
    (batch,) = out.batches
    assert batch.table == "vehicle_positions"
    assert len(batch.rows) == 2

    r = _rowdict(tables.VEHICLE_POSITIONS, batch.rows[0])
    assert r["feed_id"] == "alpha"
    assert r["vehicle_id"] == "bus-42"
    assert r["trip_id"] == "T1"
    assert r["route_id"] == "R1"
    assert r["direction_id"] == 1
    assert r["trip_start_date"] == date(2026, 9, 8)
    assert r["time"] == datetime.fromtimestamp(msg.header.timestamp - 5, tz=timezone.utc)
    assert r["feed_timestamp"] == datetime.fromtimestamp(msg.header.timestamp, tz=timezone.utc)
    assert r["fetched_at"] == NOW
    assert r["current_status"] == "IN_TRANSIT_TO"
    assert r["occupancy_status"] == "MANY_SEATS_AVAILABLE"
    assert r["congestion_level"] is None
    assert r["speed"] is None
    assert r["bearing"] == 90.0

    r2 = _rowdict(tables.VEHICLE_POSITIONS, batch.rows[1])
    assert r2["vehicle_id"] == "2"
    assert r2["time"] == r2["feed_timestamp"]  # no per-entity timestamp -> header
    assert r2["trip_id"] is None


def test_trip_updates_and_stop_time_updates():
    msg = _message()
    e = msg.entity.add()
    e.id = "T1"
    tu = e.trip_update
    tu.trip.trip_id = "T1"
    tu.trip.start_date = "20260908"
    tu.trip.schedule_relationship = pb.TripDescriptor.SCHEDULED
    tu.delay = 120
    s1 = tu.stop_time_update.add()
    s1.stop_sequence = 1
    s1.stop_id = "S1"
    s1.arrival.delay = 60
    s1.departure.delay = 90
    s1.departure.time = msg.header.timestamp + 300
    s2 = tu.stop_time_update.add()
    s2.stop_sequence = 2
    s2.stop_id = "S2"
    s2.schedule_relationship = pb.TripUpdate.StopTimeUpdate.SKIPPED

    out = decode("alpha", "trip_updates", msg.SerializeToString(), NOW)
    by_table = {b.table: b.rows for b in out.batches}
    assert set(by_table) == {"trip_updates", "stop_time_updates"}

    (tu_row,) = by_table["trip_updates"]
    r = _rowdict(tables.TRIP_UPDATES, tu_row)
    assert r["trip_id"] == "T1"
    assert r["delay"] == 120
    assert r["schedule_relationship"] == "SCHEDULED"
    assert r["stop_time_update_count"] == 2
    assert r["vehicle_id"] is None

    rows = [_rowdict(tables.STOP_TIME_UPDATES, x) for x in by_table["stop_time_updates"]]
    assert [x["stop_sequence"] for x in rows] == [1, 2]
    assert rows[0]["arrival_delay"] == 60
    assert rows[0]["arrival_time"] is None
    assert rows[0]["departure_delay"] == 90
    assert rows[0]["departure_time"] == datetime.fromtimestamp(msg.header.timestamp + 300, tz=timezone.utc)
    assert rows[0]["schedule_relationship"] is None
    assert rows[1]["schedule_relationship"] == "SKIPPED"
    assert all(x["feed_id"] == "alpha" and x["trip_id"] == "T1" for x in rows)


def test_service_alerts_hash_is_content_based():
    def build(text: str) -> bytes:
        msg = _message()
        e = msg.entity.add()
        e.id = "A1"
        a = e.alert
        a.cause = pb.Alert.CONSTRUCTION
        a.effect = pb.Alert.DETOUR
        p = a.active_period.add()
        p.start = 100
        ie = a.informed_entity.add()
        ie.route_id = "R1"
        tr = a.header_text.translation.add()
        tr.text = text
        tr.language = "en"
        return msg.SerializeToString()

    out1 = decode("alpha", "service_alerts", build("Detour on R1"), NOW)
    out2 = decode("alpha", "service_alerts", build("Detour on R1"), NOW)
    out3 = decode("alpha", "service_alerts", build("Detour on R1 (extended)"), NOW)
    r1 = _rowdict(tables.SERVICE_ALERTS, out1.batches[0].rows[0])
    r2 = _rowdict(tables.SERVICE_ALERTS, out2.batches[0].rows[0])
    r3 = _rowdict(tables.SERVICE_ALERTS, out3.batches[0].rows[0])
    assert r1["alert_hash"] == r2["alert_hash"] != r3["alert_hash"]
    assert r1["alert_id"] == "A1"
    assert r1["cause"] == "CONSTRUCTION"
    assert r1["effect"] == "DETOUR"
    assert r1["header_text"] == "Detour on R1"
    assert r1["active_periods"] == [{"start": 100, "end": None}]
    assert r1["informed_entities"] == [{"route_id": "R1"}]
    assert r1["first_seen"] == r1["last_seen"]


def test_insert_sql_shapes():
    sql = tables.VEHICLE_POSITIONS.insert_sql
    assert sql.startswith("INSERT INTO vehicle_positions (time, feed_id, vehicle_id")
    assert sql.endswith(f"${len(tables.VEHICLE_POSITIONS.columns)}) ON CONFLICT DO NOTHING")
    assert "DO UPDATE" in tables.SERVICE_ALERTS.insert_sql
