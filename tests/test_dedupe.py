"""Deduper: identical re-observations are dropped within the window, changes and heartbeats go through."""

from datetime import date, datetime, timezone

from transponder import tables
from transponder.dedupe import Deduper
from transponder.rt import Batch

T0 = datetime(2026, 9, 12, 1, 0, tzinfo=timezone.utc)
NOW = T0.timestamp()


def stu(trip="T1", seq=3, stop="S3", delay=60, entity="0", t=T0, fetched=T0):
    return (t, "alpha", entity, trip, date(2026, 9, 12), seq, stop,
            delay, None, None, delay, None, None, "SCHEDULED", fetched)


def tu(trip="T1", entity="0", delay=None, vehicle="bus-1", t=T0):
    return (t, "alpha", entity, trip, date(2026, 9, 12), "08:00:00", "R1", 0, "SCHEDULED",
            vehicle, vehicle, delay, 5, t, t)


def vp(vehicle="bus-1", lat=1.0, t=T0):
    return (t, "alpha", vehicle, vehicle, None, None, "T1", "R1", 0, date(2026, 9, 12), None, None,
            lat, 2.0, None, None, None, None, None, None, None, None, None, t, t)


def run(d, table, rows, now=NOW):
    return d.filter(Batch(table, rows), now).rows


def test_identical_rows_are_suppressed_within_window():
    d = Deduper(3600)
    first = run(d, "stop_time_updates", [stu(seq=1), stu(seq=2)])
    assert len(first) == 2
    later = run(d, "stop_time_updates", [stu(seq=1), stu(seq=2)], NOW + 30)
    assert later == []
    assert d.suppressed["stop_time_updates"] == 2


def test_changed_payload_is_written_and_becomes_the_reference():
    d = Deduper(3600)
    run(d, "stop_time_updates", [stu(delay=60)])
    assert run(d, "stop_time_updates", [stu(delay=90)], NOW + 30) == [stu(delay=90)]
    assert run(d, "stop_time_updates", [stu(delay=90)], NOW + 60) == []
    # flipping back is a change too
    assert run(d, "stop_time_updates", [stu(delay=60)], NOW + 90) == [stu(delay=60)]


def test_unchanged_row_is_reasserted_once_per_window():
    d = Deduper(3600)
    run(d, "stop_time_updates", [stu()])
    assert run(d, "stop_time_updates", [stu()], NOW + 3599) == []
    assert run(d, "stop_time_updates", [stu()], NOW + 3601) == [stu()]
    assert run(d, "stop_time_updates", [stu()], NOW + 3630) == []


def test_observation_time_columns_do_not_count_as_change():
    d = Deduper(3600)
    t1 = T0.replace(minute=1)
    run(d, "stop_time_updates", [stu()])
    assert run(d, "stop_time_updates", [stu(t=t1, fetched=t1)], NOW + 60) == []
    run(d, "trip_updates", [tu()])
    assert run(d, "trip_updates", [tu(t=t1)], NOW + 60) == []
    run(d, "vehicle_positions", [vp()])
    assert run(d, "vehicle_positions", [vp(t=t1)], NOW + 60) == []
    assert run(d, "vehicle_positions", [vp(lat=1.5, t=t1)], NOW + 60) == [vp(lat=1.5, t=t1)]


def test_identity_uses_trip_id_and_ignores_positional_entity_ids():
    d = Deduper(3600)
    run(d, "stop_time_updates", [stu(entity="0")])
    # BC Transit numbers entities by position, so the same trip may move.
    assert run(d, "stop_time_updates", [stu(entity="7")], NOW + 30) == []
    # Without a trip_id the entity id is the identity.
    run(d, "trip_updates", [tu(trip=None, entity="x")])
    assert run(d, "trip_updates", [tu(trip=None, entity="x")], NOW + 30) == []
    assert run(d, "trip_updates", [tu(trip=None, entity="y")], NOW + 30) == [tu(trip=None, entity="y")]


def test_different_stops_and_trips_are_independent():
    d = Deduper(3600)
    rows = [stu(trip="T1", seq=1), stu(trip="T1", seq=2), stu(trip="T2", seq=1)]
    assert run(d, "stop_time_updates", rows) == rows
    assert run(d, "stop_time_updates", [stu(trip="T3", seq=1)], NOW + 30) == [stu(trip="T3", seq=1)]


def test_tables_without_identity_and_disabled_window_pass_through():
    d = Deduper(3600)
    fetch = (T0, "alpha", "vehicle_positions", 200, 1, 10, 5, None, None, "K1")
    assert run(d, "rt_fetches", [fetch]) == [fetch]
    assert run(d, "rt_fetches", [fetch], NOW + 1) == [fetch]
    off = Deduper(0)
    assert run(off, "stop_time_updates", [stu()]) == [stu()]
    assert run(off, "stop_time_updates", [stu()], NOW + 1) == [stu()]


def test_entries_older_than_window_are_evicted():
    d = Deduper(3600)
    run(d, "stop_time_updates", [stu(seq=i) for i in range(50)])
    assert d.size() == 50
    run(d, "stop_time_updates", [stu(seq=99)], NOW + 3601)
    assert d.size() == 1


def test_identity_columns_exist():
    for spec in tables.RT_TABLES.values():
        for ident in spec.identity:
            for name in ((ident,) if isinstance(ident, str) else ident):
                assert name in spec.columns
        for name in spec.observed_at:
            assert name in spec.columns
