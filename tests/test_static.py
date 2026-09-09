import io
import zipfile
from datetime import date

import pytest

from transponder import static

FILES = {
    "agency.txt": "agency_name,agency_url,agency_timezone\nAlpha,https://a.example,America/Vancouver\n",
    "routes.txt": "route_id,agency_id,route_short_name,route_long_name,route_type\nR1,,1,Downtown,3\n",
    "stops.txt": "﻿stop_id,stop_name,stop_lat,stop_lon,location_type\nS1,Main St,49.28,-123.12,\nS2,Second St,49.29,-123.11,0\n",
    "trips.txt": "route_id,service_id,trip_id,direction_id,trip_headsign\nR1,WEEK,T1,0,Downtown\n",
    "stop_times.txt": (
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence,extra_col\n"
        "T1,08:00:00,08:00:30,S1,1,x\n"
        "T1, 25:15:00 ,25:15:00,S2,2,y\n"
    ),
    "calendar.txt": "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\nWEEK,1,1,1,1,1,0,0,20260901,20261231\n",
}


def build_zip(files: dict[str, str], prefix: str = "") -> zipfile.ZipFile:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, content in files.items():
            zf.writestr(prefix + name, content)
    buf.seek(0)
    return zipfile.ZipFile(buf)


def rows_for(zf: zipfile.ZipFile, filename: str) -> list[tuple]:
    table = next(t for t in static.STATIC_TABLES if t.file == filename)
    members = static.zip_members(zf)
    return list(static.iter_rows(zf, members[filename], table, "alpha", 7))


def test_members_and_validation():
    zf = build_zip(FILES, prefix="google_transit/")
    members = static.zip_members(zf)
    assert members["stop_times.txt"] == "google_transit/stop_times.txt"
    static.validate_members(members)

    missing = {k: v for k, v in FILES.items() if k != "trips.txt"}
    with pytest.raises(ValueError, match="trips.txt"):
        static.validate_members(static.zip_members(build_zip(missing)))

    no_cal = {k: v for k, v in FILES.items() if k != "calendar.txt"}
    with pytest.raises(ValueError, match="calendar"):
        static.validate_members(static.zip_members(build_zip(no_cal)))


def test_stop_times_parsing():
    rows = rows_for(build_zip(FILES), "stop_times.txt")
    assert len(rows) == 2
    feed_id, version_id, trip_id, arr, arr_secs, dep, dep_secs, stop_id, seq, *_ = rows[1]
    assert (feed_id, version_id, trip_id) == ("alpha", 7, "T1")
    assert arr == "25:15:00" and arr_secs == 25 * 3600 + 15 * 60
    assert dep_secs == arr_secs
    assert stop_id == "S2" and seq == 2
    assert rows[0][4] == 8 * 3600


def test_stops_bom_and_blank_ints():
    rows = rows_for(build_zip(FILES), "stops.txt")
    table = next(t for t in static.STATIC_TABLES if t.file == "stops.txt")
    r = dict(zip(table.copy_columns, rows[0]))
    assert r["stop_id"] == "S1"  # BOM stripped from header
    assert r["stop_lat"] == 49.28
    assert r["location_type"] is None  # blank -> NULL
    assert r["platform_code"] is None  # absent column -> NULL


def test_agency_id_defaults_to_empty_string():
    rows = rows_for(build_zip(FILES), "agency.txt")
    assert rows[0][2] == ""


def test_calendar_dates_parsed():
    rows = rows_for(build_zip(FILES), "calendar.txt")
    table = next(t for t in static.STATIC_TABLES if t.file == "calendar.txt")
    r = dict(zip(table.copy_columns, rows[0]))
    assert r["start_date"] == date(2026, 9, 1)
    assert r["end_date"] == date(2026, 12, 31)
    assert r["saturday"] == 0


def test_bad_time_reports_line():
    files = dict(FILES, **{"stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nT1,8am,,S1,1\n"})
    with pytest.raises(ValueError, match="stop_times.txt line 2"):
        rows_for(build_zip(files), "stop_times.txt")


def test_gtfs_time_to_secs():
    assert static.gtfs_time_to_secs("00:00:00") == 0
    assert static.gtfs_time_to_secs("") is None
    assert static.gtfs_time_to_secs(None) is None
    assert static.gtfs_time_to_secs("27:59:59") == 27 * 3600 + 59 * 60 + 59


async def test_async_records_streams_all_rows():
    zf = build_zip(FILES)
    table = next(t for t in static.STATIC_TABLES if t.file == "stop_times.txt")
    member = static.zip_members(zf)["stop_times.txt"]
    rows = [r async for r in static._async_records(zf, member, table, "alpha", 1)]
    assert len(rows) == 2


def test_duplicate_keys_first_row_wins():
    files = dict(FILES, **{"stops.txt": "stop_id,stop_name\nS1,First\nS2,Other\nS1,Second\n"})
    rows = rows_for(build_zip(files), "stops.txt")
    assert [(r[2], r[4]) for r in rows] == [("S1", "First"), ("S2", "Other")]


def test_stop_times_are_not_deduplicated():
    files = dict(FILES, **{"stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nT1,08:00:00,08:00:00,S1,1\nT1,08:00:00,08:00:00,S1,1\n"})
    assert len(rows_for(build_zip(files), "stop_times.txt")) == 2
