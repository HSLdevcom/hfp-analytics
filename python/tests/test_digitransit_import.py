"""Tests for flattening the Digitransit GraphQL result into jore_stop rows."""

import pytest
from api.digitransit_import import (
    flatten_result,
    has_numeric_stop_id,
    make_flat_row,
    make_route_dir,
    split_gtfs_id,
)


def gql_stop(gtfs_id, patterns=None, parent_station=None, vehicle_mode="BUS"):
    return {
        "gtfsId": gtfs_id,
        "code": "1234",
        "name": "Testikatu",
        "lat": 60.1,
        "lon": 24.9,
        "parentStation": (
            {"gtfsId": parent_station} if parent_station is not None else None
        ),
        "vehicleMode": vehicle_mode,
        "patterns": patterns,
    }


def pattern(route_gtfs_id, direction_id=0):
    return {"route": {"gtfsId": route_gtfs_id}, "directionId": direction_id}


@pytest.mark.parametrize(
    "gtfs_id, expected",
    [
        ("HSL:1140439", ("HSL", "1140439")),
        ("Uber:ita-pakila", ("Uber", "ita-pakila")),
        # A feed-local id containing a colon must stay intact.
        ("HSL:1140439:0:01", ("HSL", "1140439:0:01")),
    ],
)
def test_split_gtfs_id(gtfs_id, expected):
    assert split_gtfs_id(gtfs_id) == expected


@pytest.mark.parametrize(
    "gtfs_id, expected",
    [
        ("HSL:1140439", True),
        ("HSL: 1140439 ", True),
        # Stops of the commercial ferry feed have numeric ids and are imported.
        ("HSLlautta:384816", True),
        # GTFS-Flex on-demand areas of the Uber feed, the cause of the crash.
        ("Uber:ita-pakila", False),
        ("Uber:postipuisto-kapyla", False),
        ("HSL:not-a-number", False),
    ],
)
def test_has_numeric_stop_id(gtfs_id, expected):
    assert has_numeric_stop_id(gtfs_id) is expected


def test_make_flat_row_of_numeric_stop():
    row = make_flat_row(
        gql_stop(
            "HSL:1140439",
            patterns=[pattern("HSL:1055", 0), pattern("HSL:1055", 1)],
            parent_station="HSL:1000001",
        )
    )
    assert row["stop_id"] == 1140439
    assert row["parent_station"] == 1000001
    assert row["route_dirs_via_stop"] == "{1055-1,1055-2}"


@pytest.mark.parametrize("route_id", ["1055", "2550A", "1002H"])
def test_make_route_dir_keeps_non_numeric_route_ids(route_id):
    """Route ids are not numeric in JORE, so they must not be filtered like stop ids."""
    assert make_route_dir(pattern(f"HSL:{route_id}", 0)) == f"{route_id}-1"


def test_make_flat_row_ignores_non_numeric_parent_station():
    row = make_flat_row(gql_stop("HSL:1140439", parent_station="Uber:ita-pakila"))
    assert row["stop_id"] == 1140439
    assert row["parent_station"] is None


def test_flatten_result_skips_non_numeric_gtfs_ids():
    res = {
        "data": {
            "stops": [
                gql_stop("HSL:1140439"),
                gql_stop("Uber:ita-pakila", vehicle_mode="TAXI"),
                gql_stop("Uber:postipuisto", vehicle_mode="TAXI"),
                gql_stop("HSL:1204101", vehicle_mode="FERRY"),
                # Numeric ids of other feeds keep being imported as before.
                gql_stop("HSLlautta:384816", vehicle_mode=None),
            ]
        }
    }
    rows = flatten_result(res)
    assert [row["stop_id"] for row in rows] == [1140439, 384816]


def test_flatten_result_of_numeric_gtfs_ids_only():
    res = {"data": {"stops": [gql_stop("HSL:1140439"), gql_stop("HSL:1204101")]}}
    rows = flatten_result(res)
    assert [row["stop_id"] for row in rows] == [1140439, 1204101]
