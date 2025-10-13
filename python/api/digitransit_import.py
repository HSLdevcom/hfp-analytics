# Import current stops, stations and route-direction combos using them from Digitransit.

import csv
from datetime import date

import psycopg2
import requests
from common.config import DIGITRANSIT_APIKEY, POSTGRES_CONNECTION_STRING
from psycopg2 import sql

GRAPHQL_URL = "https://api.digitransit.fi/routing/v2/hsl/gtfs/v1"


def create_query(query_type):
    assert query_type in ("stops", "stations")
    query = """
    {
        <PLACEHOLDER> {
          gtfsId
          code
          name
          lat
          lon
          parentStation { gtfsId }
          vehicleMode
          patterns {
            route { gtfsId }
            directionId
          }
        }
    }
    """
    return query.replace("<PLACEHOLDER>", query_type)


def get_query(query):
    req = requests.post(
        url=GRAPHQL_URL,
        data=query,
        headers={
            "Content-Type": "application/graphql",
            "digitransit-subscription-key": DIGITRANSIT_APIKEY,
        },
    )
    if req.status_code == 200:
        return req.json()
    else:
        raise Exception(f"{req} failed with status code {req.status_code}")


def make_route_dir(pattern):
    route = pattern["route"]["gtfsId"].split(":")[1].strip()
    # NOTE: Digitransit uses 0/1 directions, while Jore uses 1/2 directions
    dir = str(pattern["directionId"] + 1)
    return f"{route}-{dir}"


def make_flat_row(gql_row):
    parent_station = None
    if gql_row["parentStation"] is not None:
        parent_station = gql_row["parentStation"]["gtfsId"]
        parent_station = int(parent_station.split(":")[1].strip())
    route_dirs_via_stop = None
    if gql_row["patterns"] is not None and len(gql_row["patterns"]) > 0:
        route_dirs_via_stop = list(map(make_route_dir, gql_row["patterns"]))
        route_dirs_via_stop = sorted(list(set(route_dirs_via_stop)))
        route_dirs_via_stop = "{" + ",".join(route_dirs_via_stop) + "}"
    return {
        "stop_id": int(gql_row["gtfsId"].split(":")[1].strip()),
        "stop_code": gql_row["code"],
        "stop_name": gql_row["name"],
        "parent_station": parent_station,
        "stop_mode": gql_row["vehicleMode"],
        "route_dirs_via_stop": route_dirs_via_stop,
        "date_imported": str(date.today()),
        "long": gql_row["lon"],
        "lat": gql_row["lat"],
    }


def flatten_result(res):
    assert len(res["data"].keys()) == 1
    rows = list(res["data"].values())[0]
    filtered_rows = [row for row in rows if row["vehicleMode"] != "FERRY"]
    return list(map(make_flat_row, filtered_rows))


def write_to_file(flat_res, to_file="/tmp/tmp.csv"):
    assert len(flat_res) > 0
    assert all(map(lambda x: isinstance(x, dict), flat_res))
    assert all(map(lambda x: x.keys() == flat_res[0].keys(), flat_res[1:]))
    with open(to_file, "w", newline="") as fobj:
        fieldnames = list(flat_res[0].keys())
        writer = csv.DictWriter(fobj, fieldnames=fieldnames, delimiter="\t")
        # NOTE: We are NOT writing headers since copy_from reads without them.
        writer.writerows(flat_res)


def copy_from_file_to_db(conn, to_table, from_file="/tmp/tmp.csv"):
    with open(from_file, mode="r") as fobj:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    "WITH deleted AS (DELETE FROM {} RETURNING 1)\
                                SELECT count(*) FROM deleted"
                ).format(sql.Identifier(*to_table.split(".")))
            )
            print(f'{cur.fetchone()[0]} rows deleted from "{to_table}"')
            cur.copy_expert(
                sql=sql.SQL(
                    "COPY {} FROM STDIN WITH (FORMAT CSV, DELIMITER '\t')"
                ).format(sql.Identifier(*to_table.split("."))),
                file=fobj,
            )
            cur.execute(
                sql.SQL("SELECT count(*) FROM {}").format(
                    sql.Identifier(*to_table.split("."))
                )
            )
            print(f'{cur.fetchone()[0]} rows inserted into "{to_table}"')


def import_dataset(query_type, to_table, conn):
    query = create_query(query_type)
    print(f"Fetching {query_type} ...")
    res = get_query(query)
    res = flatten_result(res)
    write_to_file(res)
    copy_from_file_to_db(conn, to_table)
    print(f"{query_type} done")


def main():
    conn = None
    try:
        with psycopg2.connect(POSTGRES_CONNECTION_STRING) as conn:
            import_dataset(
                query_type="stations", to_table="jore.jore_station", conn=conn
            )
            import_dataset(query_type="stops", to_table="jore.jore_stop", conn=conn)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
