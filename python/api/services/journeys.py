"""
Services related to /journeys data endpoint
"""

from datetime import date, datetime, timedelta

import pytz
from common.database import pool


async def get_journeys_by_oday(oday: date) -> list:
    """Query all monitored vehicle journeys filtered by oday and return them as a list of dicts"""
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                    route_id,
                    direction_id,
                    oday,
                    start_24h,
                    journey_operator_id,
                    vehicle_operator_id,
                    vehicle_number,
                    transport_mode,
                    min_timestamp,
                    max_timestamp,
                    arr_count,
                    modified_at
                FROM api.view_assumed_monitored_vehicle_journey
                WHERE oday = %(oday)s
                """,
                {"oday": oday},
            )
            rows = await cur.fetchall()

            data = [
                {
                    "route_id": r[0],
                    "direction_id": r[1],
                    "oday": r[2],
                    "start": r[3],
                    "oper": r[4],
                    "operator_id": r[5],
                    "vehicle_number": r[6],
                    "transport_mode": r[7],
                    "min_tst": r[8],
                    "max_tst": r[9],
                    "arr_count": r[10],
                    "modified_at": r[11].isoformat(timespec="seconds"),
                }
                for r in rows
            ]
            filtered_data = filter_data(data)
            adjusted_data = adjust_start_based_on_vehicle_tst(filtered_data)
            return adjusted_data


def filter_data(data):
    hashmap = {}
    filtered_keys = set()

    # We want to ignore journeys with less than two arrival events
    data = [item for item in data if item["arr_count"] >= 2]

    for item in data:
        key = (item["route_id"], item["direction_id"], item["oday"], item["start"])
        item["filtered"] = False

        if key in hashmap:
            hashmap[key].append(item)
        else:
            hashmap[key] = [item]

    items_to_keep = []

    for key, vehicle_list in hashmap.items():
        if len(vehicle_list) > 1:
            max_arr_count = max(vehicle["arr_count"] for vehicle in vehicle_list)

            for vehicle in vehicle_list:
                if vehicle["arr_count"] >= 0.9 * max_arr_count:
                    items_to_keep.append(vehicle)
                else:
                    filtered_keys.add(key)
                    vehicle["filtered"] = True
        else:
            items_to_keep.append(vehicle_list[0])

    hashmap = {}
    filtered_data = []

    root_keys = ["route_id", "direction_id", "oday", "start", "transport_mode"]
    vehicle_keys = [
        "oper",
        "operator_id",
        "vehicle_number",
        "max_tst",
        "min_tst",
        "modified_at",
    ]

    for item in items_to_keep:
        key = tuple(item[k] for k in root_keys)
        if key in hashmap:
            hashmap[key]["vehicles"].append({k: item[k] for k in vehicle_keys})
        else:
            new_item = {k: item[k] for k in root_keys}
            new_item["vehicles"] = [{k: item[k] for k in vehicle_keys}]

            itemKey = (
                item["route_id"],
                item["direction_id"],
                item["oday"],
                item["start"],
            )
            if itemKey in filtered_keys:
                new_item["filtered"] = True
            hashmap[key] = new_item

    filtered_data = list(hashmap.values())
    return filtered_data


def adjust_start_based_on_vehicle_tst(data):
    finnish_tz = pytz.timezone("Europe/Helsinki")

    for item in data:
        oday = item["oday"]
        if isinstance(oday, str):
            oday = datetime.strptime(oday, "%Y-%m-%d").date()

        start_time_str = item["start"]
        start_time = datetime.strptime(start_time_str, "%H:%M:%S")
        full_start_time = datetime.combine(oday, start_time.time())

        max_tst_local = None

        for vehicle in item["vehicles"]:
            max_tst = vehicle["max_tst"]
            if isinstance(max_tst, str):
                max_tst = datetime.strptime(max_tst, "%Y-%m-%dT%H:%M:%S.%f%z")

            max_tst_local_current = max_tst.astimezone(finnish_tz)

            if max_tst_local is None or max_tst_local_current > max_tst_local:
                max_tst_local = max_tst_local_current

        new_hour = full_start_time.hour
        if (
            max_tst_local.date() == (oday + timedelta(days=1))
            and 0 <= full_start_time.hour < 7
        ):
            new_hour = full_start_time.hour + 24

        formatted_time = "{:02}:{:02}:{:02}".format(
            new_hour, full_start_time.minute, full_start_time.second
        )

        new_start_str = formatted_time
        item["start_30h"] = new_start_str

        calendar_date_str = oday.strftime("%Y-%m-%d")
        for vehicle in item["vehicles"]:
            vehicle["calendar_date"] = calendar_date_str

    return data


async def get_last_modified_of_oday(oday: date):
    """Query the last timestamp when a certain oday has been analyzed and updated in the database"""
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT MAX(modified_at) FROM api.view_assumed_monitored_vehicle_journey WHERE oday = %(oday)s",
                {"oday": oday},
            )
            rows = await cur.fetchone()
            return rows[0] if rows else None
