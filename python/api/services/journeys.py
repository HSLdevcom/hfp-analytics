"""
Services related to /journeys data endpoint
"""
from common.database import pool
from datetime import date, timedelta, datetime
from pprint import pprint


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
                {"oday": oday}
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
                    "modified_at": r[11].isoformat(timespec="seconds")
                }
                for r in rows
            ]
            filtered_data = filter_data(data)
            converted_data = convert_to_30h_clock(filtered_data)
            return converted_data


def filter_data(data):
    hashmap = {}
    filtered_keys = set()

    for item in data:
        key = (item['route_id'], item['direction_id'], item['oday'], item['start'])
        item['filtered'] = False

        if key in hashmap:
            hashmap[key].append(item)
        else:
            hashmap[key] = [item]

    items_to_keep = []

    for key, vehicle_list in hashmap.items():
        if len(vehicle_list) > 1:
            max_arr_count = max(vehicle['arr_count'] for vehicle in vehicle_list)
            
            for vehicle in vehicle_list:
                if vehicle['arr_count'] >= 0.9 * max_arr_count:
                    items_to_keep.append(vehicle)
                else:
                    filtered_keys.add(key)
                    vehicle['filtered'] = True
        else:
            items_to_keep.append(vehicle_list[0])

    hashmap = {}
    filtered_data = []

    root_keys = ['route_id', 'direction_id', 'oday', 'start', 'transport_mode']
    vehicle_keys = ['oper', 'operator_id', 'vehicle_number', 'max_tst', 'min_tst', 'modified_at']

    for item in items_to_keep:
        key = tuple(item[k] for k in root_keys)
        if key in hashmap:
            hashmap[key]['vehicles'].append({k: item[k] for k in vehicle_keys})
        else:
            new_item = {k: item[k] for k in root_keys}
            new_item['vehicles'] = [{k: item[k] for k in vehicle_keys}]
            
            itemKey = (item['route_id'], item['direction_id'], item['oday'], item['start'])
            if itemKey in filtered_keys:
                new_item['filtered'] = True
            hashmap[key] = new_item

    filtered_data = list(hashmap.values())
    return filtered_data


def convert_to_30h_clock(data):
    for item in data:
        oday = item["oday"]
        next_day = oday + timedelta(days=1)

        for vehicle in item["vehicles"]:
            for timestamp_key in ["max_tst", "min_tst"]:
                timestamp = vehicle[timestamp_key]
                calendar_date = timestamp.strftime("%Y-%m-%d")
                vehicle["calendar_date"] = calendar_date
                if isinstance(timestamp, str):
                    timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f%z")

                if timestamp.date() == next_day and timestamp.hour < 7:
                    new_hour = timestamp.hour + 24
                    formatted_date = oday.strftime("%Y-%m-%d")
                    formatted_time = "{:02}:{:02}:{:02}.{:06}+00:00".format(
                        new_hour, timestamp.minute, timestamp.second, timestamp.microsecond
                    )
                    new_timestamp_str = "{}T{}".format(formatted_date, formatted_time)
                    vehicle[timestamp_key] = new_timestamp_str

    return data


async def get_last_modified_of_oday(oday: date):
    """Query the last timestamp when a certain oday has been analyzed and updated in the database"""
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT MAX(modified_at) FROM api.view_assumed_monitored_vehicle_journey WHERE oday = %(oday)s",
                {"oday": oday}
            )
            rows = await cur.fetchone()
            return rows[0] if rows else None
