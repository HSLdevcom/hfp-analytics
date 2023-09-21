"""
Services related to /journeys data endpoint
"""
from common.database import pool
from datetime import date
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
            hashmap = {}
            for item in data:
                key = (item['route_id'], item['direction_id'], item['oday'], item['start'])
                
                if item['arr_count'] < 2:
                    continue

                if key in hashmap:
                    hashmap[key].append(item)
                else:
                    hashmap[key] = [item]

            items_to_keep = []

            for key, value_list in hashmap.items():
                if len(value_list) > 1:
                    max_arr_count = max(value['arr_count'] for value in value_list)
                    
                    for value in value_list:
                        if value['arr_count'] >= 0.9 * max_arr_count:
                            items_to_keep.append(value)
                else:
                    items_to_keep.append(value_list[0])

            hashmap = {}
            final_data = []

            root_keys = ['route_id', 'direction_id', 'oday', 'start', 'transport_mode']
            vehicle_keys = ['oper', 'operator_id', 'vehicle_number', 'max_tst', 'min_tst', 'modified_at']

            for item in items_to_keep:
                # Filter out items with arr_count less than 2
                if item['arr_count'] < 2:
                    continue

                key = tuple(item[k] for k in root_keys)
                if key in hashmap:
                    hashmap[key]['vehicles'].append({k: item[k] for k in vehicle_keys})
                else:
                    new_item = {k: item[k] for k in root_keys}
                    new_item['vehicles'] = [{k: item[k] for k in vehicle_keys}]
                    hashmap[key] = new_item

            final_data = list(hashmap.values())

            return final_data


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
