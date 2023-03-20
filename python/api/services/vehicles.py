"""
Services related to /vehicles data endpoint
"""
from common.database import pool
from datetime import date, datetime, time


async def get_vehicles_by_timestamp(date: date, operator_id: int) -> list:
    """Query all vehicles filtered by oday and return them as a list of dicts"""
    date_str = str(date)

    query_params = {
        "start": datetime.strptime(date_str + " 14:00:00.000+00", '%Y-%m-%d %H:%M:%S.%f+00'),
        "end": datetime.strptime(date_str + " 14:01:00.000+00", '%Y-%m-%d %H:%M:%S.%f+00')
    }

    where_clause = "WHERE tst > %(start)s AND tst < %(end)s AND event_type = 'VP'"
    
    if operator_id is not None:
        where_clause += " AND vehicle_operator_id = %(vehicle_operator_id)s"
        query_params["vehicle_operator_id"] = operator_id

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                    tst,
                    event_type,
                    received_at,
                    vehicle_operator_id,
                    vehicle_number,
                    transport_mode,
                    route_id,
                    direction_id,
                    oday,
                    start,
                    odo,
                    spd,
                    drst,
                    loc,
                    stop,
                    longitude,
                    latitude
                FROM api.view_as_original_hfp_event
                {where_clause}
                """.format(where_clause=where_clause),
                query_params
            )
            rows = await cur.fetchall()

            data = [
                {
                    "tst": r[0],
                    "event_type": r[1],
                    "received_at": r[2],
                    "operator_id": r[3],
                    "vehicle_number": r[4],
                    "transport_mode": r[5],
                    "route_id": r[6],
                    "direction_id": r[7],
                    "oday": r[8],
                    "start": r[9],
                    "odo": r[10],
                    "spd": r[11],
                    "drst": r[12],
                    "loc": r[13],
                    "stop": r[14],
                    "longitude": r[15],
                    "latitude": r[16]
                }
                for r in rows
            ]
            return data
