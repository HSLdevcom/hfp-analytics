"""
Services related to /journeys data endpoint
"""
from common.database import pool
from datetime import date


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
                    "min_timestamp": r[8],
                    "max_timestamp": r[9],
                    "modified_at": r[10].isoformat(timespec="seconds")
                }
                for r in rows
            ]

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
