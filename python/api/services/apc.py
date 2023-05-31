"""
Services related to /apc endpoint
"""

from typing import Optional
from datetime import datetime

from psycopg.rows import dict_row

from common.database import pool


async def get_apc_data(
    route_id: Optional[str],
    operator_id: Optional[int],
    vehicle_number: Optional[int],
    from_tst: datetime,
    to_tst: datetime,
) -> list[dict]:
    """Query apc raw data filtered by parameters to JSON."""
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT
                    tst,
                    received_at,
                    operator_id,
                    veh,
                    mode,
                    route,
                    dir,
                    oday,
                    start,
                    oper,
                    stop,
                    vehicle_load,
                    vehicle_load_ratio,
                    door_counts,
                    count_quality,
                    long,
                    lat
                FROM api.view_as_original_apc_event
                WHERE
                    (%(route_id)s::text IS NULL OR route = %(route_id)s) AND
                    (
                        (%(operator_id)s::int IS NULL AND %(veh)s::int IS NULL ) OR
                        (operator_id = %(operator_id)s AND veh = %(veh)s)
                    ) AND
                    tst >= %(from_tst)s AND tst <= %(to_tst)s
                """,
                {
                    "route_id": route_id,
                    "operator_id": operator_id,
                    "veh": vehicle_number,
                    "from_tst": from_tst.isoformat(),
                    "to_tst": to_tst.isoformat(),
                },
            )
            data = await cur.fetchall()
    return data
