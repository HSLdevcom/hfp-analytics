from io import BytesIO
from typing import Optional
from datetime import date

from common.database import pool


async def get_hfp_data(route_id: Optional[str],
                       oper: Optional[int],
                       veh: Optional[int],
                       oday: Optional[date],
                       stream: BytesIO) -> None:
    async with pool.connection() as conn:
        async with conn.cursor().copy(
            """
            COPY (
                SELECT
                    event_type,
                    route_id,
                    direction_id,
                    vehicle_operator_id,
                    observed_operator_id,
                    vehicle_number,
                    oday,
                    "start",
                    stop,
                    tst,
                    loc,
                    latitude,
                    longitude,
                    odo,
                    drst
                FROM hfp.view_as_original_hfp_event
                WHERE
                    (%(route_id)s IS NULL OR route_id = %(route_id)s) AND
                    (
                        (%(oper)s IS NULL AND %(veh)s IS NULL ) OR
                        (vehicle_operator_id = %(oper)s AND vehicle_number = %(veh)s)
                    ) AND
                    oday = %(oday)s
            ) TO STDOUT WITH CSV HEADER
            """,
                {"route_id": route_id, "oper": oper, "veh": veh, "oday": oday}) as copy:
            async for row in copy:
                stream.write(row)
