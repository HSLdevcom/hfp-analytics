"""
Services related to /hfp data endpoint
"""

from io import BytesIO
from typing import Optional
from datetime import datetime

from common.database import pool


async def get_hfp_data(route_id: Optional[str],
                       operator_id: Optional[int],
                       vehicle_number: Optional[int],
                       from_tst: datetime,
                       to_tst: datetime,
                       stream: BytesIO) -> None:
    """Query hfp raw data filtered by parameters to CSV format. Save the result to the input stream."""
    async with pool.connection() as conn:
        async with conn.cursor().copy(
            """
            COPY (
                SELECT
                    tst,
                    event_type,
                    route_id,
                    direction_id,
                    vehicle_operator_id,
                    observed_operator_id,
                    vehicle_number,
                    oday,
                    "start",
                    stop,
                    loc,
                    latitude,
                    longitude,
                    odo,
                    spd,
                    drst
                FROM api.view_as_original_hfp_event
                WHERE
                    (%(route_id)s IS NULL OR route_id = %(route_id)s) AND
                    (
                        (%(operator_id)s IS NULL AND %(vehicle_number)s IS NULL ) OR
                        (vehicle_operator_id = %(operator_id)s AND vehicle_number = %(vehicle_number)s)
                    ) AND
                    tst >= %(from_tst)s AND tst <= %(to_tst)s
            ) TO STDOUT WITH CSV HEADER
            """,
                {
                    "route_id": route_id,
                    "operator_id": operator_id,
                    "veh": vehicle_number,
                    "from_tst": from_tst.isoformat(),
                    "to_tst": to_tst.isoformat()
                }) as copy:
            async for row in copy:
                stream.write(row)
