"""
Services related to /hfp data endpoint
"""

from io import BytesIO
from typing import Optional
from datetime import datetime

from common.database import pool


async def get_hfp_data(
    route_id: Optional[str],
    operator_id: Optional[int],
    vehicle_number: Optional[int],
    event_types: Optional[str],
    from_tst: datetime,
    to_tst: datetime,
    stream: BytesIO,
) -> int:
    """
    Query hfp raw data filtered by parameters to CSV format. Save the result to the input stream.
    Return row count.
    """
    event_types_list = []
    event_types_filter = "TRUE" 
    
    if event_types:
        event_types_list = event_types.split(',')
        event_types_filter = "event_type = ANY(%(event_types_list)s)"

    query = f"""
        COPY (
            SELECT
                *
            FROM api.view_as_original_hfp_event
            WHERE
                (%(route_id)s IS NULL OR route_id = %(route_id)s) AND
                (
                    (%(operator_id)s IS NULL AND %(vehicle_number)s IS NULL ) OR
                    (operator_id = %(operator_id)s AND vehicle_number = %(vehicle_number)s)
                ) AND
                tst >= %(from_tst)s AND tst <= %(to_tst)s AND
                {event_types_filter}
        ) TO STDOUT WITH CSV HEADER
    """

    async with pool.connection() as conn:
        async with conn.cursor().copy(
            query,
            {
                "route_id": route_id,
                "operator_id": operator_id,
                "vehicle_number": vehicle_number,
                "event_types_list": event_types_list,
                "from_tst": from_tst.isoformat(),
                "to_tst": to_tst.isoformat(),
            },
        ) as copy:
            row_count = -1  # Header is always the first row

            async for row in copy:
                row_count += 1
                stream.write(row)
        return row_count
