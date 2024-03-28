"""
Services related to /hfp data endpoint
"""

from io import BytesIO
from datetime import datetime
from psycopg.rows import dict_row
from common.database import pool
from typing import Optional

async def get_tlr_data(
    route_id: Optional[str],
    operator_id: Optional[int],
    vehicle_number: Optional[int],
    from_tst: datetime,
    to_tst: datetime,
    stream: BytesIO,
) -> int:
    """
    Query tlr raw data filtered by parameters and return as CSV or JSON.
    """

    query = f"""
        COPY (
            SELECT
                *
            FROM api.view_as_original_tlr_event
            WHERE
                (%(route_id)s IS NULL OR route_id = %(route_id)s) AND
                (
                    (%(operator_id)s IS NULL AND %(vehicle_number)s IS NULL ) OR
                    (operator_id = %(operator_id)s AND vehicle_number = %(vehicle_number)s)
                ) AND
                tst >= %(from_tst)s AND tst <= %(to_tst)s
        ) TO STDOUT WITH CSV HEADER
    """

    async with pool.connection() as conn:
        async with conn.cursor().copy(
            query,
            {
                "route_id": route_id,
                "operator_id": operator_id,
                "vehicle_number": vehicle_number,
                "from_tst": from_tst.isoformat(),
                "to_tst": to_tst.isoformat(),
            },
        ) as copy:
            row_count = -1  # Header is always the first row

            async for row in copy:
                row_count += 1
                stream.write(row)
        return row_count

async def get_tlr_data_as_json(
    route_id: Optional[str],
    operator_id: Optional[int],
    vehicle_number: Optional[int],
    from_tst: datetime,
    to_tst: datetime,
) -> list[dict]:
    """Query TLR raw data filtered by parameters to JSON."""
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT * FROM api.view_as_original_tlr_event
                WHERE
                    (%(route_id)s::text IS NULL OR route_id = %(route_id)s) AND
                    (
                        (%(operator_id)s::int IS NULL AND %(vehicle_number)s::int IS NULL ) OR
                        (operator_id = %(operator_id)s AND vehicle_number = %(vehicle_number)s)
                    ) AND
                    tst >= %(from_tst)s AND tst <= %(to_tst)s
                """,
                {
                    "route_id": route_id,
                    "operator_id": operator_id,
                    "vehicle_number": vehicle_number,
                    "from_tst": from_tst.isoformat(),
                    "to_tst": to_tst.isoformat(),
                },
            )
            data = await cur.fetchall()
    return data




