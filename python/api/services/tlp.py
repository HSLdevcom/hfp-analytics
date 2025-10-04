"""
Services related to /hfp data endpoint
"""

from datetime import datetime
from io import BytesIO
from typing import Optional

from common.database import pool
from psycopg.rows import dict_row


async def get_tlp_data(
    route_id: Optional[str],
    operator_id: Optional[str],
    vehicle_number: Optional[int],
    sid: Optional[int],
    from_tst: datetime,
    to_tst: datetime,
    stream: BytesIO,
) -> int:
    """
    Query tlp raw data filtered by parameters and return as CSV or JSON.
    """

    query = """
        COPY (
            SELECT
                *
            FROM api.view_as_original_tlp_event
            WHERE
                (%(route_id)s IS NULL OR route_id = %(route_id)s) AND
                (
                    (%(operator_id)s IS NULL AND %(vehicle_number)s IS NULL ) OR
                    (oper = %(operator_id)s AND vehicle_number = %(vehicle_number)s)
                ) AND
                tst >= %(from_tst)s AND tst <= %(to_tst)s AND
                (%(sid)s IS NULL OR sid = %(sid)s)
        ) TO STDOUT WITH CSV HEADER
    """

    async with pool.connection() as conn:
        async with conn.cursor().copy(
            query,
            {
                "route_id": route_id,
                "operator_id": operator_id,
                "vehicle_number": vehicle_number,
                "sid": sid,
                "from_tst": from_tst.isoformat(),
                "to_tst": to_tst.isoformat(),
            },
        ) as copy:
            row_count = -1  # Header is always the first row

            async for row in copy:
                row_count += 1
                stream.write(row)
        return row_count


async def get_tlp_data_as_json(
    route_id: Optional[str],
    operator_id: Optional[str],
    vehicle_number: Optional[int],
    sid: Optional[int],
    from_tst: datetime,
    to_tst: datetime,
) -> list[dict]:
    """Query TLP raw data filtered by parameters to JSON"""
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT * FROM api.view_as_original_tlp_event
                WHERE
                    (%(route_id)s::text IS NULL OR route_id = %(route_id)s) AND
                    (
                        (%(operator_id)s::int IS NULL AND %(vehicle_number)s::int IS NULL) OR
                        (oper = %(operator_id)s AND vehicle_number = %(vehicle_number)s)
                    ) AND
                    tst >= %(from_tst)s AND tst <= %(to_tst)s AND
                    (%(sid)s::int IS NULL OR sid = %(sid)s)
                """,
                {
                    "route_id": route_id,
                    "operator_id": operator_id,
                    "vehicle_number": vehicle_number,
                    "sid": sid,
                    "from_tst": from_tst.isoformat(),
                    "to_tst": to_tst.isoformat(),
                },
            )
            data = await cur.fetchall()
    return data
