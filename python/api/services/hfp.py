"""
Services related to /hfp data endpoint
"""

from io import BytesIO
from datetime import datetime
import logging
from typing import Optional, List

from common.database import pool
from common.logger_util import CustomDbLogHandler
from common.container_client import FlowAnalyticsContainerClient
from api.models.hfp import PreprocessBlobModel


logger = logging.getLogger("api")

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

async def get_speeding_data(
    route_id: int,
    min_spd: int,
    from_tst: datetime,
    to_tst: datetime,
    x_min: int,
    y_min: int,
    x_max: int,
    y_max: int,
    stream: BytesIO,
):
    # Speed limit given in km/h. Convert to m/s
    min_spd = min_spd / 3.6 

    async with pool.connection() as conn:
        async with conn.cursor().copy(
            """
            COPY (
                SELECT
                    (hp.spd * 3.6) AS spd_km,
                    hp.oday,
                    hp."start",
                    hp.direction_id,
                    hp.vehicle_number,
                    hp.point_timestamp
                FROM
                    hfp.hfp_point hp
                WHERE
                    hp.spd > %(min_spd)s
                    AND hp.point_timestamp > %(from_tst)s
                    AND hp.point_timestamp < %(to_tst)s
                    AND hp.route_id = '%(route_id)s'
                    AND hp.hfp_event = 'VP'
                    AND hp.geom && ST_MakeEnvelope(%(x_min)s, %(y_min)s, %(x_max)s, %(y_max)s, 3067)
            ) TO STDOUT WITH CSV HEADER
            """,
            {
                "min_spd": min_spd,
                "from_tst": from_tst,
                "to_tst": to_tst,
                "route_id": route_id,
                "x_min": x_min,
                "y_min": y_min,
                "x_max": x_max,
                "y_max": y_max,
            },
        ) as copy:
            row_count = 0
            async for row in copy:
                row_count += 1
                stream.write(row)
        return row_count


async def upload_missing_preprocess_data_to_db(
    client: FlowAnalyticsContainerClient, 
    missing_blobs: List[PreprocessBlobModel], 
    preprocess_type: str
) -> None:
    blobs_count = len(missing_blobs)
    
    logger.debug(
        f"starting to import missing blobs one by one, total = {blobs_count}]"
    )
    
    for blob_id, missing_blob in enumerate(missing_blobs):
        logger.debug(f'starting to import missing blob to database: {missing_blob.blob_path}, [{blob_id + 1}/{blobs_count}]')
        compressed_csv = await client.load_blob(missing_blob.blob_path)
        table_name = f"delay.preprocess_{preprocess_type}"
        query = f"""
            INSERT INTO {table_name} (route_id, oday, mode, zst)
            VALUES (%(route_id)s, %(oday)s, %(mode)s, %(zst)s);
        """
        async with pool.connection() as conn:
            await conn.execute(
                query,
                {
                    "route_id": missing_blob.route_id,
                    "oday": missing_blob.oday,
                    "mode": missing_blob.mode,
                    "zst": compressed_csv,
                },
            )
        logger.debug(
            f"Successfully added missing blob, [{blob_id + 1}/{blobs_count}] {missing_blob.blob_path}"
        )