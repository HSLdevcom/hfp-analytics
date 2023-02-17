""" Routes for /hfp endpoint """

import io
import gzip
from typing import Optional
from datetime import date, datetime
import time
import logging
from common.logger_util import CustomDbLogHandler
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from api.services.hfp import get_hfp_data

logger = logging.getLogger('api')

router = APIRouter(
    prefix="/hfp",
    tags=["HFP data"]
)


@router.get("/data",
            summary="Get HFP raw data",
            description="Returns raw HFP data in a gzip compressed csv file.")
async def get_hfp_raw_data(
    route_id: Optional[str] = Query(default=None,
                                    title="Route ID",
                                    description="JORE ID of the route. Required when no `oper` and `veh` provided.",
                                    example="2550"),
    oper: Optional[int] = Query(default=None,
                                title="Operator ID",
                                description="Operator ID of the vehicle. Required when no `route_id` provided.",
                                example="18"),
    veh: Optional[int] = Query(default=None,
                               description="Vehicle ID. Required when no `route_id` provided.",
                               example="662"),
    oday: date = Query(...,
                       title="Operating day",
                       description=("Operating day of the journey. "
                                    "Remember that the database contains data from previous 14 days. "
                                    "Format YYYY-MM-DD"),
                       example="2023-01-12")
) -> Response:
    """
    Get hfp data in raw csv format filtered by parameters.
    """
    with CustomDbLogHandler('api'):
        fetch_start_time = time.time()
        logger.debug(f"Fetching raw hfp data. route_id: {route_id}, oper: {oper}, veh: {veh}, oday: {oday}")
        if not route_id and not (oper and veh):
            logger.error("Missing required parameters.")
            raise HTTPException(400, detail="Either route_id or oper and veh -parameters are required!")

        # Input stream for csv data from database, output stream for compressed data
        input_stream = io.BytesIO()
        output_stream = io.BytesIO()

        await get_hfp_data(route_id, oper, veh, oday, input_stream)
        logger.debug("Hfp data received. Compressing.")
        data = input_stream.getvalue()

        with gzip.GzipFile(fileobj=output_stream, mode='wb') as compressed_data_stream:
            compressed_data_stream.write(data)

        response = Response(content=output_stream.getvalue(),
                            media_type="application/gzip")

        filename = f"hfp-export-{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv.gz"
        # Send as an attachment
        response.headers["Content-Disposition"] = f"attachment; filename={filename}"
        duration = time.time() - fetch_start_time
        logger.debug(f"Hfp raw data fetch finished in {int(duration)} seconds. Exported file: {filename}")
        return response
