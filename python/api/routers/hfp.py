""" Routes for /hfp endpoint """

import io
import gzip
from typing import Optional
from datetime import datetime, timezone, timedelta

import time
import logging
from common.logger_util import CustomDbLogHandler

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from api.services.hfp import get_hfp_data

logger = logging.getLogger("api")

router = APIRouter(prefix="/hfp", tags=["HFP data"])


class GzippedFileResponse(Response):
    media_type = "application/gzip"

    def __init__(self, filename: str, content: bytes, status_code: int = 200) -> None:
        super().__init__(
            content=content,
            status_code=status_code,
            headers={"content-disposition": f'attachment; filename="{filename}"'},
        )


@router.get(
    "/data",
    summary="Get HFP raw data",
    description="Returns raw HFP data in a gzip compressed csv file.",
    response_class=GzippedFileResponse,
    responses={
        200: {
            "description": "Successful query. The data is returned as an attachment in the response.",
            "content": {"application/gzip": {"schema": None, "example": None}},
            "headers": {
                "Content-Disposition": {
                    "schema": {"example": 'attachment; filename="hfp-export-20230316-133132.csv.gz"'}
                }
            },
        }
    },
)
async def get_hfp_raw_data(
    route_id: Optional[str] = Query(
        default=None,
        title="Route ID",
        description="JORE ID of the route (in HFP topic). "
        "**Required** when no `operator_id` and `vehicle_number` provided.",
        example="2550",
    ),
    operator_id: Optional[int] = Query(
        default=None,
        title="Operator ID",
        description="Operator ID of the vehicle (operator_id in HFP topic). **Required** when no `route_id` provided.",
        example="18",
    ),
    vehicle_number: Optional[int] = Query(
        default=None,
        description="Vehicle number (in HFP topic). **Required** when no `route_id` provided.",
        example="662",
    ),
    from_tst: datetime = Query(
        title="Minimum timestamp",
        description=(
            "The timestamp from which the data will be queried. (tst in HFP payload) "
            "Timestamp will be read as UTC if `tz` parameter is not specified. "
            "Format `yyyy-MM-dd'T'HH:mm:ss`. "
            "Timestamp can be shortened - optional formats are `yyyy-MM-dd'T'HH:mm` "
            "and `yyyy-MM-dd` "
            "Remember that the database contains data from previous 14 days."
        ),
        example="2023-01-12T14:20:30",
    ),
    to_tst: Optional[datetime] = Query(
        default=None,
        title="Maximum timestamp",
        description=(
            "The timestamp to which the data will be queried. (tst in HFP payload) "
            "Timestamp will be read as UTC if `tz` parameter is not specified. "
            "Default value is 24 hours later than `from_tst`"
            "Format `yyyy-MM-dd'T'HH:mm:ss`. "
            "Timestamp can be shortened - optional formats are "
            "`yyyy-MM-dd'T'HH:mm` and `yyyy-MM-dd` "
        ),
        example="2023-01-12T15:00:00",
    ),
    tz: int = Query(
        default=0,
        title="Timezone",
        description=(
            "Timezone of the timestamps in `from_tst` and `to_tst`. "
            "If not given, timestamps are expected to be in UTC time. "
            "This parameter does not convert timezones on returned timestamps in response data."
        ),
        example=2,
    ),
) -> Response:
    """
    Get hfp data in raw csv format filtered by parameters.
    """
    with CustomDbLogHandler("api"):
        fetch_start_time = time.time()
        logger.debug(
            f"Fetching raw hfp data. route_id: {route_id}, operator_id: {operator_id}, "
            f"vehicle_number: {vehicle_number}, from_tst: {from_tst}, to_tst: {to_tst}"
        )

        if not route_id and not (operator_id and vehicle_number):
            logger.error("Missing required parameters.")
            raise HTTPException(
                422, detail=[{"msg": "Either route_id or operator_id and vehicle_number -parameters are required!"}]
            )

        # Input stream for csv data from database, output stream for compressed data
        input_stream = io.BytesIO()
        output_stream = io.BytesIO()

        # Set to_tst default 24 hours
        if not to_tst:
            to_tst = from_tst + timedelta(hours=24)

        # Set timestamp information
        tzone = timezone(timedelta(hours=tz))
        from_tst = from_tst.replace(tzinfo=tzone)
        to_tst = to_tst.replace(tzinfo=tzone)

        await get_hfp_data(route_id, operator_id, vehicle_number, from_tst, to_tst, input_stream)

        logger.debug("Hfp data received. Compressing.")
        data = input_stream.getvalue()

        with gzip.GzipFile(fileobj=output_stream, mode="wb") as compressed_data_stream:
            compressed_data_stream.write(data)

        filename = f"hfp-export-{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv.gz"
        response = GzippedFileResponse(filename=filename, content=output_stream.getvalue())

        duration = time.time() - fetch_start_time
        logger.debug(f"Hfp raw data fetch finished in {int(duration)} seconds. Exported file: {filename}")
        return response
