""" Routes for /hfp endpoint """

import io
import gzip
from typing import Optional
from datetime import datetime, timezone, timedelta

import time
import logging
from common.logger_util import CustomDbLogHandler

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

from api.services.apc import get_apc_data

logger = logging.getLogger("api")

router = APIRouter(prefix="/apc", tags=["APC data"])


@router.get(
    "/data",
    summary="Get APC raw data",
    description="Returns raw APC data in a compressed json response.",
    response_class=JSONResponse,
)
async def get_apc_raw_data(
    route_id: Optional[str] = Query(
        default=None,
        title="Route ID",
        description="JORE ID of the route. " "**Required** when no `operator_id` and `vehicle_number` provided.",
        example="2550",
    ),
    operator_id: Optional[int] = Query(
        default=None,
        title="Operator ID",
        description="Operator ID of the vehicle (operator_id in APC topic). **Required** when no `route_id` provided.",
        example="18",
    ),
    vehicle_number: Optional[int] = Query(
        default=None,
        description="Vehicle number (in APC topic). **Required** when no `route_id` provided.",
        example="662",
    ),
    from_tst: datetime = Query(
        title="Minimum timestamp",
        description=(
            "The timestamp from which the data will be queried. (tst in APC payload) "
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
            "The timestamp to which the data will be queried. (tst in APC payload) "
            "Timestamp will be read as UTC if `tz` parameter is not specified. "
            "Default value is 24 hours later than `from_tst`"
            "Format `yyyy-MM-dd'T'HH:mm:ss`. "
            "Timestamp can be shortened - optional formats are "
            "`yyyy-MM-dd'T'HH:mm` and `yyyy-MM-dd` "
        ),
        example="2023-01-12T15:00",
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
) -> JSONResponse:
    """
    Get apc data in json format filtered by parameters.
    """
    with CustomDbLogHandler("api"):
        fetch_start_time = time.time()
        logger.debug(
            f"Fetching raw apc data. route_id: {route_id}, operator_id: {operator_id}, "
            f"veh: {vehicle_number}, from_tst: {from_tst}, to_tst: {to_tst}"
        )

        if not route_id and not (operator_id and vehicle_number):
            logger.error("Missing required parameters.")
            raise HTTPException(400, detail="Either route_id or oper and veh -parameters are required!")

        # Set to_tst default 24 hours
        if not to_tst:
            to_tst = from_tst + timedelta(hours=24)

        # Set timestamp information
        tzone = timezone(timedelta(hours=tz))
        from_tst = from_tst.replace(tzinfo=tzone)
        to_tst = to_tst.replace(tzinfo=tzone)

        data = await get_apc_data(route_id, operator_id, vehicle_number, from_tst, to_tst)

        return JSONResponse(content=jsonable_encoder(data))
