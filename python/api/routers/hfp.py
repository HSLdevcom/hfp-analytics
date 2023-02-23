""" Routes for /hfp endpoint """

import io
import gzip
from typing import Optional
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from api.services.hfp import get_hfp_data

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
                                    description="JORE ID of the route. **Required** when no `oper` and `veh` provided.",
                                    example="2550"),
    oper: Optional[int] = Query(default=None,
                                title="Operator ID",
                                description="Operator ID of the vehicle. **Required** when no `route_id` provided.",
                                example="18"),
    veh: Optional[int] = Query(default=None,
                               description="Vehicle ID. **Required** when no `route_id` provided.",
                               example="662"),
    from_tst: datetime = Query(title="Minimum timestamp",
                               description=("The timestamp from which the data will be queried. "
                                            "Timestamp will be read as UTC if `tz` parameter is not specified. "
                                            "Format `yyyy-MM-dd'T'HH:mm:ss`. "
                                            "Timestamp can be shortened - empty field defaults to zero. "
                                            "Remember that the database contains data from previous 14 days."),
                               example="2023-01-12T14:20:30"),
    to_tst: Optional[datetime] = Query(default=None,
                                       title="Maximum timestamp",
                                       description=("The timestamp to which the data will be queried. "
                                                    "Timestamp will be read as UTC if `tz` parameter is not specified. "
                                                    "Default value is 24 hours later than `from_tst`"
                                                    "Format `yyyy-MM-dd'T'HH:mm:ss`. "
                                                    "Timestamp can be shortened - empty field defaults to zero."),
                                       example="2023-01-12T15"),
    tz: int = Query(default=0,
                    title="Timezone",
                    description=("Timezone of the timestamps. "
                                 "If not given, timestamps are expected to be in UTC time."),
                    example="+2")):
    """
    Get hfp data in raw csv format filtered by parameters.
    """
    if not route_id and not (oper and veh):
        raise HTTPException(400, detail="Either route_id or oper and veh -parameters are required!")

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

    await get_hfp_data(route_id, oper, veh, from_tst, to_tst, input_stream)

    data = input_stream.getvalue()

    with gzip.GzipFile(fileobj=output_stream, mode='wb') as compressed_data_stream:
        compressed_data_stream.write(data)

    response = Response(content=output_stream.getvalue(),
                        media_type="application/gzip")

    filename = f"hfp-export-{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv.gz"
    # Send as an attachment
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"

    return response
