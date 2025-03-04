""" Routes for /hfp endpoint """

import io
import gzip
import zipfile
import pandas as pd
import pytz
import numpy as np

from sklearn.cluster import DBSCAN
from collections import Counter
from typing import Optional
from datetime import date, timedelta, datetime, time
from http import HTTPStatus

import logging
from common.logger_util import CustomDbLogHandler

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response, JSONResponse
from fastapi.encoders import jsonable_encoder

from api.services.hfp import get_hfp_data, get_speeding_data
from api.services.tlp import get_tlp_data, get_tlp_data_as_json
from api.services.recluster import recluster_analysis, load_compressed_cluster
from common.utils import get_previous_day_tst, create_filename, set_timezone

logger = logging.getLogger("api")

router = APIRouter(prefix="/hfp", tags=["HFP data"])

CHUNK_SIZE = 10000 # Adjust to optimize if needed


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
            "description": "Successful query. The data is returned as an attachment in the response. "
            "File format comes from query parameters: "
            "`hfp-export_<from_date>_<route_id>_<operator_id>_<vehicle_number>.csv.gz`",
            "content": {"application/gzip": {"schema": None, "example": None}},
            "headers": {
                "Content-Disposition": {
                    "schema": {"example": 'attachment; filename="hfp-export_20230316_550_18_662.csv.gz"'}
                }
            },
        },
        204: {"description": "Query returned no data with the given parameters."},
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
    event_types: Optional[str] = Query(
        default=None,
        description="Filter returned rows by event types.",
        example="DOO,DOC",
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
        to_tst = to_tst or from_tst + timedelta(hours=24)

        from_tst, to_tst = set_timezone(from_tst, tz), set_timezone(to_tst, tz)

        # Set timestamp information
        tzone = timezone(timedelta(hours=tz))

        row_count = await get_hfp_data(route_id, operator_id, vehicle_number, event_types, from_tst, to_tst, input_stream)

        if row_count == 0:
            # No data was found, return no content response
            return Response(status_code=204)

        logger.debug("Hfp data received. Compressing.")

        # Read as chunks to save memory
        input_stream.seek(0)
        chunk_size = CHUNK_SIZE 

        with gzip.GzipFile(fileobj=output_stream, mode="wb") as compressed_data_stream:
            while data := input_stream.read(chunk_size):
                compressed_data_stream.write(data)


        filename = create_filename("hfp-export_", from_tst.strftime("%Y%m%d") if from_tst else None, route_id, operator_id, vehicle_number)

        response = GzippedFileResponse(filename=filename, content=output_stream.getvalue())

        duration = time.time() - fetch_start_time
        logger.debug(f"Hfp raw data fetch finished in {int(duration)} seconds. Exported file: {filename}")
        return response

@router.get(
    "/tlp",
    summary="Get TLR & TLA data",
    description="Returns TLR & TLA data in a gzip compressed csv file.",
    response_class=GzippedFileResponse,
    responses={
        200: {
            "description": "Successful query. The data is returned as an attachment in the response. "
            "File format comes from query parameters: "
            "`tlp-export_<from_date>_<route_id>_<operator_id>_<vehicle_number>.csv.gz`",
            "content": {"application/gzip": {"schema": None, "example": None}},
            "headers": {
                "Content-Disposition": {
                    "schema": {"example": 'attachment; filename="tlp-export_20240318_1003H6.csv.gz"'}
                }
            },
        },
        204: {"description": "Query returned no data with the given parameters."},
    },
)
async def get_tlp_raw_data(
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
    sid: Optional[int] = Query(
        default=None,
        description="Filter results by SID",
        example="",
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
    json: Optional[bool] = Query(
        default=False,
        title="Return JSON",
        description="If set to true return data as JSON. Otherwise data will be returned as csv.",
    ),
) -> Response:
    """
    Get TLR & TLA data in raw csv format filtered by parameters.
    """
    with CustomDbLogHandler("api"):
        fetch_start_time = time.time()
        logger.debug(f"Fetching TLR & TLA data. route_id: {route_id}, operator_id: {operator_id}, "
                     f"vehicle_number: {vehicle_number}, sid: {sid}, from_tst: {from_tst}, to_tst: {to_tst}")

        if not route_id and not (operator_id and vehicle_number):
            logger.error("Missing required parameters.")
            raise HTTPException(status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail="Either route_id or both operator_id and vehicle_number parameters are required!")

        to_tst = to_tst or from_tst + timedelta(hours=24)
        from_tst, to_tst = set_timezone(from_tst, tz), set_timezone(to_tst, tz)

        if json:
            data = await get_tlp_data_as_json(route_id, operator_id, vehicle_number, sid, from_tst, to_tst)
            return JSONResponse(content=jsonable_encoder(data))
        else:
            input_stream = io.BytesIO()
            output_stream = io.BytesIO()
            row_count = await get_tlp_data(route_id, operator_id, vehicle_number, sid, from_tst, to_tst, input_stream)

            logger.debug("TLR & TLA data received. Compressing.")

            input_stream.seek(0)
            with gzip.GzipFile(fileobj=output_stream, mode="wb") as compressed_data_stream:
                for data in iter(lambda: input_stream.read(CHUNK_SIZE), b''):
                    compressed_data_stream.write(data)

            filename = create_filename("tlp-export_", from_tst.strftime("%Y%m%d") if from_tst else None, route_id, operator_id, vehicle_number)
            response = GzippedFileResponse(filename=filename, content=output_stream.getvalue())

            logger.debug(f"TLR & TLA raw data fetch and export completed in {int(time.time() - fetch_start_time)} seconds. Exported file: {filename}")
            return response


@router.get(
    "/speeding",
    summary="Get speeding data by route id, given speed limit, tst range and bounding box",
    description="Returns speeding data in a gzip compressed csv file.",
    response_class=GzippedFileResponse,
    responses={
        200: {
            "description": "Successful query. The data is returned as an attachment in the response. "
            "File format comes from query parameters: "
            "`speeding-export_<from_tst>_<to_tst>_<route_id>_<min_spd>.csv.gz`",
            "content": {"application/gzip": {"schema": None, "example": None}},
            "headers": {
                "Content-Disposition": {
                    "schema": {"example": 'attachment; filename="speeding-export_20240915_20240923_2015_20.csv"'}
                }
            },
        },
        204: {"description": "Query returned no data with the given parameters."},
    },
)
async def get_speeding(
    route_id: int = Query(
        default=None,
        title="Route ID",
        description="JORE ID of the route",
        example=2015,
    ),
    min_spd: int = Query(
        default=None,
        title="Speed limit",
        description="Speed limit in km/h",
        example=23,
    ),
    from_tst: datetime = Query(
        title="Minimum timestamp",
        description=(
            "The timestamp from which the data will be queried. (tst in HFP payload) "
            "Timestamp will be read as UTC"
        ),
        example="2024-09-15T00:00:00",
    ),
    to_tst: datetime = Query(
        default=None,
        title="Maximum timestamp",
        description=(
            "The timestamp to which the data will be queried. (tst in HFP payload) "
            "Timestamp will be read as UTC"
        ),
        example="2024-09-23T00:00:00",
    ),
    x_min: int = Query(
        default=None,
        title="x_min",
        description="Coordinate of south-west corner of the bounding box (x_min, y_min). Coordinate should be given in ETRS-TM35FIN coordinate system.",
        example=378651,
    ),
    y_min: int = Query(
        default=None,
        title="y_min",
        description="Coordinate of south-west corner of the bounding box (x_min, y_min). Coordinate should be given in ETRS-TM35FIN coordinate system.",
        example=6677277,
    ),
    x_max: int = Query(
        default=None,
        title="x_max",
        description="Coordinate of north-east corner of the bounding box (x_max, y_max). Coordinate should be given in ETRS-TM35FIN coordinate system.",
        example=378893,
    ),
    y_max: int = Query(
        default=None,
        title="y_max",
        description="Coordinate of north-east corner of the bounding box (x_max, y_max). Coordinate should be given in ETRS-TM35FIN coordinate system.",
        example=6677652,
    ),
) -> JSONResponse:
    with CustomDbLogHandler("api"):
        fetch_start_time = time.time()

        input_stream = io.BytesIO()
        output_stream = io.BytesIO()

        required_params = {
            "route_id": route_id,
            "min_spd": min_spd,
            "from_tst": from_tst,
            "to_tst": to_tst,
            "x_min": x_min,
            "y_min": y_min,
            "x_max": x_max,
            "y_max": y_max,
        }

        missing_params = [param_name for param_name, param_value in required_params.items() if param_value is None]

        if missing_params:
            logger.error(f"Missing required parameters: {', '.join(missing_params)}")
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                detail=f"The following parameters are missing: {', '.join(missing_params)}"
            )
        logger.debug(f"Fetching speeding data. route_id: {route_id}, min_spd: {min_spd}, from_tst: {from_tst}, to_tst:{to_tst}")
        data = await get_speeding_data(route_id, min_spd, from_tst, to_tst, x_min, y_min, x_max, y_max, input_stream)
        logger.debug(f"Speeding data for {route_id} received. Compressing.")
        input_stream.seek(0)
        with gzip.GzipFile(fileobj=output_stream, mode="wb") as compressed_data_stream:
            for data in iter(lambda: input_stream.read(CHUNK_SIZE), b''):
                compressed_data_stream.write(data)

        filename = create_filename("speeding-export_", from_tst.strftime("%Y%m%d"), to_tst.strftime("%Y%m%d"), route_id, min_spd)
        response = GzippedFileResponse(filename=filename, content=output_stream.getvalue())

        logger.debug(f"Speeding data fetch and export completed in {int(time.time() - fetch_start_time)} seconds. Exported file: {filename}")

        return response

@router.get(
    "/delay_analytics",
    summary="Get HFP raw data",
    description="Returns raw HFP data in a gzip compressed csv file.",
    response_class=GzippedFileResponse,
    responses={
        200: {
            "description": "Successful query. The data is returned as an attachment in the response. "
            "File format comes from query parameters: "
            "`hfp-export_<from_date>_<route_id>_<operator_id>_<vehicle_number>.csv.gz`",
            "content": {"application/gzip": {"schema": None, "example": None}},
            "headers": {
                "Content-Disposition": {
                    "schema": {"example": 'attachment; filename="hfp-export_20230316_550_18_662.csv.gz"'}
                }
            },
        },
        204: {"description": "Query returned no data with the given parameters."},
    },
)
async def get_delay_analytics_data(
    route_id: str = Query(
        default=None,
        title="Route ID",
        description="JORE ID of the route.",
        example="1057",
    )
) -> Response:
    """
    Get delay analytics data.
    """
    with CustomDbLogHandler("api"):
        logger.debug(f"Fetching delay hfp data. route_id: {route_id}")
        from_tst, to_tst = get_previous_day_tst() # TODO: replace with request params
        await recluster_analysis(route_id, from_tst, to_tst)

        routecluster_geojson = await load_compressed_cluster("route_clusters", route_id, from_tst, to_tst)
        modecluster_geojson = await load_compressed_cluster("mode_clusters", route_id, from_tst, to_tst)

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zf:
            zf.writestr("routecluster.geojson", routecluster_geojson)
            zf.writestr("modecluster.geojson", modecluster_geojson)

        zip_buffer.seek(0)

        return Response(
            content=zip_buffer.getvalue(),
            media_type="application/zip",
            headers={"Content-Disposition": "attachment; filename=clusters_package.zip"}
        )