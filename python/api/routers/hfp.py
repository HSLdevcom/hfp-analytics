"""Routes for /hfp endpoint"""

import gzip
import io
import logging
import re
import time
from datetime import date, datetime, timedelta, timezone
from http import HTTPStatus
from typing import List, Literal, Optional

import httpx
from common.config import DURABLE_BASE_URL
from common.container_client import FlowAnalyticsContainerClient
from common.logger_util import CustomDbLogHandler
from common.preprocess import (
    find_missing_preprocess_data_in_db_compared_to_blob_storage,
    get_existing_date_and_route_id_from_preprocess_table,
)
from common.utils import (
    create_filename,
    get_target_oday,
    is_date_range_valid,
    set_timezone,
)
from fastapi import APIRouter, HTTPException, Query, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, Response

from api.services.hfp import (
    get_hfp_data,
    get_speeding_data,
    upload_missing_preprocess_data_to_db,
)
from api.services.tlp import get_tlp_data, get_tlp_data_as_json

logger = logging.getLogger("api")

router = APIRouter(prefix="/hfp", tags=["HFP data"])

route_id_pattern = re.compile(r"^[A-Za-z0-9]+$")
CHUNK_SIZE = 10000  # Adjust to optimize if needed

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
                    "schema": {
                        "example": 'attachment; filename="hfp-export_20230316_550_18_662.csv.gz"'
                    }
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
                422,
                detail=[
                    {
                        "msg": "Either route_id or operator_id and vehicle_number -parameters are required!"
                    }
                ],
            )

        # Input stream for csv data from database, output stream for compressed data
        input_stream = io.BytesIO()
        output_stream = io.BytesIO()

        # Set to_tst default 24 hours
        to_tst = to_tst or from_tst + timedelta(hours=24)

        from_tst, to_tst = set_timezone(from_tst, tz), set_timezone(to_tst, tz)

        # Set timestamp information
        timezone(timedelta(hours=tz))

        row_count = await get_hfp_data(
            route_id,
            operator_id,
            vehicle_number,
            event_types,
            from_tst,
            to_tst,
            input_stream,
        )

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

        filename = create_filename(
            "hfp-export_",
            from_tst.strftime("%Y%m%d") if from_tst else None,
            route_id,
            operator_id,
            vehicle_number,
        )

        response = GzippedFileResponse(
            filename=filename, content=output_stream.getvalue()
        )

        duration = time.time() - fetch_start_time
        logger.debug(
            f"Hfp raw data fetch finished in {int(duration)} seconds. Exported file: {filename}"
        )
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
                    "schema": {
                        "example": 'attachment; filename="tlp-export_20240318_1003H6.csv.gz"'
                    }
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
        logger.debug(
            f"Fetching TLR & TLA data. route_id: {route_id}, operator_id: {operator_id}, "
            f"vehicle_number: {vehicle_number}, sid: {sid}, from_tst: {from_tst}, to_tst: {to_tst}"
        )

        if not route_id and not (operator_id and vehicle_number):
            logger.error("Missing required parameters.")
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                detail="Either route_id or both operator_id and vehicle_number parameters are required!",
            )

        to_tst = to_tst or from_tst + timedelta(hours=24)
        from_tst, to_tst = set_timezone(from_tst, tz), set_timezone(to_tst, tz)

        if json:
            data = await get_tlp_data_as_json(
                route_id, operator_id, vehicle_number, sid, from_tst, to_tst
            )
            return JSONResponse(content=jsonable_encoder(data))
        else:
            input_stream = io.BytesIO()
            output_stream = io.BytesIO()
            await get_tlp_data(
                route_id,
                operator_id,
                vehicle_number,
                sid,
                from_tst,
                to_tst,
                input_stream,
            )

            logger.debug("TLR & TLA data received. Compressing.")

            input_stream.seek(0)
            with gzip.GzipFile(
                fileobj=output_stream, mode="wb"
            ) as compressed_data_stream:
                for data in iter(lambda: input_stream.read(CHUNK_SIZE), b""):
                    compressed_data_stream.write(data)

            filename = create_filename(
                "tlp-export_",
                from_tst.strftime("%Y%m%d") if from_tst else None,
                route_id,
                operator_id,
                vehicle_number,
            )
            response = GzippedFileResponse(
                filename=filename, content=output_stream.getvalue()
            )

            logger.debug(
                f"TLR & TLA raw data fetch and export completed in {int(time.time() - fetch_start_time)} seconds. Exported file: {filename}"
            )
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
                    "schema": {
                        "example": 'attachment; filename="speeding-export_20240915_20240923_2015_20.csv"'
                    }
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

        missing_params = [
            param_name
            for param_name, param_value in required_params.items()
            if param_value is None
        ]

        if missing_params:
            logger.error(f"Missing required parameters: {', '.join(missing_params)}")
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                detail=f"The following parameters are missing: {', '.join(missing_params)}",
            )
        logger.debug(
            f"Fetching speeding data. route_id: {route_id}, min_spd: {min_spd}, from_tst: {from_tst}, to_tst:{to_tst}"
        )
        data = await get_speeding_data(
            route_id,
            min_spd,
            from_tst,
            to_tst,
            x_min,
            y_min,
            x_max,
            y_max,
            input_stream,
        )
        logger.debug(f"Speeding data for {route_id} received. Compressing.")
        input_stream.seek(0)
        with gzip.GzipFile(fileobj=output_stream, mode="wb") as compressed_data_stream:
            for data in iter(lambda: input_stream.read(CHUNK_SIZE), b""):
                compressed_data_stream.write(data)

        filename = create_filename(
            "speeding-export_",
            from_tst.strftime("%Y%m%d"),
            to_tst.strftime("%Y%m%d"),
            route_id,
            min_spd,
        )
        response = GzippedFileResponse(
            filename=filename, content=output_stream.getvalue()
        )

        logger.debug(
            f"Speeding data fetch and export completed in {int(time.time() - fetch_start_time)} seconds. Exported file: {filename}"
        )

        return response


@router.get(
    "/delay_analytics",
    summary="Get delay analytics data.",
    description="Returns delay analytics as packaged zip file. Initial request will start the analysis. Following requests will return the status of the analysis or the data.",
    responses={
        200: {
            "description": "The data is returned as an attachment in the response.",
            "content": {"application/gzip": {"schema": None, "example": None}}
        },
        202: {"description": "Status message returned. Analysis queued, running or created, check again later."},   
        204: {"description": "Query returned no data with the given parameters."},
        422: {"description": "Query had invalid parameters."}
    }
)
async def get_delay_analytics_data_durable(
    route_id: Optional[str] = Query(
        default=None,
        title="Route ID or Route IDs",
        description="Routes to be used in analysis. Single or multiple route ids can be used. If multiple given, then ids should be separated by a comma.",
        example="1057,1070",
    ),
    from_oday: Optional[date] = Query(
        default=None,
        title="From oday (YYYY-MM-DD)",
        description=(
            "The oday from which the preprocessed clusters and departures will be used.",
            "If same oday is used for from_oday and to_oday the analysis for that day will be returned.",
            "If no date given the default value will be used (five days prior)."
        ),
        example="2025-02-10"
    ),
    to_oday: Optional[date] = Query(
        default=None,
        title="To oday (YYYY-MM-DD)",
        description=(
            "The oday to which the preprocessed clusters and departures will be used.",
            "If same oday is used for from_oday and to_oday the analysis for that day will be returned.",
            "If no date given the default value will be used (yesterday)."
        ),
        example="2025-02-10"
    ),
    exclude_dates: Optional[str] = Query(
        default=None,
        title="Days to exclude (YYYY-MM-DD)",
        description=(
            "The days to be excluded from the analysis."
            "Provide valid date or dates separated with a comma."
        ),
        example="2025-02-10,2025-02-11"
    ),
) -> Response:
    """
    # 200: data returned
    # 202: status message (pending, queued or created) returned
    # TODO: 204: no data to do analysis
    # 422: invalid parameters
    """

    default_from_oday = get_target_oday(15)
    default_to_oday   = get_target_oday()
    if not from_oday:
        from_oday = default_from_oday
    if not to_oday:
        to_oday = default_to_oday

    is_date_range_valid_, date_range_validity_message = is_date_range_valid(
        from_oday=from_oday, to_oday=to_oday
    )
    if not is_date_range_valid_:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=date_range_validity_message)


    if route_id is None or not route_id.strip():
        route_ids = []
    else:
        route_ids = [r.strip() for r in route_id.split(",") if r.strip()]
        route_ids.sort()

        for rid in route_ids:
            if not route_id_pattern.match(rid):
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=[
                        {
                            "loc": ["query", "route_id"],
                            "msg": f"Invalid route ID: {rid}. Only letters and digits allowed.",
                            "input": rid,
                        }
                    ]
                )


    if exclude_dates is not None:
        raw_dates = [r.strip() for r in exclude_dates.split(",") if r.strip()]
        valid_dates = []
        for d in raw_dates:
            try:
                datetime.strptime(d, "%Y-%m-%d")
                valid_dates.append(d)
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=[
                        {
                            "loc": ["query", "exclude_dates"],
                            "msg": f"Invalid date: {d}. Expected format is YYYY-MM-DD.",
                            "input": d,
                        }
                    ]
                )
        valid_dates.sort()
        exclude_dates = valid_dates
    else:
        exclude_dates = []

    payload = {
        "route_ids": route_ids,
        "from_oday": str(from_oday),
        "to_oday": str(to_oday),
        "days_excluded": exclude_dates
    }

    try:
        async with httpx.AsyncClient() as client:
            orchestrator_url = f"{DURABLE_BASE_URL}/durable/orchestrator"
            resp = await client.post(orchestrator_url, json=payload, timeout=10.0)
            if resp.status_code == 202:
                return Response(
                    status_code=status.HTTP_202_ACCEPTED,
                    content=resp.content,
                    media_type=resp.headers.get("Content-Type", "application/json")
                )
            resp.raise_for_status()
            try:
                return resp.json()
            except ValueError:
                return Response(
                    status_code=status.HTTP_200_OK,
                    content=resp.content,
                    media_type=resp.headers.get("Content-Type", "application/zip"),
                    headers={
                        "Content-Disposition": 'attachment; filename="clusters.zip"'
                    }
                )
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Could not start Durable function: {e}")


@router.post(
    "/add_preprocess_data_from_blob_to_db",
    summary="Imports missing preprocess data for clusters and departures from blob storage to database.",
    responses={
        201: {
            "description": "Successful query. All data has been saved in db ",
            "content": {"application/gzip": {"schema": None, "example": None}},
        },
        422: {"description": "Query had invalid parameters."},
    },
)
async def add_preprocess_data_from_blob_to_db(
    preprocess_type: Literal["clusters", "departures"], response: Response
) -> dict[str, List[str]]:
    with CustomDbLogHandler("api"):
        client =  FlowAnalyticsContainerClient()
        
        blob_data = await client.get_existing_blob_data_from_previous_2_months(preprocess_type=preprocess_type)
        logger.debug(f'Found {len(blob_data)} blobs in blob storage')

        db_data = await get_existing_date_and_route_id_from_preprocess_table(preprocess_type=preprocess_type)
        logger.debug(f"Found {len(db_data)} rows in database")
        
        missing_data = await find_missing_preprocess_data_in_db_compared_to_blob_storage(db_data=db_data, blobs_data=blob_data)
        logger.debug(f"Found { len(missing_data)} blobs which are not in the database yet.") 

        if len(missing_data) > 0:
            await upload_missing_preprocess_data_to_db(client=client, missing_blobs=missing_data, preprocess_type=preprocess_type)
            logger.debug(f"Successfully imported {len(missing_data)} blobs from blob storage to database")
        else:
            logger.debug("There are 0 blobs to be added from blob storage to database")
        response.status_code = status.HTTP_201_CREATED
        return {'imported data': [blob.blob_path for blob in missing_data]}
