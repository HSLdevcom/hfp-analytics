"""Routes for /vehicles endpoint"""

import csv
import logging

# from starlette.responses import FileResponse
from datetime import date
from itertools import chain
from typing import Optional

from common.vehicle_analysis_utils import (
    get_all_analysis_by_date,
    get_door_analysis_by_date,
    get_odo_analysis_by_date,
    get_positioning_analysis_by_date,
)
from fastapi import APIRouter, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse, JSONResponse

from api.schemas.vehicles import (
    VehicleDoorsAnalysis,
    VehicleOdoAnalysis,
    VehiclePositionAnalysis,
)

# TODO: These should be set somewhere else. They are also used in vehicle_analysis_utils.py
SPD_THRESHOLD = 2
LOC_GPS_THRESHOLD = 0.95

ERROR_TYPES_TRANSLATIONS = {
    "Drst inverted": "Käänteinen ovitieto",
    f"Speed over {SPD_THRESHOLD} m/s when doors open": "Useita ovitapahtumia vauhdissa",
    "Drst missing": "Ovitiedon arvo puuttuu",
    "Some of the drst values are missing": "Ovitiedon arvo puuttuu osasta tapahtumia",
    "Too many door events": "Liian paljon ovitapahtumia",
    "Drst always true": "Ovitieto aina auki",
    "Drst always false": "Ovitieto aina kiinni",
    "Identical first and last odo values": "ODO-metrin arvo ei muutu",
    "Odo value over 100000": "ODO-metri tuottaa liian suuria arvoja",
    "Odo value decreased": "ODO-metri tuottaa negatiivisia arvoja",
    "Odo values missing": "ODO-metrin arvo puuttuu",
    "Some odo values missing": "ODO-metrin arvo puuttuu osasta tapahtumia",
    "Odo changed when stationary": "ODO-metrin arvo muuttuu kun ajoneuvo on paikallaan",
    f"GPS ratio below {LOC_GPS_THRESHOLD}": "GPS suhdeluku alle tavoitearvon",
}

COMMON_QUERY_PARAMS = {
    "date": Query(
        ...,
        title="Analysis date",
        description="The date of which analysis will be returned. Format YYYY-MM-DD",
    ),
    "operator_id": Query(
        default=None,
        title="Operator ID for analysis",
        description="HFP topic's operator id. Use without prefix zeros.",
    ),
    "errors_only": Query(
        default=False,
        title="Return errors only",
        description="Only return vehicles that triggered an error",
    ),
}

logger = logging.getLogger("api")

router = APIRouter(
    prefix="/vehicles",
    tags=["Vehicle analytics data"],
    responses={
        "404": {
            "description": "Not found. Analysis is not available for selected date."
        }
    },
)


class CsvFileResponse(FileResponse):
    media_type = "text/csv"


@router.get(
    "/positioning",
    summary="Get analysis results for positioning data",
    description="Returns the results of analysis made for `loc` value of HFP events.",
    response_class=JSONResponse,
    response_model=VehiclePositionAnalysis,
)
async def get_vehicle_position_analysis(
    date: date = COMMON_QUERY_PARAMS["date"],
    operator_id: Optional[int] = COMMON_QUERY_PARAMS["operator_id"],
    errors_only: Optional[bool] = COMMON_QUERY_PARAMS["errors_only"],
) -> JSONResponse:
    is_current_date = date == date.today()
    analyzed_data = []

    # TODO: Metadata for timerange should be stored in db when analysis is done
    # and then retrieved from the db with the rest of the data rather than hardcoding it here
    timerange_metadata = {"start": "00:00:00.000+00", "end": "11:59:00.000+00"}
    if is_current_date:
        # Analysis for current date disabled for now
        raise HTTPException(
            status_code=404, detail="Analysis disabled for current date"
        )
    else:
        analyzed_data = await get_positioning_analysis_by_date(date, operator_id)

    analyzed_data = sorted(analyzed_data, key=lambda x: x["vehicle_number"])
    data = {
        "data": {
            "metadata": {
                "start": timerange_metadata["start"].strip(),
                "end": timerange_metadata["end"].strip(),
                "date": date,
            },
            "vehicles": analyzed_data,
        }
    }
    return JSONResponse(content=jsonable_encoder(data))


@router.get(
    "/doors",
    summary="Get analysis results for doors data",
    description="Returns the results of analysis made for `drst` value of HFP events.",
    response_class=JSONResponse,
    response_model=VehicleDoorsAnalysis,
)
async def get_vehicle_door_analysis(
    date: date = COMMON_QUERY_PARAMS["date"],
    operator_id: Optional[int] = COMMON_QUERY_PARAMS["operator_id"],
    errors_only: Optional[bool] = COMMON_QUERY_PARAMS["errors_only"],
) -> JSONResponse:
    is_current_date = date == date.today()
    analyzed_data = []

    # TODO: Metadata for timerange should be stored in db when analysis is done
    # and then retrieved from the db with the rest of the data rather than hardcoding it here
    timerange_metadata = {"start": "00:00:00.000+00", "end": "11:59:00.000+00"}
    if is_current_date:
        # Analysis for current date disabled for now
        raise HTTPException(
            status_code=404, detail="Analysis disabled for current date"
        )
    else:
        analyzed_data = await get_door_analysis_by_date(date, operator_id)

    if errors_only:
        analyzed_data = [
            vehicle
            for vehicle in analyzed_data
            if len(vehicle["door_error_events"]["types"]) > 0
        ]

    analyzed_data = sorted(analyzed_data, key=lambda x: x["vehicle_number"])
    data = {
        "data": {
            "metadata": {
                "start": timerange_metadata["start"].strip(),
                "end": timerange_metadata["end"].strip(),
                "date": date,
            },
            "vehicles": analyzed_data,
        }
    }
    return JSONResponse(content=jsonable_encoder(data))


@router.get(
    "/odo",
    summary="Get analysis results for odo data",
    description="Returns the results of analysis made for `odo` value of HFP events.",
    response_class=JSONResponse,
    response_model=VehicleOdoAnalysis,
)
async def get_vehicle_odo_analysis(
    date: date = COMMON_QUERY_PARAMS["date"],
    operator_id: Optional[int] = COMMON_QUERY_PARAMS["operator_id"],
    errors_only: Optional[bool] = COMMON_QUERY_PARAMS["errors_only"],
) -> JSONResponse:
    # TODO: Metadata for timerange should be stored in db when analysis is done
    # and then retrieved from the db with the rest of the data rather than hardcoding it here
    timerange_metadata = {"start": "00:00:00.000+00", "end": "11:59:00.000+00"}
    is_current_date = date == date.today()
    analyzed_data = []
    if is_current_date:
        # Analysis for current date disabled for now
        raise HTTPException(
            status_code=404, detail="Analysis disabled for current date"
        )
    else:
        analyzed_data = await get_odo_analysis_by_date(date, operator_id)

    if errors_only:
        return JSONResponse(content=jsonable_encoder(analyzed_data))
        analyzed_data = [
            vehicle
            for vehicle in analyzed_data
            if len(vehicle["door_error_events"]["types"]) > 0
        ]

    analyzed_data = sorted(analyzed_data, key=lambda x: x["vehicle_number"])
    data = {
        "data": {
            "metadata": {
                "start": timerange_metadata["start"].strip(),
                "end": timerange_metadata["end"].strip(),
                "date": date,
            },
            "vehicles": analyzed_data,
        }
    }
    return JSONResponse(content=jsonable_encoder(data))


@router.get(
    "/PTO",
    summary="Get vehicle analysis as CSV",
    description="Returns all analytics results as csv for a specific operator. "
    "PTO stands for Public Transport Operators.",
    response_class=CsvFileResponse,
)
async def get_vehicle_analysis_for_operator(
    date: date = COMMON_QUERY_PARAMS["date"],
    operator_id: Optional[int] = COMMON_QUERY_PARAMS["operator_id"],
    errors_only: Optional[bool] = COMMON_QUERY_PARAMS["errors_only"],
) -> CsvFileResponse:
    is_current_date = date == date.today()
    analyzed_data = []
    if is_current_date:
        # Analysis for current date disabled for now
        raise HTTPException(
            status_code=404, detail="Analysis disabled for current date"
        )
    else:
        analyzed_data = await get_all_analysis_by_date(date, operator_id)

    if errors_only:
        analyzed_data = [
            vehicle
            for vehicle in analyzed_data
            if len(vehicle["door_error_events"]["types"]) > 0
            or len(vehicle["odo_error_events"]["types"]) > 0
            or len(vehicle["loc_error_events"]["types"]) > 0
        ]

    csv_filename = f"hfp-analysis-{date}.csv"

    with open(csv_filename, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = [
            "Päivämäärä",
            "Operaattori",
            "Kylkinumero",
            "Havaittu ongelma",
            "Syyt",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for item in analyzed_data:
            door_error_types = []
            odo_error_types = []
            loc_error_types = []
            if "door_error_events" in item:
                door_error_types = item["door_error_events"]["types"]
            if "odo_error_events" in item:
                odo_error_types = item["odo_error_events"]["types"]
            if "loc_error_events" in item:
                loc_error_types = item["loc_error_events"]["types"]

            common_data = {
                "Päivämäärä": date,
                "Operaattori": item["operator_id"],
                "Kylkinumero": item["vehicle_number"],
            }
            if (
                len(door_error_types) == 0
                and len(odo_error_types) == 0
                and len(loc_error_types) == 0
            ):
                row_data = {
                    **common_data,
                    "Havaittu ongelma": "Ei havaittu ongelmia",
                    "Syyt": "",
                }
                writer.writerow(row_data)
            else:
                translated_error_types = []
                for error_type in chain(
                    door_error_types, odo_error_types, loc_error_types
                ):
                    translated_error_types.append(ERROR_TYPES_TRANSLATIONS[error_type])
                error_types_str = ", ".join(translated_error_types)
                detected_problems = []
                if len(door_error_types) > 0:
                    detected_problems.append("Epäluotettava ovitieto")
                if len(odo_error_types) > 0:
                    detected_problems.append("Epäluotettava odometritieto")
                if len(loc_error_types) > 0:
                    detected_problems.append("Epäluotettava paikkatieto")

                row_data = {
                    **common_data,
                    "Havaittu ongelma": detected_problems,
                    "Syyt": error_types_str,
                }
                writer.writerow(row_data)

    return CsvFileResponse(csv_filename, filename=csv_filename)


def error_obj(d, event):
    start_str = str(d.get("start"))
    return {
        "tst": d.get("tst"),
        "oday": d.get("oday"),
        "type": event,
        "drst": d.get("drst"),
        "spd": d.get("spd"),
        "odo": d.get("odo"),
        "loc": d.get("loc"),
        "route_id": d.get("route_id"),
        "operator_id": d.get("operator_id"),
        "start": start_str,
        "longitude": d.get("longitude"),
        "latitude": d.get("latitude"),
        "direction_id": d.get("direction_id"),
    }
