""" Routes for /vehicles endpoint """

import csv
import logging
import itertools
import os
import pytz
from collections import defaultdict
from typing import Optional
from starlette.responses import FileResponse
from common.logger_util import CustomDbLogHandler
from datetime import date, datetime, timedelta, time
from fastapi import APIRouter, Query

from common.vehicle_analysis_utils import analyze_vehicle_data, get_vehicle_data, get_vehicles_by_date

logger = logging.getLogger('api')

router = APIRouter(
    prefix="/vehicles",
    tags=["Vehicle analytics data"]
)

spd_threshold = 1
error_types_translations = {
    'Drst inverted': 'Käänteinen ovitieto',
    f'Speed over {spd_threshold} m/s when doors open': 'Useita ovitapahtumia vauhdissa',
    'Drst missing': 'Ovitiedon arvo puuttuu',
    'Some of the drst values are missing': 'Ovitiedon arvo puuttuu osasta tapahtumia',
    'Drst always true': 'Ovitieto aina auki',
    'Drst always false': 'Ovitieto aina kiinni'
}

@router.get("/doors")
async def get_vehicles(
    date: date = Query(..., description="Format YYYY-MM-DD"),
    operator_id: Optional[int] = Query(None, description="HFP topic's vehicle id. Use without prefix zeros."),
    errorsOnly: Optional[bool] = Query(None, description="Only return vehicles that triggered an error")
):
    """
    Endpoint for drst analysis.
    """

    is_current_date = date == date.today()
    analyzed_data = []
    if is_current_date:
        formatted_data = await get_vehicle_data(date, operator_id, None)
        analyzed_data = analyze_vehicle_data(formatted_data)
    else:
        analyzed_data = await get_vehicles_by_date(date)
        
    if errorsOnly:
        analyzed_data = [vehicle for vehicle in analyzed_data if vehicle['error_events']['amount'] > 0]
    
    analyzed_data =  sorted(analyzed_data, key=lambda x: x['vehicle_number'])
    return {
        "data": {
            "vehicles": analyzed_data
        }
    }

@router.get("/doors/csv")
async def get_vehicles(
    date: date = Query(..., description="Format YYYY-MM-DD"),
    operator_id: Optional[int] = Query(None, description="HFP topic's vehicle id. Use without prefix zeros."),
    errorsOnly: Optional[bool] = Query(None, description="Only return vehicles that triggered an error")
):
    """
    Vehicle doors analysis as csv.
    """
    formatted_data = await get_vehicle_data(date, operator_id, None)
    analyzed_data = analyze_vehicle_data(formatted_data, True)
    if errorsOnly:
        analyzed_data = [vehicle for vehicle in analyzed_data if vehicle['error_events']['amount'] > 0]
    csv_filename = "doors.csv"

    with open(csv_filename, "w", newline="", encoding='utf-8') as csvfile:
        fieldnames = ["Päivämäärä", "Operaattori", "Kylkinumero", "Havaittu ongelma", "Syyt"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for item in analyzed_data:
            common_data = {                    
                "Päivämäärä": date, 
                "Operaattori": item['operator_id'], 
                "Kylkinumero": item['vehicle_number']
            }
            if len(item['error_events']['types']) == 0:
                row_data = {
                    **common_data,
                    "Havaittu ongelma": "Ei havaittu ongelmia", 
                    "Syyt": ""
                }
                writer.writerow(row_data)
            else:
                translated_error_types = []
                error_types = item['error_events']['types']
                for error_type in error_types:
                    translated_error_types.append(error_types_translations[error_type])
                error_types_str = ', '.join(translated_error_types)
                row_data = {
                    **common_data,
                    "Havaittu ongelma": "Epäluotettava ovitieto", 
                    "Syyt": error_types_str
                }
                writer.writerow(row_data)

    return FileResponse(csv_filename, media_type="text/csv", filename="doors.csv")


