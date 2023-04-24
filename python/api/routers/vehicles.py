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
from itertools import chain

from common.vehicle_analysis_utils import analyze_vehicle_door_data, get_vehicle_data, get_vehicle_ids, get_all_analysis_by_date, get_door_analysis_by_date, get_odo_analysis_by_date, analyze_odo_data

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
    'Drst always false': 'Ovitieto aina kiinni',
    'Identical first and last odo values': 'ODO-metrin arvo ei muutu',
    'Odo value over 100000': 'ODO-metri tuottaa liian suuria arvoja',
    'Odo value decreased': 'ODO-metri tuottaa negatiivisia arvoja',
    'Odo values missing': 'ODO-metrin arvo puuttuu',
    'Some odo values missing': 'ODO-metrin arvo puuttuu osasta tapahtumia',
    'Odo changed when stationary': 'ODO-metrin arvo muuttuu kun ajoneuvo on paikallaan'
}

@router.get("/doors")
async def get_vehicles(
    date: date = Query(..., description="Format YYYY-MM-DD"),
    operator_id: Optional[int] = Query(None, description="HFP topic's vehicle id. Use without prefix zeros."),
    errors_only: Optional[bool] = Query(None, description="Only return vehicles that triggered an error")
):
    """
    Endpoint for drst analysis.
    """
    is_current_date = date == date.today()
    analyzed_data = []
    if is_current_date:
        customTimeInterval = {
            "start": " 14:00:00.000+00",
            "end": " 15:00:00.000+00"
        }
        vehicle_ids = await get_vehicle_ids(date, customTimeInterval, operator_id)
        results = []
        for vehicle in vehicle_ids:
            vehicle_number = vehicle['vehicle_number']
            vehicle_operator_id = vehicle['operator_id']
            formatted_data = await get_vehicle_data(date, vehicle_operator_id, vehicle_number, customTimeInterval)
            analyzed_door_data = analyze_vehicle_door_data(formatted_data)
            for d in analyzed_door_data:
                results.append(d)
        analyzed_data = results
    else:
        analyzed_data = await get_door_analysis_by_date(date, operator_id)
        
    if errors_only:
        analyzed_data = [vehicle for vehicle in analyzed_data if len(vehicle['door_error_events']["types"]) > 0]
    
    analyzed_data = sorted(analyzed_data, key=lambda x: x['vehicle_number'])
    return {
        "data": {
            "vehicles": analyzed_data
        }
    }

@router.get("/odo")
async def get_vehicles(
    date: date = Query(..., description="Format YYYY-MM-DD"),
    operator_id: Optional[int] = Query(None, description="HFP topic's vehicle id. Use without prefix zeros."),
    errors_only: Optional[bool] = Query(None, description="Only return vehicles that triggered an error")
):
    """
    Odo analysis endpoint
    """

    is_current_date = date == date.today()
    analyzed_data = []
    if is_current_date:
        customTimeInterval = {
            "start": " 14:00:00.000+00",
            "end": " 15:00:00.000+00"
        }
        vehicle_ids = await get_vehicle_ids(date, customTimeInterval, operator_id)
        results = []
        for vehicle in vehicle_ids:
            vehicle_number = vehicle['vehicle_number']
            vehicle_operator_id = vehicle['operator_id']
            formatted_data = await get_vehicle_data(date, vehicle_operator_id, vehicle_number, customTimeInterval)
            analyzed_odo_data = analyze_odo_data(formatted_data)
            for d in analyzed_odo_data:
                results.append(d)
        analyzed_data = results
    else:
        analyzed_data = await get_odo_analysis_by_date(date, operator_id)

    if errors_only:
        return analyzed_data
        analyzed_data = [vehicle for vehicle in analyzed_data if len(vehicle['door_error_events']["types"]) > 0]

    analyzed_data = sorted(analyzed_data, key=lambda x: x['vehicle_number'])
    return {
        "data": {
            "vehicles": analyzed_data
        }
    }

@router.get("/csv")
async def get_vehicles(
    date: date = Query(..., description="Format YYYY-MM-DD"),
    operator_id: Optional[int] = Query(None, description="HFP topic's vehicle id. Use without prefix zeros."),
    errors_only: Optional[bool] = Query(None, description="Only return vehicles that triggered an error")
):
    """
    Vehicle doors analysis as csv.
    """
    is_current_date = date == date.today()
    analyzed_data = []
    if is_current_date:
        customTimeInterval = {
            "start": " 14:00:00.000+00",
            "end": " 15:00:00.000+00"
        }
        vehicle_ids = await get_vehicle_ids(date, customTimeInterval, operator_id)
        results = []
        for vehicle in vehicle_ids:
            vehicle_number = vehicle['vehicle_number']
            vehicle_operator_id = vehicle['operator_id']
            formatted_data = await get_vehicle_data(date, vehicle_operator_id, vehicle_number, customTimeInterval)
            analyzed_odo_data = analyze_odo_data(formatted_data)
            analyzed_door_data = analyze_vehicle_door_data(formatted_data)
            combined_obj = {}
            for obj in chain(analyzed_door_data, analyzed_odo_data):
                combined_obj.update(obj)
            results.append(combined_obj)
        analyzed_data = results
    else:
        analyzed_data = await get_all_analysis_by_date(date, operator_id)

    if errors_only:
        analyzed_data = [vehicle for vehicle in analyzed_data if len(vehicle['door_error_events']["types"]) > 0 or len(vehicle['odo_error_events']["types"]) > 0]


    csv_filename = f'hfp-analysis-{date}.csv'

    with open(csv_filename, "w", newline="", encoding='utf-8') as csvfile:
        fieldnames = ["Päivämäärä", "Operaattori", "Kylkinumero", "Havaittu ongelma", "Syyt"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for item in analyzed_data:
            door_error_types = []
            odo_error_types = []
            if 'door_error_events' in item:
                door_error_types = item['door_error_events']['types']
            if 'odo_error_events' in item:
                odo_error_types = item['odo_error_events']['types']

            common_data = {                    
                "Päivämäärä": date, 
                "Operaattori": item['operator_id'], 
                "Kylkinumero": item['vehicle_number']
            }
            if len(door_error_types) == 0 and len(odo_error_types) == 0:
                row_data = {
                    **common_data,
                    "Havaittu ongelma": "Ei havaittu ongelmia", 
                    "Syyt": ""
                }
                writer.writerow(row_data)
            else:
                translated_error_types = []
                for error_type in chain(door_error_types, odo_error_types):
                    translated_error_types.append(error_types_translations[error_type])
                error_types_str = ', '.join(translated_error_types)
                detected_problem = ""
                if len(door_error_types) > 0:
                    detected_problem = "Epäluotettava ovitieto"
                if len(odo_error_types) > 0:
                    detected_problem = "Epäluotettava odometritieto"
                if len(door_error_types) > 0 and len(odo_error_types) > 0:
                    detected_problem = "Epäluotettava odometri- ja ovitieto"

                row_data = {
                    **common_data,
                    "Havaittu ongelma": detected_problem, 
                    "Syyt": error_types_str
                }
                writer.writerow(row_data)

    return FileResponse(csv_filename, media_type="text/csv", filename=csv_filename)

def error_obj(d, event):
    start_str = str(d.get('start'))
    return {
        'tst': d.get('tst'),
        'oday': d.get('oday'),
        'type': event,
        'drst': d.get('drst'),
        'spd': d.get('spd'),
        'odo': d.get('odo'),
        'loc': d.get('loc'),
        'route_id': d.get('route_id'),
        'operator_id': d.get('operator_id'),
        'start': start_str,
        'longitude': d.get('longitude'),
        'latitude': d.get('latitude'),
        'direction_id': d.get('direction_id')
    }
