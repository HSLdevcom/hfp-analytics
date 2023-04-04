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
    'Drst always false': 'Ovitieto aina kiinni',
    'Identical first and last odo values': 'ODO-metrin arvo ei muutu',
    'Odo value over 100000': 'ODO-metri tuottaa liian suuria arvoja',
    'Odo value decreased between events': 'ODO-metri tuottaa negatiivisia arvoja',
    'Odo values missing': 'ODO-metrin arvo puuttuu',
    'Some odo values missing': 'ODO-metrin arvo puuttuu osasta tapahtumia',
    'Odo changed when stationary': 'ODO-metrin arvo muuttuu kun ajoneuvo on paikallaan'
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

@router.get("/odo/csv")
async def get_vehicles(
    date: date = Query(..., description="Format YYYY-MM-DD"),
    operator_id: Optional[int] = Query(None, description="HFP topic's vehicle id. Use without prefix zeros."),
    errorsOnly: Optional[bool] = Query(None, description="Only return vehicles that triggered an error")
):
    """
    Vehicle odo analysis as csv.
    """
    formatted_data = await get_vehicle_data(date, operator_id, None)
    analyzed_data = analyze_odo(formatted_data)
    if errorsOnly:
        analyzed_data = [vehicle for vehicle in analyzed_data if vehicle['errors']['amount'] > 0]
    csv_filename = "odo.csv"
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
            if item['errors']['amount'] == 0:
                row_data = {
                    **common_data,
                    "Havaittu ongelma": "Ei havaittu ongelmia", 
                    "Syyt": ""
                }
                writer.writerow(row_data)
            else:
                error_types = set()
                for event in item['errors']['events']:
                    event_type = event['type']
                    error_types.add(error_types_translations[event_type])
                error_types = list(error_types)
                error_types_str = ', '.join(error_types)
                row_data = {
                    **common_data,
                    "Havaittu ongelma": "Epäluotettava odo-tieto", 
                    "Syyt": error_types_str
                }
                writer.writerow(row_data)

    return FileResponse(csv_filename, media_type="text/csv", filename="odo.csv")

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


def analyze_odo(vehicle_data):
    analysis = defaultdict(lambda: {'odo': 0, 'null': 0, 'errors': {'amount': 0, 'events': []}})
    for data in vehicle_data:
        data_list = data.get('data', [])
        operator_id = data.get('operator_id')
        analysis[data['vehicle_number']]['operator_id'] = operator_id
        firstEvent = data_list[0]
        lastEvent = data_list[len(data_list) - 1]
        if firstEvent is not None and lastEvent is not None:
            firstOdo = firstEvent.get('odo')
            lastOdo = lastEvent.get('odo')
            if firstOdo == lastOdo:
                analysis[data['vehicle_number']]['errors']['amount'] += 1
                analysis[data['vehicle_number']]['errors']['events'].append(error_obj(d, "Identical first and last odo values"))
            if lastOdo is not None and lastOdo > 100000:
                analysis[data['vehicle_number']]['errors']['amount'] += 1
                analysis[data['vehicle_number']]['errors']['events'].append(error_obj(d, "Odo value over 100000"))

        prevOdo = None
        stationaryEventChunks = []
        chunk = []
        for d in data_list:
            odo = d.get('odo')
            loc = d.get('loc')
            tst = d.get('tst')
            drst = d.get('drst')
            spd = d.get('spd')
            if prevOdo is not None and odo is not None and prevOdo > odo:
                analysis[data['vehicle_number']]['errors']['amount'] += 1
                analysis[data['vehicle_number']]['errors']['events'].append(error_obj(d, "Odo value decreased between events"))

            if odo is None:
                analysis[data['vehicle_number']]['errors']['amount'] += 1
                analysis[data['vehicle_number']]['null'] += 1
                analysis[data['vehicle_number']]['errors']['events'].append(error_obj(d, "Odo values missing"))
            else:
                analysis[data['vehicle_number']]['odo'] += 1
            prevOdo = odo
            if spd == 0:
                chunk.append(d)
            else:
                if chunk:
                    stationaryEventChunks.append(chunk)
                    chunk = []
        if chunk:
            stationaryEventChunks.append(chunk)

        for stationaryChunk in stationaryEventChunks:
            if len(stationaryChunk) > 2:
                firstOdo = stationaryChunk[1].get('odo')
                lastOdo = stationaryChunk[len(stationaryChunk) - 1].get('odo')
                if firstOdo is not None and lastOdo is not None and firstOdo != lastOdo:
                    analysis[data['vehicle_number']]['errors']['amount'] += 1
                    analysis[data['vehicle_number']]['errors']['events'].append(error_obj(stationaryChunk[len(stationaryChunk) - 1], "Odo changed when stationary"))

    result = []
    for vehicle_number, analysis_data in analysis.items():
        total = sum([analysis_data[key] for key in analysis_data if key in ['odo', 'null']])
        odo_ratio = round(analysis_data['odo']/total, 3)
        null_ratio = round(analysis_data['null']/total, 3)
        if odo_ratio > 0 and odo_ratio < 1:
            analysis_data['errors']['amount'] += 1
            analysis_data['errors']['events'].append({'type': "Some odo values missing"})

        analysis_data = {
            'odo_ratio': odo_ratio,
            'null_ratio': null_ratio,
            'vehicle_number': vehicle_number,
            'errors': analysis_data['errors'],
            'operator_id': analysis_data['operator_id'],
        }
        result.append(analysis_data)
    
    return result

@router.get("/odo")
async def get_vehicles(
    date: date = Query(..., description="Format YYYY-MM-DD"),
    operator_id: Optional[int] = Query(None, description="HFP topic's vehicle id. Use without prefix zeros."),
    errorsOnly: Optional[bool] = Query(None, description="Only return vehicles that triggered an error")
):
    """
    Comment
    """
    formatted_data = await get_vehicle_data(date, operator_id, None)
    analyzed_data = analyze_odo(formatted_data)
    if errorsOnly:
        analyzed_data = [vehicle for vehicle in analyzed_data if vehicle['errors']['amount'] > 0]

    return {
        "data": {
            "vehicles": analyzed_data
        }
    }
