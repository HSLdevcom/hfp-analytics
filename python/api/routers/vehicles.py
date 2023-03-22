""" Routes for /vehicles endpoint """

from datetime import date, datetime, timedelta, time
from fastapi import APIRouter, Query
import logging
from common.logger_util import CustomDbLogHandler
import itertools
from collections import defaultdict
from typing import Optional
from starlette.responses import FileResponse
import csv
import os

from api.services.vehicles import get_vehicles_by_timestamp

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
    Comment
    """
    formatted_data = await get_vehicle_data(date, operator_id)
    analyzed_data = analyze_vehicle_data(formatted_data)
    if errorsOnly:
        analyzed_data = [vehicle for vehicle in analyzed_data if vehicle['errors']['amount'] > 0]

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
    formatted_data = await get_vehicle_data(date, operator_id)
    analyzed_data = analyze_vehicle_data(formatted_data)
    if errorsOnly:
        analyzed_data = [vehicle for vehicle in analyzed_data if vehicle['errors']['amount'] > 0]
    csv_filename = "empty.csv"

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
                    "Havaittu ongelma": "Epäluotettava ovitieto", 
                    "Syyt": error_types_str
                }
                writer.writerow(row_data)

    return FileResponse(csv_filename, media_type="text/csv", filename="empty.csv")

async def get_vehicle_data(date, operator_id):
    vehicle_data = await get_vehicles_by_timestamp(date, operator_id)
    grouped_data = {}

    for item in vehicle_data:
        vehicle_number = item["vehicle_number"]
        if vehicle_number not in grouped_data:
            grouped_data[vehicle_number] = []
        grouped_data[vehicle_number].append(item)

    formatted_data = []
    for vehicle_number, data in grouped_data.items():
        sortedData = sorted(data, key=lambda x: x['tst'])
        operator_id = sortedData[0]['operator_id']
        formatted_data.append({
            "vehicle_number": vehicle_number,
            "operator_id": operator_id,
            "data": sortedData
        })

    return formatted_data

def analyze_vehicle_data(vehicle_data):
    analysis = defaultdict(lambda: {'null': 0, 'true': 0, 'false': 0, 'errors': {'amount': 0, 'events': []}})

    for data in vehicle_data:
        data_list = data.get('data', [])
        operator_id = data.get('operator_id')
        analysis[data['vehicle_number']]['operator_id'] = operator_id
        for d in data_list:
            drst = d.get('drst')
            stop = d.get('stop')
            spd = d.get('spd')
            start = d.get('start')
            start_seconds = int(start.total_seconds())
            start_hours, remainder = divmod(start_seconds, 3600)
            start_minutes, start_seconds = divmod(remainder, 60)
            start_utc = time(start_hours, start_minutes, start_seconds)
            tst_str = d.get('tst').strftime('%Y-%m-%d %H:%M:%S.%f%z')
            tst = datetime.strptime(tst_str, '%Y-%m-%d %H:%M:%S.%f%z')
            tst += timedelta(hours=2)
            if tst.time() < start_utc:
                continue
            if drst is None:
                analysis[data['vehicle_number']]['null'] += 1
            elif drst:
                analysis[data['vehicle_number']]['true'] += 1
            else:
                analysis[data['vehicle_number']]['false'] += 1
            if drst and spd is not None and spd > spd_threshold:
                analysis[data['vehicle_number']]['errors']['amount'] += 1
                event = f'Speed over {spd_threshold} m/s when doors open'
                analysis[data['vehicle_number']]['errors']['events'].append(error_obj(d, event))   

    result = []
    for vehicle_number, analysis_data in analysis.items():
        total = sum([analysis_data[key] for key in analysis_data if key in ['null', 'true', 'false']])
        events_amount = analysis_data['null'] + analysis_data['true'] + analysis_data['false']
        if total == 0:
            continue
        if events_amount < 100:
            continue
        true_ratio = round(analysis_data['true']/total, 3)
        false_ratio = round(analysis_data['false']/total, 3)
        null_ratio = round(analysis_data['null']/total, 3)

        if true_ratio > 0.5 and true_ratio < 1:
            analysis_data['errors']['amount'] += 1
            analysis_data['errors']['events'].append(error_obj(d, "Drst inverted"))
        if null_ratio == 1:
            analysis_data['errors']['amount'] += 1
            analysis_data['errors']['events'].append(error_obj(d, "Drst missing"))
        if null_ratio > 0 and null_ratio < 1:
            analysis_data['errors']['amount'] += 1
            analysis_data['errors']['events'].append(error_obj(d, "Some of the drst values are missing"))
        if true_ratio == 1:
            analysis_data['errors']['amount'] += 1
            analysis_data['errors']['events'].append(error_obj(d, "Drst always true"))
        if false_ratio == 1:
            analysis_data['errors']['amount'] += 1
            analysis_data['errors']['events'].append(error_obj(d, "Drst always false"))

        analysis_data = {
            'null': null_ratio,
            'true': true_ratio,
            'false': false_ratio,
            'operator_id': analysis_data['operator_id'],
            'eventsAmount': events_amount,
            'vehicle_number': vehicle_number,
            'errors': analysis_data['errors']
        }

        result.append(analysis_data)
    
    sortedResult = sorted(result, key=lambda x: x['vehicle_number'])
    return sortedResult

def error_obj(d, event):
    start_str = str(d.get('start'))
    return {
        'tst': d.get('tst'),
        'oday': d.get('oday'),
        'type': event,
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

