""" Routes for /vehicles endpoint """

from datetime import date
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
    'Drst missing': 'Drst arvo puuttuu',
    'Some of the drst values are missing': 'Ovitiedon arvo puuttuu osista tapahtumia',
    'Drst always true': 'Ovitieto aina auki',
    'Drst always false': 'Ovitieto aina kiinni'
}

@router.get("/doors")
async def get_vehicles(
    date: date = Query(..., description="Format YYYY-MM-DD"),
    vehicle_operator_id: Optional[int] = Query(None, description="ID of the vehicle operator"),
    errorsOnly: Optional[bool] = Query(None, description="Return vehicles that triggered an error")
):
    """
    Comment
    """
    formatted_data = await get_vehicle_data(date, vehicle_operator_id)
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
    vehicle_operator_id: Optional[int] = Query(None, description="ID of the vehicle operator"),
    errorsOnly: Optional[bool] = Query(None, description="Return vehicles that triggered an error")
):
    """
    Vehicle drst analysis as csv
    """
    formatted_data = await get_vehicle_data(date, vehicle_operator_id)
    analyzed_data = analyze_vehicle_data(formatted_data)
    csv_filename = "empty.csv"

    with open(csv_filename, "w", newline="") as csvfile:
        fieldnames = ["Päivämäärä", "Operaattori", "Kylkinumero", "Havaittu ongelma", "Syyt"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for item in analyzed_data:
            common_data = {                    
                "Päivämäärä": date, 
                "Operaattori": item['vehicle_operator_id'], 
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
                    "Havaittu ongelma": "Ongelmia havaittu", 
                    "Syyt": error_types_str
                }
                writer.writerow(row_data)

    return FileResponse(csv_filename, media_type="text/csv", filename="empty.csv")

async def get_vehicle_data(date, vehicle_operator_id):
    vehicle_data = await get_vehicles_by_timestamp(date, vehicle_operator_id)
    grouped_data = {}

    for item in vehicle_data:
        vehicle_number = item["vehicle_number"]
        if vehicle_number not in grouped_data:
            grouped_data[vehicle_number] = []
        grouped_data[vehicle_number].append(item)

    formatted_data = []
    for vehicle_number, data in grouped_data.items():
        sortedData = sorted(data, key=lambda x: x['tst'])
        vehicle_operator_id = sortedData[0]['vehicle_operator_id']
        formatted_data.append({
            "vehicle_number": vehicle_number,
            "vehicle_operator_id": vehicle_operator_id,
            "data": sortedData
        })

    return formatted_data

def analyze_vehicle_data(vehicle_data):
    analysis = defaultdict(lambda: {'null': 0, 'true': 0, 'false': 0, 'errors': {'amount': 0, 'events': []}})

    for data in vehicle_data:
        data_list = data.get('data', [])
        vehicle_operator_id = data.get('vehicle_operator_id')
        analysis[data['vehicle_number']]['vehicle_operator_id'] = vehicle_operator_id
        for d in data_list:
            drst = d.get('drst')
            stop = d.get('stop')
            spd = d.get('spd')
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
        total = sum([analysis_data[key] for key in analysis_data if key not in ['errors', 'vehicle_operator_id']])
        events_amount = analysis_data['null'] + analysis_data['true'] + analysis_data['false']
        true_ratio = round(analysis_data['true']/total, 3)
        false_ratio = round(analysis_data['false']/total, 3)
        null_ratio = round(analysis_data['null']/total, 3)

        if true_ratio > false_ratio:
            analysis_data['errors']['amount'] += 1
            analysis_data['errors']['events'].append({'type': "Drst inverted"})
        if null_ratio == 1:
            analysis_data['errors']['amount'] += 1
            analysis_data['errors']['events'].append({'type': "Drst missing"})
        if null_ratio > 0:
            analysis_data['errors']['amount'] += 1
            analysis_data['errors']['events'].append({'type': "Some of the drst values are missing"})
        if true_ratio == 1:
            analysis_data['errors']['amount'] += 1
            analysis_data['errors']['events'].append({'type': "Drst always true"})
        if false_ratio == 1:
            analysis_data['errors']['amount'] += 1
            analysis_data['errors']['events'].append({'type': "Drst always false"})

        analysis_data = {
            'null': null_ratio,
            'true': true_ratio,
            'false': false_ratio,
            'vehicle_operator_id': analysis_data['vehicle_operator_id'],
            'eventsAmount': events_amount,
            'vehicle_number': vehicle_number,
            'errors': analysis_data['errors']
        }

        result.append(analysis_data)
    
    return result

def error_obj(d, event):
    return {
        'tst': d.get('tst'),
        'oday': d.get('oday'),
        'type': event,
        'spd': d.get('spd'),
        'route_id': d.get('route_id'),
        'vehicle_operator_id': d.get('vehicle_operator_id'),
        'start': d.get('start'),
        'longitude': d.get('longitude'),
        'latitude': d.get('latitude'),
        'direction_id': d.get('direction_id')
    }

