""" Routes for /vehicles endpoint """

from datetime import date
from fastapi import APIRouter, Query
import logging
from common.logger_util import CustomDbLogHandler
import itertools
from collections import defaultdict
from typing import Optional

from api.services.vehicles import get_vehicles_by_timestamp

logger = logging.getLogger('api')

router = APIRouter(
    prefix="/vehicles",
    tags=["Vehicle analytics data"]
)

spd_threshold = 1

@router.get("/doors")
async def get_vehicles(
    date: date = Query(..., description="Format YYYY-MM-DD"),
    vehicle_operator_id: Optional[int] = Query(None, description="ID of the vehicle operator"),
    errorsOnly: Optional[bool] = Query(None, description="Return vehicles that triggered an error")
):
    """
    Comment
    """
    testData = await get_vehicles_by_timestamp(date, vehicle_operator_id)

    grouped_data = {}
    for item in testData:
        vehicle_number = item["vehicle_number"]
        if vehicle_number not in grouped_data:
            grouped_data[vehicle_number] = []
        grouped_data[vehicle_number].append(item)

    formatted_data = []
    for vehicle_number, data in grouped_data.items():
        sortedData = sorted(data, key=lambda x: x['tst'])
        formatted_data.append({
            "vehicle_number": vehicle_number,
            "data": sortedData
        })
    analyzed_data = analyze_vehicle_data(formatted_data)
    if errorsOnly:
        analyzed_data = [vehicle for vehicle in analyzed_data if vehicle['errors']['amount'] > 0]

    return {
        "data": {
            "vehicles": analyzed_data
        }
    }

def analyze_vehicle_data(vehicle_data):
    analysis = defaultdict(lambda: {'null': 0, 'true': 0, 'false': 0, 'errors': {'amount': 0, 'events': []}})
    for data in vehicle_data:
        data_list = data.get('data', [])
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
            
            if stop is None and drst:
                analysis[data['vehicle_number']]['errors']['amount'] += 1
                event = "Drst true outside stop zone"
                analysis[data['vehicle_number']]['errors']['events'].append(error_obj(d, event))

            if drst and spd is not None and spd > spd_threshold:
                analysis[data['vehicle_number']]['errors']['amount'] += 1
                event = f'Speed over {spd_threshold} m/s when doors open'
                analysis[data['vehicle_number']]['errors']['events'].append(error_obj(d, event))

    result = []
    for vehicle_number, analysis_data in analysis.items():
        total = sum([analysis_data[key] for key in analysis_data if key != 'errors'])
        events_amount = analysis_data['null'] + analysis_data['true'] + analysis_data['false']
        true_ratio = round(analysis_data['true']/total, 3)
        false_ratio = round(analysis_data['false']/total, 3)
        if true_ratio > false_ratio:
            analysis_data['errors']['amount'] += 1
            analysis_data['errors']['events'].append({'type': "Drst inverted"})

        analysis_data = {
            'null': round(analysis_data['null']/total, 3),
            'true': true_ratio,
            'false': false_ratio,
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

