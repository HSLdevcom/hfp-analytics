import os
import pptx
import logging as logger
import psycopg2
import logging
import time
import json
import pytz
from psycopg_pool import AsyncConnectionPool
from collections import defaultdict
from typing import Optional
from starlette.responses import FileResponse
from common.logger_util import CustomDbLogHandler
from datetime import date, datetime, timedelta, time
from fastapi import APIRouter, Query

logger = logging.getLogger('importer')

def get_conn_params() -> str:
    return os.getenv("POSTGRES_CONNECTION_STRING", "")

pool = AsyncConnectionPool(get_conn_params(), max_size=20)
spd_threshold = 1


async def get_vehicle_ids(date: date, customTimeInterval=None, operator_id=None) -> list:
    """Query all vehicles filtered by date and return them as a list of dicts"""
    date_str = str(date)
    query_params = {
        "start": datetime.strptime(date_str + " 00:00:00.000+00", '%Y-%m-%d %H:%M:%S.%f+00'),
        "end": datetime.strptime(date_str + " 23:59:00.000+00", '%Y-%m-%d %H:%M:%S.%f+00')
    }

    if customTimeInterval:
        query_params = {
            "start": datetime.strptime(date_str + customTimeInterval["start"], '%Y-%m-%d %H:%M:%S.%f+00'),
            "end": datetime.strptime(date_str + customTimeInterval["end"], '%Y-%m-%d %H:%M:%S.%f+00')
        }

    where_clause = "WHERE tst > %(start)s AND tst < %(end)s AND event_type = 'VP'"
    if operator_id is not None:
        query_params['operator_id'] = operator_id
        where_clause += " AND vehicle_operator_id = %(operator_id)s"
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT DISTINCT vehicle_number, vehicle_operator_id FROM api.view_as_original_hfp_event {where_clause}
                """.format(where_clause=where_clause),
                query_params
            )
            rows = await cur.fetchall()

            unique_data = [{ "vehicle_number": r[0], "operator_id": r[1] } for r in rows]
            return unique_data

async def get_vehicle_data(date, operator_id, vehicle_number, customTimeInterval=None):
    vehicle_operator_id = operator_id
    vehicle_data = await get_vehicles_by_timestamp(date, vehicle_operator_id, vehicle_number, customTimeInterval)
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
        tst_str = sortedData[0]['tst'].strftime("%Y-%m-%d")
        formatted_data.append({
            "vehicle_number": vehicle_number,
            "operator_id": operator_id,
            "date": tst_str,
            "data": sortedData
        })

    return formatted_data

def tz_diff(tz1, tz2):
    date = datetime.now()
    return (tz1.localize(date) - 
            tz2.localize(date).astimezone(tz1))\
            .seconds/3600

def analyze_vehicle_door_data(vehicle_data):
    analysis = defaultdict(lambda: {'null': 0, 'true': 0, 'false': 0, 'door_error_events': {'amount': 0, 'events': []}})
    for data in vehicle_data:
        data_list = data.get('data', [])
        data_list_sorted = sorted(data_list, key=lambda x: x['tst'])
        operator_id = data.get('operator_id')
        date = data.get('date')
        analysis[data['vehicle_number']]['operator_id'] = operator_id
        analysis[data['vehicle_number']]['date'] = date

        for d in data_list_sorted:
            drst = d.get('drst')
            stop = d.get('stop')
            spd = d.get('spd')
            utc = pytz.timezone('UTC')
            helsinki = pytz.timezone('Europe/Helsinki')

            # Get timezone offset
            tz_offset = tz_diff(utc, helsinki)

            # 'start' is stored as timedelta
            start_duration = d.get('start')

            # Convert to seconds, hours, minutes, and seconds
            start_seconds_total = int(start_duration.total_seconds())
            start_hours, remainder = divmod(start_seconds_total, 3600)
            start_minutes, start_seconds = divmod(remainder, 60)

            # Create a time object using hours, minutes, and seconds
            start_time = time(start_hours, start_minutes, start_seconds)

            # Convert tst to datetime object
            tst_string = d.get('tst').strftime('%Y-%m-%d %H:%M:%S.%f%z')
            tst_datetime = datetime.strptime(tst_string, '%Y-%m-%d %H:%M:%S.%f%z')

            # Add the timezone offset to the tst datetime object
            tst_datetime += timedelta(hours=tz_offset)

            # Don't iterate through events where tst is before journey start time
            if tst_datetime.time() < start_time:
                continue
            
            if drst is None:
                analysis[data['vehicle_number']]['null'] += 1
            elif drst:
                analysis[data['vehicle_number']]['true'] += 1
            else:
                analysis[data['vehicle_number']]['false'] += 1
            if drst and spd is not None and spd > spd_threshold:
                event = f'Speed over {spd_threshold} m/s when doors open'
                analysis[data['vehicle_number']]['door_error_events']['events'].append(error_obj(d, event))  
                analysis[data['vehicle_number']]['door_error_events']['amount'] += 1 

    result = []
    for vehicle_number, analysis_data in analysis.items():
        total = sum([analysis_data[key] for key in analysis_data if key in ['null', 'true', 'false']])
        if total < 100:
            result.append({
                'vehicle_number': vehicle_number,
                'operator_id': analysis_data['operator_id'],
                'date': analysis_data['date'],
                'drst_null_ratio': None,
                'drst_true_ratio': None,
                'drst_false_ratio': None,
                'events_amount': None,
                'door_error_types': [],
                'door_error_events': {
                    "events": [],
                    "types": []
                }
            })
            continue
        true_ratio = round(analysis_data['true']/total, 3)
        false_ratio = round(analysis_data['false']/total, 3)
        null_ratio = round(analysis_data['null']/total, 3)

        error_types = set()
        for event in analysis_data['door_error_events']['events']:
            event_type = event['type']
            error_types.add(event_type)

        if true_ratio > 0.5 and true_ratio < 1:
            error_types.add("Drst inverted")  
        if null_ratio == 1:
            error_types.add("Drst missing")  
        if null_ratio > 0 and null_ratio < 1:
            error_types.add("Some of the drst values are missing")  
        if true_ratio == 1:
            error_types.add("Drst always true")  
        if false_ratio == 1:
            error_types.add("Drst always false")  

        if error_types:
            error_types = list(error_types)
            analysis_data['door_error_events']['types'] = error_types
        else:
            analysis_data['door_error_events']['types'] = []

        analysis_data = {
            'vehicle_number': vehicle_number,
            'operator_id': analysis_data['operator_id'],
            'date': analysis_data['date'],
            'drst_null_ratio': null_ratio,
            'drst_true_ratio': true_ratio,
            'drst_false_ratio': false_ratio,
            'events_amount': total,
            'door_error_types': analysis_data['door_error_events']['types'],
            'door_error_events': analysis_data['door_error_events']
        }

        result.append(analysis_data)
    
    sortedResult = sorted(result, key=lambda x: x['vehicle_number'])
    return sortedResult


def analyze_odo_data(vehicle_data):
    analysis = defaultdict(lambda: {'odo': 0, 'null': 0, 'odo_error_events': {'amount': 0, 'events': []}})
    for data in vehicle_data:
        data_list = data.get('data', [])
        data_list_sorted = sorted(data_list, key=lambda x: x['tst'])
        analysis[data['vehicle_number']]['operator_id'] = data.get('operator_id')
        firstEvent = data_list_sorted[0]
        lastEvent = data_list_sorted[len(data_list_sorted) - 1]
        if firstEvent is not None and lastEvent is not None:
            firstOdo = firstEvent.get('odo')
            lastOdo = lastEvent.get('odo')
            if firstOdo == lastOdo:
                analysis[data['vehicle_number']]['odo_error_events']['events'].append(error_obj(firstEvent, "Identical first and last odo values"))
                analysis[data['vehicle_number']]['odo_error_events']['events'].append(error_obj(lastEvent, "Identical first and last odo values"))
            if lastOdo is not None and lastOdo > 1000000:
                analysis[data['vehicle_number']]['odo_error_events']['events'].append(error_obj(lastEvent, "Odo value over 1000000"))

        prevOdo = None
        previousEvent = None
        stationaryEventChunks = []
        chunk = []
        for d in data_list_sorted:
            odo = d.get('odo')
            spd = d.get('spd')
            # Check if odo has decreased
            if all((prevOdo, odo, previousEvent)):
                # Odo resets between route departures so we only want to compare odos with same 'start'
                if prevOdo > odo and d.get('start') == previousEvent.get('start'):
                    analysis[data['vehicle_number']]['odo_error_events']['events'].append(error_obj(previousEvent, "Odo value decreased"))
                    analysis[data['vehicle_number']]['odo_error_events']['events'].append(error_obj(d, "Odo value decreased"))

            # Check if odo null otherwise add to odo count
            if odo is None:
                analysis[data['vehicle_number']]['null'] += 1
            else:
                analysis[data['vehicle_number']]['odo'] += 1

            prevOdo = odo
            previousEvent = d

            # Get chunks of events where spd is 0
            if spd == 0:
                chunk.append(d)
            else:
                if chunk:
                    stationaryEventChunks.append(chunk)
                    chunk = []
        if chunk:
            stationaryEventChunks.append(chunk)

        # Check if odo changes during the event chunks where spd is 0
        for stationaryChunk in stationaryEventChunks:
            stationaryChunkLength = len(stationaryChunk) 
            if stationaryChunkLength > 2:
                firstOdo = stationaryChunk[0].get('odo')
                lastOdo = stationaryChunk[stationaryChunkLength - 1].get('odo')
                if firstOdo is not None and lastOdo is not None and firstOdo != lastOdo:
                    analysis[data['vehicle_number']]['odo_error_events']['events'].append(error_obj(stationaryChunk[0], "Odo changed when stationary"))
                    analysis[data['vehicle_number']]['odo_error_events']['events'].append(error_obj(stationaryChunk[stationaryChunkLength - 1], "Odo changed when stationary"))

    result = []
    for vehicle_number, analysis_data in analysis.items():
        total = sum([analysis_data[key] for key in analysis_data if key in ['odo', 'null']])
        odo_ratio = round(analysis_data['odo']/total, 3)
        null_ratio = round(analysis_data['null']/total, 3)

        error_types = set()
        for event in analysis_data['odo_error_events']['events']:
            event_type = event['type']
            error_types.add(event_type)

        if odo_ratio > 0 and odo_ratio < 1:
            error_types.add("Some odo values missing")
        if odo_ratio == 0:
            error_types.add("Odo values missing")

        if error_types:
            error_types = list(error_types)
            analysis_data['odo_error_events']['types'] = error_types
        else:
            analysis_data['odo_error_events']['types'] = []

        analysis_data = {
            'vehicle_number': vehicle_number,
            'operator_id': analysis_data['operator_id'],
            'odo_exists_ratio': odo_ratio,
            'odo_null_ratio': null_ratio,
            'odo_error_types': analysis_data['odo_error_events']['types'],
            'odo_error_events': analysis_data['odo_error_events'],
        }
        result.append(analysis_data)
    
    return result

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
        'vehicle_number': d.get('vehicle_number'),
        'start': start_str,
        'longitude': d.get('longitude'),
        'latitude': d.get('latitude'),
        'direction_id': d.get('direction_id')
    }

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, date):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)

async def get_door_analysis_by_date(date: date, operator_id=None) -> list:
    """Query door analysis filtered by date and operator_id and return them as a list of dicts"""
    date_str = str(date)
    where_clause = f"WHERE date = '{date_str}'"
    query_params = {"date": date_str}

    if operator_id is not None:
        where_clause += " AND vehicle_operator_id = %(operator_id)s"
        query_params["operator_id"] = operator_id
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                    vehicle_number,
                    vehicle_operator_id,
                    date,
                    events_amount,
                    drst_null_ratio,
                    drst_true_ratio,
                    drst_false_ratio,
                    door_error_events,
                    door_error_types
                FROM hfp.vehicle_analysis
                {where_clause}
                """.format(where_clause=where_clause),
                query_params
            )
            rows = await cur.fetchall()

            data = [
                {
                    "vehicle_number": r[0],
                    "operator_id": r[1],
                    "date": r[2],
                    "events_amount": r[3],
                    "drst_null_ratio": r[4],
                    "drst_true_ratio": r[5],
                    "drst_false_ratio": r[6],
                    "door_error_events": {
                        "events": r[7],
                        "types": r[8]
                    }
                }
                for r in rows
            ]
            return data

async def get_odo_analysis_by_date(date: date, operator_id=None) -> list:
    """Query odo analysis filtered by date and operator_id and return them as a list of dicts"""
    date_str = str(date)
    where_clause = f"WHERE date = '{date_str}'"
    query_params = {"date": date_str}

    if operator_id is not None:
        where_clause += " AND vehicle_operator_id = %(operator_id)s"
        query_params["operator_id"] = operator_id
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                    vehicle_number,
                    vehicle_operator_id,
                    date,
                    events_amount,
                    odo_exists_ratio,
                    odo_null_ratio,
                    odo_error_events,
                    odo_error_types
                FROM hfp.vehicle_analysis
                {where_clause}
                """.format(where_clause=where_clause),
                query_params
            )
            rows = await cur.fetchall()
            data = [
                {
                    "vehicle_number": r[0],
                    "operator_id": r[1],
                    "date": r[2],
                    "events_amount": r[3],
                    "odo_exists_ratio": r[4],
                    "odo_null_ratio": r[5],
                    "odo_error_events": {
                        "events": r[6],
                        "types": r[7]
                    }
                }
                for r in rows
            ]
            return data

async def get_all_analysis_by_date(date: date, operator_id=None) -> list:
    """Query all analysis filtered by date and operator_id and return them as a list of dicts"""
    date_str = str(date)
    where_clause = f"WHERE date = '{date_str}'"
    query_params = {"date": date_str}

    if operator_id is not None:
        where_clause += " AND vehicle_operator_id = %(operator_id)s"
        query_params["operator_id"] = operator_id
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                    vehicle_number,
                    vehicle_operator_id,
                    date,
                    events_amount,
                    drst_null_ratio,
                    drst_true_ratio,
                    drst_false_ratio,
                    door_error_events,
                    door_error_types,
                    odo_exists_ratio,
                    odo_null_ratio,
                    odo_error_events,
                    odo_error_types
                FROM hfp.vehicle_analysis
                {where_clause}
                """.format(where_clause=where_clause),
                query_params
            )
            rows = await cur.fetchall()

            data = [
                {
                    "vehicle_number": r[0],
                    "operator_id": r[1],
                    "date": r[2],
                    "events_amount": r[3],
                    "drst_null_ratio": r[4],
                    "drst_true_ratio": r[5],
                    "drst_false_ratio": r[6],
                    "odo_exists_ratio": r[9],
                    "odo_null_ratio": r[10],
                    "door_error_events": {
                        "events": r[7],
                        "types": r[8]
                    },
                    "odo_error_events": {
                        "events": r[11],
                        "types": r[12]
                    }
                }
                for r in rows
            ]
            return data

async def get_vehicles_by_timestamp(date: date, vehicle_operator_id: int, vehicle_number=None, customTimeInterval=None) -> list:
    """Query all vehicles filtered by oday and return them as a list of dicts"""
    date_str = str(date)

    query_params = {
        "start": datetime.strptime(date_str + " 14:00:00.000+00", '%Y-%m-%d %H:%M:%S.%f+00'),
        "end": datetime.strptime(date_str + " 15:00:00.000+00", '%Y-%m-%d %H:%M:%S.%f+00')
    }

    if customTimeInterval:
        query_params = {
            "start": datetime.strptime(date_str + customTimeInterval["start"], '%Y-%m-%d %H:%M:%S.%f+00'),
            "end": datetime.strptime(date_str + customTimeInterval["end"], '%Y-%m-%d %H:%M:%S.%f+00')
        }

    where_clause = "WHERE tst > %(start)s AND tst < %(end)s AND event_type = 'VP'"
    if vehicle_operator_id is not None:
        where_clause += " AND vehicle_operator_id = %(vehicle_operator_id)s"
        query_params["vehicle_operator_id"] = vehicle_operator_id
    if vehicle_number is not None:
        where_clause += " AND vehicle_number = %(vehicle_number)s"
        query_params["vehicle_number"] = vehicle_number
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                    tst,
                    vehicle_operator_id,
                    vehicle_number,
                    route_id,
                    direction_id,
                    oday,
                    start,
                    odo,
                    spd,
                    drst,
                    loc,
                    stop,
                    longitude,
                    latitude
                FROM api.view_as_original_hfp_event
                {where_clause}
                """.format(where_clause=where_clause),
                query_params
            )
            rows = await cur.fetchall()

            data = [
                {
                    "tst": r[0],
                    "operator_id": r[1],
                    "vehicle_number": r[2],
                    "route_id": r[3],
                    "direction_id": r[4],
                    "oday": r[5],
                    "start": r[6],
                    "odo": r[7],
                    "spd": r[8],
                    "drst": r[9],
                    "loc": r[10],
                    "stop": r[11],
                    "longitude": r[12],
                    "latitude": r[13]
                }
                for r in rows
            ]
            return data

async def insert_vehicle_data(vehicle_data):
    """Insert analysis data to db"""
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            for data in vehicle_data:
                if 'date' not in data or 'vehicle_number' not in data:
                    continue
                vehicleDate = data['date']
                vehicleNumber = data['vehicle_number']
                try:
                    await cur.execute(
                        "INSERT INTO hfp.vehicle_analysis (vehicle_number, vehicle_operator_id, date, drst_null_ratio, drst_true_ratio, drst_false_ratio, door_error_events, door_error_types, odo_exists_ratio, odo_null_ratio, odo_error_events, odo_error_types, events_amount) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (data['vehicle_number'], data['operator_id'], data['date'], data['drst_null_ratio'], data['drst_true_ratio'], data['drst_false_ratio'], json.dumps(data['door_error_events']['events'], cls=DateTimeEncoder), data['door_error_events']['types'], data['odo_exists_ratio'], data['odo_null_ratio'], json.dumps(data['odo_error_events']['events'], cls=DateTimeEncoder), data['odo_error_events']['types'], data['events_amount'])
                    )
                except Exception as e:
                    print(f"Error: {e}. Skipping row with date={data['date']} and vehicle_number={data['vehicle_number']}")
                    continue
            await conn.commit()
            return "Inserts done"
