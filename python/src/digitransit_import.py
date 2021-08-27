# Import current stops, stations and route-direction combos using them from Digitransit.

import requests
import json
from datetime import date

GRAPHQL_URL = 'https://api.digitransit.fi/routing/v1/routers/hsl/index/graphql'

def create_query(query_type):
    assert query_type in ('stops', 'stations')
    query = '''
    {
        <PLACEHOLDER> {
          gtfsId
          code
          name
          lat
          lon
          parentStation { gtfsId }
          vehicleMode
          patterns {
            route { gtfsId }
            directionId
          }
        }
    }
    '''
    return query.replace('<PLACEHOLDER>', query_type)

def get_query(query):
    req = requests.post(
        url=GRAPHQL_URL,
        data=query,
        headers={'Content-Type': 'application/graphql'}
    )
    if req.status_code == 200:
        return req.json()
    else:
        raise Exception(f'{req} failed with status code {req.status_code}')

def make_route_dir(pattern):
    route = pattern['route']['gtfsId'].split(':')[1].strip()
    # NOTE: Digitransit uses 0/1 directions, while Jore uses 1/2 directions
    dir = str(pattern['directionId'] + 1)
    return f"'{route}-{dir}'"

def make_flat_row(gql_row):
    parent_station = None
    if gql_row['parentStation'] is not None:
        parent_station = gql_row['parentStation']['gtfsId']
        parent_station = int(parent_station.split(':')[1].strip())
    route_dirs_via_stop = None
    if gql_row['patterns'] is not None and len(gql_row['patterns']) > 0:
        route_dirs_via_stop = list(map(make_route_dir, gql_row['patterns']))
        route_dirs_via_stop = sorted(list(set(route_dirs_via_stop)))
        route_dirs_via_stop = '{' + ','.join(route_dirs_via_stop) + '}'
    return {
        'stop_id': int(gql_row['gtfsId'].split(':')[1].strip()),
        'stop_code': gql_row['code'],
        'stop_name': gql_row['name'],
        'parent_station': parent_station,
        'stop_mode': gql_row['vehicleMode'],
        'route_dirs_via_stop': route_dirs_via_stop,
        'date_imported': str(date.today()),
        'long': gql_row['lon'],
        'lat': gql_row['lat']
    }

def flatten_result(res):
    assert len(res['data'].keys()) == 1
    rows = list(res['data'].values())[0]
    return list(map(make_flat_row, rows))

def main():
    query = create_query('stops')
    res = get_query(query)
    res = flatten_result(res)
    print(res[0:min(len(res)-1, 10)])
    # TODO: DB insert

    query = create_query('stations')
    res = get_query(query)
    res = flatten_result(res)
    print(res[0:min(len(res)-1, 10)])
    # TODO: DB insert

if __name__ == '__main__':
    main()
