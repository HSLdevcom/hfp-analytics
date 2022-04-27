"""Stop correspondence REST API"""
import azure.functions as func
from fastapi import FastAPI, HTTPException, Request
from fastapi.openapi.docs import (
    get_redoc_html,
    get_swagger_ui_html,
)
from .stopcorr.utils import get_conn_params, get_geojson_point, get_feature_collection
from .run_analysis import main as run_analysis_func
import psycopg2 as psycopg
from .hfp_import import main as run_hfp_import
from .digitransit_import import main as run_digitransit_import

description = """
   This REST API is used to get results from analytics done with Jore-data and HFP-data.
"""

app = FastAPI(
    title="HSL Analytics REST API",
    description=description,
    contact={
        "name": "HSL",
        "url": "https://github.com/HSLdevcom/hfp-analytics"
    },
    license_info={
        "name": "MIT License",
        "url": "https://github.com/HSLdevcom/hfp-analytics/blob/main/LICENSE"
    },
    docs_url=None,
    redoc_url=None,
)

def main(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    return func.AsgiMiddleware(app).handle(req, context)

@app.get("/")
async def root():
    """API root"""
    return "Welcome to HFP Analytics REST API root! The documentation can be found from /docs or /redoc"

@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html(request: Request):
    """
    API documentation, taken from: https://fastapi.tiangolo.com/advanced/extending-openapi/
    Note: to authenticate openapi, you can also check: https://github.com/tiangolo/fastapi/issues/364#issuecomment-890853577
    """
    code = request.query_params['code']
    return get_swagger_ui_html(
        openapi_url="/openapi.json?code=" + code,
        title=app.title + " Swagger UI",
    )

@app.get("/redoc", include_in_schema=False)
async def custom_swagger_ui_html(request: Request):
    """
    API documentation, taken from: https://fastapi.tiangolo.com/advanced/extending-openapi/
    Note: to authenticate openapi, you can also check: https://github.com/tiangolo/fastapi/issues/364#issuecomment-890853577
    """
    code = request.query_params['code']
    return get_redoc_html(
        openapi_url="/openapi.json?code=" + code,
        title=app.title + " ReDoc",
    )

@app.get("/run_import")
async def run_import():
    """Runs data import"""
    print("Running import...")

    run_hfp_import()
    run_digitransit_import()

    # TODO: update /job_status here?
    return "Import done."

@app.get("/run_analysis")
async def run_analysis():
    """Runs analysis"""
    print("Running analysis...")
    run_analysis_func()

    # TODO: update /job_status here?
    return "Analysis done."


@app.get("/jore_stops")
async def get_jore_stops(stop_id = -1):
    """Returns either all jore stops or one jore stop found with given stop_id"""
    with psycopg.connect(**get_conn_params()) as conn:
        with conn.cursor() as cur:

            cur.execute("SELECT row_to_json(row) FROM (SELECT * FROM jore_stop) row")
            stops = cur.fetchall()

            print(f'Found {len(stops)} Jore stops.')

            if len(stops) == 0:
                raise HTTPException(
                    status_code=404,
                    detail="Have you ran Jore & HFP data imports and then analysis?"
                )

            stop_geojson_features = []

            if stop_id != -1:
                stops = list(filter(lambda item: str(item[0]['stop_id']) == stop_id, stops))

                if len(stops) == 0:
                    raise HTTPException(
                        status_code=404,
                        detail=f'Did not find stop with given stop_id: {stop_id}'
                    )

            for stop_tuple in stops:
                stop = stop_tuple[0]
                stop_feature = get_geojson_point([stop['long'], stop['lat']], dict(
                    stop_id=stop['stop_id'],
                    stop_code=stop['stop_code'],
                    stop_name=stop['stop_name'],
                    parent_station=stop['parent_station'],
                    stop_mode=stop['stop_mode'],
                    route_dirs_via_stop=stop['route_dirs_via_stop'],
                    date_imported=stop['date_imported']
                ))
                stop_geojson_features.append(stop_feature)

    return get_feature_collection(stop_geojson_features)


@app.get("/stop_medians")
async def get_stop_medians(stop_id = -1):
    """Returns stop medians and their percentile radii"""
    with psycopg.connect(**get_conn_params()) as conn:
        with conn.cursor() as cur:

            cur.execute("SELECT \
                json_build_object( \
                    'stop_id', sm.stop_id, \
                    'from_date', sm.from_date, \
                    'to_date', sm.to_date, \
                    'n_stop_known', sm.n_stop_known, \
                    'n_stop_guessed', sm.n_stop_guessed, \
                    'n_stop_null_near', sm.n_stop_null_near, \
                    'dist_to_jore_point_m', sm.dist_to_jore_point_m, \
                    'observation_route_dirs', sm.observation_route_dirs, \
                    'result_class', sm.result_class, \
                    'recommended_min_radius_m', sm.recommended_min_radius_m, \
                    'manual_acceptance_needed', sm.manual_acceptance_needed, \
                    'geom', sm.geom, \
                    'percentile_radii', json_build_object( \
                        'stop_id', pr.stop_id, \
                        'percentile', pr.percentile, \
                        'radius_m', pr.radius_m, \
                        'n_observations', pr.n_observations \
                    ) \
                ) \
            FROM stop_median sm \
            LEFT JOIN percentile_radii pr ON sm.stop_id = pr.stop_id"
            )

            stop_medians = cur.fetchall()

            print(f'Found {len(stop_medians)} Jore stop medians.')

            if len(stop_medians) == 0:
                raise HTTPException(
                    status_code=404,
                    detail="Have you ran Jore & HFP data imports and then analysis?"
                )

            if stop_id != -1:
                stop_medians = list(filter(
                    lambda item: str(item[0]['stop_id']) == stop_id,
                    stop_medians
                ))
                if len(stop_medians) == 0:
                    raise HTTPException(
                        status_code=404,
                        detail=f'Did not find stop median with given stop_id: {stop_id}'
                    )

            stop_median_dict = {}

            for stop_median_tuple in stop_medians:
                stop_median = stop_median_tuple[0]
                current_key = stop_median['stop_id']
                current_stop_median = stop_median_dict.get(current_key)
                if current_stop_median is None:
                    current_stop_median = get_geojson_point(
                        stop_median['geom']['coordinates'],
                        dict(
                            stop_id=stop_median['stop_id'],
                            from_date=stop_median['from_date'],
                            to_date=stop_median['to_date'],
                            n_stop_known=stop_median['n_stop_known'],
                            n_stop_guessed=stop_median['n_stop_guessed'],
                            n_stop_null_near=stop_median['n_stop_null_near'],
                            dist_to_jore_point_m=stop_median['dist_to_jore_point_m'],
                            observation_route_dirs=stop_median['observation_route_dirs'],
                            result_class=stop_median['result_class'],
                            recommended_min_radius_m=stop_median['recommended_min_radius_m'],
                            manual_acceptance_needed=stop_median['manual_acceptance_needed'],
                            percentile_radii_list=[dict(
                                percentile=stop_median['percentile_radii']['percentile'],
                                radius_m=stop_median['percentile_radii']['radius_m'],
                                n_observations=stop_median['percentile_radii']['n_observations']
                            )]
                        ))
                    stop_median_dict[current_key] = current_stop_median
                else:
                    current_stop_median['properties']['percentile_radii_list'].append(dict(
                        percentile=stop_median['percentile_radii']['percentile'],
                        radius_m=stop_median['percentile_radii']['radius_m'],
                        n_observations=stop_median['percentile_radii']['n_observations']
                    ))
                    stop_median_dict[current_key] = current_stop_median

            stop_median_geojson_features = []

            for key in stop_median_dict:
                stop_median_geojson_features.append(stop_median_dict[key])

    return get_feature_collection(stop_median_geojson_features)


@app.get("/hfp_points/{stop_id}")
async def get_hfp_points(stop_id: str):
    """
    Returns a GeoJSON with HFP (door) observations which were used for analysis of that stop_id
    OR which have NULL stop_id value but are located max nullStopIdDistanceInMeters around the stop
    """
    with psycopg.connect(**get_conn_params()) as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT row_to_json(row) FROM ( \
                SELECT * FROM \
                observation \
                WHERE stop_id = %(stop_id)s) row', {'stop_id': stop_id})
            observations = cur.fetchall()

            cur.execute('SELECT row_to_json(row) FROM ( \
                WITH null_stop_ids_within_meters AS ( \
                SELECT \
                    found_ob_stops.lat, \
                    found_ob_stops.long, \
                    found_ob_stops.stop_id, \
                    found_ob_stops.stop_id_guessed, \
                    found_ob_stops.event, \
                    found_ob_stops.dist_to_jore_point_m, \
                    found_ob_stops.dist_to_median_point_m \
                FROM jore_stop js \
                INNER JOIN LATERAL ( \
                    SELECT * FROM observation AS ob \
                    WHERE ob.stop_id IS NULL AND \
                    ST_DWithin(ob.geom, js.geom, %(nullStopIdDistanceInMeters)s) \
                ) as found_ob_stops \
                ON true \
                WHERE js.stop_id = %(stop_id)s \
                ) \
                SELECT * FROM null_stop_ids_within_meters \
            ) row', {'stop_id': stop_id, 'nullStopIdDistanceInMeters': 100})
            null_stop_id_observations = cur.fetchall()

            observations = observations + null_stop_id_observations

            print(f'Found {len(observations)} observations.')

            if len(observations) == 0:
                raise HTTPException(
                    status_code=404,
                    detail=f'Did not find hfp data for given stop: {stop_id}'
                )

            hfp_geojson_features = []

            for observation_tuple in observations:
                observation = observation_tuple[0]
                hfp_feature = get_geojson_point([observation['long'], observation['lat']], dict(
                    stop_id=observation['stop_id'],
                    stop_id_guessed=observation['stop_id_guessed'],
                    event=observation['event'],
                    dist_to_jore_point_m=observation['dist_to_jore_point_m'],
                    dist_to_median_point_m=observation['dist_to_median_point_m']
                ))
                hfp_geojson_features.append(hfp_feature)

    return get_feature_collection(hfp_geojson_features)
