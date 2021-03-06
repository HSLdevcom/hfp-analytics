"""HFP Analytics REST API"""
import azure.functions as func
from fastapi import FastAPI, HTTPException, Request
from fastapi.openapi.docs import (
    get_redoc_html,
    get_swagger_ui_html,
)
from common.utils import get_conn_params, get_feature_collection
import psycopg2 as psycopg
from .digitransit_import import main as run_digitransit_import

app = FastAPI(
    title="HSL Analytics REST API",
    description="This REST API is used to get results from analytics done with Jore-data and HFP-data.",
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

    run_digitransit_import()

    return "Import done."

@app.get("/jore_stops")
async def get_jore_stops(stop_id = -1):
    """Returns a GeoJSON FeatureCollection of either all jore stops or one jore stop found with given stop_id"""
    with psycopg.connect(**get_conn_params()) as conn:
        with conn.cursor() as cur:

            cur.execute("SELECT * FROM api.view_jore_stop_4326")
            stops = cur.fetchall()

            print(f'Found {len(stops)} Jore stops.')

            if len(stops) == 0:
                raise HTTPException(
                    status_code=404,
                    detail="Have you ran Jore & HFP data imports and then analysis?"
                )

            stop_geojson_features = []

            if stop_id != -1:
                stops = list(filter(lambda item: str(item[0]['properties']['stop_id']) == stop_id, stops))

                if len(stops) == 0:
                    raise HTTPException(
                        status_code=404,
                        detail=f'Did not find stop with given stop_id: {stop_id}'
                    )

            for stop_tuple in stops:
                stop = stop_tuple[0]
                stop_geojson_features.append(stop)

    return get_feature_collection(stop_geojson_features)

@app.get("/stop_medians")
async def get_stop_medians(stop_id = -1):
    """
    Returns a GeoJSON FeatureCollection of stop medians and their percentile radii
    """
    with psycopg.connect(**get_conn_params()) as conn:
        with conn.cursor() as cur:

            cur.execute("SELECT * FROM api.view_stop_median_4326")
            stop_medians = cur.fetchall()

            print(f'Found {len(stop_medians)} Jore stop medians.')

            if len(stop_medians) == 0:
                raise HTTPException(
                    status_code=404,
                    detail="Have you ran Jore & HFP data imports and then analysis?"
                )

            if stop_id != -1:
                stop_medians = list(filter(
                    lambda item: str(item[0]['properties']['stop_id']) == stop_id,
                    stop_medians
                ))
                if len(stop_medians) == 0:
                    raise HTTPException(
                        status_code=404,
                        detail=f'Did not find stop median with given stop_id: {stop_id}'
                    )

            stop_median_geojson_features = []

            for stop_median_tuple in stop_medians:
                stop_median = stop_median_tuple[0]
                stop_median_geojson_features.append(stop_median)

    return get_feature_collection(stop_median_geojson_features)


@app.get("/hfp_points/{stop_id}")
async def get_hfp_points(stop_id: str):
    """
    Returns a GeoJSON FeatureCollection with HFP (door) observations which were used for analysis of that stop_id
    OR which have NULL stop_id value but are located max search_distance_m (default 100) around the stop
    """
    with psycopg.connect(**get_conn_params()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM api.view_observation_4326 \
                WHERE st_asgeojson -> 'properties' ->> 'stop_id' = %(stop_id)s", {'stop_id': stop_id})
            stopIdObservations = cur.fetchall()

            print(f'Found {len(stopIdObservations)} observations with given stop_id: {stop_id}.')

            search_distance_m = 100
            cur.execute("SELECT api.get_observations_with_null_stop_id_4326(%(stop_id)s, %(search_distance_m)s)", {'stop_id': stop_id, 'search_distance_m': search_distance_m})
            observationsWithNullStopIds = cur.fetchall()

            print(f'Found {len(observationsWithNullStopIds)} observations with NULL stop_id')

            totalObservations = stopIdObservations + observationsWithNullStopIds

            if len(totalObservations) == 0:
                raise HTTPException(
                    status_code=404,
                    detail=f'Did not find hfp data for given stop: {stop_id}'
                )

            observation_geojson_features = []

            for observation_tuple in totalObservations:
                observation = observation_tuple[0]
                observation_geojson_features.append(observation)

    return get_feature_collection(observation_geojson_features)
