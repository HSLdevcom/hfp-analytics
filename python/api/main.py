"""HFP Analytics REST API"""
import io
import gzip
from typing import Optional
from datetime import date, datetime

import azure.functions as func
from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.openapi.docs import (
    get_redoc_html,
    get_swagger_ui_html,
)
from fastapi.responses import Response
import psycopg2 as psycopg

from common.utils import get_conn_params, tuples_to_feature_collection
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
    openapi_tags=[{"name": "HFP data", "description": "API to query raw HFP data."}],
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
async def custom_redoc_ui_html(request: Request):
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
    with psycopg.connect(get_conn_params()) as conn:
        with conn.cursor() as cur:

            cur.execute("SELECT * FROM api.view_jore_stop_4326")
            stops = cur.fetchall()

            print(f'Found {len(stops)} Jore stops.')

            if len(stops) == 0:
                raise HTTPException(
                    status_code=404,
                    detail="Have you ran Jore & HFP data imports and then analysis?"
                )


            if stop_id != -1:
                stops = list(filter(lambda item: str(item[0]['properties']['stop_id']) == stop_id, stops))

                if len(stops) == 0:
                    raise HTTPException(
                        status_code=404,
                        detail=f'Did not find stop with given stop_id: {stop_id}'
                    )

            return tuples_to_feature_collection(geom_tuples=stops)

@app.get("/stop_medians")
async def get_stop_medians(stop_id = -1):
    """
    Returns a GeoJSON FeatureCollection of stop medians and their percentile radii
    """
    with psycopg.connect(get_conn_params()) as conn:
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

            return tuples_to_feature_collection(geom_tuples=stop_medians)


@app.get("/hfp_points/{stop_id}")
async def get_hfp_points(stop_id: str):
    """
    Returns a GeoJSON FeatureCollection with HFP (door) observations which were used for analysis of that stop_id
    OR which have NULL stop_id value but are located max search_distance_m (default 100) around the stop
    """
    with psycopg.connect(get_conn_params()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM api.view_observation_4326 \
                WHERE st_asgeojson -> 'properties' ->> 'stop_id' = %(stop_id)s", {'stop_id': stop_id})
            stop_id_observations = cur.fetchall()

            print(f'Found {len(stop_id_observations)} observations with given stop_id: {stop_id}.')

            search_distance_m = 100
            cur.execute("SELECT api.get_observations_with_null_stop_id_4326(%(stop_id)s, %(search_distance_m)s)", {'stop_id': stop_id, 'search_distance_m': search_distance_m})
            observations_with_null_stop_ids = cur.fetchall()

            print(f'Found {len(observations_with_null_stop_ids)} observations with NULL stop_id')

            total_observations = stop_id_observations + observations_with_null_stop_ids

            if len(total_observations) == 0:
                raise HTTPException(
                    status_code=404,
                    detail=f'Did not find hfp data for given stop: {stop_id}'
                )

            return tuples_to_feature_collection(geom_tuples=total_observations)

@app.get("/percentile_circles/{stop_id}")
async def get_percentile_circles(stop_id: str):
    """
    Returns a GeoJSON FeatureCollection of percentile circles around the given stop by stop_id.
    """
    with psycopg.connect(get_conn_params()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT api.get_percentile_circles_with_stop_id(%(stop_id)s)", {'stop_id': stop_id })
            percentile_circles = cur.fetchall()

        return tuples_to_feature_collection(geom_tuples=percentile_circles)

@app.get("/monitored_vehicle_journeys")
async def get_monitored_vehicle_journeys(operating_day: date = Query(..., description="Format YYYY-MM-DD")):
    """
    Returns assumed monitored vehicle journeys from given operating day. Assumed here means
    that the journeys might be valid or not, API doesn't know it. Invalid journey is example
    a journey where bus driver signed in to a wrong departure.
    """

    with psycopg.connect(get_conn_params()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM api.view_assumed_monitored_vehicle_journey where oday = %(operating_day)s", {'operating_day': operating_day})
            vehicle_journeys = cur.fetchall()

            cur.execute("SELECT MAX(modified_at) FROM api.view_assumed_monitored_vehicle_journey where oday = %(operating_day)s", {'operating_day': operating_day})
            last_updated = cur.fetchone()[0]

            results = []
            for vehicle_journey in vehicle_journeys:
                results.append({
                    "route_id": vehicle_journey[0],
                    "direction_id": vehicle_journey[1],
                    "oday": vehicle_journey[2],
                    "start_24h": str(vehicle_journey[3]),
                    "operator_id": vehicle_journey[4],
                    "vehicle_operator_id": vehicle_journey[5],
                    "vehicle_number": vehicle_journey[6],
                    "min_timestamp": vehicle_journey[7],
                    "max_timestamp": vehicle_journey[8],
                    "modified_at": vehicle_journey[9].isoformat(timespec="seconds")
                })
            return {
                "data": {
                    "monitoredVehicleJourneys": results
                },
                "last_updated": last_updated.isoformat(timespec="seconds")
            }


@app.get("/hfp/fetch",
         summary="Get HFP raw data",
         tags=["HFP data"],
         description="Returns raw HFP data in a gzip compressed csv file.")
async def get_hfp_raw_data(
    route_id: Optional[str] = Query(default=None,
                                    title="Route ID",
                                    description="JORE ID of the route. Required when no `oper` and `veh` provided.",
                                    example="2550"),
    oper: Optional[int] = Query(default=None,
                                title="Operator ID",
                                description="Operator ID of the vehicle. Required when no `route_id` provided.",
                                example="18"),
    veh: Optional[int] = Query(default=None,
                               description="Vehicle ID. Required when no `route_id` provided.",
                               example="662"),
    oday: date = Query(...,
                       title="Operating day",
                       description=("Operating day of the journey. "
                                    "Remember that the database contains data from previous 14 days. "
                                    "Format YYYY-MM-DD"),
                       example="2023-01-12")
) -> Response:
    """
    Get hfp data in raw csv format filtered by parameters.
    """
    if not route_id and not (oper and veh):
        raise HTTPException(400, detail="Either route_id or oper and veh -parameters are required!")

    with psycopg.connect(get_conn_params()) as conn:
        with conn.cursor() as cur:
            # IS NULL statements make route_id, oper and veh optional
            query = cur.mogrify("""
                SELECT
                    event_type,
                    route_id,
                    direction_id,
                    vehicle_operator_id,
                    observed_operator_id,
                    vehicle_number,
                    oday,
                    "start",
                    stop,
                    tst,
                    loc,
                    latitude,
                    longitude,
                    odo,
                    drst
                FROM hfp.view_as_original_hfp_event
                WHERE
                    (%s IS NULL OR route_id = %s) AND
                    ((%s IS NULL AND %s IS NULL ) OR (vehicle_operator_id = %s AND vehicle_number = %s))
                    AND oday = %s
            """, (route_id, route_id, oper, veh, oper, veh, oday,))

            # Input stream for csv data from database, output stream for compressed data
            input_stream = io.StringIO()
            output_stream = io.BytesIO()

            cur.copy_expert(f"COPY ({query.decode()}) TO STDOUT WITH CSV HEADER", input_stream)

            data = input_stream.getvalue().encode()

            with gzip.GzipFile(fileobj=output_stream, mode='wb') as compressed_data_stream:
                compressed_data_stream.write(data)

            response = Response(content=output_stream.getvalue(),
                                media_type="application/gzip")

            filename = f"hfp-export-{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv.gz"
            # Send as an attachment
            response.headers["Content-Disposition"] = f"attachment; filename={filename}"

    return response
