""" Routes for /stops endpoint """
# TODO: Move data queries to services/stops

from fastapi import APIRouter, HTTPException
import psycopg2 as psycopg

from common.utils import tuples_to_feature_collection
from common.config import POSTGRES_CONNECTION_STRING
from api.digitransit_import import main as run_digitransit_import

router = APIRouter(
    prefix="/stops",
    tags=["Stop analytics data"]
)


@router.get("/run_import")
async def run_import():
    """Runs data import"""
    print("Running import...")

    run_digitransit_import()

    return "Import done."


@router.get("/jore_stops")
async def get_jore_stops(stop_id = -1):
    """Returns a GeoJSON FeatureCollection of either all jore stops or one jore stop found with given stop_id"""
    with psycopg.connect(POSTGRES_CONNECTION_STRING) as conn:
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


@router.get("/stop_medians")
async def get_stop_medians(stop_id = -1):
    """
    Returns a GeoJSON FeatureCollection of stop medians and their percentile radii
    """
    with psycopg.connect(POSTGRES_CONNECTION_STRING) as conn:
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


@router.get("/hfp_points/{stop_id}")
async def get_hfp_points(stop_id: str):
    """
    Returns a GeoJSON FeatureCollection with HFP (door) observations which were used for analysis of that stop_id
    OR which have NULL stop_id value but are located max search_distance_m (default 100) around the stop
    """
    with psycopg.connect(POSTGRES_CONNECTION_STRING) as conn:
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


@router.get("/percentile_circles/{stop_id}")
async def get_percentile_circles(stop_id: str):
    """
    Returns a GeoJSON FeatureCollection of percentile circles around the given stop by stop_id.
    """
    with psycopg.connect(POSTGRES_CONNECTION_STRING) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT api.get_percentile_circles_with_stop_id(%(stop_id)s)", {'stop_id': stop_id })
            percentile_circles = cur.fetchall()

        return tuples_to_feature_collection(geom_tuples=percentile_circles)
