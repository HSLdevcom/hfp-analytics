""" Routes for /stops endpoint """
# TODO: Move data queries to services/stops

from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Path
from fastapi.responses import JSONResponse, PlainTextResponse

import psycopg2 as psycopg

from common.utils import get_conn_params, tuples_to_feature_collection
from api.digitransit_import import main as run_digitransit_import
from api.schemas.stops import (
    JoreStopFeatureCollection,
    StopMedianFeatureCollection,
    HFPStopPointFeatureCollection,
    StopMedianPercentileFeatureCollection,
)

router = APIRouter(
    prefix="/stops",
    tags=["Stop analytics data"],
    responses={"404": {"description": "Not found. JORE stops not found on database with the given id, or at all."}},
)


@router.post(
    "/run_import",
    summary="Run stop import",
    description="Runs import to fetch JORE stop data from Digitransit to HFP analytics",
    response_class=PlainTextResponse,
    responses={"200": {"content": {"text/plain": {"example": "Importer done."}}}},
)
async def run_import() -> PlainTextResponse:
    print("Running import...")
    run_digitransit_import()
    return PlainTextResponse(content="Import done.")


@router.get(
    "/jore_stops",
    summary="Get JORE stops",
    description="Returns a GeoJSON FeatureCollection of either all jore stops "
    "or one jore stop found with given stop_id",
    response_class=JSONResponse,
    response_model=JoreStopFeatureCollection,
)
async def get_jore_stops(
    stop_id: Optional[int] = Query(
        default=None,
        title="Stop ID",
        description="JORE ID of the stop used to get only one specific stop.",
        example=1140439,
    )
) -> JSONResponse:
    with psycopg.connect(get_conn_params()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM api.view_jore_stop_4326")
            stops = cur.fetchall()

            print(f"Found {len(stops)} Jore stops.")

            if len(stops) == 0:
                raise HTTPException(status_code=404, detail="Have you ran Jore & HFP data imports and then analysis?")

            if stop_id:
                stops = list(filter(lambda item: item[0]["properties"]["stop_id"] == stop_id, stops))

                if len(stops) == 0:
                    raise HTTPException(status_code=404, detail=f"Did not find stop with given stop_id: {stop_id}")

            data = tuples_to_feature_collection(geom_tuples=stops)
            return JSONResponse(content=data)


@router.get(
    "/stop_medians",
    summary="Get analyzed stop medians",
    description="Returns a GeoJSON FeatureCollection of stop medians and their percentile radii. "
    "Values are calculated from DOO/DOC events of HFP data.",
    response_class=JSONResponse,
    response_model=StopMedianFeatureCollection,
)
async def get_stop_medians(
    stop_id: Optional[int] = Query(
        default=None,
        title="Stop ID",
        description="JORE ID of the stop used to get only one specific stop.",
        example=1140439,
    )
) -> JSONResponse:
    with psycopg.connect(get_conn_params()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM api.view_stop_median_4326")
            stop_medians = cur.fetchall()

            print(f"Found {len(stop_medians)} Jore stop medians.")

            if len(stop_medians) == 0:
                raise HTTPException(status_code=404, detail="Have you ran Jore & HFP data imports and then analysis?")

            if stop_id:
                stop_medians = list(filter(lambda item: item[0]["properties"]["stop_id"] == stop_id, stop_medians))
                if len(stop_medians) == 0:
                    raise HTTPException(
                        status_code=404, detail=f"Did not find stop median with given stop_id: {stop_id}"
                    )

            data = tuples_to_feature_collection(geom_tuples=stop_medians)

            return JSONResponse(content=data)


@router.get(
    "/hfp_points/{stop_id}",
    summary="Get HFP points observed for a stop",
    description="Returns a GeoJSON FeatureCollection with HFP (door) observations "
    "which were used for analysis of that stop_id OR which have NULL stop_id value "
    "but are located max search_distance_m (default 100) around the stop",
    response_class=JSONResponse,
    response_model=HFPStopPointFeatureCollection,
)
async def get_hfp_points(
    stop_id: int = Path(title="Stop ID", description="JORE ID of the stop.", example=1140439)
) -> JSONResponse:
    with psycopg.connect(get_conn_params()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM api.view_observation_4326 \
                WHERE st_asgeojson -> 'properties' ->> 'stop_id' = %(stop_id)s",
                {"stop_id": stop_id},
            )
            stop_id_observations = cur.fetchall()

            print(f"Found {len(stop_id_observations)} observations with given stop_id: {stop_id}.")

            search_distance_m = 100
            cur.execute(
                "SELECT api.get_observations_with_null_stop_id_4326(%(stop_id)s, %(search_distance_m)s)",
                {"stop_id": stop_id, "search_distance_m": search_distance_m},
            )
            observations_with_null_stop_ids = cur.fetchall()

            print(f"Found {len(observations_with_null_stop_ids)} observations with NULL stop_id")

            total_observations = stop_id_observations + observations_with_null_stop_ids

            if len(total_observations) == 0:
                raise HTTPException(status_code=404, detail=f"Did not find hfp data for given stop: {stop_id}")

            data = tuples_to_feature_collection(geom_tuples=total_observations)

            return JSONResponse(content=data)


@router.get(
    "/percentile_circles/{stop_id}",
    summary="Get percentile circles of HFP observations",
    description="Returns a GeoJSON FeatureCollection of percentile circles around the given stop median by stop_id. "
    "The geometry is returned in ETRS-TM35FIN coordinate reference system (EPSG:3067)",
    response_class=JSONResponse,
    response_model=StopMedianPercentileFeatureCollection,
)
async def get_percentile_circles(
    stop_id: int = Path(title="Stop ID", description="JORE ID of the stop.", example=1140439)
) -> JSONResponse:
    with psycopg.connect(get_conn_params()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT api.get_percentile_circles_with_stop_id(%(stop_id)s)", {"stop_id": stop_id})
            percentile_circles = cur.fetchall()

        data = tuples_to_feature_collection(geom_tuples=percentile_circles)

        return JSONResponse(content=data)
