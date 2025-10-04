"""Routes for /stops endpoint"""

from typing import Optional

from common.utils import tuples_to_feature_collection
from fastapi import APIRouter, HTTPException, Path, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, PlainTextResponse

from api.digitransit_import import main as run_digitransit_import
from api.schemas.stops import (
    HFPStopPointFeatureCollection,
    JoreStopFeatureCollection,
    StopMedianFeatureCollection,
    StopMedianPercentileFeatureCollection,
)
from api.services.stops import (
    get_medians,
    get_null_observations_for_stop,
    get_percentiles,
    get_stop_observations,
    get_stops,
    is_stop_medians_table_empty,
    is_stops_table_empty,
)

router = APIRouter(
    prefix="/stops",
    tags=["Stop analytics data"],
    responses={
        "404": {
            "description": "Not found. JORE stops not found on database with the given id, or at all."
        }
    },
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
    ),
) -> JSONResponse:
    stops = await get_stops(stop_id)

    if len(stops) == 0:
        if await is_stops_table_empty():
            raise HTTPException(
                status_code=404,
                detail="Have you ran Jore & HFP data imports and then analysis?",
            )
        else:
            raise HTTPException(
                status_code=404,
                detail=f"Did not find stop with given stop_id: {stop_id}",
            )

    data = tuples_to_feature_collection(geom_tuples=stops)
    return JSONResponse(content=jsonable_encoder(data))


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
    ),
) -> JSONResponse:
    stop_medians = await get_medians(stop_id)

    if len(stop_medians) == 0:
        if await is_stop_medians_table_empty():
            raise HTTPException(
                status_code=404,
                detail="Have you ran Jore & HFP data imports and then analysis?",
            )
        else:
            raise HTTPException(
                status_code=404,
                detail=f"Did not find stop median with given stop_id: {stop_id}",
            )

    data = tuples_to_feature_collection(geom_tuples=stop_medians)
    return JSONResponse(content=jsonable_encoder(data))


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
    stop_id: int = Path(
        title="Stop ID", description="JORE ID of the stop.", example=6150219
    ),
) -> JSONResponse:
    stop_id_observations = await get_stop_observations(stop_id)
    null_observations = await get_null_observations_for_stop(
        stop_id
    )  # possibility to parametrize radius for search
    total_observations = stop_id_observations + null_observations

    if len(total_observations) == 0:
        raise HTTPException(
            status_code=404, detail=f"Did not find hfp data for given stop: {stop_id}"
        )

    data = tuples_to_feature_collection(geom_tuples=total_observations)
    return JSONResponse(content=jsonable_encoder(data))


@router.get(
    "/percentile_circles/{stop_id}",
    summary="Get percentile circles of HFP observations",
    description="Returns a GeoJSON FeatureCollection of percentile circles around the given stop median by stop_id. "
    "The geometry is returned in ETRS-TM35FIN coordinate reference system (EPSG:3067)",
    response_class=JSONResponse,
    response_model=StopMedianPercentileFeatureCollection,
)
async def get_percentile_circles(
    stop_id: int = Path(
        title="Stop ID", description="JORE ID of the stop.", example=1140439
    ),
) -> JSONResponse:
    percentile_circles = await get_percentiles(stop_id)
    data = tuples_to_feature_collection(geom_tuples=percentile_circles)
    return JSONResponse(content=jsonable_encoder(data))
