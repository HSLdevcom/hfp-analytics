""" Routes for /journeys endpoint """

from datetime import date
import logging

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder

from api.services.journeys import get_last_modified_of_oday, get_journeys_by_oday
from api.schemas.journeys import JourneyResponse

logger = logging.getLogger("api")

router = APIRouter(prefix="/journeys", tags=["Journey analytics data"])


@router.get(
    "/monitored_vehicle_journeys",
    summary="Get monitored vehicle journeys of an operating day.",
    description="Returns assumed monitored vehicle journeys from the given operating day. "
    "'Assumed' here means that the returned journeys might be valid or not, but the API doesn't know it. "
    "Invalid journey is, for example, a journey where a bus driver signed in to a wrong departure.",
    response_class=JSONResponse,
    response_model=JourneyResponse,
)
async def get_monitored_vehicle_journeys(
    oday: date = Query(
        title="Operating day", description="Operating day from which the journeys will be queried. Format YYYY-MM-DD"
    )
) -> JSONResponse:
    logger.debug(f"Monitored vehicle journeys. Operating_day: {oday}")

    vehicle_journeys = await get_journeys_by_oday(oday)
    last_updated = await get_last_modified_of_oday(oday)

    data = {
        "data": {"monitoredVehicleJourneys": vehicle_journeys},
        "last_updated": last_updated.isoformat(timespec="seconds") if last_updated else None,
    }

    return JSONResponse(content=jsonable_encoder(data))
