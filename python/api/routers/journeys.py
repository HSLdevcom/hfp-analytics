""" Routes for /journeys endpoint """
# TODO: Move data queries to services/journeys

from datetime import date
from fastapi import APIRouter, Query
import logging
from common.logger_util import CustomDbLogHandler

from api.services.journeys import get_last_modified_of_oday, get_journeys_by_oday

logger = logging.getLogger('api')

router = APIRouter(
    prefix="/journeys",
    tags=["Journey analytics data"]
)


@router.get("/monitored_vehicle_journeys")
async def get_monitored_vehicle_journeys(operating_day: date = Query(..., description="Format YYYY-MM-DD")):
    """
    Returns assumed monitored vehicle journeys from given operating day. Assumed here means
    that the journeys might be valid or not, API doesn't know it. Invalid journey is example
    a journey where bus driver signed in to a wrong departure.
    """
    logger.debug(f'Monitored vehicle journeys. Operating_day: {operating_day}')

    vehicle_journeys = await get_journeys_by_oday(operating_day)
    last_updated = await get_last_modified_of_oday(operating_day)

    return {
        "data": {
            "monitoredVehicleJourneys": vehicle_journeys
        },
        "last_updated": last_updated.isoformat(timespec="seconds") if last_updated else None
    }
