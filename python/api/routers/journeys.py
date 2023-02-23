""" Routes for /journeys endpoint """
# TODO: Move data queries to services/journeys

from datetime import date
from fastapi import APIRouter, Query
import psycopg2 as psycopg
import logging
from common.logger_util import CustomDbLogHandler
from common.utils import get_conn_params

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
