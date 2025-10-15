import logging

from common.enums import ReclusterStatus
from common.logger_util import CustomDbLogHandler
from common.recluster import set_recluster_status

logger = logging.getLogger("importer")

async def main(input: dict) -> None:
    with CustomDbLogHandler("importer"):
        try:
            table: str = input["table"]
            route_ids: list = input["route_ids"]
            from_oday: str = input["from_oday"]
            to_oday: str = input["to_oday"]
            days_excluded: list = input.get("days_excluded", [])
            status: str = input["status"]

            logger.debug(f"SetStatusActivity called: {table}, {route_ids}, {from_oday}, {to_oday}, {days_excluded}, {status}")

            await set_recluster_status(
                table=table, 
                from_oday=from_oday, 
                to_oday=to_oday, 
                route_id=route_ids, 
                days_excluded=days_excluded, 
                status=ReclusterStatus[status]
            )

        except Exception as e:
            logger.debug(f"Error in SetStatusActivity: {e}")
            raise
