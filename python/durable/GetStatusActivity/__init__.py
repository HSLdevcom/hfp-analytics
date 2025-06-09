import logging
from datetime import datetime
from common.recluster import get_recluster_status
from common.logger_util import CustomDbLogHandler

logger = logging.getLogger("importer")

async def main(input: dict) -> dict:
    with CustomDbLogHandler("importer"):
        table = input["table"]
        route_ids = input["route_ids"]
        from_oday = input["from_oday"]
        to_oday = input["to_oday"]
        days_excluded = input.get("days_excluded", [])

        logger.debug(
            f"GetStatusActivity: checking status for {table}, {route_ids}, {from_oday}, {to_oday}, {days_excluded}"
        )

        try:
            status = await get_recluster_status(
                    table,
                    from_oday,
                    to_oday,
                    route_ids,
                    days_excluded
                )

        except Exception as e:
            logger.debug(f"Error in GetStatusActivity: {e}")
            return {"status": None, "progress": None}

        return {
            "status": status.get("status"),
            "progress": status.get("progress")
        }
