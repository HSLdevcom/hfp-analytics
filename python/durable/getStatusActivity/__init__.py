import logging
from datetime import datetime
from common.recluster import get_recluster_status
from common.logger_util import CustomDbLogHandler

logger = logging.getLogger("importer")

async def main(input: dict) -> dict:
    with CustomDbLogHandler("importer"):
        table: str = input["table"]
        route_ids: list = input["route_ids"]
        from_oday: str = input["from_oday"]
        to_oday: str = input["to_oday"]
        days_excluded: list = input.get("days_excluded", [])

        logger.debug(
            f"GetStatusActivity: checking status for {table}, {route_ids}, {from_oday}, {to_oday}, {days_excluded}"
        )
        
        print('<<<< GET STATUS  ACTIVITY >>>> MAIN')

        try:
            status = await get_recluster_status(
                    table=table,
                    from_oday=from_oday,
                    to_oday=to_oday,
                    route_id=route_ids,
                    exclude_dates=days_excluded
                )
            print("<<<< GET STATUS  ACTIVITY >>>> status after get_reculster_status")
            print(status)
        except Exception as e:
            logger.debug(f"Error in GetStatusActivity: {e}")
            return {"status": None, "progress": None}

        progres = status.get("progress")
        status = status.get("status")
        
        print("<<<< GET STATUS  ACTIVITY >>>> return value")
        print({
            "status": status.value if status else None,
            "progress": progres
        })
        print('======')
        return {
            "status": status.value if status else None,
            "progress": progres
        }
