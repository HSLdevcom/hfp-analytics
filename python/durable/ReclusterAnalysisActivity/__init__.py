import logging
import functools
import asyncio
from datetime import date

from common.recluster import run_asyncio_task, recluster_analysis, set_recluster_status
from common.logger_util import CustomDbLogHandler

logger = logging.getLogger("importer")

async def main(input: dict) -> None:
    with CustomDbLogHandler("importer"):
        table = input["table"]
        route_ids = input["route_ids"]
        from_oday_str = input["from_oday"]
        to_oday_str = input["to_oday"]
        days_excluded_str = input.get("days_excluded", [])

        try:
            from_oday = date.fromisoformat(from_oday_str)
            to_oday = date.fromisoformat(to_oday_str)

            days_excluded: list[date] = [
                date.fromisoformat(d_str) for d_str in days_excluded_str
            ]
        except Exception as e:
            logger.debug(f"Invalid date format in ReclusterAnalysisActivity: {e}")
            raise

        logger.debug(f"ReclusterAnalysisActivity starting: {route_ids}, {from_oday}, {to_oday}, {days_excluded}")

        try:
            await asyncio.to_thread(
                functools.partial(
                    run_asyncio_task,
                    recluster_analysis,
                    route_ids,
                    from_oday,
                    to_oday,
                    days_excluded
                )
            )

            logger.debug("ReclusterAnalysisActivity completed successfully.")

        except Exception as e:
            logger.debug(f"ReclusterAnalysisActivity error: {e}")
            await asyncio.to_thread(
                functools.partial(
                    run_asyncio_task,
                    set_recluster_status,
                    table,
                    from_oday,
                    to_oday,
                    route_ids,
                    days_excluded,
                    "FAILED"
                )
            )
            raise
