import logging

import azure.durable_functions as durableFunc

from common.logger_util import CustomDbLogHandler
from common.enums import ReclusterStatus

logger = logging.getLogger("importer")


def orchestrator_function(context: durableFunc.DurableOrchestrationContext):
    with CustomDbLogHandler("importer"):
        input_payload = context.get_input()
        route_ids: list = input_payload.get("route_ids", [])
        from_oday: str = input_payload.get("from_oday")
        to_oday: str = input_payload.get("to_oday")
        days_excluded: list = input_payload.get("days_excluded", [])

        logger.debug(
            f"Orchestrator started with: "
            f"route_ids={route_ids}, from_oday={from_oday}, to_oday={to_oday}, days_excluded={days_excluded}"
        )

        status_check = yield context.call_activity(
            "getStatusActivity",
            {
                "table": "recluster_routes",
                "route_ids": route_ids,
                "from_oday": from_oday,
                "to_oday": to_oday,
                "days_excluded": days_excluded,
            },
        )

        status = status_check.get("status")

        if status is None or ReclusterStatus[status] == ReclusterStatus.QUEUED:
            logger.debug(
                f"Orchestrator: status is {ReclusterStatus.QUEUED.value} or not found. Set status to {ReclusterStatus.RUNNING.value}"
            )
            yield context.call_activity(
                "setStatusActivity",
                {
                    "table": "recluster_routes",
                    "route_ids": route_ids,
                    "from_oday": from_oday,
                    "to_oday": to_oday,
                    "days_excluded": days_excluded,
                    "status": ReclusterStatus.RUNNING.value,
                },
            )

            yield context.call_activity(
                "reclusterAnalysisActivity",
                {
                    "table": "recluster_routes",
                    "route_ids": route_ids,
                    "from_oday": from_oday,
                    "to_oday": to_oday,
                    "days_excluded": days_excluded,
                },
            )

            yield context.call_activity(
                "setStatusActivity",
                {
                    "table": "recluster_routes",
                    "route_ids": route_ids,
                    "from_oday": from_oday,
                    "to_oday": to_oday,
                    "days_excluded": days_excluded,
                    "status": ReclusterStatus.DONE.value,
                },
            )

            # Currently not necessary but useful in the future is durable internal status urls are used
            return {"status": ReclusterStatus.DONE.value}


main = durableFunc.Orchestrator.create(orchestrator_function)
