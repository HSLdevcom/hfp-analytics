import azure.durable_functions as durableFunc
import logging
from common.logger_util import CustomDbLogHandler

logger = logging.getLogger("importer")

def orchestrator_function(context: durableFunc.DurableOrchestrationContext):
    with CustomDbLogHandler("importer"):
        input_payload = context.get_input()
        route_ids = input_payload.get("route_ids", [])
        from_oday = input_payload.get("from_oday")
        to_oday = input_payload.get("to_oday")
        days_excluded = input_payload.get("days_excluded", [])

        logger.debug(
            f"Orchestrator started with: "
            f"route_ids={route_ids}, from_oday={from_oday}, to_oday={to_oday}, days_excluded={days_excluded}"
        )

        status_check = yield context.call_activity(
            "GetStatusActivity",
            {
                "table": "recluster_routes",
                "route_ids": route_ids,
                "from_oday": from_oday,
                "to_oday": to_oday,
                "days_excluded": days_excluded
            }
        )

        status = status_check.get("status")

        if status == "QUEUED" or status is None:
            logger.debug("Orchestrator: status is QUEUED or not found. Set status to PENDING")
            yield context.call_activity(
                "SetStatusActivity",
                {
                    "table": "recluster_routes",
                    "route_ids": route_ids,
                    "from_oday": from_oday,
                    "to_oday": to_oday,
                    "days_excluded": days_excluded,
                    "status": "PENDING"
                }
            )

            yield context.call_activity(
                "ReclusterAnalysisActivity",
                {
                    "table": "recluster_routes",
                    "route_ids": route_ids,
                    "from_oday": from_oday,
                    "to_oday": to_oday,
                    "days_excluded": days_excluded
                }
            )

            yield context.call_activity(
                "SetStatusActivity",
                {
                    "table": "recluster_routes",
                    "route_ids": route_ids,
                    "from_oday": from_oday,
                    "to_oday": to_oday,
                    "days_excluded": days_excluded,
                    "status": "DONE"
                }
            )

            # Currently not necessary but useful in the future is durable internal status urls are used
            return { "status": "DONE" }

main = durableFunc.Orchestrator.create(orchestrator_function)
