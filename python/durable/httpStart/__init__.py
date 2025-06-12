import io
import logging
import json

import azure.functions as func
import azure.durable_functions as durableFunc
from fastapi import status as status_code
import zipfile

from common.logger_util import CustomDbLogHandler

from common.recluster import (
    get_recluster_status,
    set_recluster_status,
    load_recluster_geojson,
    load_recluster_csv
)
from common.enums import ReclusterStatus

logger = logging.getLogger("importer")

async def main(req: func.HttpRequest, starter: str) -> func.HttpResponse:
    with CustomDbLogHandler("importer"):
        try:
            payload = req.get_json()
        except Exception:
            payload = {}

        route_ids: list = payload.get("route_ids", [])
        from_oday: str = payload.get("from_oday")
        to_oday: str = payload.get("to_oday")
        days_excluded: list = payload.get("days_excluded", [])

        table = "recluster_routes"

        try:
            analysis_status = await get_recluster_status(
                table, 
                from_oday, 
                to_oday, 
                route_ids, 
                days_excluded
            )
        except Exception as e:
            logger.debug(f"Error querying DB status in HttpStart: {e}")
            return func.HttpResponse(
                body=f"Error: could not get analysis status: {e}", 
                status_code=500
            )

        status: ReclusterStatus | None = analysis_status.get("status")
        progress = analysis_status.get("progress")

        if status == ReclusterStatus.RUNNING or status == ReclusterStatus.QUEUED:
            return func.HttpResponse(
                body=json.dumps({
                    "status": status.value,
                    "progress": progress,
                    "params": payload
                }),
                status_code=202,
                mimetype="application/json"
            )

        if status == ReclusterStatus.DONE:
            try:
                geojson_bytes = await load_recluster_geojson(
                    "recluster_routes",
                    from_oday,
                    to_oday,
                    days_excluded,
                    route_ids
                )
                csv_bytes = await load_recluster_csv(
                    "recluster_routes",
                    from_oday,
                    to_oday,
                    days_excluded,
                    route_ids
                )
            except Exception as e:
                logger.debug(f"Error loading results: {e}")
                return func.HttpResponse(
                    body=f"Could not load results: {e}",
                    status_code=500
                )
            parent_buffer = io.BytesIO()
            with zipfile.ZipFile(parent_buffer, "w") as parent_zip:
                if geojson_bytes is not None:
                    parent_zip.writestr("routecluster.geojson", geojson_bytes)
                if csv_bytes is not None:
                    parent_zip.writestr("routecluster.csv", csv_bytes)

            parent_buffer.seek(0)

            return func.HttpResponse(
                body=parent_buffer.getvalue(),
                status_code=200,
                mimetype="application/zip",
                headers={
                    "Content-Disposition": 'attachment; filename="clusters.zip"'
                }
            )

        try:
            await set_recluster_status(
                table=table,
                from_oday=from_oday,
                to_oday=to_oday,
                route_id=route_ids,
                days_excluded=days_excluded,
                status=ReclusterStatus.QUEUED
            )
        except Exception as e:
            logger.debug(f"Error setting status QUEUED: {e}")
            return func.HttpResponse(
                body=json.dumps({"error": f"Could not insert QUEUED row: {e}"}),
                status_code=500,
                mimetype="application/json"
            )

        client = durableFunc.DurableOrchestrationClient(starter)
        await client.start_new("orchestrator", None, payload)
        status_msg = status
        if status_msg is None:
            status_msg = ReclusterStatus.CREATED.value
        
        return func.HttpResponse(
            body=json.dumps(
                {"status": status_msg, "progress": progress, "params": payload}
            ),
            status_code=status_code.HTTP_202_ACCEPTED,
            mimetype="application/json",
        )
