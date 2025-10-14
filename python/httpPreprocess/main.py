"""HFP Analytics data importer"""
import logging
from datetime import datetime

import azure.functions as func
from common.logger_util import CustomDbLogHandler

from .run_analysis import run_delay_analysis

logger = logging.getLogger("importer")

async def main(req: func.HttpRequest) -> func.HttpResponse:
    data = req.get_json() 
    oday = None
    if data and data.get("date"):
        try:
            oday = datetime.strptime(data["date"], "%Y-%m-%d").date()
        except ValueError:
            return func.HttpResponse("Invalid date format. Use YYYY-MM-DD.", status_code=400)

    with CustomDbLogHandler("importer"):
        await run_delay_analysis(oday)
    return func.HttpResponse(f"Http triggered preprocess started. {data}")
