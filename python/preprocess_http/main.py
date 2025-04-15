"""HFP Analytics data importer"""
import azure.functions as func
import logging
import pandas as pd
import httpx

from io import BytesIO
from datetime import date, timedelta, datetime, time
from common.logger_util import CustomDbLogHandler
from .run_analysis import run_delay_analysis

logger = logging.getLogger("importer")

async def main(req: func.HttpRequest) -> func.HttpResponse:
    data = req.get_json() 
    oday = None
    if data and data["date"]:
        oday = data["date"]

    with CustomDbLogHandler("importer"):
        await run_delay_analysis(oday)
    return func.HttpResponse(f"Http triggered preprocess started. {data}")
