"""HFP Analytics data importer"""
import azure.functions as func
import logging
import pandas as pd
import httpx

from io import BytesIO
from datetime import date, timedelta, datetime, time
from common.logger_util import CustomDbLogHandler
from .run_analysis import run_delay_analysis
from .preprocess import preprocess, load_delay_hfp_data

logger = logging.getLogger("importer")

async def start_analysis():
    logger.debug("Going to run delay analysis.")
    await run_delay_analysis()
    logger.debug("Delay analysis done.")


async def main(preprocess: func.TimerRequest, context: func.Context):
    with CustomDbLogHandler("importer"):
        await start_analysis()
