"""HFP Analytics data importer"""
import azure.functions as func
import logging
import pandas as pd
import httpx

from io import BytesIO
from datetime import date, timedelta, datetime, time
from common.logger_util import CustomDbLogHandler
from .run_analysis import run_analysis, run_vehicle_analysis, run_delay_analysis
from .remove_old_data import remove_old_data
from .preprocess import preprocess, load_delay_hfp_data

logger = logging.getLogger("importer")

async def start_analysis():
    start_time = datetime.now()
    logger.debug("Going to remove old data.")
    remove_old_data()

    removal_end = datetime.now()

    logger.debug("Going to run delay analysis.")

    await run_delay_analysis()

    delay_analysis_end = datetime.now()

    logger.debug("Going to run analysis.")

    run_analysis()

    analysis_end = datetime.now()

    logger.debug("Going to run vehicle analysis.")
    await run_vehicle_analysis()
    vehicle_analysis_end = datetime.now()


    logger.info(
        f"Analyzer done. It took {removal_end - start_time} for data removal, "
        f"and {delay_analysis_end - removal_end} for delay analysis. "
        f"{analysis_end - delay_analysis_end} for stop and journey analysis, "
        f"and {vehicle_analysis_end - analysis_end} for vehicle analysis, "
        f"Total time: {analysis_end - start_time}"
    )


async def main(analyzer: func.TimerRequest, context: func.Context):
    with CustomDbLogHandler("importer"):
        await start_analysis()
