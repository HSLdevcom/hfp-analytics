"""HFP Analytics data importer"""
import azure.functions as func

import logging
from datetime import datetime

from common.logger_util import CustomDbLogHandler
from .run_analysis import run_analysis, run_vehicle_analysis
from .remove_old_data import remove_old_data


logger = logging.getLogger('importer')


async def start_analysis():
    start_time = datetime.now()
    logger.debug("Going to remove old data.")
    remove_old_data()

    removal_end = datetime.now()

    logger.debug("Going to run analysis.")
    run_analysis()

    analysis_end = datetime.now()

    logger.debug("Going to run vehicle analysis.")
    await run_vehicle_analysis()

    vehicle_analysis_end = datetime.now()

    logger.debug(f"Analyzer done. It took {removal_end - start_time} for data removal, "
                 f"{analysis_end - removal_end} for stop and journey analysis, "
                 f"and {vehicle_analysis_end - analysis_end} for vehicle analysis. "
                 f"Total time: {vehicle_analysis_end - start_time}")


async def main(analyzer: func.TimerRequest, context: func.Context):
    with CustomDbLogHandler('importer'):
        await start_analysis()
