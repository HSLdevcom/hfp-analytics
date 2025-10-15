"""HFP Analytics data importer"""
import logging

import azure.functions as func
from common.logger_util import CustomDbLogHandler

from .run_analysis import run_delay_analysis

logger = logging.getLogger("importer")

async def start_analysis():
    logger.debug("Going to run delay analysis.")
    await run_delay_analysis()
    logger.debug("Delay analysis done.")


async def main(preprocess: func.TimerRequest, context: func.Context):
    with CustomDbLogHandler("importer"):
        await start_analysis()
