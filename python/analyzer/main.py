"""HFP Analytics data importer"""
import azure.functions as func
import logging
from common.logger_util import CustomDbLogHandler
from .run_analysis import run_analysis
from .remove_old_data import remove_old_data


logger = logging.getLogger('importer')


def start_analysis():
    logger.info("Going to remove old data.")
    remove_old_data()

    logger.info("Going to run analysis.")
    run_analysis()

    logger.info("Analyzer done.")


def main(analyzer: func.TimerRequest, context: func.Context):
    with CustomDbLogHandler('importer'):
        start_analysis()
