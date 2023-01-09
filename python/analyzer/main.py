"""HFP Analytics data importer"""
import azure.functions as func
from azure.storage.blob import ContainerClient
from io import StringIO
import os
import csv
import logging
import zstandard
from datetime import datetime, timedelta
import psycopg2 as psycopg
from common.logger_util import CustomDbLogHandler
from common.utils import get_conn_params
import common.constants as constants
from .run_analysis import run_analysis
from .remove_old_data import remove_old_data
import time

def main(analyzer: func.TimerRequest, context: func.Context):
    custom_db_log_handler = CustomDbLogHandler(function_name='importer')
    logger = logging.getLogger('importer')

    logger.info("Going to remove old data.")
    remove_old_data()

    logger.info("Going to run analysis.")
    run_analysis()


    logger.info("Analyzer done.")

    custom_db_log_handler.remove_handlers()
