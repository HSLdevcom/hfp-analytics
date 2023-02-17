# Call analysis functions in the db.

import psycopg2
import logging
import time
from common.utils import get_conn_params
import common.constants as constants

start_time = 0
logger = logging.getLogger('importer')


def get_time():
    return f'[{round(time.time() - start_time)}s]'


def run_analysis(info = {}):
    global start_time

    conn = psycopg2.connect(get_conn_params())
    try:
        with conn:
            with conn.cursor() as cur:
                start_time = time.time()

                cur.execute("SELECT is_lock_enabled(%s)", (constants.IMPORTER_LOCK_ID,))
                is_importer_locked = cur.fetchone()[0]

                if is_importer_locked:
                    logger.info("Importer is LOCKED which means that importer should be already running. You can get"
                                "rid of the lock by restarting the database if needed.")
                    return

                logger.info("Running analysis.")
                cur.execute("SELECT pg_advisory_lock(%s)", (constants.IMPORTER_LOCK_ID,))

                min_tst = info.get('min_tst')
                logger.info(f"Min timestamp for analysis is {min_tst}")
                cur.execute('SELECT insert_assumed_monitored_vehicle_journeys(%s)', (min_tst,))

                logger.info(f'Assumed monitored vehicle journeys updated.')
                duration = time.time() - start_time
                logger.info(f'{get_time()} Analysis complete in {int(duration)} seconds.')
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
    finally:
        conn.cursor().execute("SELECT pg_advisory_unlock(%s)", (constants.IMPORTER_LOCK_ID,))
        conn.close()
