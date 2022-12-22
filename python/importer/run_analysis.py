# Call analysis functions in the db.

import psycopg2
import logging
import time
from common.utils import env_with_default, comma_separated_floats_to_list, comma_separated_integers_to_list, get_conn_params
import common.constants as constants

start_time = 0

def get_time():
    return f'[{round(time.time() - start_time)}s]'

def run_analysis():
    global start_time

    logger = logging.getLogger('importer')

    stop_near_limit_m = env_with_default('STOP_NEAR_LIMIT_M', 50.0)
    min_observations_per_stop = env_with_default('MIN_OBSERVATIONS_PER_STOP', 10)
    max_null_stop_dist_m = env_with_default('MAX_NULL_STOP_DIST_M', 100.0)
    radius_percentiles_str = env_with_default('RADIUS_PERCENTILES', '0.5,0.75,0.9,0.95')
    radius_percentiles = comma_separated_floats_to_list(radius_percentiles_str)

    min_radius_percentiles_to_sum_str = env_with_default('MIN_RADIUS_PERCENTILES_TO_SUM', '0.5,0.95')
    min_radius_percentiles_to_sum = comma_separated_floats_to_list(min_radius_percentiles_to_sum_str)
    default_min_radius_m = env_with_default('DEFAULT_MIN_RADIUS_M', 20.0)
    manual_acceptance_min_radius_m = env_with_default('MANUAL_ACCEPTANCE_MIN_RADIUS_M', 40.0)
    large_scatter_percentile = env_with_default('LARGE_SCATTER_PERCENTILE', 0.9)
    large_scatter_radius_m = env_with_default('LARGE_SCATTER_RADIUS_M', 10.0)
    large_jore_dist_m = env_with_default('LARGE_JORE_DIST_M', 25.0)
    stop_guessed_percentage = env_with_default('STOP_GUESSED_PERCENTAGE', 0.05)
    terminal_ids_str = env_with_default('TERMINAL_IDS', '1000001,1000015,2000002,2000003,2000212,4000011')
    terminal_ids = comma_separated_integers_to_list(terminal_ids_str)

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

                cur.execute('SELECT insert_assumed_monitored_vehicle_journeys()')

                logger.info(f'Assumed monitored vehicle journeys updated.')

                logger.info(f'{get_time()} Analysis complete.')
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
    finally:
        conn.cursor().execute("SELECT pg_advisory_unlock(%s)", (constants.IMPORTER_LOCK_ID,))
        conn.close()

