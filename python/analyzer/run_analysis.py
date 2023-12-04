# Call analysis functions in the db.

import psycopg2
import logging
import time
import common.constants as constants
from datetime import date, timedelta, datetime
from itertools import chain
from common.vehicle_analysis_utils import (
    analyze_vehicle_door_data,
    analyze_positioning_data,
    analyze_odo_data,
    get_vehicle_data,
    get_vehicle_ids,
    insert_vehicle_data,
)
from common.config import (
    POSTGRES_CONNECTION_STRING,
    STOP_NEAR_LIMIT_M,
    MIN_OBSERVATIONS_PER_STOP,
    MAX_NULL_STOP_DIST_M,
    RADIUS_PERCENTILES,
    MIN_RADIUS_PERCENTILES_TO_SUM,
    DEFAULT_MIN_RADIUS_M,
    MANUAL_ACCEPTANCE_MIN_RADIUS_M,
    LARGE_SCATTER_PERCENTILE,
    LARGE_SCATTER_RADIUS_M,
    LARGE_JORE_DIST_M,
    STOP_GUESSED_PERCENTAGE,
    TERMINAL_IDS
)


start_time = 0
logger = logging.getLogger('importer')


async def run_vehicle_analysis():
    try:
        now = datetime.now()
        # Situations where the analysis is run before midnight causes the date
        # for "yesterday" to be the day before yesterday
        # We're adding 6 hours to current datetime to make sure the date
        # for "yesterday" is yesterday from next morning's perspective
        now_plus_6_hours = now + timedelta(hours=6)
        yesterday_datetime = now_plus_6_hours - timedelta(days=1)
        yesterday = yesterday_datetime.strftime("%Y-%m-%d")
        
        logger.debug(f"Starting vehicle analysis for day {yesterday}.")

        vehicles = await get_vehicle_ids(yesterday)
        logger.debug(f"Vehicle ids fetched: {len(vehicles)}")

        analyze_count = 0

        for vehicle in vehicles:
            vehicle_number = vehicle['vehicle_number']
            vehicle_operator_id = vehicle['operator_id']

            try:
                formatted_data = await get_vehicle_data(yesterday, vehicle_operator_id, vehicle_number, None)
                analyzed_door_data = analyze_vehicle_door_data(formatted_data)
                analyzed_odo_data = analyze_odo_data(formatted_data)
                analyzed_positioning_data = analyze_positioning_data(formatted_data)
                combined_obj = {}
                for obj in chain(analyzed_door_data, analyzed_odo_data, analyzed_positioning_data):
                    combined_obj.update(obj)

                await insert_vehicle_data([combined_obj])
                analyze_count += 1

            except Exception:
                logger.exception(f"Error while analyzing vehicle: {vehicle_operator_id}/{vehicle_number}")

        logger.debug(f"Vehicle analysis done. Analyzed {analyze_count} vehicles.")
    except Exception:
        logger.exception("Vehicle analysis failed.")


def get_time():
    return f'[{round(time.time() - start_time)}s]'


def run_analysis():
    global start_time

    conn = psycopg2.connect(POSTGRES_CONNECTION_STRING)
    try:
        with conn:
            with conn.cursor() as cur:
                start_time = time.time()

                cur.execute("SELECT is_lock_enabled(%s)", (constants.IMPORTER_LOCK_ID,))
                is_importer_locked = cur.fetchone()[0]

                if is_importer_locked:
                    logger.warn("Importer is LOCKED which means that importer should be already running. You can get"
                                "rid of the lock by restarting the database if needed.")
                    return

                cur.execute("SELECT pg_advisory_lock(%s)", (constants.IMPORTER_LOCK_ID,))

                cur.execute('SELECT stopcorr.refresh_observation()')
                logger.debug(
                    f'{get_time()} {cur.fetchone()[0]} observations inserted.')

                cur.execute(
                    'UPDATE stopcorr.observation \
                    SET stop_id_guessed = false \
                    WHERE stop_id IS NOT NULL'
                )

                cur.execute('SELECT * FROM stopcorr.guess_missing_stop_ids(%s)',
                            (STOP_NEAR_LIMIT_M, ))
                logger.debug(f'{get_time()} {cur.fetchone()[0]} observations updated with guessed stop_id')

                cur.execute('SELECT stop_id FROM stopcorr.observed_stop_not_in_jore_stop')
                res = [str(x[0]) for x in cur.fetchall()]
                n_stops = len(res)
                if n_stops > 10:
                    logger.debug(f'{n_stops} stop_id values in "observation" not found in "jore_stop"')
                elif n_stops > 0:
                    stops_str = ', '.join(res)
                    logger.debug(f'stop_id values in "observation" not found in "jore_stop": {stops_str}')

                cur.execute('SELECT * FROM stopcorr.calculate_jore_distances()')
                logger.debug(f'{get_time()} {cur.fetchone()[0]} observations updated with dist_to_jore_point_m')

                cur.execute('WITH deleted AS (DELETE FROM stopcorr.stop_median RETURNING 1)\
                            SELECT count(*) FROM deleted')
                logger.debug(f'{get_time()} {cur.fetchone()[0]} rows deleted from "stop_median"')

                cur.execute('SELECT * FROM stopcorr.calculate_medians(%s, %s)',
                            (MIN_OBSERVATIONS_PER_STOP, MAX_NULL_STOP_DIST_M))
                logger.debug(f'{get_time()} {cur.fetchone()[0]} rows inserted into "stop_median"')

                cur.execute('SELECT * FROM stopcorr.calculate_median_distances()')
                logger.debug(f'{get_time()} {cur.fetchone()[0]} observations updated with dist_to_median_point_m')

                cur.execute('SELECT * FROM stopcorr.calculate_percentile_radii(%s)',
                            (RADIUS_PERCENTILES, ))
                logger.debug(f'{get_time()} {cur.fetchone()[0]} "percentile_radii" created using percentiles {RADIUS_PERCENTILES}')

                cur.execute('CALL stopcorr.classify_medians(%s, %s, %s, %s, %s, %s, %s, %s)',
                            (MIN_RADIUS_PERCENTILES_TO_SUM,
                             DEFAULT_MIN_RADIUS_M,
                             MANUAL_ACCEPTANCE_MIN_RADIUS_M,
                             LARGE_SCATTER_PERCENTILE,
                             LARGE_SCATTER_RADIUS_M,
                             LARGE_JORE_DIST_M,
                             STOP_GUESSED_PERCENTAGE,
                             TERMINAL_IDS)
                            )
                cur.execute('SELECT count(*) FROM stopcorr.stop_median WHERE result_class IS NOT NULL')
                logger.debug(f'{get_time()} {cur.fetchone()[0]} "stop_median" updated with "result_class", "recommended_min_radius_m" and "manual_acceptance_needed"')

                cur.execute('CALL staging.remove_accidental_signins()')
                logger.debug(f'Accidental signins removed from api.assumed_monitored_vehicle_journey')

                duration = time.time() - start_time
                logger.debug(f'{get_time()} Analysis complete in {int(duration)} seconds.')

    except Exception:
        logger.exception("Analysis failed.")
    finally:
        conn.cursor().execute("SELECT pg_advisory_unlock(%s)", (constants.IMPORTER_LOCK_ID,))
        conn.close()
