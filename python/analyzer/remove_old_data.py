import psycopg2
import logging
import common.constants as constants
import common.slack as slack
from common.config import POSTGRES_CONNECTION_STRING

logger = logging.getLogger('importer')


def remove_old_data():

    conn = psycopg2.connect(POSTGRES_CONNECTION_STRING)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT is_lock_enabled(%s)", (constants.IMPORTER_LOCK_ID,))
                is_importer_locked = cur.fetchone()[0]

                if is_importer_locked:
                    logger.info("Importer is LOCKED which means that importer should be already running. You can get"
                                "rid of the lock by restarting the database if needed.")
                    return
                conn.commit()

                logger.info("Running analysis.")
                cur.execute("SELECT pg_advisory_lock(%s)", (constants.IMPORTER_LOCK_ID,))
                logger.info("Removing  observation data older than 3 weeks.")
                cur.execute("DELETE FROM stopcorr.observation WHERE oday < now() - interval '3 week'")
                logger.info(f"{cur.rowcount} rows deleted from observation.")

                logger.info("Removing hfp_point data older than 2 weeks")
                cur.execute("SELECT drop_chunks('hfp.hfp_point', interval '2 week')")

                logger.info(f"Removing old logs and blob info")
                cur.execute("DELETE FROM importer.blob WHERE listed_at < now() - interval '4 week'")
                logger.info(f"{cur.rowcount} rows deleted from importer.blob .")
                cur.execute("DELETE FROM logs.importer_log WHERE log_timestamp < now() - interval '4 week'")
                logger.info(f"{cur.rowcount} rows deleted from logs.importer_log.")
                cur.execute("DELETE FROM logs.api_log WHERE log_timestamp < now() - interval '4 week'")
                logger.info(f"{cur.rowcount} rows deleted from logs.api_log.")

    except psycopg2.OperationalError as err:
        logger.error(f"Old data removal failed: {err}")
        slack.send_to_channel(f"Old data removal failed: {err}", alert=True)
    finally:
        conn.cursor().execute("SELECT pg_advisory_unlock(%s)", (constants.IMPORTER_LOCK_ID,))
        conn.close()
