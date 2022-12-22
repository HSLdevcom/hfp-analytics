import psycopg2
import logging
from common.utils import get_conn_params

def remove_old_data():
    logger = logging.getLogger('importer')

    conn = psycopg2.connect(get_conn_params())
    try:
        with conn:
            with conn.cursor() as cur:
                logger.info("Removing data older than 2 weeks.")
                cur.execute("DELETE FROM hfp.observed_journey WHERE oday < now() - interval '2 week'")
                logger.info(f"{cur.rowcount} rows deleted from hfp.observed_journey, and all related rows in hfp.hfp_point.")
                cur.execute("DELETE FROM observation WHERE oday < now() - interval '2 week'")
                logger.info(f"{cur.rowcount} rows deleted from observation.")
                cur.execute("DELETE FROM hfp.hfp_point WHERE point_timestamp < now() - interval '2 week'")
                logger.info(f"{cur.rowcount} remaining hfp.hfp_point rows deleted.")
    except psycopg2.OperationalError as err:
        logger.error(f"Old data removal failed: {err}")
    finally:
        conn.close()
