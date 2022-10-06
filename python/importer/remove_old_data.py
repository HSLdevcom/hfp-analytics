import psycopg2
import logging
from common.utils import get_conn_params

def main():
    logger = logging.getLogger('importer')

    conn = psycopg2.connect(**get_conn_params())
    try:
        with conn:
            with conn.cursor() as cur:
                logger.info("Removing data.")
                # Remove data that is older than 2 weeks
                cur.execute("DELETE FROM hfp.observed_journey WHERE oday < now() - interval '2 week'")
                cur.execute("DELETE FROM observation WHERE oday < now() - interval '2 week'")
                # Not all hfp_points will get deleted due to ON DELETE CASCADE rule, so we have to clean up the rest (that are not related to observed_journeys) with:
                cur.execute("DELETE FROM hfp.hfp_point WHERE point_timestamp < now() - interval '2 week'")
    finally:
        conn.close()

if __name__ == '__main__':
    main()