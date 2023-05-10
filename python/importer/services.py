import logging

from psycopg_pool import ConnectionPool  # todo: refactor to use common.database pool

from common.config import (
    HFP_STORAGE_CONTAINER_NAME,
    HFP_STORAGE_CONNECTION_STRING,
    POSTGRES_CONNECTION_STRING,
    HFP_EVENTS_TO_IMPORT,
    IMPORT_COVERAGE_DAYS,
)
import common.constants as constants


logger = logging.getLogger("importer")

pool = ConnectionPool(POSTGRES_CONNECTION_STRING, max_size=20)


def create_db_lock() -> bool:
    """ Create a lock for the process. Returns false if another lock found. """
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Check if importer is locked or not. We use lock strategy to prevent executing importer
                # and analysis more than once at a time
                cur.execute("SELECT is_lock_enabled(%s)", (constants.IMPORTER_LOCK_ID,))
                is_importer_locked = cur.fetchone()[0]

                if is_importer_locked:
                    logger.error(
                        "Importer is LOCKED which means that importer should be already running. "
                        "You can get rid of the lock by restarting the database if needed."
                    )
                    return False

                logger.info("Going to run importer.")
                cur.execute("SELECT pg_advisory_lock(%s)", (constants.IMPORTER_LOCK_ID,))
    except Exception as e:
        logger.error(f"Error when creating locks for importer: {e}")
        return False

    return True


def release_db_lock():
    """ Release a previously created lock. """
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (constants.IMPORTER_LOCK_ID,))
