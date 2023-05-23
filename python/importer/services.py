""" Module contains db queries for importer """
from collections.abc import Iterable
from datetime import datetime
import logging

from psycopg import sql
from psycopg_pool import ConnectionPool  # todo: refactor to use common.database pool

from common.config import POSTGRES_CONNECTION_STRING
import common.constants as constants

from .schemas import DBSchema

logger = logging.getLogger("importer")

pool = ConnectionPool(POSTGRES_CONNECTION_STRING, max_size=20)


def create_db_lock() -> bool:
    """Create a lock for the process. Returns false if another lock found."""
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Check if importer is locked or not. We use lock strategy to prevent executing importer
                # and analysis more than once at a time
                cur.execute("SELECT is_lock_enabled(%s)", (constants.IMPORTER_LOCK_ID,))
                res = cur.fetchone()
                is_importer_locked = res[0] if res else False

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


def release_db_lock() -> None:
    """Release a previously created lock."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (constants.IMPORTER_LOCK_ID,))


def add_new_blob(blob_data: dict):
    """Add new blob details in the database."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO importer.blob
                (name, type, min_oday, max_oday, min_tst, max_tst, row_count, invalid, covered_by_import)
                VALUES (
                    %(blob_name)s,
                    %(event_type)s,
                    %(min_oday)s,
                    %(max_oday)s,
                    %(min_tst)s,
                    %(max_tst)s,
                    %(row_count)s,
                    %(invalid)s,
                    %(covered_by_import)s
                ) ON CONFLICT DO NOTHING
                """,
                blob_data,
            )


def is_blob_listed(blob_name: str) -> bool:
    """Returns true if the blob is found in the table."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT EXISTS( SELECT 1 FROM importer.blob WHERE name = %s )", (blob_name,))
            res = cur.fetchone()

    exists_in_list = res[0] if res else False
    return exists_in_list


def mark_blob_status_started(blob_name: str) -> dict:
    """Update the blob status started and return metadata (row_count, invalid flag)."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE importer."blob"
                SET import_started = %s, import_status = 'importing'
                WHERE name = %s
                RETURNING type, row_count, invalid
                """,
                (
                    datetime.utcnow(),
                    blob_name,
                ),
            )
            res = cur.fetchone()

    data = {}

    if res and len(res) == 3:
        data["type"] = res[0]
        data["row_count"] = res[1]
        data["invalid"] = res[2]
    else:
        raise Exception("Invalid db query or data for import")
    return data


def mark_blob_status_finished(blob_name: str, failed: bool = False) -> float:
    """Update the blob status finished (failed or imported) and return the processing time as seconds."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE importer."blob"
                SET import_finished = %s, import_status = %s
                WHERE name = %s
                RETURNING EXTRACT(EPOCH FROM (import_finished - import_started))
                """,
                (
                    datetime.utcnow(),
                    "failed" if failed else "imported",
                    blob_name,
                ),
            )
            res = cur.fetchone()

    if res:
        processing_time = res[0]
    else:
        raise Exception("Invalid db query or data for import")
    return processing_time


def pickup_blobs_for_import() -> list:
    """Queries blobs waiting for import.
    Sets their status to pending and returns their names for importer."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH updated AS (
                    UPDATE importer."blob"
                    SET import_status = 'pending'
                    WHERE covered_by_import AND import_status IN ('not started', 'pending')
                    RETURNING name
                )
                SELECT * FROM updated ORDER BY name
                """
            )
            res = cur.fetchall()

    blob_names = [r[0] for r in res]
    return blob_names


def copy_data_to_db(db_schema: DBSchema, data_rows: Iterable[dict], invalid_blob: bool = False) -> None:
    """Copy data from storage downloader to db staging table,
    and call procedures to move data from staging to the master storage."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            raw_field_names = db_schema["fields"]["mapping"].keys()
            db_field_names = db_schema["fields"]["mapping"].values()
            required_fields = db_schema["fields"]["required"]

            truncate_query = sql.SQL("DELETE FROM {schema}.{table}").format(
                schema=sql.Identifier(db_schema["copy_target"]["schema"]),
                table=sql.Identifier(db_schema["copy_target"]["table"]),
            )

            # Create a copy statement from selected field list
            copy_query = sql.SQL("COPY {schema}.{table} ({fields}) FROM STDIN").format(
                schema=sql.Identifier(db_schema["copy_target"]["schema"]),
                table=sql.Identifier(db_schema["copy_target"]["table"]),
                fields=sql.SQL(",").join([sql.Identifier(f) for f in db_field_names]),
            )

            invalid_row_count = 0

            cur.execute(truncate_query)

            with cur.copy(copy_query) as copy:
                for row in data_rows:
                    if any(row[key] is None for key in required_fields):
                        logger.error(f"Found a row with an unique key error: {row}")
                        invalid_row_count += 1
                        continue

                    # Construct a tuple from a row based on selected rows
                    # Convert empty string to None to avoid db type errors
                    row_obj = tuple(row[f] if row[f] != "" else None for f in raw_field_names)
                    copy.write_row(row_obj)

            if invalid_row_count > 0:
                logger.error(f"Unique key error count for the blob: {invalid_row_count}")

            if not invalid_blob:
                cur.execute(db_schema["scripts"]["process"])
            elif db_schema["scripts"]["process_invalid"]:
                cur.execute(db_schema["scripts"]["process_invalid"])

            cur.execute(truncate_query)
