""" Module contains db queries for importer """
import csv
from datetime import datetime
from io import TextIOWrapper
import logging
import zstandard

from psycopg import sql
from psycopg_pool import ConnectionPool  # todo: refactor to use common.database pool

from azure.storage.blob import StorageStreamDownloader

from common.config import POSTGRES_CONNECTION_STRING
import common.constants as constants


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


def release_db_lock():
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
                    %(name)s,
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


def is_blob_listed(blob_name: str):
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
                RETURNING row_count, invalid
                """,
                (
                    datetime.utcnow(),
                    blob_name,
                ),
            )
            res = cur.fetchone()

    data = {}

    if res and len(res) == 2:
        data["row_count"] = res[0]
        data["invalid"] = res[1]
    else:
        raise Exception("Invalid db query or data for import")
    return data


def mark_blob_status_finished(blob_name: str, failed: bool = False) -> int:
    """Update the blob status finished (failed or imported) and return the processing time as seconds."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE importer."blob"
                SET import_finished = %s, import_status = %s
                WHERE name = %s
                RETURNING FLOOR(EXTRACT(EPOCH FROM (import_finished - import_started)))
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


def copy_data_from_downloader_to_db(downloader: StorageStreamDownloader, invalid_blob: bool = False) -> None:
    """Copy data from storage downloader to db staging table,
    and call procedures to move data from staging to the master storage."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            compressed_content = downloader.content_as_bytes()
            reader = zstandard.ZstdDecompressor().stream_reader(compressed_content)
            hfp_dict_reader = csv.DictReader(TextIOWrapper(reader, encoding="utf-8"))

            # These fields will be imported. Keys are from csv, values are db columns.
            # Order is guaranteed, so they are used with .keys() and .values() -methods
            selected_fields = {
                "tst": "tst",
                "eventType": "event_type",
                "receivedAt": "received_at",
                "ownerOperatorId": "vehicle_operator_id",
                "vehicleNumber": "vehicle_number",
                "mode": "transport_mode",
                "routeId": "route_id",
                "dir": "direction_id",
                "oday": "oday",
                "start": "start",
                "oper": "observed_operator_id",
                "odo": "odo",
                "spd": "spd",
                "drst": "drst",
                "locationQualityMethod": "loc",
                "stop": "stop",
                "longitude": "longitude",
                "latitude": "latitude",
            }

            required_fields = ["tst", "oper", "vehicleNumber"]  # These are used for unique index

            # Create a copy statement from selected field list
            copy_sql = sql.SQL("COPY staging.hfp_raw ({fields}) FROM STDIN").format(
                fields=sql.SQL(",").join([sql.Identifier(f) for f in selected_fields.values()])
            )

            invalid_row_count = 0

            cur.execute("DELETE FROM staging.hfp_raw")

            with cur.copy(copy_sql) as copy:
                for row in hfp_dict_reader:
                    if any(row[key] is None for key in required_fields):
                        logger.error(f"Found a row with an unique key error: {row}")
                        invalid_row_count += 1
                        continue

                    # Construct a tuple from a row based on selected rows
                    # Convert empty string to None to avoid db type errors
                    row_obj = (row[f] if row[f] != "" else None for f in selected_fields.keys())
                    copy.write_row(row_obj)

            if invalid_row_count > 0:
                logger.error(f"Unique key error count for the blob: {invalid_row_count}")

            if not invalid_blob:
                cur.execute("CALL staging.import_and_normalize_hfp()")
            else:
                cur.execute("CALL stating.import_invalid_hfp()")

            cur.execute("DELETE FROM staging.hfp_raw")
