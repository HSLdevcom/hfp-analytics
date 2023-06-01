"""HFP Analytics data importer"""
import azure.functions as func
from azure.storage.blob import ContainerClient
from io import StringIO, TextIOWrapper
import csv
import logging
import zstandard
import time
from datetime import datetime, timedelta

from psycopg_pool import ConnectionPool  # todo: refactor to use common.database pool
from psycopg import sql

from common.logger_util import CustomDbLogHandler
import common.constants as constants
from common.config import (
    HFP_STORAGE_CONTAINER_NAME,
    HFP_STORAGE_CONNECTION_STRING,
    POSTGRES_CONNECTION_STRING,
    HFP_EVENTS_TO_IMPORT,
    IMPORT_COVERAGE_DAYS,
)


logger = logging.getLogger("importer")

pool = ConnectionPool(POSTGRES_CONNECTION_STRING, max_size=20)


def get_azure_container_client() -> ContainerClient:
    return ContainerClient.from_connection_string(
        conn_str=HFP_STORAGE_CONNECTION_STRING, container_name=HFP_STORAGE_CONTAINER_NAME
    )


def start_import():
    # Create a lock for import
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
                    return

                logger.info("Going to run importer.")
                cur.execute("SELECT pg_advisory_lock(%s)", (constants.IMPORTER_LOCK_ID,))

    except Exception as e:
        logger.error(f"Error when creating locks for importer: {e}")

    try:
        import_day_data_from_past(IMPORT_COVERAGE_DAYS)

    except Exception as e:
        logger.error(f"Error when running importer: {e}")
    finally:
        # Remove lock at this point
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_unlock(%s)", (constants.IMPORTER_LOCK_ID,))

    logger.info("Importer done.")


def import_day_data_from_past(day_since_today):
    logger.info(f"Importing HFP data {day_since_today} days from past.")

    container_client = get_azure_container_client()
    storage_blob_names = []

    import_date = datetime.now() - timedelta(day_since_today)

    while import_date <= datetime.now():
        current_date_str = import_date.strftime("%Y-%m-%d")
        blobs = container_client.list_blobs(name_starts_with=current_date_str)
        for blob in blobs:
            storage_blob_names.append(blob.name)
        import_date += timedelta(days=1)

    blob_names = []

    with pool.connection() as conn:
        with conn.cursor() as cur:
            for i, name in enumerate(storage_blob_names):
                cur.execute("SELECT EXISTS( SELECT 1 FROM importer.blob WHERE name = %s )", (name,))
                res = cur.fetchone()
                exists_in_list = res[0] if res else False

                if exists_in_list:
                    # Already imported, no need to fetch tags or try to insert
                    continue

                blob_client = container_client.get_blob_client(name)
                tags = blob_client.get_blob_tags()

                event_type = tags.get("eventType")
                covered_by_import = event_type in HFP_EVENTS_TO_IMPORT

                cur.execute(
                    """
                    INSERT INTO importer.blob
                    (name, type, min_oday, max_oday, min_tst, max_tst, row_count, invalid, covered_by_import)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                    """,
                    (
                        name,
                        event_type,
                        tags.get("min_oday"),
                        tags.get("max_oday"),
                        tags.get("min_tst"),
                        tags.get("max_tst"),
                        tags.get("row_count"),
                        tags.get("invalid"),
                        covered_by_import,
                    ),
                )

            conn.commit()

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

            blob_names = [r[0] for r in cur.fetchall()]

    logger.debug(f"Running import for {blob_names}")

    for b in blob_names:
        import_blob(b)


def import_blob(blob_name):
    logger.debug(f"Processing blob: {blob_name}")
    with pool.connection() as conn:
        with conn.cursor() as cur:
            blob_start_time = time.time()

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

            if res and len(res) == 2:
                blob_row_count = res[0]
                blob_invalid = res[1]
            else:
                raise Exception("Invalid db query or data for import")

            conn.commit()

            try:
                container_client = get_azure_container_client()
                blob_client = container_client.get_blob_client(blob=blob_name)
                storage_stream_downloader = blob_client.download_blob()

                read_imported_data_to_db(cur=cur, downloader=storage_stream_downloader, blob_invalid=blob_invalid)

                duration = time.time() - blob_start_time

                logger.debug(
                    f"{blob_name} is done. "
                    f"Imported {blob_row_count} rows in {int(duration)} seconds "
                    f"({int(blob_row_count/duration)} rows/second)"
                )
                conn.commit()
                success_status = "imported"

            except Exception as e:
                if "ErrorCode:BlobNotFound" in str(e):
                    logger.error(f"Blob {blob_name} not found.")
                else:
                    logger.error(
                        f"Error after {int(time.time() - blob_start_time)} seconds when reading blob chunks: {e}"
                    )

                conn.rollback()
                success_status = "failed"

            cur.execute(
                "UPDATE importer.blob SET (import_finished, import_status) = (%s, %s) WHERE name = %s",
                (
                    datetime.utcnow(),
                    success_status,
                    blob_name,
                ),
            )
            conn.commit()


def read_imported_data_to_db(cur, downloader, blob_invalid: bool):
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
        "directionId": "direction_id",
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

    if not blob_invalid:
        cur.execute("CALL staging.import_and_normalize_hfp()")
    else:
        cur.execute("CALL stating.import_invalid_hfp()")

    cur.execute("DELETE FROM staging.hfp_raw")


def main(importer: func.TimerRequest, context: func.Context) -> None:
    """Main function to be called by Azure Function"""
    with CustomDbLogHandler("importer"):
        start_import()
