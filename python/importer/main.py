"""HFP Analytics data importer"""
import azure.functions as func
from azure.storage.blob import ContainerClient
from io import StringIO
import csv
import logging
import zstandard
import time
from datetime import datetime, timedelta
import psycopg2 as psycopg
from common.logger_util import CustomDbLogHandler
import common.constants as constants
import common.slack as slack
from common.config import (
    HFP_STORAGE_CONTAINER_NAME,
    HFP_STORAGE_CONNECTION_STRING,
    POSTGRES_CONNECTION_STRING,
    HFP_EVENTS_TO_IMPORT,
    IMPORT_COVERAGE_DAYS,
)


logger = logging.getLogger('importer')


def get_azure_container_client() -> ContainerClient:
    return ContainerClient.from_connection_string(
        conn_str=HFP_STORAGE_CONNECTION_STRING, container_name=HFP_STORAGE_CONTAINER_NAME
    )


def start_import():
    global is_importer_locked
    conn = psycopg.connect(POSTGRES_CONNECTION_STRING)

    # Create a lock for import
    try:
        with conn:
            with conn.cursor() as cur:
                # Check if importer is locked or not. We use lock strategy to prevent executing importer
                # and analysis more than once at a time
                cur.execute("SELECT is_lock_enabled(%s)", (constants.IMPORTER_LOCK_ID,))
                is_importer_locked = cur.fetchone()[0]

                if is_importer_locked:
                    logger.error("Importer is LOCKED which means that importer should be already running. You can get"
                                "rid of the lock by restarting the database if needed.")
                    return

                logger.info("Going to run importer.")
                cur.execute("SELECT pg_advisory_lock(%s)", (constants.IMPORTER_LOCK_ID,))
                conn.commit()
    except Exception as e:
        logger.error(f'Error when creating locks for importer: {e}')
        slack.send_to_channel(f'Error when creating locks for importer: {e}', alert=True)

    try:
        import_day_data_from_past(IMPORT_COVERAGE_DAYS)

    except Exception as e:
        logger.error(f'Error when running importer: {e}')
        slack.send_to_channel(f'Error when running importer: {e}', alert=True)
    finally:
        # Remove lock at this point
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (constants.IMPORTER_LOCK_ID,))
            conn.commit()
        conn.close()

    logger.info("Importer done.")


def import_day_data_from_past(day_since_today):
    logger.info(f"Importing HFP data {day_since_today} days from past.")

    import_date = datetime.now() - timedelta(day_since_today)
    import_date = datetime.strftime(import_date, '%Y-%m-%d')
    import_data(import_date=import_date)


def import_data(import_date):
    info = {}
    container_client = get_azure_container_client()
    storage_blob_names = []
    import_date_obj = datetime.strptime(import_date, "%Y-%m-%d")

    while import_date_obj <= datetime.now():
        current_date_str = import_date_obj.strftime("%Y-%m-%d")
        blobs = container_client.list_blobs(name_starts_with=current_date_str)
        for blob in blobs:
            storage_blob_names.append(blob.name)
        import_date_obj += timedelta(days=1)

    blob_names = []

    conn = psycopg.connect(POSTGRES_CONNECTION_STRING)
    with conn:
        with conn.cursor() as cur:
            for i, name in enumerate(storage_blob_names):
                cur.execute("SELECT EXISTS( SELECT 1 FROM importer.blob WHERE name = %s)", (name,))
                exists_in_list = cur.fetchone()[0]

                if exists_in_list:
                    # Already imported, no need to fetch tags or try to insert
                    continue

                blob_client = container_client.get_blob_client(name)
                tags = blob_client.get_blob_tags()

                event_type = tags.get('eventType')

                min_oday = datetime.strptime(tags.get('min_oday'), "%Y-%m-%d")
                blob_date = datetime.strptime(name[0:10], "%Y-%m-%d")

                # Warn if min_oday is older than from yesterday
                if blob_date - min_oday > timedelta(1):
                    logger.warning(f"Bad oday data found in {name}")
                    slack.send_to_channel(f"Bad oday data found in {name}", alert=True)

                covered_by_import = event_type in HFP_EVENTS_TO_IMPORT


                cur.execute("INSERT INTO importer.blob(name, type, min_oday, max_oday, min_tst, max_tst, row_count, covered_by_import) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                                    (name, event_type, tags.get('min_oday'), tags.get('max_oday'), tags.get('min_tst'), tags.get('max_tst'), tags.get('row_count'), covered_by_import,))

            conn.commit()

            cur.execute(
                """
                SELECT name
                FROM importer.blob
                WHERE covered_by_import AND import_status IN ('not started', 'pending')
                ORDER BY type, name
                """)
            names = cur.fetchall()

            for n in names:
                blob_names.append(n[0])

                cur.execute("UPDATE importer.blob SET import_status = 'pending' WHERE name = %s", (n,))

    conn.close()

    logger.debug(f"Running import for {blob_names}")

    for b in blob_names:
        import_blob(b)


def import_blob(blob_name):
    # TODO: Use connection pooling
    connection = psycopg.connect(POSTGRES_CONNECTION_STRING)
    cur = connection.cursor()
    logger.debug(f"Processing blob: {blob_name}")
    blob_start_time = time.time()
    cur.execute("UPDATE importer.blob SET import_started = %s, import_status = 'importing' WHERE name = %s", (datetime.utcnow(), blob_name,))
    connection.commit()
    try:
        container_client = get_azure_container_client()

        blob_client = container_client.get_blob_client(blob=blob_name)
        storage_stream_downloader = blob_client.download_blob()

        row_count = read_imported_data_to_db(cur=cur, downloader=storage_stream_downloader, blob_name=blob_name)
        duration = time.time() - blob_start_time
        logger.debug(f"{blob_name} is done. Imported {row_count} rows in {int(duration)} seconds ({int(row_count/duration)} rows/second)")
        cur.execute("UPDATE importer.blob SET (import_finished, import_status) = (%s, 'imported') WHERE name = %s", (datetime.utcnow(), blob_name,))
        connection.commit()

    except Exception as e:
        if "ErrorCode:BlobNotFound" in str(e):
            logger.error(f'Blob {blob_name} not found.')
        else:
            logger.error(f'Error after {int(time.time() - blob_start_time)} seconds when reading blob chunks: {e}')
        connection.rollback()
        cur.execute("UPDATE importer.blob SET (import_finished, import_status) = (%s, 'failed') WHERE name = %s", (datetime.utcnow(), blob_name,))
        connection.commit()

    cur.close()
    connection.close()


def read_imported_data_to_db(cur, downloader, blob_name):
    compressed_content = downloader.content_as_bytes()
    reader = zstandard.ZstdDecompressor().stream_reader(compressed_content)
    bytes = reader.readall()
    hfp_dict_reader = csv.DictReader(StringIO(bytes.decode('utf-8')))
    import_io = StringIO()

    invalid_row_count = 0
    selected_fields = ["tst", "eventType", "receivedAt", "ownerOperatorId", "vehicleNumber", "mode",
                       "routeId", "directionId", "oday", "start", "oper", "odo", "spd", "drst", "locationQualityMethod",
                       "stop", "longitude", "latitude"]
    writer = csv.DictWriter(import_io, fieldnames=selected_fields)

    calculator = 0
    for old_row in hfp_dict_reader:
        calculator += 1
        new_row = {key: old_row[key] for key in selected_fields}
        if not any(old_row[key] is None for key in ["tst", "oper", "vehicleNumber"]):
            writer.writerow(new_row)
        else:
            invalid_row_count += 1

    if invalid_row_count > 0:
        logger.error(f'Analytics found {invalid_row_count} invalid rows in blob {blob_name}')
        slack.send_to_channel(f'Analytics found {invalid_row_count} invalid rows in blob {blob_name}', alert=True)

    import_io.seek(0)
    cur.execute("DELETE FROM staging.hfp_raw")
    cur.copy_expert(sql="COPY staging.hfp_raw FROM STDIN WITH CSV",
                    file=import_io)
    cur.execute("CALL staging.import_and_normalize_hfp()")
    cur.execute("DELETE FROM staging.hfp_raw")

    return calculator


def main(importer: func.TimerRequest, context: func.Context) -> None:
    """ Main function to be called by Azure Function """
    with CustomDbLogHandler('importer'):
        start_import()
