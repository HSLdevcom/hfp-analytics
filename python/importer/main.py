"""HFP Analytics data importer"""
import azure.functions as func
from azure.storage.blob import ContainerClient
from io import StringIO
import os
import csv
import logging
import zstandard
from datetime import datetime, timedelta
import psycopg2 as psycopg
from psycopg2.pool import SimpleConnectionPool
from common.logger_util import CustomDbLogHandler
from common.utils import get_conn_params
import common.constants as constants
from .run_analysis import run_analysis
import time

# Import other event types as well when needed.
event_types_to_import = ['VP', 'DOC', 'DOO']
pool = SimpleConnectionPool(1, 20, get_conn_params())


logger = logging.getLogger('importer')


def get_azure_container_client() -> ContainerClient:
    hfp_storage_container_name = os.getenv('HFP_STORAGE_CONTAINER_NAME', '')
    if not hfp_storage_container_name:
        logger.error("HFP_STORAGE_CONTAINER_NAME env not found, have you defined it?")
    hfp_storage_connection_string = os.getenv('HFP_STORAGE_CONNECTION_STRING', '')
    if not hfp_storage_connection_string:
        logger.error("HFP_STORAGE_CONNECTION_STRING env not found, have you defined it?")
    return ContainerClient.from_connection_string(conn_str=hfp_storage_connection_string, container_name=hfp_storage_container_name)


def start_import():
    global is_importer_locked
    conn = psycopg.connect(get_conn_params())

    info = {}

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

    try:
        info = import_day_data_from_past(13)
        logger.info("Importing done - next up: analysis.")

    except Exception as e:
        logger.error(f'Error when running importer: {e}')
    finally:
        # Remove lock at this point
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (constants.IMPORTER_LOCK_ID,))
            conn.commit()
        conn.close()

    logger.info("Importer done. Starting minianalysis")

    run_analysis(info)


def import_day_data_from_past(day_since_today):
    logger.info(f"Importing HFP data {day_since_today} days from past.")

    import_date = datetime.now() - timedelta(day_since_today)
    import_date = datetime.strftime(import_date, '%Y-%m-%d')
    info = import_data(import_date=import_date)
    return info


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

    conn = psycopg.connect(get_conn_params())
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

                covered_by_import = event_type in event_types_to_import


                cur.execute("INSERT INTO importer.blob(name, type, min_oday, max_oday, min_tst, max_tst, row_count, covered_by_import) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                                    (name, event_type, tags.get('min_oday'), tags.get('max_oday'), tags.get('min_tst'), tags.get('max_tst'), tags.get('row_count'), covered_by_import,))

            conn.commit()

            logger.info("Importer ready for next step")

            cur.execute("SELECT name FROM importer.blob WHERE covered_by_import AND import_status IN ('not started', 'pending')")
            names = cur.fetchall()
            cur.execute("SELECT min(min_oday), max(max_oday), min(min_tst), max(max_tst), count(*), sum(row_count) FROM importer.blob WHERE covered_by_import AND import_status IN ('not started', 'pending')")
            data = cur.fetchone() or {}

            info = {
                'min_oday': data[0],
                'max_oday': data[1],
                'min_tst': data[2],
                'max_tst': data[3],
                'files': data[4],
                'rows': data[5]
            }

            for n in names:
                blob_names.append(n[0])

                cur.execute("UPDATE importer.blob SET import_status = 'pending' WHERE name = %s", (n,))

    conn.close()

    logger.debug(f"Running import for {blob_names}")

    try:
        for b in blob_names:
            import_blob(b)
    except Exception as e:
        print(e)
    finally:
        pool.closeall()

    return info


def read_imported_data_to_db(cur, downloader, chunk_size=10000):
    logger = logging.getLogger('importer')
    compressed_content = downloader.content_as_bytes()
    reader = zstandard.ZstdDecompressor().stream_reader(compressed_content)
    bytes = reader.readall()
    hfp_dict_reader = csv.DictReader(StringIO(bytes.decode('utf-8')))
    invalid_row_count = 0

    selected_fields = ["tst", "eventType", "receivedAt", "ownerOperatorId", "vehicleNumber", "mode",
                       "routeId", "dir", "oday", "start", "oper", "odo", "drst", "locationQualityMethod",
                       "stop", "longitude", "latitude"]

    calculator = 0
    rows_processed = 0
    chunk = []
    for old_row in hfp_dict_reader:
        calculator += 1
        new_row = {key: old_row[key] for key in selected_fields}
        if not any(old_row[key] is None for key in ["tst", "oper", "vehicleNumber"]):
            rows_processed += 1
            chunk.append(new_row)
            if len(chunk) >= chunk_size:
                yield chunk, rows_processed
                chunk = []
                rows_processed = 0
        else:
            invalid_row_count += 1

    if chunk:
        yield chunk, rows_processed

    if invalid_row_count > 0:
        logger.error(f'Import invalid row count: {invalid_row_count}')


def import_chunk(cur, chunk):
    import_io = StringIO()
    selected_fields = ["tst", "eventType", "receivedAt", "ownerOperatorId", "vehicleNumber", "mode",
                    "routeId", "dir", "oday", "start", "oper", "odo", "drst", "locationQualityMethod",
                    "stop", "longitude", "latitude"]
    writer = csv.DictWriter(import_io, fieldnames=selected_fields)

    for row in chunk:
        writer.writerow(row)

    import_io.seek(0)
    cur.copy_expert(sql="COPY hfp.view_as_original_hfp_event FROM STDIN WITH CSV",
                    file=import_io)
    import_io.seek(0)
    import_io.truncate()

def import_blob(blob_name, chunk_size=10000):
    logger = logging.getLogger('importer')
    logger.debug(f"Processing blob: {blob_name}")
    blob_start_time = time.time()

    connection = pool.getconn()
    cur = connection.cursor()

    cur.execute("UPDATE importer.blob SET import_started = %s, import_status = 'importing' WHERE name = %s", (datetime.utcnow(), blob_name,))
    connection.commit()
    try:
        container_client = get_azure_container_client()

        blob_client = container_client.get_blob_client(blob=blob_name)
        storage_stream_downloader = blob_client.download_blob()

        row_count = 0
        for chunk, rows_processed in read_imported_data_to_db(cur=cur, downloader=storage_stream_downloader, chunk_size=chunk_size):
            import_chunk(cur, chunk)
            row_count += rows_processed
        
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
    pool.putconn(connection)


def main(importer: func.TimerRequest, context: func.Context) -> None:
    """ Main function to be called by Azure Function """
    with CustomDbLogHandler('importer'):
        start_import()
