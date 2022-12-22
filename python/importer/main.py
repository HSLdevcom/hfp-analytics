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
from common.logger_util import CustomDbLogHandler
from common.utils import get_conn_params
import common.constants as constants
from .run_analysis import run_analysis
from .remove_old_data import remove_old_data
import time

# Import other event types as well when needed.
event_types_to_import = ['VP', 'DOC', 'DOO']

def get_azure_container_client() -> ContainerClient:
    logger = logging.getLogger('importer')
    hfp_storage_container_name = os.getenv('HFP_STORAGE_CONTAINER_NAME', '')
    if not hfp_storage_container_name:
        logger.error("HFP_STORAGE_CONTAINER_NAME env not found, have you defined it?")
    hfp_storage_connection_string = os.getenv('HFP_STORAGE_CONNECTION_STRING', '')
    if not hfp_storage_connection_string:
        logger.error("HFP_STORAGE_CONNECTION_STRING env not found, have you defined it?")
    return ContainerClient.from_connection_string(conn_str=hfp_storage_connection_string, container_name=hfp_storage_container_name)

def main(importer: func.TimerRequest, context: func.Context):
    custom_db_log_handler = CustomDbLogHandler(function_name='importer')
    logger = logging.getLogger('importer')

    global is_importer_locked
    conn = psycopg.connect(get_conn_params())
    
    import_success = False

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
        import_day_data_from_past(1)
        logger.info("Importing done - next up: analysis.")
        import_success = True
    except Exception as e:
        logger.error(f'Error when running importer: {e}')
    finally:
        # Remove lock at this point
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (constants.IMPORTER_LOCK_ID,))
            conn.commit()
        conn.close()


    logger.info("Importer done. Starting minianalysis")


    run_analysis()

    custom_db_log_handler.remove_handlers()


def import_day_data_from_past(day_since_today):
    logger = logging.getLogger('importer')
    logger.info(f"Importing HFP data {day_since_today} days from past.")

    import_date = datetime.now() - timedelta(day_since_today)
    import_date = datetime.strftime(import_date, '%Y-%m-%d')
    import_data(import_date=import_date)

def import_data(import_date):
    logger = logging.getLogger('importer')

    container_client = get_azure_container_client()
    result = container_client.find_blobs_by_tags(f"min_oday <= '{import_date}' AND max_oday >= '{import_date}'")
    # result = container_client.list_blob_names(name_starts_with=import_date)

    blob_names = []

    conn = psycopg.connect(get_conn_params())
    with conn:
        with conn.cursor() as cur:
            
            for i, r in enumerate(result):
                name = str(r.name)
                # File format is e.g. 2022-05-11T00-3_ARR.csv.zst
                # extract event type as we know what the format is:
                # type = name.split('T')[1].split('_')[1].split('.')[0]
                # if type in event_types_to_import:
                #     blob_names.append(r.name)

                cur.execute("SELECT EXISTS( SELECT 1 FROM importer.blob WHERE name = %s)", (name,))
                exists_in_list = cur.fetchone()[0]

                if exists_in_list:
                    # Already imported, no need to fetch tags or try to insert
                    continue

                blob_client = container_client.get_blob_client(blob=name)
                tags = blob_client.get_blob_tags()

                event_type = tags.get('eventType')

                covered_by_import = event_type in event_types_to_import


                cur.execute("INSERT INTO importer.blob(name, type, min_oday, max_oday, min_tst, max_tst, row_count, covered_by_import) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                                    (name, event_type, tags.get('min_oday'), tags.get('max_oday'), tags.get('min_tst'), tags.get('max_tst'), tags.get('row_count'), covered_by_import,))


    # for blob in blob_names:
    #     import_blob(blob)

            logger.info("Importer ready for next step")

            cur.execute("SELECT name FROM importer.blob WHERE covered_by_import AND import_status = 'not started'")
            
            names = cur.fetchall()

            for n in names:
                blob_names.append(n[0])

                cur.execute("UPDATE importer.blob SET import_status = 'pending' WHERE name = %s", (n,))

    conn.close()
    # print(blob_names)
    logger.debug(f"Running import for {blob_names}")

    
    for b in blob_names:
        import_blob(b)





def import_blob(blob_name):
    # TODO: Use connection pooling
    connection = psycopg.connect(get_conn_params())
    cur = connection.cursor()
    logger = logging.getLogger('importer')
    logger.debug(f"Processing blob: {blob_name}")
    blob_start_time = time.time()
    cur.execute("UPDATE importer.blob SET import_started = NOW(), import_status = 'importing' WHERE name = %s", (blob_name,))
    connection.commit()
    try:
        container_client = get_azure_container_client()

        blob_client = container_client.get_blob_client(blob=blob_name)
        storage_stream_downloader = blob_client.download_blob()

        row_count = read_imported_data_to_db(cur=cur, downloader=storage_stream_downloader)
        duration = time.time() - blob_start_time
        logger.debug(f"{blob_name} is done. Imported {row_count} rows in {int(duration)} seconds ({int(row_count/duration)} rows/second)")
        cur.execute("UPDATE importer.blob SET (import_finished, import_status) = (NOW(), 'imported') WHERE name = %s", (blob_name,))
        connection.commit()

    except Exception as e:
        if "ErrorCode:BlobNotFound" in str(e):
            logger.error(f'Blob {blob_name} not found.')
        else:
            logger.error(f'Error after {int(time.time() - blob_start_time)} seconds when reading blob chunks: {e}')
        connection.rollback()
        cur.execute("UPDATE importer.blob SET (import_finished, import_status) = (NOW(), 'failed') WHERE name = %s", (blob_name,))
        connection.commit()

    cur.close()
    connection.close()


def read_imported_data_to_db(cur, downloader):
    logger = logging.getLogger('importer')
    compressed_content = downloader.content_as_bytes()
    reader = zstandard.ZstdDecompressor().stream_reader(compressed_content)
    bytes = reader.readall()
    hfp_dict_reader = csv.DictReader(StringIO(bytes.decode('utf-8')))
    import_io = StringIO()

    invalid_row_count = 0
    selected_fields = ["tst", "eventType", "receivedAt", "ownerOperatorId", "vehicleNumber", "mode",
                       "routeId", "dir", "oday", "start", "oper", "odo", "drst", "locationQualityMethod",
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
        logger.error(f'Import invalid row count: {invalid_row_count}')

    import_io.seek(0)
    cur.copy_expert(sql="COPY hfp.view_as_original_hfp_event FROM STDIN WITH CSV",
                    file=import_io)

    return calculator
