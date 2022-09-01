"""HFP Analytics data importer"""
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from io import StringIO
import os
import csv
import logging
import zstandard
from datetime import datetime, timedelta
import psycopg2 as psycopg
from common.logger_util import PostgresDBHandler
from common.utils import get_conn_params
import common.constants as constants
from .run_analysis import main as run_analysis

# TODO: import other event types as well when needed.
event_types_to_import = ['DOC', 'DOO']

def main(importer: func.TimerRequest, context: func.Context):
    
    logger = logging.getLogger()
    fmt = logging.Formatter("%(name)-12s %(asctime)s %(levelname)-8s %(filename)s:%(funcName)s %(message)s")
    console_log_handler = logging.StreamHandler()
    console_log_handler.setFormatter(fmt)
    logger.addHandler(console_log_handler)

    db_log_handler = PostgresDBHandler(function_name='importer', conn_params=get_conn_params())
    db_log_handler.setFormatter(fmt)
    logger.addHandler(db_log_handler)

    global is_importer_locked
    conn = psycopg.connect(**get_conn_params())
    try:
        with conn:
            with conn.cursor() as cur:
                # Check if importer is locked or not. We use lock strategy to prevent executing importer
                # and analysis more than once at a time
                cur.execute("SELECT is_lock_enabled(%s)", (constants.IMPORTER_LOCK_ID,))
                is_importer_locked = cur.fetchone()[0]

                if is_importer_locked == False:
                    logger.info("Going to run importer.")
                    cur.execute("SELECT pg_advisory_lock(%s)", (constants.IMPORTER_LOCK_ID,))
                else:
                    logger.info("Importer is LOCKED which means that importer should be already running. You can get"
                                "rid of the lock by restarting the database if needed.")
                    return
                print("Running import_day_data_from_past")
                # import_day_data_from_past(1, cur, logger)
                # import_day_data_from_past(2, cur, logger)
                # import_day_data_from_past(3, cur, logger)
                # import_day_data_from_past(4, cur, logger)
                # import_day_data_from_past(5, cur, logger)
                # import_day_data_from_past(6, cur, logger)
                # import_day_data_from_past(7, cur, logger)
                logger.info("Importing done - next up: analysis.")
    finally:
        conn.cursor().execute("SELECT pg_advisory_unlock(%s)", (constants.IMPORTER_LOCK_ID,))

        if is_importer_locked == False:
            logger.info("Going to run analysis.")
            # run_analysis()
        else:
            logger.info("Skipping analysis - importer is locked.")

        logger.info("Importer done.")
        logger.removeHandler(console_log_handler)
        logger.removeHandler(db_log_handler)

        conn.close()

def import_day_data_from_past(day_since_today, cur, logger):
    import_date = datetime.now() - timedelta(day_since_today)
    import_date = datetime.strftime(import_date, '%Y-%m-%d')
    import_data(cur=cur, import_date=import_date, logger=logger)

def import_data(cur, import_date, logger):
    hfp_storage_container_name = os.getenv('HFP_STORAGE_CONTAINER_NAME')
    hfp_storage_connection_string = os.getenv('HFP_STORAGE_CONNECTION_STRING')
    service = BlobServiceClient.from_connection_string(conn_str=hfp_storage_connection_string)
    result = service.find_blobs_by_tags(f"@container='{hfp_storage_container_name}' AND min_oday <= '{import_date}' AND max_oday >= '{import_date}'")

    blob_names = []
    for i, r in enumerate(result):
        # File format is e.g. 2022-05-11T00-3_ARR.csv.zst
        # extract event type as we know what the format is:
        type = r.name.split('T')[1].split('_')[1].split('.')[0]
        if type in event_types_to_import:
            blob_names.append(r.name)

    blob_index = 0
    for blob in blob_names:
        try:
            blob_client = service.get_blob_client(container="hfp-v2-test", blob=blob)
            storage_stream_downloader = blob_client.download_blob()
            read_imported_data_to_db(cur=cur, downloader=storage_stream_downloader, logger=logger)
            blob_index += 1
            # Limit downloading all the blobs when developing. Enable if needed.
            # if os.getenv('IS_DEBUG') == 'True' and blob_index > 1:
            #   return
        except Exception as e:
            logger.error(f'Error in reading blob chunks: {e}')

def read_imported_data_to_db(cur, downloader, logger):
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
    for old_row in hfp_dict_reader:
        new_row = {key: old_row[key] for key in selected_fields}
        if not any(old_row[key] is None for key in ["tst", "oper", "vehicleNumber"]):
            writer.writerow(new_row)
        else:
            invalid_row_count += 1
    # TODO: log invalid row count into db
    if invalid_row_count > 0:
        logger.info(f'Import invalid row count: {invalid_row_count}')
    import_io.seek(0)
    cur.copy_expert(sql="COPY hfp.view_as_original_hfp_event FROM STDIN WITH CSV",
                    file=import_io)
