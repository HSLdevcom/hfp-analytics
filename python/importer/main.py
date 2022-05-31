"""HFP Analytics data importer"""
import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobBlock
from io import StringIO
import os
import csv
import zstandard
from datetime import datetime, timedelta
import psycopg2 as psycopg
from common.utils import get_conn_params, get_logger
from .run_analysis import main as run_analysis

# TODO: import other event types as well when needed.
event_types_to_import = ['DOC', 'DOO']

def main(dataImporter: func.TimerRequest):
    logger = get_logger()
    logger.debug("###   Going to run importer.   ###")
    imported_successfully = False
    conn = psycopg.connect(**get_conn_params())
    try:
        with conn:
            with conn.cursor() as pg_cursor:
                imported_successfully = import_data_to_db(pg_cursor=pg_cursor)
    finally:
        conn.close()
        if imported_successfully == True:
            print("###   Import done successfully.   ###")
            run_analysis()

def import_data_to_db(pg_cursor):
    yesterday = datetime.now() - timedelta(1)
    yesterday = datetime.strftime(yesterday, '%Y-%m-%d')
    hfp_storage_container_name = os.getenv('HFP_STORAGE_CONTAINER_NAME')
    hfp_storage_connection_string = os.getenv('HFP_STORAGE_CONNECTION_STRING')
    service = BlobServiceClient.from_connection_string(conn_str=hfp_storage_connection_string)
    result = service.find_blobs_by_tags(f"@container='{hfp_storage_container_name}' AND min_oday <= '{yesterday}' AND max_oday >= '{yesterday}'")

    blob_names = []
    for i, r in enumerate(result):
        # File format is e.g. 2022-05-11T00-3_ARR.csv.zst
        # extract event type as we know what the format is:
        type = r.name.split('T')[1].split('_')[1].split('.')[0]
        if type in event_types_to_import:
            print(r.name)
            blob_names.append(r.name)

    imported_successfully = True

    blob_index = 0
    try:
        for blob in blob_names:
            blob_client = service.get_blob_client(container="hfp-v2-test", blob=blob)
            storage_stream_downloader = blob_client.download_blob()
            read_imported_data_to_db(pg_cursor=pg_cursor, downloader=storage_stream_downloader)
            blob_index += 1
            # Limit downloading all the blobs when developing.
            if os.getenv('IS_DEBUG') == 'True' and blob_index > 1:
                return imported_successfully
    except Exception as e:
        imported_successfully = False
        print(f'Error in reading blob chunks: {e}')

    return imported_successfully

def read_imported_data_to_db(pg_cursor, downloader):
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
        print(f'Import invalid row count: {invalid_row_count}')
    import_io.seek(0)
    pg_cursor.copy_expert(sql="COPY hfp.view_as_original_hfp_event FROM STDIN WITH CSV",
                    file=import_io)
