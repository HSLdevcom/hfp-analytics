"""HFP Analytics data importer"""
import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobBlock
from io import StringIO
import os
import csv
import zstandard
from datetime import datetime, timedelta
import pytz
from dateutil import parser
import psycopg2 as psycopg
from common.utils import get_conn_params
from .run_analysis import main as run_analysis

def main(dataImporter: func.TimerRequest):
    conn = psycopg.connect(**get_conn_params())
    try:
        with conn:
            imported_successfully = import_data_to_db(conn=conn)
            print(f' imported_successfully {imported_successfully}')
            if imported_successfully == True:
                print("Import done successfully, running analysis.")
                run_analysis()
    finally:
        conn.close()

def import_data_to_db(conn):
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
        if type == 'VP' or type == 'DOC' or type == 'DOO':
            blob_names.append(r.name)

    # TODO: we want to download all of the blobs, not just blob_names[0]
    blob_client = service.get_blob_client(container="hfp-v2-test", blob=blob_names[0])
    imported_successfully = True
    try:
        stream = blob_client.download_blob()
        read_imported_data_to_db(conn=conn, stream=stream)

    except Exception as e:
        imported_successfully = False
        print(f'Error in reading blob chunks: {e}')

    return imported_successfully

def read_imported_data_to_db(conn, stream):
    # Read data in chunks to avoid loading all into memory at once
    for chunk in stream.chunks():
        reader = zstandard.ZstdDecompressor().stream_reader(chunk)
        bytes = reader.readall()
        hfp_dict_reader = csv.DictReader(StringIO(bytes.decode('utf-8')))

        import_io = StringIO()

        ind = 0
        for hfp in hfp_dict_reader:
            if ind == 0:
                if hfp["longitude"] != None and hfp["latitude"] != None:
                    import_io.write(
                        f'{hfp["tst"]},{hfp["eventType"]},{hfp["receivedAt"]}{hfp["ownerOperatorId"]},{hfp["vehicleNumber"]},{hfp["mode"]}{hfp["routeId"]},{hfp["dir"]}, {hfp["oday"]},{hfp["start"]},{hfp["oper"]}{hfp["odo"]}{hfp["drst"]},{hfp["locationQualityMethod"]}{hfp["stop"]}{hfp["longitude"]},{hfp["latitude"]}')
                ind = + 1

        with conn.cursor() as cur:
            import_io.seek(0)
            cur.copy_from(
                file=import_io,
                table='hfp.view_as_original_hfp_event',
                columns=('tst','event_type','received_at','vehicle_operator_id','vehicle_number','transport_mode','route_id','direction_id','oday','start','observed_operator_id','odo','drst','loc','stop','longitude','latitude')
            )

