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

def main(dataImporter: func.TimerRequest) -> None:
    yesterday = datetime.now() - timedelta(1)
    yesterday = datetime.strftime(yesterday, '%Y-%m-%d')
    hfp_storage_container_name = os.getenv('HFP_STORAGE_CONTAINER_NAME')
    hfp_storage_connection_string = os.getenv('HFP_STORAGE_CONNECTION_STRING')
    print(hfp_storage_container_name)
    print(hfp_storage_connection_string)
    service = BlobServiceClient.from_connection_string(conn_str=hfp_storage_connection_string)
    result = service.find_blobs_by_tags(f"@container='{hfp_storage_container_name}' AND min_oday <= '{yesterday}' AND max_oday >= '{yesterday}'")

    blob_names = []
    for i, r in enumerate(result):
        # File format is e.g. 2022-05-11T00-3_ARR.csv.zst
        # extract event type as we know what the format is:
        type = r.name.split('T')[1].split('_')[1].split('.')[0]
        if type == 'VP' or type == 'DOC' or type == 'DOO':
            blob_names.append(r.name)

    print(blob_names[0])

    blob_client = service.get_blob_client(container="hfp-v2-test", blob=blob_names[0])
    import_succeeded = True
    try:
        stream = blob_client.download_blob()

        for chunk in stream.chunks():
            reader = zstandard.ZstdDecompressor().stream_reader(chunk)
            bytes = reader.readall()
            dict_reader = csv.DictReader(StringIO(bytes.decode('utf-8')))

            ind = 0
            for row in dict_reader:
                if ind == 0:
                    # TODO: remove if - insert all rows when insert_hfp_row works.
                    insert_hfp_row(row)
                    ind = 1

    except Exception as e:
        import_succeeded = False
        print(f'Error in reading blob chunks: {e}')

    if import_succeeded == True:
        print("Import done successfully, running analysis.")
        run_analysis()

def insert_hfp_row(hfp):
    conn = psycopg.connect(**get_conn_params())

    tst_with_timezone = parser.parse(hfp["tst"])
    start_time_with_milliseconds = parser.parse(hfp["start"] + ":00")
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(f'INSERT INTO observation \
                (tst,event,oper,veh,route,dir,oday,start,stop_id,stop_id_guessed,long,lat) \
                    VALUES ({tst_with_timezone.timestamp()}, {hfp["eventType"]}, {hfp["oper"]}, {hfp["veh"]}, \
                            {hfp["route"]}, {hfp["dir"]}, {hfp["oday"]}, {start_time_with_milliseconds}, \
                            {hfp["stop"]}, {hfp["stop"]}, {hfp["longitude"]}, {hfp["latitude"]});')
    except Exception as e:
        print(f'Error in inserting hfp row: {e}')
    finally:
        conn.close()
