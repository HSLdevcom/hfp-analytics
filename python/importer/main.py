"""HFP Analytics data importer"""
import azure.functions as func
from azure.storage.blob import ContainerClient

import logging
from datetime import datetime, timedelta

from common.logger_util import CustomDbLogHandler
from common.config import (
    HFP_STORAGE_CONTAINER_NAME,
    HFP_STORAGE_CONNECTION_STRING,
    HFP_EVENTS_TO_IMPORT,
    IMPORT_COVERAGE_DAYS,
)

from .services import (
    create_db_lock,
    release_db_lock,
    add_new_blob,
    is_blob_listed,
    mark_blob_status_started,
    mark_blob_status_finished,
    pickup_blobs_for_import,
    copy_data_from_downloader_to_db,
)

logger = logging.getLogger("importer")


def get_azure_container_client() -> ContainerClient:
    return ContainerClient.from_connection_string(
        conn_str=HFP_STORAGE_CONNECTION_STRING, container_name=HFP_STORAGE_CONTAINER_NAME
    )


def update_blob_list_for_import(day_since_today):
    container_client = get_azure_container_client()
    storage_blob_names = []

    import_date = datetime.now() - timedelta(day_since_today)

    while import_date <= datetime.now():
        current_date_str = import_date.strftime("%Y-%m-%d")
        blobs = container_client.list_blobs(name_starts_with=current_date_str)
        for blob in blobs:
            storage_blob_names.append(blob.name)
        import_date += timedelta(days=1)

    for blob_name in storage_blob_names:
        if is_blob_listed(blob_name):
            # Already imported, no need to fetch tags or try to insert
            continue

        blob_client = container_client.get_blob_client(blob_name)
        tags = blob_client.get_blob_tags()

        blob_data = {}

        blob_data["blob_name"] = blob_name
        blob_data["event_type"] = tags.get("eventType")
        blob_data["min_oday"] = tags.get("min_oday")
        blob_data["max_oday"] = tags.get("max_oday")
        blob_data["min_tst"] = tags.get("min_tst")
        blob_data["max_tst"] = tags.get("max_tst")
        blob_data["row_count"] = tags.get("row_count")
        blob_data["invalid"] = tags.get("invalid")
        blob_data["covered_by_import"] = blob_data["event_type"] in HFP_EVENTS_TO_IMPORT

        add_new_blob(blob_data)


def import_blob(blob_name):
    logger.debug(f"Processing blob: {blob_name}")

    blob_metadata = mark_blob_status_started(blob_name)
    blob_row_count = blob_metadata.get("row_count", 0)
    blob_is_invalid = bool(blob_metadata.get("invalid"))

    try:
        container_client = get_azure_container_client()
        blob_client = container_client.get_blob_client(blob=blob_name)
        storage_stream_downloader = blob_client.download_blob()

        copy_data_from_downloader_to_db(downloader=storage_stream_downloader, invalid_blob=blob_is_invalid)

        processing_time = mark_blob_status_finished(blob_name)

        logger.debug(
            f"{blob_name} is done. "
            f"Imported {blob_row_count} rows in {processing_time} seconds "
            f"({int(blob_row_count/processing_time)} rows/second)"
        )

    except Exception as e:
        processing_time = mark_blob_status_finished(blob_name, failed=True)

        if "ErrorCode:BlobNotFound" in str(e):
            logger.error(f"Blob {blob_name} not found.")
        else:
            logger.error(f"Error after {processing_time} seconds when reading blob chunks: {e}")


def run_import() -> None:
    """Function to init and run importer procedures"""
    logger.info(f"Update blob list to cover last {IMPORT_COVERAGE_DAYS} days.")
    update_blob_list_for_import(IMPORT_COVERAGE_DAYS)

    logger.info("Selecting blobs for import.")
    blob_names = pickup_blobs_for_import()

    logger.debug(f"Running import for {blob_names}")
    for blob in blob_names:
        import_blob(blob)


def main(importer: func.TimerRequest, context: func.Context) -> None:
    """Main function to be called by Azure Function"""
    with CustomDbLogHandler("importer"):
        # Create a lock for import
        success = create_db_lock()

        if not success:
            return

        try:
            run_import()
        except Exception as e:
            logger.error(f"Error when running importer: {e}")
        finally:
            # Remove lock at this point
            release_db_lock()

        logger.info("Importer done.")
