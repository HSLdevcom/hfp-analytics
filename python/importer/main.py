"""HFP Analytics data importer"""
import azure.functions as func

import logging
from datetime import datetime, timedelta

from common.logger_util import CustomDbLogHandler
import common.slack as slack
from common.config import (
    APC_STORAGE_CONTAINER_NAME,
    HFP_STORAGE_CONTAINER_NAME,
    HFP_EVENTS_TO_IMPORT,
    IMPORT_COVERAGE_DAYS,
)
from .importer import Importer, parquet_to_dict_decoder, zst_csv_to_dict_decoder
from .schemas import APC as APCSchema, HFP as HFPSchema
from .services import (
    create_db_lock,
    release_db_lock,
    add_new_blob,
    is_blob_listed,
    mark_blob_status_started,
    mark_blob_status_finished,
    pickup_blobs_for_import,
    copy_data_to_db,
)

logger = logging.getLogger("importer")

importers = {
    "APC": Importer(
        APC_STORAGE_CONTAINER_NAME, data_converter=parquet_to_dict_decoder, db_schema=APCSchema, blob_name_prefix="apc_"
    ),
    "HFP": Importer(HFP_STORAGE_CONTAINER_NAME, data_converter=zst_csv_to_dict_decoder, db_schema=HFPSchema),
}


def update_blob_list_for_import(day_since_today):
    for importer_type, importer in importers.items():
        import_date = datetime.now() - timedelta(day_since_today)

        while import_date <= datetime.now():
            for blob_name in importer.list_blobs_for_date(import_date):
                if is_blob_listed(blob_name):
                    # Already imported, no need to fetch tags or try to insert
                    continue

                metadata = importer.get_metadata_for_blob(blob_name)

                blob_data = {}

                blob_data["blob_name"] = blob_name
                blob_data["event_type"] = metadata.get("eventType") if importer_type == "HFP" else "APC"
                blob_data["min_oday"] = metadata.get("min_oday")
                blob_data["max_oday"] = metadata.get("max_oday")
                blob_data["min_tst"] = metadata.get("min_tst")
                blob_data["max_tst"] = metadata.get("max_tst")
                blob_data["row_count"] = metadata.get("row_count")
                blob_data["invalid"] = metadata.get("invalid", False)
                blob_data["covered_by_import"] = blob_data["event_type"] in HFP_EVENTS_TO_IMPORT

                add_new_blob(blob_data)

            import_date += timedelta(days=1)


def import_blob(blob_name):
    logger.debug(f"Processing blob: {blob_name}")

    blob_metadata = mark_blob_status_started(blob_name)
    blob_row_count = blob_metadata.get("row_count", 0)
    blob_is_invalid = bool(blob_metadata.get("invalid"))

    try:
        importer = importers["APC"] if blob_metadata.get("type") == "APC" else importers["HFP"]
        data_rows = importer.get_data_from_blob(blob_name)

        copy_data_to_db(db_schema=importer.db_schema, data_rows=data_rows, invalid_blob=blob_is_invalid)

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
            slack.send_to_channel(
                f"Error after {processing_time} seconds when reading blob chunks: {e}",
                alert=True,
            )


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
