""" Class module for importer. Importer can be initialized with different parameters depending on data source. """

from azure.storage.blob import ContainerClient

from collections.abc import Callable, Iterable
import csv
from io import BytesIO, TextIOWrapper
from datetime import date
import zstandard

import pyarrow.parquet as pq

from common.config import HFP_STORAGE_CONNECTION_STRING

from .schemas import DBSchema


def parquet_to_dict_decoder(buffer: BytesIO) -> Iterable[dict]:
    """Convert parquet file to list of dict objects"""
    data_table = pq.read_table(buffer)
    data = data_table.to_pylist()
    return data


def zst_csv_to_dict_decoder(buffer: BytesIO) -> Iterable[dict]:
    """Convert csv file to dict reader"""
    reader = zstandard.ZstdDecompressor().stream_reader(buffer)
    dict_reader = csv.DictReader(TextIOWrapper(reader, encoding="utf-8"))
    return dict_reader


class Importer:
    def __init__(
        self,
        container_name: str,
        data_converter: Callable[[BytesIO], Iterable[dict]],
        db_schema: DBSchema,
        blob_name_prefix: str = "",
    ) -> None:
        self.container_client = ContainerClient.from_connection_string(
            conn_str=HFP_STORAGE_CONNECTION_STRING, container_name=container_name
        )
        self.data_converter = data_converter
        self.db_schema = db_schema
        self.blob_name_prefix = blob_name_prefix

    def list_blobs_for_date(self, date_to_list: date) -> list:
        """List blobs from container based founf on the given date."""
        date_str = date_to_list.strftime("%Y-%m-%d")
        filter_str = self.blob_name_prefix + date_str
        blobs = self.container_client.list_blobs(name_starts_with=filter_str)
        blob_names = [blob.name for blob in blobs]
        return blob_names

    def get_metadata_for_blob(self, blob_name: str) -> dict:
        """Get blob tag metadata from container."""
        blob_client = self.container_client.get_blob_client(blob=blob_name)
        # merge metadata and tags, tags preferred
        metadata = {**blob_client.get_blob_properties().metadata, **blob_client.get_blob_tags()}
        return metadata

    def get_data_from_blob(self, blob_name: str) -> Iterable[dict]:
        """Download data from container to csv reader"""
        blob_client = self.container_client.get_blob_client(blob=blob_name)
        downloader = blob_client.download_blob()
        download_stream = BytesIO()
        downloader.readinto(download_stream)
        download_stream.seek(0)
        return self.data_converter(download_stream)
