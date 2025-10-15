import datetime
import math
from typing import List

from azure.storage.blob.aio import ContainerClient

from common.config import FLOW_ANALYTICS_SAS_CONNECTION_STRING
from common.models.hfp import PreprocessBlobModel


class FlowAnalyticsContainerClient:

    async def save_preprocess_data(
        self,
        preprocess_type: str,  # clusters or departures
        compressed_csv: bytes,
        route_id: str,
        mode: str,
        oday: str,
    ) -> None:
        path = f"preprocess/{preprocess_type}/{oday}/{oday}_{route_id}_{mode}"

        def parse_metadata(v) -> str:
            if v is None:
                return ""
            if isinstance(v, float) and math.isnan(v):
                return ""
            return str(v)

        metadata = {
            "oday": parse_metadata(oday),
            "route_id": parse_metadata(route_id),
            "mode": parse_metadata(mode),
        }

        async with self._get_container_client() as client:
            await client.upload_blob(
                name=path, data=compressed_csv, overwrite=True, metadata=metadata
            )

    async def save_cluster_data(
        self,
        recluster_type: str,  # routes or modes
        compressed_data: bytes,
        from_oday: str,
        to_oday: str,
        route_id: str,
    ) -> None:
        metadata = {"from_oday": from_oday, "to_oday": to_oday, "route_id": route_id}

        path = f"recluster/{recluster_type}/{from_oday}_{to_oday}/{from_oday}_{to_oday}_{route_id}"

        async with self._get_container_client() as client:
            await client.upload_blob(
                name=path, data=compressed_data, overwrite=True, metadata=metadata
            )
            
    def _get_container_client(self) -> ContainerClient:
        return ContainerClient.from_container_url(
            FLOW_ANALYTICS_SAS_CONNECTION_STRING
        )
    
    async def get_existing_blob_data_from_previous_2_months(self, preprocess_type: str):      
        # returns data for last 2 months
        current_date = datetime.date.today()
        earliest_date = current_date - datetime.timedelta(weeks=9)
        
        available_dates = [
            (earliest_date + datetime.timedelta(days=x)).strftime('%Y-%m-%d')
            for x in range((current_date - earliest_date).days + 1)
        ]
        
        blob_names = []
        async with self._get_container_client() as client:
            blob_list = client.list_blobs(name_starts_with=f'preprocess/{preprocess_type}/')
            async for blob in blob_list:
                blob_names.append(blob.name)
                
        filtered_blob_names =  self._filter_blob_names(blob_names=blob_names, available_dates=available_dates)
        
        return self._get_preprocess_blob_models_from_blob_names(blob_names=filtered_blob_names)
    
    def _filter_blob_names(self, blob_names: List[str], available_dates: List[str]):
        filtered_blob_names = []

        for blob_name in blob_names:
            for date in available_dates:
                if date in blob_name:
                    filtered_blob_names.append(blob_name)
                    break
                
        # remove blob folders
        filtered_blob_names = [blob_name for blob_name in filtered_blob_names if blob_name.count('/') == 3]
        
        return filtered_blob_names
    
    async def load_blob(self, blob_path: str):
        async with self._get_container_client() as client:
            blob_client = client.get_blob_client(blob_path)
            stream = await blob_client.download_blob()
            blob_data = await stream.readall()
        
        return blob_data
    
    def _get_preprocess_blob_models_from_blob_names(self, blob_names: List[str]) -> List[PreprocessBlobModel]:
        preprocess_blob_models = []
        for blob_name in blob_names:
            file_name = blob_name.split('/')[-1]  # e.i. filename = '2025-06-01_4587_bus'
            date, route_id, mode = file_name.split('_')
            
            preprocess_blob_models.append(PreprocessBlobModel(blob_path=blob_name, oday=date, route_id=route_id, mode=mode))

        return preprocess_blob_models
