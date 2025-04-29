import os
from azure.storage.blob.aio import ContainerClient


class FlowAnalyticsContainerClient:
    def __init__(self) -> None:
        connection_string = os.environ["FLOW_ANALYTICS_SAS_CONNECTION_STRING"]

        self.client = ContainerClient.from_container_url(connection_string)

    async def save_preprocess_data(
        self,
        preprocess_type: str,  # clusters or departures
        compressed_csv: bytes,
        route_id: str,
        mode: str,
        oday: str,
    ) -> None:
        metadata = {"oday": oday, "route_id": route_id, "mode": mode}

        path = f"preprocess/{preprocess_type}/{oday}/{oday}_{route_id}_{mode}"

        await self.client.upload_blob(
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
        metadata = {"from_oday": from_oday, "to_odday": to_oday, "route_id": route_id}

        path = f"recluster/{recluster_type}/{from_oday}_{to_oday}/{from_oday}_{to_oday}_{route_id}"

        await self.client.upload_blob(
            name=path, data=compressed_data, overwrite=True, metadata=metadata
        )
