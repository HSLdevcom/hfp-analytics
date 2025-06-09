from pydantic import BaseModel


class PreprocessDBDistinctModel(BaseModel):
    oday: str
    route_id: str


class PreprocessBlobModel(BaseModel):
    blob_path: str
    oday: str
    route_id: str
    mode: str