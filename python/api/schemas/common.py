"""GeoJSON models"""

from typing import Dict, Generic, List, Literal, Optional, TypeVar

from pydantic import BaseModel, Field

GeoJSONPropertyModelType = TypeVar("GeoJSONPropertyModelType")
GeoJSONGeometryModelType = TypeVar("GeoJSONGeometryModelType")


class Geometry(BaseModel):
    type: str
    crs: Optional[Dict] = Field(
        title="Coordinate reference system",
        description="Coordinate reference system is an optional object. "
        "Usually it's given if the CRS is something else than WGS84 (EPSG:4326).",
        default=None,
    )
    coordinates: List


class PointGeometry(Geometry):
    type: Literal["Point"]
    coordinates: List[float] = Field(examples=[[24.92371, 60.17971]])


class PolygonGeometry(Geometry):
    type: Literal["Polygon"]
    coordinates: List[List[float]] = Field(
        examples=[[[384792, 6673806], [384678, 6673810], [384880, 6673826]]]
    )


class GeoJSONFeature(
    BaseModel, Generic[GeoJSONPropertyModelType, GeoJSONGeometryModelType]
):
    type: Literal["Feature"]
    geometry: GeoJSONGeometryModelType
    properties: GeoJSONPropertyModelType


class GeoJSONFeatureCollection(
    BaseModel, Generic[GeoJSONPropertyModelType, GeoJSONGeometryModelType]
):
    type: Literal["FeatureCollection"]
    features: List[GeoJSONFeature[GeoJSONPropertyModelType, GeoJSONGeometryModelType]]
