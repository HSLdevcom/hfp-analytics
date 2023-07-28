"""GeoJSON models"""
from typing import Dict, List, Literal, Optional, Type
from pydantic import BaseModel, Field, create_model


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
    coordinates: List[List[float]] = Field(examples=[[[384792, 6673806], [384678, 6673810], [384880, 6673826]]])


class GeoJSONFeature(BaseModel):
    type: Literal["Feature"]
    geometry: Geometry
    properties: Dict


class GeoJSONFeatureCollection(BaseModel):
    type: Literal["FeatureCollection"]
    features: List[GeoJSONFeature]


def get_feature_model(
    name: str, props_model: Type[BaseModel], geometry_type: Literal["Point", "Polygon"] = "Point"
) -> Type[BaseModel]:
    """Helper function to dynamically create new GeoJSON feature models"""
    if geometry_type == "Point":
        geometry_model = PointGeometry
    elif geometry_type == "Polygon":
        geometry_model = PolygonGeometry

    return create_model(
        f"{name}Feature", geometry=(geometry_model, ...), properties=(props_model, ...), __base__=GeoJSONFeature
    )


def get_feature_collection_model(
    name: str, props_model: Type[BaseModel], geometry_type: Literal["Point", "Polygon"] = "Point"
) -> Type[BaseModel]:
    """Helper function to dynamically create new GeoJSON feature collection models"""
    feature_model = get_feature_model(name, props_model, geometry_type)
    return create_model(
        f"{name}FeatureCollection", features=(List[feature_model], ...), __base__=GeoJSONFeatureCollection
    )
