from datetime import date, time
from enum import Enum
from typing import Generic, List, Literal, Optional, Type, TypeVar

from pydantic import BaseModel, Field, PositiveFloat, PositiveInt, create_model

AnalysisModelType = TypeVar("AnalysisModelType")


class VehicleAnalysisErrorEvents(BaseModel):
    events: List
    types: List


class VehicleAnalysisObject(BaseModel):
    vehicle_number: int
    operator_id: int
    date: date
    events_amount: PositiveInt


class VehiclePositionAnalysisObject(VehicleAnalysisObject):
    loc_null_ratio: float
    loc_gps_ratio: float
    loc_dr_ratio: float
    loc_error_events: VehicleAnalysisErrorEvents


class VehicleDoorsAnalysisObject(VehicleAnalysisObject):
    drst_null_ratio: float
    drst_true_ratio: float
    drst_false_ratio: float
    door_error_events: VehicleAnalysisErrorEvents


class VehicleOdoAnalysisObject(VehicleAnalysisObject):
    odo_exists_ratio: float
    odo_null_ratio: float
    odo_error_events: VehicleAnalysisErrorEvents


class VehicleAnalysisMetadata(BaseModel):
    start: time
    end: time
    date: date


class VehicleAnalysisData(BaseModel, Generic[AnalysisModelType]):
    metadata: VehicleAnalysisMetadata
    vehicles: List[AnalysisModelType]


class VehicleAnalysis(BaseModel, Generic[AnalysisModelType]):
    data: VehicleAnalysisData[AnalysisModelType]


VehiclePositionAnalysis = create_model(
    "VehiclePositionAnalysis", __base__=VehicleAnalysis[VehiclePositionAnalysisObject]
)
VehicleDoorsAnalysis = create_model("VehicleDoorsAnalysis", __base__=VehicleAnalysis[VehicleDoorsAnalysisObject])
VehicleOdoAnalysis = create_model("VehicleOdoAnalysis", __base__=VehicleAnalysis[VehicleOdoAnalysisObject])
