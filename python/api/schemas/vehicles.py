import datetime
from typing import Generic, List, TypeVar

from pydantic import BaseModel, Field, PositiveInt, create_model

AnalysisModelType = TypeVar("AnalysisModelType")


class VehicleAnalysisErrorEvents(BaseModel):
    events: List = Field(description="List of event objects that triggered errors.")
    types: List = Field(description="List of error types found for vehicle.")


class VehicleAnalysisObject(BaseModel):
    vehicle_number: int = Field(description="Analyzed vehicle's number.")
    operator_id: int = Field(description="Operator id of the analyzed vehicle.")
    date: datetime.date = Field(description="Date of analyzed data for vehicle.")
    events_amount: PositiveInt = Field(
        description="Total amount of all analyzed events for the vehicle."
    )


class VehiclePositionAnalysisObject(VehicleAnalysisObject):
    loc_null_ratio: float = Field(
        description="The relative proportion of `null` values in `loc` fields of analyzed data.",
        ge=0,
        le=1,
    )
    loc_gps_ratio: float = Field(
        description="The relative proportion of `gps` values in `loc` fields of analyzed data.",
        ge=0,
        le=1,
    )
    loc_dr_ratio: float = Field(
        description="The relative proportion of `dr` values in `loc` fields of analyzed data.",
        ge=0,
        le=1,
    )
    loc_error_events: VehicleAnalysisErrorEvents


class VehicleDoorsAnalysisObject(VehicleAnalysisObject):
    drst_null_ratio: float = Field(
        description="The relative proportion of `null` values in `drst` fields of analyzed data.",
        ge=0,
        le=1,
    )
    drst_true_ratio: float = Field(
        description="The relative proportion of `true` values in `drst` fields of analyzed data.",
        ge=0,
        le=1,
    )
    drst_false_ratio: float = Field(
        description="The relative proportion of `false` values in `drst` fields of analyzed data.",
        ge=0,
        le=1,
    )
    door_error_events: VehicleAnalysisErrorEvents


class VehicleOdoAnalysisObject(VehicleAnalysisObject):
    odo_exists_ratio: float = Field(
        description="The relative proportion of non-null values in `odo` fields of analyzed data.",
        ge=0,
        le=1,
    )
    odo_null_ratio: float = Field(
        description="The relative proportion of `null` values in `odo` fields of analyzed data.",
        ge=0,
        le=1,
    )
    odo_error_events: VehicleAnalysisErrorEvents


class VehicleAnalysisMetadata(BaseModel):
    start: datetime.time = Field(description="Starting timestamp for analysis window.")
    end: datetime.time = Field(description="Ending timestamp for analysis window.")
    date: datetime.date = Field(description="Date for analysis.")


class VehicleAnalysisData(BaseModel, Generic[AnalysisModelType]):
    metadata: VehicleAnalysisMetadata
    vehicles: List[AnalysisModelType]


class VehicleAnalysis(BaseModel, Generic[AnalysisModelType]):
    data: VehicleAnalysisData[AnalysisModelType]


VehiclePositionAnalysis = create_model(
    "VehiclePositionAnalysis", __base__=VehicleAnalysis[VehiclePositionAnalysisObject]
)
VehicleDoorsAnalysis = create_model(
    "VehicleDoorsAnalysis", __base__=VehicleAnalysis[VehicleDoorsAnalysisObject]
)
VehicleOdoAnalysis = create_model(
    "VehicleOdoAnalysis", __base__=VehicleAnalysis[VehicleOdoAnalysisObject]
)
