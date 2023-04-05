from datetime import date
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, create_model

from .common import get_feature_collection_model


class JoreStop(BaseModel):
    stop_id: int = Field(title="Stop ID", description="Long stop ID", example="2222214")
    stop_code: str = Field(title="Stop code", description="Short stop ID", example="E2210")
    stop_name: str = Field(title="Stop name", description="Finnish name of the stop", example="Tekniikantie")
    parent_station: Optional[int] = Field(
        title="Parent station", description="JORE ID of the parent station / terminal, if exists.", example=1000001
    )
    stop_mode: str = Field(title="Stop mode", description="Type of vehicles using the stop", example="BUS")
    route_dirs_via_stop: List[str] = Field(
        title="Routes via the stop",
        description="Routes that stops on the stop. The field includes the direction. `<route_id>-<direction_id>`.",
        example=["1052-1", "2108N-2"],
    )
    date_imported: date = Field(
        title="Import date", description="The date when the stop was imported last time to Analytics."
    )


class PercendileRadii(BaseModel):
    percentile: float = Field(
        title="Percentile", description="0.0 to 1.0, percentage of observations that the radius encloses.", example=0.5
    )
    radius_m: float = Field(title="Radius", description="Radius size in meters.", example=5.764)
    n_observations: int = Field(
        title="Observation count", description="Number of observations that the radius encloses.", default=340
    )


class StopMedian(BaseModel):
    stop_id: int = Field(title="Stop ID", description="Long stop ID", example="2222214")
    from_date: date = Field(
        title="Min date of observations",
        description="Minimum date of the observations of the stop used in analysis.",
        example="2023-03-02",
    )
    n_stop_known: int = Field(
        title="Number of observations with stop_id from HFP/LIJ",
        description="N observations with stop_id from HFP/LIJ.",
        example=312,
    )
    n_stop_guessed: int = Field(
        title="Number of observations with stop_id analyzed",
        description="N observations with stop_id guessed by the analysis process.",
        example=23,
    )
    n_stop_null_near: int = Field(
        title="Number of observations with no stop_id close to the stop",
        description="N observations with NULL stop_id closer than STOP_NEAR_LIMIT_M to the stop.",
        example=18,
    )
    dist_to_jore_point_m: float = Field(
        title="Distance to JORE",
        description="Distance from observed stop median to JORE point geometry in meters.",
        example=4.776,
    )
    observation_route_dirs: List[str] = Field(
        title="Routes observed on the stop",
        description="Routes that have observed to use the stop and used for analysis. "
        "The field includes the direction. `<route_id>-<direction_id>`.",
        example=["1052-1", "2108N-2"],
    )
    result_class: str = Field(
        title="Result class", description="Result class for reporting.", example="Tarkista (ratikka)"
    )
    recommended_min_radius_m: float = Field(
        title="Recommended radius", description="Recommended minimum stop radius for Jore in meters.", example=18.998
    )
    manual_acceptance_needed: bool = Field(
        title="Manual acceptance needed",
        description="If true, the reported result needs manual inspection and acceptance.",
        example=False,
    )
    percentile_radii_list: List[PercendileRadii] = Field(
        title="Percentile radii list",
        description="List of radiis around `stop_median` containing a given percentage of observations",
    )


class StopMedianPercentile(BaseModel):
    stop_id: int = Field(title="Stop ID", description="Long stop ID", example="2222214")
    percentile: float = Field(title="Percentile", description="Percentile value.", example=0.75)
    radius_m: float = Field(
        title="Radius of the percentile", description="Radius of the percentile in meters.", example=4.223
    )
    n_observations: int = Field(
        title="Number of observations", description="Number of observations covered by the percentile", example=4334
    )


class HFPStopPoint(BaseModel):
    stop_id: int = Field(title="Stop ID", description="Long stop ID", example="2222214")
    stop_id_guessed: bool = Field(
        title="Stop ID is guessed",
        description="Boolean value wheather the stop id was found on HFP payload or guessed",
        example=False,
    )
    event: str = Field(
        title="HFP event type", description="Event type of the observation. Either `DOO` or `DOC`", example="DOO"
    )
    dist_to_jore_point_m: float = Field(
        title="Distance to JORE",
        description="Distance from observation to JORE point geometry in meters.",
        example=4.776,
    )
    dist_to_median_point_m: float = Field(
        title="Distance to median",
        description="Distance from observation to observed stop median in meters.",
        example=2.556,
    )


JoreStopFeatureCollection = get_feature_collection_model("JoreStop", JoreStop)
StopMedianFeatureCollection = get_feature_collection_model("StopMedian", StopMedian)
StopMedianPercentileFeatureCollection = get_feature_collection_model(
    "StopMedianPercentile", StopMedianPercentile, "Polygon"
)
HFPStopPointFeatureCollection = get_feature_collection_model("HFPStopPoint", HFPStopPoint)
