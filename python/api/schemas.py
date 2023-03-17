from enum import Enum
from datetime import datetime, date, time

from pydantic import BaseModel, Field
from typing import Dict, List, Union


class TransportMode(str, Enum):
    bus = "bus"
    tram = "tram"
    train = "train"
    ferry = "ferry"
    metro = "metro"


RouteID = Field(title="Route ID", example="4611")


class Journey(BaseModel):
    route_id: str = RouteID
    direction_id: int = Field(title="Direction ID", example=1)
    oday: date = Field(title="Operating day", example="2023-03-18")
    start_24h: time = Field(title="Start time of journey", example="10:18:00")
    operator_id: int = Field(title="Operator of a journey", example=12)
    vehicle_operator_id: int = Field(title="Operator of a vehicle driving on a journey", example=12)
    vehicle_number: int = Field(title="Number of a vehicle", example="114")
    transport_mode: TransportMode = Field(title="The type of the vehicle", example="bus")
    min_timestamp: datetime = Field(title="Minimum timestamp of events", description="The first timestamp when the record", example="2023-03-18T18:26:00.000")
    max_timestamp: datetime = Field(title="Maximum timestamp of events", example="2023-03-18T19:03:00.000")
    modified_at: datetime = Field(title="Timestamp of the last modification time of a vehicle on HFP analytics", example="2023-03-18T21:03:12.000")


class JourneyData(BaseModel):
    monitoredVehichleJourneys: Union[List[Journey], None]


class JourneyResponse(BaseModel):
    data: JourneyData
    last_updated: Union[datetime, None]
