from enum import Enum
from datetime import datetime, date, time

from pydantic import BaseModel, Field
from typing import Dict, List, Optional


# Enum for transport modes
class TransportModeEnum(str, Enum):
    bus = "bus"
    tram = "tram"
    train = "train"
    ferry = "ferry"
    metro = "metro"


# /monitored_vehicle_journeys response schema

class Journey(BaseModel):
    route_id: str = Field(title="Route ID", description="`route_id` in HFP topic", example="4611")
    direction_id: int = Field(title="Direction ID", description="`direction_id` in HFP topic", example="1")
    oday: date = Field(title="Operating day", description="`oday` in HFP payload", example="2023-03-18")
    start: time = Field(title="Start time of the journey", description="`start` in HFP payload", example="10:12:00")
    operator_id: int = Field(title="Operator owning the vehicle", description="`operator_id` in HFP topic", example=12)
    oper: int = Field(
        title="Operator running the journey", description="`oper` in HFP payload", example=12
    )
    vehicle_number: int = Field(title="Vehicle number", description="`vehicle_number` in HFP topic", example="114")
    transport_mode: TransportModeEnum = Field(
        title="The type of the vehicle", description="`transport_mode` in HFP topic", example="bus"
    )
    min_tst: datetime = Field(
        title="Minimum timestamp of events",
        description="The first timestamp of the events of the journey. (tst in HFP payload)",
        example="2023-03-18T18:26:00.000",
    )
    max_tst: datetime = Field(
        title="Maximum timestamp of events",
        description="The last timestamp of the events of the journey. (tst in HFP payload)",
        example="2023-03-18T19:03:00.000",
    )
    modified_at: datetime = Field(
        title="Last modification time",
        description="When the journey record was last updated (received new events)",
        example="2023-03-18T21:03:12.000",
    )


class JourneyData(BaseModel):
    monitoredVehichleJourneys: Optional[List[Journey]]


class JourneyResponse(BaseModel):
    data: JourneyData
    last_updated: Optional[datetime] = Field(
        title="Last update time of a operating day",
        description="The timestamp when the journeys of the operating day have been updated last time.",
    )
