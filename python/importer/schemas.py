import json
from typing import Callable, Optional, TypedDict  # TODO change optional to NotRequired after Python 3.11
from psycopg.sql import SQL


class SchemaFields(TypedDict):
    # These fields will be imported. Keys are from csv, values are db columns.
    # Order is guaranteed, so they are used with .keys() and .values() -methods
    mapping: dict[str, str]
    required: list[str]  # These are used for unique index
    modifier_function: Optional[Callable[[dict], dict]]  # Function to map new fields based on others


class StagingScripts(TypedDict):
    process: SQL  # Move data from staging to permanent storage
    process_invalid: Optional[SQL]  # Import script for invalid data


class StagingTarget(TypedDict):
    schema: str
    table: str


class DBSchema(TypedDict):
    copy_target: StagingTarget
    fields: SchemaFields
    scripts: StagingScripts


def apc_row_modifier(row: dict) -> dict:
    topic = row.get("topic")
    if topic:
        parts = topic.split("/")  # format /hfp/v2/journey/ongoing/apc/bus/0012/02210'
        row["mode"] = parts[6]
        row["operator_id"] = parts[7]

    door_counts = row.get("door_counts")
    if door_counts:
        row["door_counts"] = json.dumps(door_counts)  # Door counts is json, that should be inserted as text

    # Convert dir and countquality which are read as byte format
    dir = row.get("dir")
    if dir:
        row["dir"] = int(dir)
    count_quality = row.get("count_quality")
    if count_quality:
        row["count_quality"] = count_quality.decode()

    return row


APC: DBSchema = {
    "copy_target": {
        "schema": "staging",
        "table": "apc_raw",
    },
    "fields": {
        "mapping": {
            "tst": "point_timestamp",
            "received_at": "received_at",
            "operator_id": "vehicle_operator_id",
            "veh": "vehicle_number",
            "mode": "transport_mode",
            "route": "route_id",
            "dir": "direction_id",
            "oday": "oday",
            "start": "start",
            "oper": "observed_operator_id",
            "stop": "stop",
            "vehicle_load": "vehicle_load",
            "vehicle_load_ratio": "vehicle_load_ratio",
            "door_counts": "doors_data",
            "count_quality": "count_quality",
            "long": "longitude",
            "lat": "latitude",
        },
        "required": ["tst", "oper", "veh"],
        "modifier_function": apc_row_modifier,
    },
    "scripts": {
        "process": SQL("CALL staging.import_and_normalize_apc()"),
        "process_invalid": None,
    },
}

HFP: DBSchema = {
    "copy_target": {
        "schema": "staging",
        "table": "hfp_raw",
    },
    "fields": {
        "mapping": {
            "tst": "tst",
            "eventType": "event_type",
            "receivedAt": "received_at",
            "ownerOperatorId": "vehicle_operator_id",
            "vehicleNumber": "vehicle_number",
            "mode": "transport_mode",
            "routeId": "route_id",
            "dir": "direction_id",
            "oday": "oday",
            "start": "start",
            "oper": "observed_operator_id",
            "odo": "odo",
            "spd": "spd",
            "drst": "drst",
            "locationQualityMethod": "loc",
            "stop": "stop",
            "longitude": "longitude",
            "latitude": "latitude",
        },
        "required": ["tst", "oper", "vehicleNumber"],
        "modifier_function": None,
    },
    "scripts": {
        "process": SQL("CALL staging.import_and_normalize_hfp()"),
        "process_invalid": SQL("CALL staging.import_invalid_hfp()"),
    },
}

TLP: DBSchema = {
    "copy_target": {
        "schema": "staging",
        "table": "tlp_raw",
    },
    "fields": {
        "mapping": {
            "eventType": "event_type",
            "latitude": "latitude",
            "locationQualityMethod": "location_quality_method",
            "longitude": "longitude",
            "oday": "oday",
            "oper": "oper",
            "receivedAt": "received_at",
            "dir": "direction_id",
            "routeId": "route_id",
            "sid": "sid",
            "signalGroupId": "signal_group_id",
            "start": "start",
            "tlpAttSeq": "tlp_att_seq",
            "tlpDecision": "tlp_decision",
            "tlpPriorityLevel": "tlp_priority_level",
            "tlpReason": "tlp_reason",
            "tlpRequestType": "tlp_request_type",
            "tlpSignalGroupNbr": "tlp_signal_group_nbr",
            "tst": "point_timestamp",
            "vehicleNumber": "vehicle_number",
        },
        "required": ["tst", "oper", "vehicleNumber"],
        "modifier_function": None,
    },
    "scripts": {
        "process": SQL("CALL staging.import_and_normalize_tlp()"),
        "process_invalid": None,
    },
}