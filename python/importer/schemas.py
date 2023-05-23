from typing import Optional, TypedDict
from psycopg.sql import SQL


class SchemaFields(TypedDict):
    # These fields will be imported. Keys are from csv, values are db columns.
    # Order is guaranteed, so they are used with .keys() and .values() -methods
    mapping: dict[str, str]
    required: list[str]  # These are used for unique index


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


APC: DBSchema = {
    "copy_target": {
        "schema": "staging",
        "table": "apc_raw",
    },
    "fields": {
        "mapping": {
            "point_timestamp": "point_timestamp",
            "receivedAt": "received_at",
            "ownerOperatorId": "vehicle_operator_id",
            "vehicleNumber": "vehicle_number",
            "mode": "transport_mode",
            "routeId": "route_id",
            "dir": "direction_id",
            "oday": "oday",
            "start": "start",
            "oper": "observed_operator_id",
            "stop": "stop",
            "vehicle_load": "vehicle_load",
            "vehicle_load_ratio": "vehicle_load_ratio",
            "doors_data": "doors_data",
            "count_quality": "count_quality",
            "longitude": "longitude",
            "latitude": "latitude",
        },
        "required": ["point_timestamp", "oper", "vehicleNumber"],
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
    },
    "scripts": {
        "process": SQL("CALL staging.import_and_normalize_hfp()"),
        "process_invalid": SQL("CALL staging.import_invalid_hfp()"),
    },
}
