"""
Module to read values from env

How to add new env:
- Make sure new variable is defined as env variable
- Add the env as a new variable and call get_env function
- You can give default value for the env. If the env is required, leave the default field empty
- Add modifier function if the env should be converted to another type, e.g. for str to int.

How to use envs in the app:
- import variable like
`from common.config import MY_ENV`

"""

import os
import logging
from collections.abc import Callable
from typing import Union


def dummy(env: str) -> str:
    """Dummy function to be called if env should be used as str"""
    return env


def env_as_int(env: str) -> int:
    return int(env)


def env_as_float(env: str) -> float:
    return float(env)


def env_as_upper_str(env: str) -> str:
    return env.strip().upper()


def env_as_upper_str_list(env: str) -> list[str]:
    return [item.strip().upper() for item in filter(None, env.split(","))]


def env_as_int_list(env: str) -> list[int]:
    return [int(item.strip()) for item in filter(None, env.split(","))]


def env_as_float_list(env: str) -> list[float]:
    return [float(item.strip()) for item in filter(None, env.split(","))]


def get_env(var_name: str, default_value: Union[str, None] = None, modifier: Callable = dummy):
    """Function to read env and modify it with modifier function.
    Raises error if env is not available and default_value is not given."""
    env = os.getenv(var_name)
    if not env:
        if default_value is not None:
            env = default_value
            logging.warning(f"{var_name} not set in env, falling back to default value {default_value}")
        else:
            raise ValueError(f"{var_name} is required, but was not found in env.")
    return modifier(env)


# Environment, e.g., for loggers to inform
ENVIRONMENT: str = get_env("ENVIRONMENT", "LOCAL", modifier=env_as_upper_str)
BUILD_VERSION: str = get_env("BUILD_VERSION", "Build NaN")  # Azure pipeline will give this as a build argument.

# Envs for connections to Posgtres and Azure
APC_STORAGE_CONTAINER_NAME: str = get_env("APC_STORAGE_CONTAINER_NAME")
HFP_STORAGE_CONTAINER_NAME: str = get_env("HFP_STORAGE_CONTAINER_NAME")
HFP_STORAGE_CONNECTION_STRING: str = get_env("HFP_STORAGE_CONNECTION_STRING")
POSTGRES_CONNECTION_STRING: str = get_env("POSTGRES_CONNECTION_STRING")
FLOW_ANALYTICS_SAS_CONNECTION_STRING: str = get_env("FLOW_ANALYTICS_SAS_CONNECTION_STRING")
DURABLE_BASE_URL: str = get_env("DURABLE_BASE_URL")
AzureWebJobsStorage: str = get_env("AzureWebJobsStorage")

# Envs related to importing blob
HFP_EVENTS_TO_IMPORT: list[str] = get_env("HFP_EVENTS_TO_IMPORT", modifier=env_as_upper_str_list)
IMPORT_COVERAGE_DAYS: int = get_env("IMPORT_COVERAGE_DAYS", "2", modifier=env_as_int)

# Days to exclude from delay analysis
DAYS_TO_EXCLUDE: list[str] = get_env("DAYS_TO_EXCLUDE","",modifier=env_as_upper_str_list)

# Authentication str for docs.
DEFAULT_AUTH_CODE: str = get_env("DEFAULT_AUTH_CODE", "")

# Digitransit apikey for stop import
DIGITRANSIT_APIKEY: str = get_env("DIGITRANSIT_APIKEY", "")

# Envs for slack alerts
SLACK_WEBHOOK_URL: str = get_env("SLACK_WEBHOOK_URL", "")
SLACK_USERS_TO_ALERT: str = get_env("SLACK_USERS_TO_ALERT", "", modifier=env_as_upper_str_list)

# Envs for stop analysis
STOP_NEAR_LIMIT_M: float = get_env("STOP_NEAR_LIMIT_M", "50.0", modifier=env_as_float)
MIN_OBSERVATIONS_PER_STOP: int = get_env("MIN_OBSERVATIONS_PER_STOP", "10", modifier=env_as_int)
MAX_NULL_STOP_DIST_M: float = get_env("MAX_NULL_STOP_DIST_M", "100.0", modifier=env_as_float)
RADIUS_PERCENTILES: list[float] = get_env("RADIUS_PERCENTILES", "0.5,0.75,0.9,0.95", modifier=env_as_float_list)
MIN_RADIUS_PERCENTILES_TO_SUM: list[float] = get_env(
    "MIN_RADIUS_PERCENTILES_TO_SUM", "0.5,0.95", modifier=env_as_float_list
)
DEFAULT_MIN_RADIUS_M: float = get_env("DEFAULT_MIN_RADIUS_M", "20.0", modifier=env_as_float)
MANUAL_ACCEPTANCE_MIN_RADIUS_M: float = get_env("MANUAL_ACCEPTANCE_MIN_RADIUS_M", "40.0", modifier=env_as_float)
LARGE_SCATTER_PERCENTILE: float = get_env("LARGE_SCATTER_PERCENTILE", "0.9", modifier=env_as_float)
LARGE_SCATTER_RADIUS_M: float = get_env("LARGE_SCATTER_RADIUS_M", "10.0", modifier=env_as_float)
LARGE_JORE_DIST_M: float = get_env("LARGE_JORE_DIST_M", "25.0", modifier=env_as_float)
STOP_GUESSED_PERCENTAGE: float = get_env("STOP_GUESSED_PERCENTAGE", "0.05", modifier=env_as_float)
TERMINAL_IDS: list[int] = get_env(
    "TERMINAL_IDS", "1000001,1000015,2000002,2000003,2000212,4000011", modifier=env_as_int_list
)
