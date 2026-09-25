"""Test configuration.

common.config reads its values from env at import time and raises if a
required one is missing, so dummy values are set before any api/common module
gets imported.
"""

import os

REQUIRED_ENVS = {
    "APC_STORAGE_CONTAINER_NAME": "test",
    "HFP_STORAGE_CONTAINER_NAME": "test",
    "HFP_STORAGE_CONNECTION_STRING": "test",
    "POSTGRES_CONNECTION_STRING": "postgresql://test:test@localhost:5432/test",
    "DURABLE_BASE_URL": "http://localhost",
    "AzureWebJobsStorage": "test",
    "HFP_EVENTS_TO_IMPORT": "DOO,DOC",
}

for name, value in REQUIRED_ENVS.items():
    os.environ.setdefault(name, value)
