from enum import Enum


class ReclusterStatus(Enum):
    CREATED = "CREATED"
    DONE = "DONE"
    FAILED = "FAILED"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    QUEUED = "QUEUED"
