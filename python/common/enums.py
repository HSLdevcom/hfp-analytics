from enum import Enum

class ReclusterStatus(Enum):
    CREATED = "CREATED"
    DONE = "DONE"
    FAILED = "FAILED"
    RUNNING = "RUNNING"
    QUEUED = "QUEUED"
