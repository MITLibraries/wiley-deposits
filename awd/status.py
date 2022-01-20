from enum import Enum


class Status(Enum):
    PROCESSING = 1
    SUCCESS = 2
    FAILED = 3
    PERMANENTLY_FAILED = 4
