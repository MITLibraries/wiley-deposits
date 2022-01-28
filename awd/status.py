from enum import Enum


class Status(Enum):
    UNPROCESSED = 1
    MESSAGE_SENT = 2
    SUCCESS = 3
    FAILED = 4
