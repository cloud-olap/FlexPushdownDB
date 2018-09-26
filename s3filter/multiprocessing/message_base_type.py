from enum import Enum


class MessageBaseType(Enum):
    start = 1
    stop = 2
    data = 3
    header = 4
    stopped = 5
    request_buffer = 7
    grant_buffer = 8
