from enum import Enum


class MessageBaseType(Enum):
    start = 1
    stop = 2
    data = 3
    header = 4
    stopped = 5
    request_buffer = 7
    grant_buffer = 8
    tuple = 9
    producer_completed = 10
    consumer_completed = 11
    operator_completed = 12
    eval = 13
    evaluated = 14
    hash_table = 15
