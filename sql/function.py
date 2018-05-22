# -*- coding: utf-8 -*-
"""SQL function support

"""
from util.datetime_util import str_to_millis

timestamp = 'type_timestamp'


def cast(v, t):
    if t == timestamp:
        return str_to_millis(v)
    else:
        raise Exception('Unrecognized type {}'.format(t))



