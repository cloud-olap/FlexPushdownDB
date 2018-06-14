# -*- coding: utf-8 -*-
"""SQL function support

"""

from datetime import datetime

from s3filter.util.datetime_util import str_to_millis, dt_to_millis

timestamp = 'type_timestamp'


def cast(ex, t):
    if t == timestamp:
        if type(ex) == str:
            return str_to_millis(ex)
        elif type(ex) == datetime:
            return dt_to_millis(ex)
        else:
            raise Exception("Unrecognized type {}".format(t))
    else:
        raise Exception('Unrecognized type {}'.format(t))

