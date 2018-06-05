# -*- coding: utf-8 -*-
"""SQL function support

TODO: This can be tidied up, at least to remove the need to pass ctx around.

"""

import numbers
from datetime import datetime

from util.datetime_util import str_to_millis, dt_to_millis

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
