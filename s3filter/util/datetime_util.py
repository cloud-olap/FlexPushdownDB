# -*- coding: utf-8 -*-
"""
Datetime utility functions

"""

from datetime import datetime


def str_to_millis(dt_str):
    dt = datetime.strptime(dt_str, '%Y-%m-%d')
    return dt_to_millis(dt)


def dt_to_millis(dt):
    d = dt.strftime('%s.%f')
    return int(float(d) * 1000)
