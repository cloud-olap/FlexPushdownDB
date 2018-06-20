# -*- coding: utf-8 -*-
"""
Datetime utility functions

"""

from datetime import datetime
import numpy


def py_str_to_millis(dt_str):
    dt = datetime.strptime(dt_str, '%Y-%m-%d')
    return dt_to_millis(dt)


def py_dt_to_millis(dt):
    d = dt.strftime('%s.%f')
    return int(float(d) * 1000)


def str_to_millis(dt_str):

    ndt = numpy.datetime64(dt_str, 'ms')
    return ndt.astype('int')


def dt_to_millis(dt):
    ndt = numpy.datetime64(dt, 'ms')
    return ndt.astype('int')


def millis_to_str(millis):
    dt = numpy.datetime64(millis, 'ms')
    py_dt = dt.astype(datetime)
    return py_dt.strftime('%Y-%m-%d')
