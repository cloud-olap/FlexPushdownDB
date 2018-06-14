# -*- coding: utf-8 -*-
"""Metric tests

"""

from s3filter.util import datetime_util
from s3filter.util.timer import Timer


def test_datetime_epoch():
    dt = "1970-01-01"
    m = datetime_util.str_to_millis(dt)

    assert m == 0


def test_datetime_epoch_plus_1_day():
    dt = "1970-01-02"
    m = datetime_util.str_to_millis(dt)

    assert m == 86400000


def test_datetime_bench_numpy():
    dt = "1970-01-02"

    timer = Timer()
    timer.start()

    for i in range(0, 100000):
        datetime_util.str_to_millis(dt)

    print(timer.elapsed())


def test_datetime_bench_py():
    dt = "1970-01-02"

    timer = Timer()
    timer.start()

    for i in range(0, 100000):
        datetime_util.py_str_to_millis(dt)

    print(timer.elapsed())
