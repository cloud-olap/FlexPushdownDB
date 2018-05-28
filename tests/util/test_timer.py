# -*- coding: utf-8 -*-
"""Metric tests

"""

import time
from util.timer import Timer


def test_timer_start():

    t = Timer()

    t.start()  # time[0], start = 0
    time.sleep(1)  # time[1]
    elapsed = t.elapsed()  # time[1], start = 0, elapsed = 1

    print("Elapsed time {}".format(elapsed))

    assert 0.9 < elapsed < 1.1


def test_timer_start_stop():

    t = Timer()

    t.start()  # time[0]
    time.sleep(1)  # time[1]
    t.stop()  # time[1]
    time.sleep(1)  # time[2]
    elapsed = t.elapsed()  # time[2]

    print("Elapsed time {}".format(elapsed))

    assert 0.9 < elapsed < 1.1


def test_timer_start_stop_start():

    t = Timer()

    t.start()
    time.sleep(1)
    t.stop()
    time.sleep(1)
    t.start()
    time.sleep(1)

    print("Elapsed time {}".format(t.elapsed()))

    assert 1.9 < t.elapsed() < 2.1


def test_timer_start_stop_start_stop():
    t = Timer()

    t.start()
    time.sleep(1)
    t.stop()
    time.sleep(1)
    t.start()
    time.sleep(1)
    t.stop()
    time.sleep(1)

    print("Elapsed time {}".format(t.elapsed()))

    assert 1.9 < t.elapsed() < 2.1





