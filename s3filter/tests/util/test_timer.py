# -*- coding: utf-8 -*-
"""Metric tests

"""

import time
from s3filter.util.timer import Timer


def test_timer_start():

    sleep_duration = 0.1

    t = Timer()

    t.start()
    time.sleep(sleep_duration)

    # print("Elapsed time {}".format(t.elapsed()))

    expected_elapsed_time = 1 * sleep_duration
    assert expected_elapsed_time - 0.1 < t.elapsed() < expected_elapsed_time + 0.1


def test_timer_start_stop():

    sleep_duration = 0.1

    t = Timer()

    t.start()
    time.sleep(sleep_duration)
    t.stop()
    time.sleep(sleep_duration)

    # print("Elapsed time {}".format(t.elapsed()))

    expected_elapsed_time = 1 * sleep_duration
    assert expected_elapsed_time - 0.1 < t.elapsed() < expected_elapsed_time + 0.1


def test_timer_start_stop_start():

    sleep_duration = 0.1

    t = Timer()

    t.start()
    time.sleep(sleep_duration)
    t.stop()
    time.sleep(sleep_duration)
    t.start()
    time.sleep(sleep_duration)

    # print("Elapsed time {}".format(t.elapsed()))

    expected_elapsed_time = 2 * sleep_duration
    assert expected_elapsed_time - 0.1 < t.elapsed() < expected_elapsed_time + 0.1


def test_timer_start_stop_start_stop():

    sleep_duration = 0.1

    t = Timer()

    t.start()
    time.sleep(sleep_duration)
    t.stop()
    time.sleep(sleep_duration)
    t.start()
    time.sleep(sleep_duration)
    t.stop()
    time.sleep(sleep_duration)

    # print("Elapsed time {}".format(t.elapsed()))

    expected_elapsed_time = 2 * sleep_duration
    assert expected_elapsed_time - 0.1 < t.elapsed() < expected_elapsed_time + 0.1


def test_timer_loop():

    num_sleeps = 100
    sleep_duration = 0.1

    t1 = Timer()
    t2 = Timer()

    t1.start()

    for i in range(0, num_sleeps):
        t2.start()
        time.sleep(sleep_duration)
        t2.stop()

    t1.stop()

    # print("Elapsed time {}".format(t1.elapsed()))
    # print("Elapsed time {}".format(t2.elapsed()))

    expected_elapsed_time = num_sleeps * sleep_duration
    assert expected_elapsed_time - 0.1 < t1.elapsed() < expected_elapsed_time + 0.1
    assert expected_elapsed_time - 0.1 < t2.elapsed() < expected_elapsed_time + 0.1
