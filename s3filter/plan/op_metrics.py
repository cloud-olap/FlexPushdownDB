# -*- coding: utf-8 -*-
"""Metrio support

"""
from s3filter.util.timer import Timer


class OpMetrics(object):

    def __init__(self):
        self.__timer = Timer()

    def timer_start(self):
        self.__timer.start()

    def timer_stop(self):
        self.__timer.stop()

    def elapsed_time(self):
        return self.__timer.elapsed()

    def start_time(self):
        return self.__timer.start_time

    def timer_running(self):
        return self.__timer.running

    @staticmethod
    def print_metrics(op_list):
        for op in op_list:
            OpMetrics.pretty_print(op)

    @staticmethod
    def print_overall_metrics(op_list, name=None):
        total_elapsed = 0
        for op in op_list:
            total_elapsed += op.op_metrics.elapsed_time()
        if name is None:
            name = " + ".join([o.name for o in op_list])
        print('{}: {}'.format(total_elapsed, name))

    @staticmethod
    def pretty_print(op):
        print("{}: {}".format(op, op.op_metrics))

    def __repr__(self):
        return {'elapsed_time': round(self.elapsed_time(), 5)}.__repr__()
