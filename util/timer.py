# -*- coding: utf-8 -*-
"""

"""
import timeit


class Timer(object):

    def __init__(self):
        self.start_time = None
        self.running_time = 0
        self.running = False

    def start(self):
        if not self.running:
            self.start_time = timeit.default_timer()
            self.running = True

    def stop(self):
        if self.running:
            self.running_time = (timeit.default_timer() - self.start_time) + self.running_time
            self.running = False

    def elapsed(self):
        if self.running:
            return (timeit.default_timer() - self.start_time) + self.running_time
        else:
            return self.running_time
