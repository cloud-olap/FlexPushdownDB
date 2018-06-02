# -*- coding: utf-8 -*-
"""Timer support

"""

import timeit


class Timer(object):
    """Provides a stopwatch like timer that can be started and stopped and return the total elapsed time. Useful for
    profiling test runs.

    It's not super accurate, but as accurate as Python 2 will support (Python 3 provides greater accuracy).

    """

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

    def __repr__(self):
        return {'start_time': self.start_time, 'running_time': self.running_time, 'running': self.running}.__repr__()
