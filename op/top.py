# -*- coding: utf-8 -*-
"""

"""
from op.operator import Operator


class Top(Operator):
    """Represents a top operator which consumes and produces tuples until it has reached a set maximum number of tuples.

    """
    def __init__(self, max):
        Operator.__init__(self)
        self.max = max
        self.current = 0
        self.producer = None

    def set_producer(self, operator):
        self.producer = operator

    def set_consumer(self, operator):
        self.consumer = operator

    def emit(self, t):
        """Consumes tuples as they are produced. When the number of tuples reaches max it informs the producer to stop
        producing. This allows table scans to stop once enough tuples have been retrieved. It also informs any consumers
        that it is done producing tuples.

        :param t: The produced tuple.
        :return: None
        """
        # print("Top | {}".format(t))
        self.consumer.emit(t)
        self.current += 1
        if self.current == self.max:
            self.producer.stop()
            self.consumer.done()

    def done(self):
        pass
