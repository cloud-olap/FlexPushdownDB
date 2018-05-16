# -*- coding: utf-8 -*-
"""

"""
from op.operator_base import Operator


class Top(Operator):
    """Represents a top operator which consumes and produces tuples until it has reached a set maximum number of tuples.

    """
    def __init__(self, max_tuples):
        Operator.__init__(self)
        self.max_tuples = max_tuples
        self.current = 0

    def on_emit(self, t, producer=None):
        """Consumes tuples as they are produced. When the number of tuples reaches max it informs the producer to stop
        producing. This allows table scans to stop once enough tuples have been retrieved. It also informs any consumers
        that it is done producing tuples.

        :param t: The produced tuple.
        :param producer: The producer that emitted the tuple
        :return: None
        """
        # print("Top | {}".format(t))
        self.do_emit(t)
        self.current += 1
        if self.current == self.max_tuples:
            self.do_stop()
            self.do_done()

    def on_done(self):
        pass
