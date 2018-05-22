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
        self.first_tuple = True

    # noinspection PyUnusedLocal
    def on_receive(self, t, producer):
        """Consumes tuples as they are produced. When the number of tuples reaches max it informs the producer to stop
        producing. This allows table scans to stop once enough tuples have been retrieved. It also informs any consumers
        that it is done producing tuples.

        :param t: The produced tuple.
        :param producer: The producer that emitted the tuple
        :return: None
        """

        # print("Top | {}".format(t))

        self.send(t)

        if not self.first_tuple:
            self.current += 1
        else:
            self.first_tuple = False

        if self.current == self.max_tuples:

            # Set this operator to complete
            self.complete()

