# -*- coding: utf-8 -*-
"""

"""
from metric.op_metrics import OpMetrics
from op.operator_base import Operator
from op.message import TupleMessage


class Top(Operator):
    """Represents a top operator which consumes and produces tuples until it has reached a set maximum number of tuples.

    """
    def __init__(self, max_tuples, name, log_enabled):

        super(Top, self).__init__(name, OpMetrics(), log_enabled)

        self.max_tuples = max_tuples
        self.current = 0
        self.first_tuple = True

    def on_receive(self, m, _producer):
        """Consumes tuples as they are produced. When the number of tuples reaches max it informs the producer to stop
        producing. This allows table scans to stop once enough tuples have been retrieved. It also informs any consumers
        that it is done producing tuples.

        :param t: The produced tuple.
        :param _producer: The producer that emitted the tuple
        :return: None
        """

        # print("Top | {}".format(t))

        if type(m) is TupleMessage:

            self.on_receive_tuple(m.tuple_)

        else:
            raise Exception("Unrecognized message {}".format(m))

    def on_receive_tuple(self, tuple_):
        self.send(TupleMessage(tuple_), self.consumers)
        if not self.first_tuple:
            self.current += 1
        else:
            self.first_tuple = False
        if self.current == self.max_tuples:
            # Set this operator to complete
            self.complete()

