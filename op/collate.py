# -*- coding: utf-8 -*-
"""Collation support

"""
from metric.op_metrics import OpMetrics
from op.operator_base import Operator
from op.message import TupleMessage


class Collate(Operator):
    """This operator simply collects emitted tuples into a list. Useful for interrogating results at the end of a
    query to see if they are what is expected.

    """

    def __init__(self, name, log_enabled):
        """Constructs a new Collate operator.

        """

        super(Collate, self).__init__(name, OpMetrics(), log_enabled)

        self.__tuples = []

    def tuples(self):
        """Accessor for the collated tuples

        :return: The collated tuples
        """

        return self.__tuples

    def on_receive(self, m, _producer):
        """Handles the event of receiving a message from a producer. Will simply append the tuple to the internal
        list.

        :param m: The received message
        :param _producer: The producer of the tuple
        :return: None
        """

        # print("Collate | {}".format(t))
        if type(m) is TupleMessage:
            self.__on_receive_tuple(m.tuple_)
        else:
            raise Exception("Unrecognized message {}".format(m))

    def __on_receive_tuple(self, tuple_):
        """

        :param tuple_:
        :return:
        """

        self.__tuples.append(tuple_)
