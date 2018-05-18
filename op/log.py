# -*- coding: utf-8 -*-
"""Logging support

"""
from op.operator_base import Operator


class Log(Operator):
    """This operator simply prints out each tuple, useful for debugging.

    """

    def __init__(self):
        """Constructs a new Log operator

        """
        Operator.__init__(self)

    # noinspection PyUnusedLocal
    def on_receive(self, t, producer):
        """Handles the event of receiving a new tuple from a producer. Will simply print the tuple.

        :param t: The received tuples
        :param producer: The producer of the tuple
        :return: None
        """

        print("Log | {}".format(t))
        self.send(t)
