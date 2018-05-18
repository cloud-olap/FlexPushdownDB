# -*- coding: utf-8 -*-
"""Collation support

"""

from op.operator_base import Operator


class Collate(Operator):
    """This operator simply collects emitted tuples into a list. Useful for interrogating results at the end of a
    query to see if they are what is expected.

    """

    def __init__(self):
        """Constructs a new Collate operator.

        """

        Operator.__init__(self)
        self.__tuples = []

    def tuples(self):
        """Accessor for the collated tuples

        :return: The collated tuples
        """
        return self.__tuples

    # noinspection PyUnusedLocal
    def on_receive(self, t, producer):
        """Handles the event of receiving a new tuple from a producer. Will simply append the tuple to the internal
        list.

        :param t: The received tuples
        :param producer: The producer of the tuple
        :return: None
        """

        # print("Collate | {}".format(t))
        self.__tuples.append(t)
