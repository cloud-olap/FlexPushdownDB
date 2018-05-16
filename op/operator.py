# -*- coding: utf-8 -*-
"""

"""


class Operator(object):
    """Base class for an operator. An operator is a class that can receive tuples from other
    operators (a.k.a. producers) and send tuples to other operators (a.k.a. receivers).

    """

    def __init__(self):
        self.consumers = []

    def connect(self, consumer):
        """Utility method that appends the given consuming operators to this operators list of consumers and sets the
        given consumers producer to this operator. Shorthand for two set_consumer, set_producer method calls.

        TODO: Not sure if this should be here, sometimes consuming operators don't need a ref to a producer or indeed
        may have multiple.

        :param consumer: An operator that will consume the results of this operator.
        :return:
        """
        self.consumers.append(consumer)
        consumer.producer = self

    def set_consumer(self, consumer):
        """Appends the given consuming operators to this operators list of consumers

        :param consumer: An operator that will consume the results of this operator.
        :return:
        """
        self.consumers.append(consumer)

    def do_emit(self, t):
        """Emits the given tuple to each of the connected consuming operators.

        :param t: The tuple to emit
        :return: None
        """
        for c in self.consumers:
            c.emit(t, self)

    def do_done(self):
        """ Signals to consuming operators that this operator has completed what it was doing.

        :return: None
        """
        for c in self.consumers:
            c.done()
