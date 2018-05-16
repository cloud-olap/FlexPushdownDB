# -*- coding: utf-8 -*-
"""

"""


class Operator(object):
    """Base class for an operator. An operator is a class that can receive tuples from other
    operators (a.k.a. producers) and send tuples to other operators (a.k.a. consumers).

    """

    def __init__(self):
        self.producers = []
        self.consumers = []

    def connect(self, consumer):
        """Utility method that appends the given consuming operators to this operators list of consumers and appends the
        given consumers producer to this operator. Shorthand for two add consumer, add producer calls.

        TODO: Not sure if this should be here, sometimes consuming operators don't need a ref to a producer or indeed
        may have multiple.

        :param consumer: An operator that will consume the results of this operator.
        :return: None
        """
        self.consumers.append(consumer)
        consumer.producers.append(self)

    def add_consumer(self, consumer):
        """Appends the given consuming operator to this operators list of consumers

        :param consumer: An operator that will consume the results of this operator.
        :return: None
        """
        self.consumers.append(consumer)

    def add_producer(self, producer):
        """Appends the given producing operator to this operators list of producers.

        :param producer: An operator that will produce the tuples for this operator.
        :return: None
        """
        self.producers.append(producer)

    def do_emit(self, t):
        """Emits the given tuple to each of the connected consuming operators.

        :param t: The tuple to emit
        :return: None
        """
        for c in self.consumers:
            c.on_emit(t, self)

    def do_done(self):
        """ Signals to consuming operators that this operator has completed what it was doing.

        :return: None
        """
        for c in self.consumers:
            c.on_done()

    def do_stop(self):
        """ Signals to producing operators that this operator needs no more tuples.

        :return: None
        """
        for p in self.producers:
            p.on_stop()
