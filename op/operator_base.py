# -*- coding: utf-8 -*-
"""Operator support

"""


def switch_context(from_op, to_op):
    """Handles a context switch from one operator to another. This is used to stop the sending operators
    timer and start the receiving operators timer.

    :param from_op: Operator switching context from
    :param to_op: Operator switching context to
    :return: None
    """

    from_op.op_metrics.timer_stop()
    to_op.op_metrics.timer_start()


class Operator(object):
    """Base class for an operator. An operator is a class that can receive tuples from other
    operators (a.k.a. producers) and send tuples to other operators (a.k.a. consumers).

    """

    def __init__(self, name, op_metrics, log_enabled=False):
        """Constructs a new operator

        """

        self.name = name

        self.op_metrics = op_metrics

        self.log_enabled = log_enabled

        self.producers = []
        self.consumers = []

        self.__completed = False

    def is_completed(self):
        """Accessor for completed status.

        :return: Boolean indicating whether the operator has completed or not.
        """
        return self.__completed

    def connect(self, consumer):
        """Utility method that appends the given consuming operators to this operators list of consumers and appends the
        given consumers producer to this operator. Shorthand for two add consumer, add producer calls.

        TODO: Not sure if this should be here, sometimes consuming operators don't need a ref to a producer or indeed
        may have multiple.

        :param consumer: An operator that will consume the results of this operator.
        :return: None
        """

        self.add_consumer(consumer)
        consumer.add_producer(self)

    def add_consumer(self, consumer):
        """Appends the given consuming operator to this operators list of consumers

        :param consumer: An operator that will consume the results of this operator.
        :return: None
        """

        if any(consumer.name == name for name in self.consumers):
            raise Exception("Consumer with name '{}' already added".format(consumer.name))

        self.consumers.append(consumer)

    def add_producer(self, producer):
        """Appends the given producing operator to this operators list of producers.

        :param producer: An operator that will produce the tuples for this operator.
        :return: None
        """
        if any(producer.name == name for name in self.producers):
            raise Exception("Producer with name '{}' already added".format(producer.name))

        self.producers.append(producer)

    def send(self, message, operators):
        """Emits the given tuple to each of the connected consumers.

        :param operators:
        :param message: The message to emit
        :return: None
        """

        for op in operators:
            self.fire_on_received(message, op)

    def fire_on_received(self, message, consumer):

        switch_context(self, consumer)

        consumer.on_receive(message, self)

        switch_context(consumer, self)

    def complete(self):
        """Sets the operator to complete, meaning it has completed what it needed to do. This includes marking the
        operator as completed and signalling to to connected operators that this operator has
        completed what it was doing.

        :return: None
        """

        if not self.is_completed():

            if self.log_enabled:
                print("{}('{}') | Completed".format(self.__class__.__name__, self.name))

            self.__completed = True

            for c in self.consumers:
                c.on_producer_completed(self)

            for p in self.producers:
                p.on_consumer_completed(self)

        else:
            raise Exception("Cannot complete an already completed operator")

    def on_producer_completed(self, producer):
        """Handles a signal from producing operators that they have completed what they needed to do. This is useful in
        circumstances where a producer has no more tuples to supply (such as completion of a table scan). This is often
        overridden but this default implementation simply completes this operator.

        :param producer: The producer that has completed
        :return: None
        """

        switch_context(producer, self)

        if not self.is_completed():
            self.complete()

        switch_context(self, producer)

    def on_consumer_completed(self, consumer):
        """Handles a signal from consuming operators that they have completed what they needed to do. This is useful in
        circumstances where a consumer needs no more tuples (such as a top operator reaching the number of tuples it
        needs). This is often overridden but this default implementation simply simply completes this operator.

        :param consumer: The consumer that has completed
        :return: None
        """

        switch_context(consumer, self)

        if not self.is_completed():
            self.complete()

        switch_context(self, consumer)

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, {'name': self.name})
