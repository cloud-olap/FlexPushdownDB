# -*- coding: utf-8 -*-
"""Operator support

"""
# import cProfile
import multiprocessing
# import threading
import traceback

import dill


def switch_context(from_op, to_op):
    """Handles a context switch from one operator to another. This is used to stop the sending operators
    timer and start the receiving operators timer.

    :param from_op: Operator switching context from
    :param to_op: Operator switching context to
    :return: None
    """

    if not from_op.op_metrics.timer_running():
        raise Exception("Illegal context switch. "
                        "Attempted to switch from operator '{}' with stopped timer"
                        .format(from_op.name))

    if to_op.op_metrics.timer_running():
        raise Exception("Illegal context switch. "
                        "Attempted to switch to operator '{}' with running timer"
                        .format(from_op.name))

    from_op.op_metrics.timer_stop()
    to_op.op_metrics.timer_start()


class StartMessage(object):
    pass


class StopMessage(object):
    pass


class ProducerCompletedMessage(object):

    def __init__(self, producer_name):
        self.producer_name = producer_name


class ConsumerCompletedMessage(object):

    def __init__(self, consumer_name):
        self.consumer_name = consumer_name


class OperatorCompletedMessage(object):

    def __init__(self, name):
        self.name = name


class EvalMessage(object):

    def __init__(self, expr):
        self.expr = expr


class EvaluatedMessage(object):

    def __init__(self, val):
        self.val = val


class Operator(object):
    """Base class for an operator. An operator is a class that can receive tuples from other
    operators (a.k.a. producers) and send tuples to other operators (a.k.a. consumers).

    """

    def work(self, queue):

        try:
            while True:

                self.op_metrics.timer_stop()
                pickled_item = queue.get()
                self.op_metrics.timer_start()

                item = dill.loads(pickled_item)

                # print(item)

                if type(item) == StartMessage:
                    self.run()
                elif type(item) == StopMessage:
                    break
                elif type(item) == ProducerCompletedMessage:
                    self.on_producer_completed(item.producer_name)
                elif type(item) == ConsumerCompletedMessage:
                    self.on_consumer_completed(item.consumer_name)
                elif type(item) == EvalMessage:
                    evaluated = eval(item.expr)
                    self.completion_queue.put(dill.dumps(EvaluatedMessage(evaluated)))
                else:
                    message = item[0]
                    sender = item[1]
                    self.on_receive(message, sender)

        except BaseException as e:
            tb = traceback.format_exc(e)
            print(tb)
            self.exception = e

    def run(self):
        """Abstract method for running the execution of this operator. This is different from the start method which
        is for sending a start message to this process.

        Only active operators need to implement this, reactive operators (ones that simply listen for tuples are never
        "run")

        :return: None
        """

        pass

    def start(self):
        """Sends a start message to this operators process or calls run if the operator is not async

        :return: None
        """

        if self.async_:
            m = dill.dumps(StartMessage())
            self.queue.put(m)
        else:
            self.run()

    def __init__(self, name, op_metrics, log_enabled=False):
        """Constructs a new operator

        """

        self.name = name

        self.op_metrics = op_metrics

        self.log_enabled = log_enabled

        self.producers = []
        self.consumers = []

        self.exception = None

        # TODO: Tidy up how completions are handled
        self.producer_completions = {}
        for p in self.producers:
            self.producer_completions[p.name] = False

        self.consumer_completions = {}
        for p in self.producers:
            self.consumer_completions[p.name] = False

        self.__completed = False

        self.__buffer = []

        self.is_streamed = True

        self.async_ = False
        self.runner = None
        self.queue = None
        self.completion_queue = None

        self.is_profiled = False
        self.profile_file_name = None

    def init_async(self, completion_queue):

        self.async_ = True

        self.queue = multiprocessing.Queue()

        self.completion_queue = completion_queue

        # self.runner = threading.Thread(target=self.work, args=(self.queue, ))

        self.runner = multiprocessing.Process(target=self.work, args=(self.queue, ))

        self.runner.start()

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
        self.consumers = sorted(self.consumers, key=lambda c: c.name)

    def add_producer(self, producer):
        """Appends the given producing operator to this operators list of producers.

        :param producer: An operator that will produce the tuples for this operator.
        :return: None
        """
        if any(producer.name == name for name in self.producers):
            raise Exception("Producer with name '{}' already added".format(producer.name))

        self.producers.append(producer)
        self.producers = sorted(self.producers, key=lambda p: p.name)

    def join(self):
        if self.async_:
            self.runner.join()
        else:
            pass

    def send(self, message, operators):
        """Emits the given tuple to each of the connected consumers.

        :param operators:
        :param message: The message to emit
        :return: None
        """

        if self.is_streamed:

            if len(operators) == 0:
                raise Exception("Producer {} has 0 consumers. Cannot send message to 0 consumers.".format(self.name))

            for op in operators:
                if op.async_:
                    pickled_item = dill.dumps([message, self.name])
                    op.queue.put(pickled_item)
                else:
                    self.fire_on_receive(message, op)
        else:
            self.__buffer.append(message)

    def fire_on_receive(self, message, consumer):
        switch_context(self, consumer)
        consumer.on_receive(message, self.name)
        switch_context(consumer, self)

    def on_receive(self, message, producer_name):
        raise NotImplementedError

    def batch_fire_on_receive(self, messages, consumer):

        def iterate_messages():

            switch_context(self, consumer)
            for m_ in messages:
                consumer.on_receive(m_, self.name)
            switch_context(consumer, self)

        if not consumer.is_profiled:
            iterate_messages()
        else:
            pass
            # cProfile.runctx('iterate_messages()', globals(), locals(), consumer.profile_file_name)

    def fire_on_producer_completed(self, consumer):
        switch_context(self, consumer)
        consumer.on_producer_completed(self.name)
        switch_context(consumer, self)

    def fire_on_consumer_completed(self, producer):
        switch_context(self, producer)
        producer.on_consumer_completed(self.name)
        switch_context(producer, self)

    def complete(self):
        """Sets the operator to complete, meaning it has completed what it needed to do. This includes marking the
        operator as completed and signalling to to connected operators that this operator has
        completed what it was doing.

        :return: None
        """

        if not self.is_completed():

            if self.log_enabled:
                print("{}('{}') | Completed".format(self.__class__.__name__, self.name))

            if self.async_:
                self.completion_queue.put(dill.dumps(OperatorCompletedMessage(self.name)))

            self.__completed = True

            if not self.is_streamed:
                for c in self.consumers:
                    self.batch_fire_on_receive(self.__buffer, c)

                del self.__buffer

            for p in self.producers:
                if p.async_:
                    p.queue.put(dill.dumps(ConsumerCompletedMessage(self.name)))
                else:
                    self.fire_on_consumer_completed(p)

            for c in self.consumers:
                if c.async_:
                    c.queue.put(dill.dumps(ProducerCompletedMessage(self.name)))
                else:
                    self.fire_on_producer_completed(c)

        else:
            raise Exception("Cannot complete an already completed operator")

    def set_streamed(self, is_streamed):
        self.is_streamed = is_streamed

    def set_profiled(self, is_profiled, profile_file_name=None):
        self.is_profiled = is_profiled
        self.profile_file_name = profile_file_name

    def on_producer_completed(self, producer_name):
        """Handles a signal from producing operators that they have completed what they needed to do. This is useful in
        circumstances where a producer has no more tuples to supply (such as completion of a table scan). This is often
        overridden but this default implementation simply completes this operator.

        :param producer_name: The producer that has completed
        :return: None
        """

        self.producer_completions[producer_name] = True

        # Check if all consumers are completed
        if all(self.producer_completions.values()):
            if not self.is_completed():
                self.complete()

    def on_consumer_completed(self, consumer_name):
        """Handles a signal from consuming operators that they have completed what they needed to do. This is useful in
        circumstances where a consumer needs no more tuples (such as a top operator reaching the number of tuples it
        needs). This is often overridden but this default implementation simply simply completes this operator.

        :param consumer_name: The consumer that has completed
        :return: None
        """

        self.consumer_completions[consumer_name] = True

        # Check if all consumers are completed
        if all(self.consumer_completions.values()):
            if not self.is_completed():
                self.complete()

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, {'name': self.name})
