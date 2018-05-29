# -*- coding: utf-8 -*-
"""Join support

"""
from metric.op_metrics import OpMetrics
from op.operator_base import Operator
from op.message import TupleMessage
from op.tuple import Tuple


class JoinExpression(object):
    """Represents a join expression, as in the table name (key) and column name (field) to join on.

    """

    def __init__(self, l_name, l_field, r_name, r_field):
        """Creates a new join expression.

        :param l_key: Key of the producer of the left tuple to join on
        :param l_field: Field of the left tuple to join on
        :param r_key: Key of the producer of the right tuple to join on
        :param r_field: Field of the right tuple to join on
        """

        self.l_name = l_name
        self.l_field = l_field
        self.r_name = r_name
        self.r_field = r_field


class Join(Operator):
    """Implements a join using nested loops.

    """

    def __init__(self, join_expr, name, log_enabled):
        """
        Creates a new join operator.

        :param join_expr: The join expression indicating which fields of which key to join on
        """

        super(Join, self).__init__(name, OpMetrics(), log_enabled)

        self.join_expr = join_expr

        self.l_tuples = []
        self.r_tuples = []

        # Dict of field names indexed by producer key
        self.field_names = {}

        self.__producer_completions = {}

    def add_producer(self, producer):
        self.__producer_completions[producer.name] = False
        Operator.add_producer(self, producer)

    def on_receive(self, m, producer):
        """Handles the event of receiving a new tuple from a producer. Will simply append the tuple to the internal
        lists corresponding to the producer that sent the tuple.

        :param m: The received message
        :param producer: The producer of the tuple
        :return: None
        """

        if type(m) is TupleMessage:
            self.on_receive_tuple(m.tuple_, producer)
        else:
            raise Exception("Unrecognized message {}".format(m))

    def on_receive_tuple(self, tuple_, producer):

        if not any(producer.name == p.name for p in self.producers):
            raise Exception("Received tuple from unknown producer {}. Producer must be connected".format(producer.name))
        else:
            if producer.name not in self.field_names:
                # This is essentially testing if we have seen tuples from a producer before. If we haven't then the tuple
                # should be the first tuple from that producer meaning it will be the tuple of field names.
                self.field_names[producer.name] = tuple_
            else:
                # The tuple is a data tuple, we just need to store it for joining later.
                if producer.name == self.join_expr.l_name:
                    self.l_tuples.append(tuple_)
                elif producer.name == self.join_expr.r_name:
                    self.r_tuples.append(tuple_)

    def on_producer_completed(self, _producer):
        """Handles the event where a producer has completed producing all the tuples it will produce. Note that the
        Join operator may have multiple producers. Once all producers are complete the operator can send the tuples
        it contains to downstream consumers.

        :type _producer: The producer that has completed
        :return: None
        """

        self.__producer_completions[_producer.name] = True

        is_all_producers_done = all(pc for pc in self.__producer_completions.values())

        # Check that we have received a completed event from all the producers
        # is_all_producers_done = all(p.is_completed() for p in self.producers)

        if self.log_enabled:
            print("{}('{}') | Producer completed [{}]".format(
                self.__class__.__name__,
                self.name,
                {'completed_producer': _producer.name, 'all_producers_completed': is_all_producers_done}))

        if is_all_producers_done and not self.is_completed():

            # Send the field names first, each field name is prepended with the key of the producer who sent it.
            self.join_field_names()

            # Send the joined data tuples
            self.nested_loop()

            Operator.on_producer_completed(self, _producer)

    def nested_loop(self):
        """Performs the join on data tuples using a nested loop joining algorithm. The joined tuples are each sent.
        Allows for the loop to be broken if the operator completes while executing.

        :return: None
        """

        for l_tuple in self.l_tuples:
            for r_tuple in self.r_tuples:

                # TODO: Can probably use a dict to speed up lookups
                l_field_name_index = self.field_names[self.join_expr.l_name].index(self.join_expr.l_field)
                r_field_name_index = self.field_names[self.join_expr.r_name].index(self.join_expr.r_field)

                if l_tuple[l_field_name_index] == r_tuple[r_field_name_index]:
                    t = l_tuple + r_tuple

                    if self.log_enabled:
                        print("{}('{}') | Sending data [{}]".format(
                            self.__class__.__name__,
                            self.name,
                            {'data': t}))

                    self.send(TupleMessage(Tuple(t)), self.consumers)

                if self.is_completed():
                    break

            if self.is_completed():
                break

    def join_field_names(self):
        """Examines the collected field names and joins them into a single list, left field names followed by right
        field names. The joined field names tuple is then sent.

        :return: None
        """

        joined_field_names = []

        l_field_names = self.field_names[self.join_expr.l_name]
        r_field_names = self.field_names[self.join_expr.r_name]

        for field_name in l_field_names:
            joined_field_names.append(self.join_expr.l_name + '.' + field_name)

        for field_name in r_field_names:
            joined_field_names.append(self.join_expr.r_name + '.' + field_name)

        if self.log_enabled:
            print("{}('{}') | Sending field names [{}]".format(
                self.__class__.__name__,
                self.name,
                {'field_names': joined_field_names}))

        self.send(TupleMessage(Tuple(joined_field_names)), self.consumers)
