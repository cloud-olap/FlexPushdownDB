# -*- coding: utf-8 -*-
"""Join support

"""

from op.operator_base import Operator


class JoinExpression(object):
    """Represents a join expression, as in the table name (key) and column name (field) to join on.

    """

    def __init__(self, l_key, l_field, r_key, r_field):
        """Creates a new join expression.

        :param l_key: Key of the producer of the left tuple to join on
        :param l_field: Field of the left tuple to join on
        :param r_key: Key of the producer of the right tuple to join on
        :param r_field: Field of the right tuple to join on
        """

        self.l_key = l_key
        self.l_field = l_field
        self.r_key = r_key
        self.r_field = r_field


class Join(Operator):
    """Implements a join using nested loops.

    """

    def __init__(self, join_expr):
        """
        Creates a new join operator.

        :param join_expr: The join expression indicating which fields of which key to join on
        """

        Operator.__init__(self)

        self.join_expr = join_expr

        self.l_tuples = []
        self.r_tuples = []

        # Dict of field names indexed by producer key
        self.field_names = {}

    def on_receive(self, t, producer):
        """Handles the event of receiving a new tuple from a producer. Will simply append the tuple to the internal
        lists corresponding to the producer that sent the tuple.

        :param t: The received tuples
        :param producer: The producer of the tuple
        :return: None
        """

        if producer.key not in self.field_names:
            # This is essentially testing if we have seen tuples from a producer before. If we haven't then the tuple
            # should be the first tuple from that producer meaning it will be the tuple of field names.
            self.field_names[producer.key] = t
        else:
            # The tuple is a data tuple, we just need to store it for joining later.
            if producer.key == self.join_expr.l_key:
                self.l_tuples.append(t)
            elif producer.key == self.join_expr.r_key:
                self.r_tuples.append(t)

    def on_producer_completed(self, producer):
        """Handles the event where a producer has completed producing all the tuples it will produce. Note that the
        Join operator may have multiple producers. Once all producers are complete the operator can send the tuples
        it contains to downstream consumers.

        :type producer: The producer that has completed
        :return: None
        """

        # Check that we have received a completed event from all the producers
        is_all_producers_done = all(p.is_completed() for p in self.producers)

        if is_all_producers_done:

            # Send the field names first, each field name is prepended with the key of the producer who sent it.
            self.join_field_names()

            # Send the joined data tuples
            self.nested_loop()

            Operator.on_producer_completed(self, producer)

    def nested_loop(self):
        """Performs the join on data tuples using a nested loop joining algorithm. The joined tuples are each sent.
        Allows for the loop to be broken if the operator completes while executing.

        :return: None
        """

        for l_tuple in self.l_tuples:
            for r_tuple in self.r_tuples:

                # TODO: Can probably use a dict to speed up lookups
                l_field_name_index = self.field_names[self.join_expr.l_key].index(self.join_expr.l_field)
                r_field_name_index = self.field_names[self.join_expr.r_key].index(self.join_expr.r_field)

                if l_tuple[l_field_name_index] == r_tuple[r_field_name_index]:
                    self.send(l_tuple + r_tuple)

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

        l_field_names = self.field_names[self.join_expr.l_key]
        r_field_names = self.field_names[self.join_expr.r_key]

        for field_name in l_field_names:
            joined_field_names.append(self.join_expr.l_key + '.' + field_name)

        for field_name in r_field_names:
            joined_field_names.append(self.join_expr.r_key + '.' + field_name)

        self.send(joined_field_names)
