# -*- coding: utf-8 -*-
"""Join support

"""

from op.operator_base import Operator


class Join(Operator):
    """Implements a crude join using nested loops.

    TODO: This is a bit of a hack at the moment for a few reasons but mostly because A) it's inefficient and B) only
    allows joins on two columns.
    """

    def __init__(self, join_key_1, join_col_1_index, join_key_2, join_col_2_index):
        """
        Creates a new join operator.

        :param join_key_1: The 1st key to join on (effectively join table 1 in a SQL statement)
        :param join_col_1_index: The 1st column index to join on (effectively join column 1 in a SQL statement)
        :param join_key_2: The 2nd key to join on (effectively join table 1 in a SQL statement)
        :param join_col_2_index: The 2nd column index to join on (effectively join column 1 in a SQL statement)
        """
        Operator.__init__(self)

        self.join_key_1 = join_key_1
        self.join_col_1_index = join_col_1_index
        self.join_key_2 = join_key_2
        self.join_col_2_index = join_col_2_index

        self.tuples_1 = []
        self.tuples_2 = []
        self.joined_tuples = []

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
            self.field_names[producer.key] = t
        else:
            if producer.key == self.join_key_1:
                self.tuples_1.append(t)
            elif producer.key == self.join_key_2:
                self.tuples_2.append(t)

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

            # Send the field names first, each field name is prepended with the key of the producer who send it.
            joined_field_names = []
            field_names_1 = self.field_names[self.join_key_1]
            field_names_2 = self.field_names[self.join_key_2]
            for field_name in field_names_1:
                joined_field_names.append(self.join_key_1 + '.' + field_name)
            for field_name in field_names_2:
                joined_field_names.append(self.join_key_2 + '.' + field_name)

            self.send(joined_field_names)

            for t1 in self.tuples_1:
                for t2 in self.tuples_2:
                    field_name_index_1 = self.field_names[self.join_key_1].index(self.join_col_1_index)
                    field_name_index_2 = self.field_names[self.join_key_2].index(self.join_col_2_index)
                    if t1[field_name_index_1] == t2[field_name_index_2]:
                        self.send(t1 + t2)

                    if self.is_completed():
                        break

                if self.is_completed():
                    break

            Operator.on_producer_completed(self, producer)
