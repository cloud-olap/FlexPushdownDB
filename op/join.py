# -*- coding: utf-8 -*-
"""

"""

from op.operator import Operator


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

        self.producer1 = None
        self.producer2 = None

        self.running = True

    def set_producer1(self, operator):
        self.producer1 = operator

    def set_producer2(self, operator):
        self.producer2 = operator

    def emit(self, t, producer):
        if producer.key == self.join_key_1:
            self.tuples_1.append(t)
        elif producer.key == self.join_key_2:
            self.tuples_2.append(t)

    def stop(self):
        """This allows consuming producers to indicate that the operator can stop.

        TODO: Need to verify that this is actually useful.

        :return: None
        """

        # print("Sort Stop | ")
        self.running = False
        self.producer1.stop()
        self.producer2.stop()

    def done(self):
        """When this operator receives a done it emits the joined tuples.

        :return: None
        """
        # print("Sort Done | ")
        for t1 in self.tuples_1:
            for t2 in self.tuples_2:
                if t1[self.join_col_1_index] == t2[self.join_col_2_index]:
                    self.do_emit(t1 + t2)

                if not self.running:
                    break

            if not self.running:
                break

        self.do_done()
