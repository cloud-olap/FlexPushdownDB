# -*- coding: utf-8 -*-
"""

"""
from op.operator import Operator


class Group(Operator):
    """A crude group by operator. Allows a group by column and an aggregate to be specified, which will produce a list
    of tuples where the structure of each tuple is:

    [group_col, aggregate]

    TODO: Extend this to support aggregates other than COUNT and SUM, flexibility on the structure of returned
    tuples, etc.
    """

    def __init__(self, group_col_index, aggregate_col_index, aggregate_col_type, aggregate_fn):
        """Creates a new group by operator.

        :param group_col_index: The index of the column to group by
        :param aggregate_col_index: The index of the column being aggregated
        :param aggregate_col_type: The type of the aggregate column (essentially a Python cast operator to apply)
        :param aggregate_fn: The aggregate function.
        """
        Operator.__init__(self)

        self.group_col_index = group_col_index
        self.aggregate_col_index = aggregate_col_index
        self.aggregate_col_type = aggregate_col_type
        self.aggregate_fn = aggregate_fn

        self.tuples = {}

        self.running = True

    def on_emit(self, t, producer=None):
        """Incrementally applies an aggregate function to groups of tuples as specified by the group_col_index
        The grouped tuples are not emitted to downstream operators until a done signal is received.

        """

        # print("Group | {}".format(t))

        group_col = t[self.group_col_index]
        aggregate_col = t[self.aggregate_col_index]

        grouped_tuple = self.tuples.get(group_col, [group_col, 0])

        if self.aggregate_fn is 'COUNT':
            grouped_tuple[1] += 1
        elif self.aggregate_fn is 'SUM':
            grouped_tuple[1] += self.aggregate_col_type(aggregate_col)
        else:
            raise Exception('Unrecognized aggregate funcion {}.'.format(self.aggregate_fn))

        self.tuples[group_col] = grouped_tuple

    def on_stop(self):
        """This allows consuming producers to indicate that the operator can stop.

        TODO: Need to verify that this is actually useful.

        :return: None
        """

        # print("Group Stop | ")
        self.running = False
        self.do_stop()

    def on_done(self):
        """Once a done signal is received the grouped tuples can be emitted to downstream operators.

        """

        for t in self.tuples.values():
            if self.running:
                self.do_emit(t)
            else:
                break
