# -*- coding: utf-8 -*-
"""

"""
from operator import itemgetter

from op.operator_base import Operator


class Group(Operator):
    """A crude group by operator. Allows a group by column and an aggregate to be specified, which will produce a list
    of tuples where the structure of each tuple is:

    [group_col, aggregate]

    TODO: Extend this to support aggregates other than COUNT and SUM, flexibility on the structure of returned
    tuples, etc.
    """

    def __init__(self, group_col_indexes, aggregate_col_index, aggregate_col_type, aggregate_fn):
        """Creates a new group by operator.

        :param group_col_index: The index of the column to group by
        :param aggregate_col_index: The index of the column being aggregated
        :param aggregate_col_type: The type of the aggregate column (essentially a Python cast operator to apply)
        :param aggregate_fn: The aggregate function.
        """
        Operator.__init__(self)

        self.group_col_indexes = group_col_indexes
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

        def get_items():
            """Needed to handle single item tuples properly, note the trailing comma

            """
            if len(self.group_col_indexes) == 1:
                return itemgetter(*self.group_col_indexes)(t),
            else:
                return itemgetter(*self.group_col_indexes)(t)

        group_cols_tuple = get_items()

        aggregate_col = t[self.aggregate_col_index]

        aggregate_val = self.tuples.get(group_cols_tuple, 0)

        if self.aggregate_fn is 'COUNT':
            aggregate_val += 1
        elif self.aggregate_fn is 'SUM':
            aggregate_val += self.aggregate_col_type(aggregate_col)
        else:
            raise Exception('Unrecognized aggregate function {}.'.format(self.aggregate_fn))

        self.tuples[group_cols_tuple] = aggregate_val

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

        for k, v in self.tuples.iteritems():
            if self.running:
                t = list(k) + [v]
                self.do_emit(t)
            else:
                break
