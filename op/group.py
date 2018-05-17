# -*- coding: utf-8 -*-
"""

"""

import itertools
from operator import itemgetter
from op.operator_base import Operator
from util.aggregateexpression import AggregateExpression


class Group(Operator):
    """A crude group by operator. Allows a group by column and an aggregate to be specified, which will produce a list
    of tuples where the structure of each tuple is:

    [group_col, aggregate]

    TODO: Extend this to support aggregates other than COUNT and SUM, flexibility on the structure of returned
    tuples, etc.
    """

    def __init__(self, group_col_indexes, aggregate_expr_strs):
        """Creates a new group by operator.

        :param group_col_index: The indexes of the columns to group by
        :param aggregate_expr_strs: The list of aggregate expressions
        """
        Operator.__init__(self)

        self.group_col_indexes = group_col_indexes
        self.aggregate_expr_strs = aggregate_expr_strs
        self.aggregate_exprs = list(AggregateExpression(a_expr) for a_expr in self.aggregate_expr_strs)

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

        # Create a tuple of columns to group by, we use this as the key for a dict of groups and their associated
        # aggregate values
        group_cols_tuple = get_items()
        # print(group_cols_tuple)

        # Get the current list of aggregates for this group
        group_aggregate_vals = self.tuples.get(group_cols_tuple, list(itertools.repeat(0, len(self.aggregate_exprs))))
        # print(group_aggregate_vals)

        for i in range(0, len(self.aggregate_exprs)):
            e = self.aggregate_exprs[i]
            v = group_aggregate_vals[i]
            e.val = v
            e.eval(t)
            group_aggregate_vals[i] = e.val

        # print(list(group_cols_tuple) + group_aggregate_vals)

        self.tuples[group_cols_tuple] = group_aggregate_vals

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
                t = list(k) + v
                self.do_emit(t)
            else:
                break
