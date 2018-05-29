# -*- coding: utf-8 -*-
"""Group by with aggregate support

"""

from metric.op_metrics import OpMetrics
from op.operator_base import Operator
from op.message import TupleMessage
from op.tuple import Tuple, LabelledTuple


class AggregateExprContext2(object):

    def __init__(self, result, vars_):
        self.result = result
        self.vars_ = vars_

    def __repr__(self):
        return "({}, {})".format(self.result, self.vars_)


class Group(Operator):
    """A crude group by operator. Allows multiple grouping columns to be specified along with multiple aggregate
    expressions.

    """

    def __init__(self, group_col_indexes, aggregate_exprs, name, log_enabled):
        """Creates a new group by operator.

        :param group_col_indexes: The indexes of the columns to group by
        :param aggregate_exprs: The list of aggregate expressions
        """

        super(Group, self).__init__(name, OpMetrics(), log_enabled)

        self.group_field_names = group_col_indexes
        self.aggregate_exprs = aggregate_exprs

        self.field_names = None

        # Dict of group contexts. Each item is indexed by a tuple of the group columns and contains a dict of the
        # evaluated aggregate results
        self.ctx = {}

    def on_receive(self, m, _producer):
        """ Handles the event of receiving a new tuple from a producer. Applies each aggregate function to the tuple and
        then inserts those aggregates into the tuples dict indexed by the grouping columns values.

        Once downstream producers are completed the tuples will be send to downstream consumers.

        :param m: The received tuples
        :param _producer: The producer of the tuple
        :return: None
        """

        # print("Group | {}".format(t))

        if type(m) is TupleMessage:
            self.on_receive_tuple(m.tuple_)
        else:
            raise Exception("Unrecognized message {}".format(m))

    def on_receive_tuple(self, tuple_):

        if not self.field_names:

            # Save the field names
            self.field_names = tuple_

        else:

            # Create a tuple of column values to group by, we use this as the key for a dict of groups and their associated
            # aggregate values
            lt = LabelledTuple(tuple_, self.field_names)
            group_fields =[]
            for f in self.group_field_names:
                group_fields.append(lt[f])
            group_fields_tuple = tuple(group_fields)
            # print(group_fields_tuple)

            # Get or create the group context
            group_ctxs = self.ctx.get(group_fields_tuple, {})

            # Evaluate all the expressions for the group
            i = 0
            for e in self.aggregate_exprs:
                group_ctx = group_ctxs.get(i, AggregateExprContext2(0.0, {}))
                e.eval(tuple_, self.field_names, group_ctx)
                group_ctxs[i] = group_ctx
                i += 1

            # Store the group context indexed by the group
            self.ctx[group_fields_tuple] = group_ctxs

    def on_producer_completed(self, producer):
        """Handles the event where the producer has completed producing all the tuples it will produce. Once this
        occurs the tuples can be sent to consumers downstream.

        :param producer: The producer that has completed
        :return: None
        """

        # Send the field names
        lt = LabelledTuple(self.group_field_names + self.aggregate_exprs)
        self.send(TupleMessage(Tuple(lt.labels)), self.consumers)

        for group_tuple, group_aggregate_contexts in self.ctx.items():

            if self.is_completed():
                break

            # Convert the aggregate contexts to their results
            group_fields = list(group_tuple)
            group_aggregate_values = list(v.result for v in group_aggregate_contexts.values())

            t_ = group_fields + group_aggregate_values
            self.send(TupleMessage(Tuple(t_)), self.consumers)

        Operator.on_producer_completed(self, producer)
