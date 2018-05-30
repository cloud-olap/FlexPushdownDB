# -*- coding: utf-8 -*-
"""Group by with aggregate support

"""

from metric.op_metrics import OpMetrics
from op.aggregate_expression import AggregateExpressionContext
from op.operator_base import Operator
from op.message import TupleMessage
from op.tuple import Tuple, LabelledTuple


class Group(Operator):
    """A crude group by operator. Allows multiple grouping columns to be specified along with multiple aggregate
    expressions.

    """

    def __init__(self, group_field_names, aggregate_expressions, name, log_enabled):
        """Creates a new group by operator.

        :param group_field_names: The names of the columns to group by
        :param aggregate_expressions: The list of aggregate expressions
        """

        super(Group, self).__init__(name, OpMetrics(), log_enabled)

        self.group_field_names = group_field_names
        self.aggregate_expressions = aggregate_expressions

        self.field_names = None

        # Dict of group contexts. Each item is indexed by a tuple of the group columns and contains a dict of the
        # evaluated aggregate results
        self.group_contexts = {}

    def on_receive(self, m, _producer):
        """ Handles the event of receiving a new tuple from a producer. Applies each aggregate function to the tuple and
        then inserts those aggregates into the tuples dict indexed by the grouping columns values.

        Once downstream producers are completed the tuples will be send to downstream consumers.

        :param m: The received tuples
        :param _producer: The producer of the tuple
        :return: None
        """

        if type(m) is TupleMessage:
            self.__on_receive_tuple(m.tuple_)
        else:
            raise Exception("Unrecognized message {}".format(m))

    def __on_receive_tuple(self, tuple_):

        if not self.field_names:
            # Save the field names
            self.field_names = tuple_
        else:

            # Create a tuple of column values to group by, we use this as the key for a dict of groups and their associated
            # aggregate values
            group_field_values_tuple = self.__build_group_field_values_tuple(tuple_)
            # print(group_fields_tuple)

            # Get or create the group context
            group_aggregate_expression_contexts = self.group_contexts.get(group_field_values_tuple, {})

            # Evaluate all the expressions for the group
            i = 0
            for e in self.aggregate_expressions:
                group_aggregate_expression_context = group_aggregate_expression_contexts.get(i, AggregateExpressionContext(0.0, {}))
                e.eval(tuple_, self.field_names, group_aggregate_expression_context)
                group_aggregate_expression_contexts[i] = group_aggregate_expression_context
                i += 1

            # Store the group context indexed by the group
            self.group_contexts[group_field_values_tuple] = group_aggregate_expression_contexts

    def __build_group_field_values_tuple(self, tuple_):
        lt = LabelledTuple(tuple_, self.field_names)
        group_fields = []
        for f in self.group_field_names:
            group_fields.append(lt[f])
        group_fields_tuple = tuple(group_fields)
        return group_fields_tuple

    def on_producer_completed(self, producer):
        """Handles the event where the producer has completed producing all the tuples it will produce. Once this
        occurs the tuples can be sent to consumers downstream.

        :param producer: The producer that has completed
        :return: None
        """

        # Send the field names
        lt = LabelledTuple(self.group_field_names + self.aggregate_expressions)
        self.send(TupleMessage(Tuple(lt.labels)), self.consumers)

        for group_tuple, group_aggregate_contexts in self.group_contexts.items():

            if self.is_completed():
                break

            # Convert the aggregate contexts to their results
            group_fields = list(group_tuple)
            group_aggregate_values = list(v.result for v in group_aggregate_contexts.values())

            t_ = group_fields + group_aggregate_values
            self.send(TupleMessage(Tuple(t_)), self.consumers)

        Operator.on_producer_completed(self, producer)
