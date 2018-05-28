# -*- coding: utf-8 -*-
"""Projection support

"""
from metric.op_metrics import OpMetrics
from op.operator_base import Operator
from op.message import TupleMessage
from op.tuple import Tuple


class ProjectExpr(object):
    """Represents a projection expression, where one record field can be renamed to another.

    """

    def __init__(self, field_name, new_field_name):
        """Creates a new projection expression

        :param field_name: The old field name.
        :param new_field_name The new field name.
        """
        self.field_name = field_name
        self.new_field_name = new_field_name


class Project(Operator):
    """This operator removes unreferenced elements from the received tuples and maps an optional alias to each of them.
    It does not block but rather emits tuples as they are received.

    """

    def __init__(self, project_exprs, name, log_enabled):
        """Constructs a new Project operator.

        :param project_exprs: The expressions defining the projection.
        """

        super(Project, self).__init__(name, OpMetrics(), log_enabled)

        self.project_exprs = project_exprs

        self.first_tuple = True

        self.projection_mappings = None

    def on_receive(self, m, _producer):
        """Handles the event of receiving a new tuple from a producer. Will remove unreferenced tuples and apply an alias
        to each element if specified.

        :param t: The received tuple
        :param _producer: The producer of the tuple
        :return: None
        """

        # print("Project | {}".format(t))

        if type(m) is TupleMessage:

            self.on_receive_tuple(m.tuple_)

        else:
            raise Exception("Unrecognized message {}".format(m))

    def on_receive_tuple(self, tuple_):
        if not self.projection_mappings:
            # The record field names come from the first tuple, collect them and project them, while also storing the
            # mappings so subsequent tuple values can be projected.

            projected_field_names_tuple = Tuple()

            # TODO: I'm sure there is a more functional/concise way to do this
            self.projection_mappings = []
            for expr in self.project_exprs:
                if expr.field_name in tuple_:
                    field_index = tuple_.index(expr.field_name)
                    self.projection_mappings.append(field_index)
                    projected_field_names_tuple.append(expr.new_field_name)

            self.send(TupleMessage(projected_field_names_tuple), self.consumers)

        else:
            # The record field values come from the remaining tuples, project them using the mappings collected
            # earlier.

            projected_field_values_tuple = Tuple()

            for field_index in self.projection_mappings:
                projected_field_values_tuple.append(tuple_[field_index])

            self.send(TupleMessage(projected_field_values_tuple), self.consumers)