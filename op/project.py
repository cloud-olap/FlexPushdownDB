# -*- coding: utf-8 -*-
"""Projection support

"""
from plan.op_metrics import OpMetrics
from op.operator_base import Operator
from op.message import TupleMessage
from op.tuple import Tuple, LabelledTuple


class ProjectExpr(object):
    """Represents a projection expression, where one record field can be renamed to another.

    """

    def __init__(self, expr, new_field_name):
        """Creates a new projection expression

        :param field_name: The old field name.
        :param new_field_name The new field name.
        """
        self.expr = expr
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

        self.field_names = None

        self.projection_mappings = None

    def on_receive(self, m, _producer):
        """Handles the event of receiving a new tuple from a producer. Will remove unreferenced tuples and apply an alias
        to each element if specified.

        :param m: The received tuple
        :param _producer: The producer of the tuple
        :return: None
        """

        # print("Project | {}".format(t))

        if type(m) is TupleMessage:
            self.on_receive_tuple(m.tuple_)
        else:
            raise Exception("Unrecognized message {}".format(m))

    def on_receive_tuple(self, tuple_):

        if not self.field_names:
            self.field_names = tuple_

            projected_fields = []
            for e in self.project_exprs:
                projected_field = e.new_field_name
                projected_fields.append(projected_field)

            self.send(TupleMessage(Tuple(projected_fields)), self.consumers)

        else:

            lt = LabelledTuple(tuple_, self.field_names)

            projected_fields = []
            for e in self.project_exprs:
                projected_field = e.expr(lt)
                projected_fields.append(projected_field)

            self.send(TupleMessage(Tuple(projected_fields)), self.consumers)
