# -*- coding: utf-8 -*-
"""Projection support

"""
from s3filter.plan.op_metrics import OpMetrics
from s3filter.op.operator_base import Operator
from s3filter.op.message import TupleMessage
from s3filter.op.tuple import Tuple, IndexedTuple


class ProjectMetrics(OpMetrics):
    """Extra metrics for a project

    """

    def __init__(self):
        super(ProjectMetrics, self).__init__()

        self.rows_projected = 0

    def __repr__(self):
        return {
            'elapsed_time': round(self.elapsed_time(), 5),
            'rows_projected': self.rows_projected
        }.__repr__()


class ProjectExpression(object):
    """Represents a projection expression, where one record field can be renamed to another.

    """

    def __init__(self, expr, new_field_name):
        """Creates a new projection expression

        :param expr: The projection expression
        :param new_field_name The new field name.
        """

        self.expr = expr
        self.new_field_name = new_field_name.strip()


class Project(Operator):
    """This operator mapes fields from the received tuples using the given projection expressions and maps the given
    alias to each of them. It does not block but rather emits tuples as they are received.

    """

    def __init__(self, project_exprs, name,  query_plan, log_enabled):
        """Constructs a new Project operator.

        :param project_exprs: The expressions defining the projection.
        :param name: The operator name
        :param log_enabled: Whether logging is enabled
        """

        super(Project, self).__init__(name, ProjectMetrics(),  query_plan, log_enabled)

        self.project_exprs = project_exprs

        self.field_names_index = None

    def on_receive(self, ms, _producer):
        """Handles the event of receiving a new message from a producer.

        :param ms: The received tuples
        :param _producer: The producer of the tuple
        :return: None
        """

        # print("Project | {}".format(t))
        for m in ms:
            if type(m) is TupleMessage:
                self.on_receive_tuple(m.tuple_)
            else:
                raise Exception("Unrecognized message {}".format(m))

        pass

    def on_receive_tuple(self, tuple_):
        """Handles the receipt of a tuple. The tuple is mapped to a new tuple using the given projection expressions.
        The field names are modified according to the new field names in the projection expressions.

        :param tuple_: The received tuple
        :return: None
        """

        if not self.field_names_index:

            self.field_names_index = IndexedTuple.build_field_names_index(tuple_)

            # Map the old field names to the new
            projected_field_names = []
            for e in self.project_exprs:
                fn = e.new_field_name
                projected_field_names.append(fn)

            if self.log_enabled:
                print("{}('{}') | Sending projected field names: from: {} to: {}"
                      .format(self.__class__.__name__, self.name, tuple_, projected_field_names))

            self.send(TupleMessage(Tuple(projected_field_names)), self.consumers)

        else:

            # Perform the projection using the given expressions
            it = IndexedTuple(tuple_, self.field_names_index)

            projected_field_values = []
            for e in self.project_exprs:
                fv = e.expr(it)
                projected_field_values.append(fv)

            self.op_metrics.rows_projected += 1

            # if self.log_enabled:
            #     print("{}('{}') | Sending projected field values: from: {} to: {}"
            #           .format(self.__class__.__name__, self.name, tuple_, projected_field_values))

            self.send(TupleMessage(Tuple(projected_field_values)), self.consumers)
