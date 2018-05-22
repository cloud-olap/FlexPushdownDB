# -*- coding: utf-8 -*-
"""Projection support

"""

from op.operator_base import Operator
from op.tuple import Tuple


class ProjectExpr:
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

    def __init__(self, project_exprs):
        """Constructs a new Project operator.

        :param project_exprs: The expressions defining the projection.
        """

        Operator.__init__(self)

        self.project_exprs = project_exprs

        self.first_tuple = True

        self.projection_mappings = None

    def on_receive(self, t, _producer):
        """Handles the event of receiving a new tuple from a producer. Will remove unreferenced tuples and apply an alias
        to each element if specified.

        :param t: The received tuple
        :param _producer: The producer of the tuple
        :return: None
        """

        # print("Project | {}".format(t))

        if not self.projection_mappings:
            # The record field names come from the first tuple, collect them and project them, while also storing the
            # mappings so subsequent tuple values can be projected.

            projected_field_names_tuple = Tuple()

            # TODO: I'm sure there is a more functional/concise way to do this
            self.projection_mappings = []
            for expr in self.project_exprs:
                if expr.field_name in t:
                    field_index = t.index(expr.field_name)
                    self.projection_mappings.append(field_index)
                    projected_field_names_tuple.append(expr.new_field_name)

            self.send(projected_field_names_tuple)

        else:
            # The record field values come from the remaining tuples, project them using the mappings collected
            # earlier.

            projected_field_values_tuple = Tuple()

            for field_index in self.projection_mappings:
                projected_field_values_tuple.append(t[field_index])

            self.send(projected_field_values_tuple)
