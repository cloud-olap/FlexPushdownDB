# -*- coding: utf-8 -*-
"""Projection support

"""
from s3filter.plan.op_metrics import OpMetrics
from s3filter.op.operator_base import Operator
from s3filter.op.message import TupleMessage
from s3filter.op.tuple import Tuple, IndexedTuple
# noinspection PyCompatibility,PyPep8Naming
import cPickle as pickle
import pandas as pd

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

    def __init__(self, project_exprs, name,  query_plan, log_enabled, pandas_fn=None):
        """Constructs a new Project operator.

        :param project_exprs: The expressions defining the projection.
        :param name: The operator name
        :param log_enabled: Whether logging is enabled
        """

        super(Project, self).__init__(name, ProjectMetrics(),  query_plan, log_enabled)

        self.project_exprs = project_exprs

        self.pandas_fn = pandas_fn

        self.field_names_index = None

        self.producers_received = {}

    def on_receive(self, ms, producer_name):
        """Handles the event of receiving a new message from a producer.

        :param ms: The received tuples
        :param producer_name: The producer of the tuple
        :return: None
        """

        # print("Project | {}".format(t))
        for m in ms:
            if type(m) is TupleMessage:
                self.on_receive_tuple(m.tuple_, producer_name)
            elif type(m) is pd.DataFrame:
                self.__on_receive_dataframe(m)
            else:
                raise Exception("Unrecognized message {}".format(m))

    def __on_receive_dataframe(self, df):
        """Event handler for a received tuple

        :param tuple_: The received tuple
        :return: None
        """

        def fn(t):

            field_names = map(lambda e: e.new_field_name, self.project_exprs)

            #
            # field_names = []
            # for e in self.project_exprs:
            #     field_names.append(e.new_field_name)

            field_values = map(lambda e: e.expr(t), self.project_exprs)

            # field_values = []
            # for e in self.project_exprs:
            #     field_values.append(e.expr(t))

            return pd.Series(field_values, index=field_names)

        if self.pandas_fn is None:
            df = df.apply(fn, axis=1, result_type='expand')
        else:
            df = self.pandas_fn(df)

        self.op_metrics.rows_projected += len(df)

        self.send(df, self.consumers)

    def on_receive_tuple(self, tuple_, producer_name):
        """Handles the receipt of a tuple. The tuple is mapped to a new tuple using the given projection expressions.
        The field names are modified according to the new field names in the projection expressions.

        :param tuple_: The received tuple
        :return: None
        """

        assert(len(tuple_) > 0)

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

            self.producers_received[producer_name] = True

            assert (len(projected_field_names) == len(self.project_exprs))

            self.send(TupleMessage(Tuple(projected_field_names)), self.consumers)

        else:

            assert (len(tuple_) == len(self.field_names_index))

            if producer_name not in self.producers_received.keys():
                # This will be the field names tuple, skip it
                self.producers_received[producer_name] = True
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

                assert(len(projected_field_values) == len(self.project_exprs))

                self.send(TupleMessage(Tuple(projected_field_values)), self.consumers)
