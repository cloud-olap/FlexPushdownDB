# -*- coding: utf-8 -*-
"""A dev/null operator

"""
# noinspection PyCompatibility,PyPep8Naming
import cPickle as pickle
import sys

from pandas import DataFrame

from s3filter.op.message import TupleMessage
from s3filter.op.operator_base import Operator, EvalMessage, EvaluatedMessage
from s3filter.plan.op_metrics import OpMetrics


class Null(Operator):
    """Just swallows tuples, useful if you aren't interested in results but need somewhere for tuples to go

    """

    def __init__(self, name, query_plan, log_enabled):
        """Constructs a new Collate operator.

        """

        super(Null, self).__init__(name, OpMetrics(), query_plan, log_enabled)

    def on_receive(self, ms, _producer):
        """Does nothing

        :param ms: The received messages
        :param _producer: The producer of the tuple
        :return: None
        """

        pass
