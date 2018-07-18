# -*- coding: utf-8 -*-
"""

"""

import string
from datetime import datetime
from random import randint, choice, randrange

from s3filter.op.message import TupleMessage
from s3filter.op.operator_base import Operator
from s3filter.op.tuple import Tuple, IndexedTuple
from s3filter.plan.op_metrics import OpMetrics
from s3filter.util.datetime_util import dt_to_millis, millis_to_str


class RandomTableScanMetrics(OpMetrics):
    """Extra metrics for a random table scan

    """

    def __init__(self):
        super(RandomTableScanMetrics, self).__init__()

        self.rows_returned = 0

    def __repr__(self):
        return {
            'elapsed_time': round(self.elapsed_time(), 5),
            'rows_returned': self.rows_returned

        }.__repr__()


class RandomColumnDef(object):

    def __init__(self, col_type):
        self.col_type = col_type


class RandomIntColumnDef(RandomColumnDef):

    def __init__(self, min_int, max_int):
        super(RandomIntColumnDef, self).__init__(int)
        self.min_int = min_int
        self.max_int = max_int

    def generate(self):
        return randint(self.min_int, self.max_int)


class RandomStringColumnDef(RandomColumnDef):

    _CHARS = string.ascii_uppercase + string.digits

    def __init__(self, min_num_chars, max_num_chars):
        super(RandomStringColumnDef, self).__init__(str)
        self.min_num_chars = min_num_chars
        self.max_num_chars = max_num_chars

    def generate(self):
        num_chars = randint(self.min_num_chars, self.max_num_chars)
        return ''.join(choice(RandomStringColumnDef._CHARS) for _ in range(num_chars))


class RandomDateColumnDef(RandomColumnDef):

    def __init__(self, min_date, max_date):
        super(RandomDateColumnDef, self).__init__(datetime)
        self.min_date = min_date
        self.max_date = max_date

    def generate(self):
        min_date_millis = dt_to_millis(self.min_date)
        max_date_millis = dt_to_millis(self.max_date)
        dt_millis = randrange(min_date_millis, max_date_millis)
        return millis_to_str(dt_millis)


class RandomTableScan(Operator):

    def on_receive(self, message, producer_name):
        raise NotImplementedError

    def __init__(self, num_rows, col_defs, name, log_enabled):

        super(RandomTableScan, self).__init__(name, RandomTableScanMetrics(), log_enabled)

        self.num_rows = num_rows
        self.col_defs = col_defs

    def start(self):

        self.op_metrics.timer_start()

        it = IndexedTuple.build_default(self.col_defs)

        if self.log_enabled:
            print("{}('{}') | Sending field names: {}"
                  .format(self.__class__.__name__, self.name, it.field_names()))

        self.send(TupleMessage(Tuple(it.field_names())), self.consumers)

        for i in range(0, self.num_rows):

            if self.is_completed():
                break

            self.op_metrics.rows_returned += 1

            t = Tuple()
            for col_def in self.col_defs:
                col_val = col_def.generate()
                t.append(col_val)

            if self.log_enabled:
                print("{}('{}') | Sending field values: {}".format(self.__class__.__name__, self.name, t))

            self.send(TupleMessage(t), self.consumers)

        if not self.is_completed():
            self.complete()

        self.op_metrics.timer_stop()

    def on_producer_completed(self, producer_name):
        """This event is overridden really just to indicate that it never fires.

        :param producer_name: The completed producer
        :return: None
        """

        raise NotImplementedError
