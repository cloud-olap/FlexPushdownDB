# -*- coding: utf-8 -*-
"""

"""
from s3filter.op.sql_table_scan import SQLTableScan
# noinspection PyCompatibility,PyPep8Naming
import cPickle as pickle


class TableScan(SQLTableScan):
    """Wrapper class for SQLTableScan that uses 'select * from S3Object' as the sql.

    """

    def on_producer_completed(self, producer_name):
        raise NotImplementedError

    def on_receive(self, message, producer_name):
        raise NotImplementedError

    def __init__(self, s3key, name, query_plan, log_enabled):
        """Creates a new TableScan operator using the given s3 object key

        :param s3key: The object key to select against
        """

        super(TableScan, self).__init__(
            s3key,
            "select * from S3Object ",
            name, query_plan,
            log_enabled)
