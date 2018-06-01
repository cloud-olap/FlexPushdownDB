# -*- coding: utf-8 -*-
"""

"""
from op.sql_table_scan import SQLTableScan


class TableScan(SQLTableScan):
    """Wrapper class for SQLTableScan that uses 'select * from S3Object' as the sql.

    """

    def __init__(self, s3key, name, log_enabled):
        """Creates a new TableScan operator using the given s3 object key

        :param s3key: The object key to select against
        """

        super(TableScan, self).__init__(
            s3key,
            "select * from S3Object ",
            name,
            log_enabled)
