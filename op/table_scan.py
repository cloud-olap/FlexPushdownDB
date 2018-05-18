# -*- coding: utf-8 -*-
"""

"""
from op.operator_base import Operator
from sql.cursor import Cursor


class TableScan(Operator):
    """Represents a table scan operator which reads from an s3 table and emits tuples to consuming operators
    as they are received. Generally starting this operator is what begins a query.

    """

    def __init__(self, key, sql):
        """Creates a new Table Scan operator using the given s3 object key and s3 select sql

        :param key: The object key to select against
        :param sql: The s3 select sql
        """

        Operator.__init__(self)

        self.key = key
        self.sql = sql

    def start(self):
        """Executes the query and begins emitting tuples.

        :return: None
        """
        # print("Table Scan | Start {} {}".format(self.key, self.sql))

        cur = Cursor().select(self.key, self.sql)

        tuples = cur.execute()

        # Push the tuples to the consumer
        for t in tuples:

            if self.is_completed():
                break

            # print("Table Scan | {}".format(t))

            self.send(t)

        if not self.is_completed():
            self.complete()
