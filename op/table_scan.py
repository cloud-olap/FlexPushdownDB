# -*- coding: utf-8 -*-
"""

"""
from op.operator_base import Operator
from op.tuple import Tuple, LabelledTuple
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

        first_tuple = True
        for t in tuples:

            # print("Table Scan | {}".format(t))

            if first_tuple:
                # Create and send the record field names
                lt = LabelledTuple(t)
                first_tuple = False
                self.send(Tuple(lt.labels))

            if self.is_completed():
                break

            self.send(Tuple(t))

        if not self.is_completed():
            self.complete()
