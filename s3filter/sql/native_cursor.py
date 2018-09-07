# -*- coding: utf-8 -*-
"""Cursor support

"""

import pandas as pd

from s3filter.util.timer import Timer


class NativeCursor(object):
    """Represents a database cursor for managing the context of a fetch operation.

    Intended to be modelled after the Python DB API. All it supports at present is select, taking a s3 key and sql
    string to execute. The rows returned from s3 are returned as an iterator.

    Importantly supports streaming of records which gives more control over simply passing Python data structures
    around.

    """

    def __init__(self, fast_s3):
        self.fast_s3 = fast_s3

        self.s3key = None
        self.s3sql = None

        self.timer = Timer()

        self.time_to_first_record_response = None
        self.time_to_last_record_response = None

        self.query_bytes = 0
        self.bytes_scanned = 0
        self.bytes_processed = 0
        self.bytes_returned = 0

        self.num_http_get_requests = 0

    def select(self, s3key, s3sql):
        """Creates a select cursor

        :param s3sql:  the sql to execute
        :param s3key: the s3 key to select against
        :return: the cursor
        """

        self.s3key = s3key
        self.s3sql = s3sql

        # There doesn't seem to be a way to capture the bytes sent to s3, but we can use this for comparison purposes
        self.query_bytes = len(self.s3key.encode('utf-8')) + len(self.s3sql.encode('utf-8'))

        return self

    def execute(self, on_data):
        """Executes the fetch operation. This is different to the DB API as it returns an iterable. Of course we could
        model that API more precisely in future.

        :return: An iterable of the records fetched
        """

        # print("Executing select_object_content")

        # self.last_df = None

        def on_records(np_array):

            # print("|||")
            # print(type(np_array))
            # print(np_array)
            # print("|||")

            elapsed_time = self.timer.elapsed()
            if self.time_to_first_record_response is None:
                self.time_to_first_record_response = elapsed_time

            self.time_to_last_record_response = elapsed_time

            df = pd.DataFrame(np_array)
            df = df.add_prefix('_')

            # self.last_df = df

            on_data(df)

        self.timer.start()

        try:
            self.fast_s3.execute(self.s3key, self.s3sql, on_records)
        except Exception as e:
            # print (e)
            pass

        self.bytes_scanned = self.fast_s3.get_bytes_scanned()
        self.bytes_processed = self.fast_s3.get_bytes_processed()
        self.bytes_returned = self.fast_s3.get_bytes_returned()

