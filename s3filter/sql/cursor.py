# -*- coding: utf-8 -*-
"""Cursor support

"""

import boto3
import csv
import s3filter.util.constants
import io

from s3filter.util.timer import Timer


class Cursor(object):
    """Represents a database cursor for managing the context of a fetch operation.

    Intended to be modelled after the Python DB API. All it supports at present is select, taking a s3 key and sql
    string to execute. The rows returned from s3 are returned as an iterator.

    Importantly supports streaming of records which gives more control over simply passing Python data structures
    around.

    """

    def __init__(self):
        self.s3key = None
        self.s3sql = None
        self.event_stream = None

        self.timer = Timer()

        self.time_to_first_record_response = None
        self.time_to_last_record_response = None

        self.bytes_scanned = 0
        self.bytes_processed = 0
        self.bytes_returned = 0

    def select(self, s3key, s3sql):
        """Creates a select cursor

        :param s3sql:  the sql to execute
        :param s3key: the s3 key to select against
        :return: the cursor
        """

        self.s3key = s3key
        self.s3sql = s3sql

        return self

    def execute(self):
        """Executes the fetch operation. This is different to the DB API as it returns an iterable. Of course we could
        model that API more precisely in future.

        :return: An iterable of the records fetched
        """

        s3 = boto3.client('s3')

        # print("Executing select_object_content")

        self.timer.start()

        # Note:
        #
        # CSV files use | as a delimiter and have a trailing delimiter so record delimiter is |\n
        #
        # NOTE: As responses are chunked the file headers are only returned in the first chunk. We ignore them for now
        # just because its simpler. It does mean the records are returned as a list instead of a dict though (can change
        # in future).
        #
        response = s3.select_object_content(
            Bucket=s3filter.util.constants.S3_BUCKET_NAME,
            Key=self.s3key,
            ExpressionType='SQL',
            Expression=self.s3sql,
            InputSerialization={'CSV': {'FileHeaderInfo': 'Use', 'RecordDelimiter': '|\n', 'FieldDelimiter': '|'}},
            OutputSerialization={'CSV': {}}
        )

        self.event_stream = response['Payload']

        return self.parse_event_stream()

    def parse_event_stream(self):
        """Generator that hands out records from the event stream lazily

        :return: Generator of the records returned from s3
        """

        prev_record_str = None

        # first_record = True

        for event in self.event_stream:

            if 'Records' in event:

                elapsed_time = self.timer.elapsed()
                if self.time_to_first_record_response is None:
                    self.time_to_first_record_response = elapsed_time

                self.time_to_last_record_response = elapsed_time

                records_str = event['Records']['Payload'].decode('utf-8')

                # print("{} Records Event {}".format(timeit.default_timer(), records_str))
                # print("{} Records Event".format(timeit.default_timer()))

                # Note that the select_object_content service may return partial chunks so we cant simply pass the
                # str to the csv reader. We need to examine the string itself to see if it's an incomplete record
                # (it won't end with a newline if its incomplete)

                records_str_rdr = io.StringIO(records_str)

                for record_str in records_str_rdr:

                    # Check record ends with newline (excluding an escaped newline)
                    if record_str.endswith('\n') and not record_str.endswith('\\n'):  # It's a complete record

                        if prev_record_str is not None:  # There was an incomplete record present in the last payload
                            # Append the current record to the previous incomplete record
                            # print("Appending: {} {}".format(prev_record_str, record_str))
                            record_str = prev_record_str + record_str
                            prev_record_str = None

                        # print("Complete: {}".format(record_str))

                        # Parse CSV
                        record_rdr = csv.reader([record_str])
                        record = record_rdr.next()

                        # print("Parsed: {}".format(record))

                        # if first_record:
                        #     # This will be the headers, keep them around
                        #     header = record
                        #     first_record = False
                        # else:
                        #     record_dict = dict(zip(header, record))
                        #     yield record_dict


                        # TODO: Not sure how to handle timers exactly, while time will be taken yielding it may
                        # genuinely be time that this method is waiting for data from s3. May be a case of measuring
                        # both, something to think about.

                        # self.timer.stop()
                        yield record
                        # self.timer.start()

                    else:
                        # It's an incomplete record, save for next iteration
                        # print("Incomplete: {}".format(record_str))
                        prev_record_str = record_str

            elif 'Stats' in event:
                self.bytes_scanned += event['Stats']['Details']['BytesScanned']
                self.bytes_processed += event['Stats']['Details']['BytesProcessed']
                self.bytes_returned += event['Stats']['Details']['BytesReturned']
                # print("{} Stats Event: bytes scanned: {}, bytes processed: {}, bytes returned: {}"
                #       .format(timeit.default_timer(), self.bytes_scanned, self.bytes_processed, self.bytes_returned))

            elif 'Progress' in event:
                pass
                # print("{} Progress Event".format(timeit.default_timer()))

            elif 'End' in event:
                # print("{} End Event".format(timeit.default_timer()))
                return

            elif 'Cont' in event:
                pass
                # print("{} Cont Event".format(timeit.default_timer()))

    def close(self):
        """Closes the s3 event stream

        :return: None
        """
        if self.event_stream:
            self.event_stream.close()
