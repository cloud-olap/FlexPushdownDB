# -*- coding: utf-8 -*-
"""Cursor support

"""
import boto3
import csv
import util.constants
import io


class Cursor(object):
    """Represents a database cursor for managing the context of a fetch operation.

    Intended to be modelled after the Python DB API. Though (at present anyway) you don't pass it arbitrary SQL strings
    but rather call various methods to build up the cursor definition before execution.

    Importantly supports streaming of records which gives more control over simply passing Python data structures
    around.

    WORK IN PROGRESS

    """

    def __init__(self):
        self.key = None
        self.sql = None
        self.event_stream = None

    def select(self, key, sql):
        """Creates a select cursor

        :param sql:  the sql to execute
        :param key: the s3 key to select against
        :return: the cursor
        """
        self.key = key
        self.sql = sql

        return self

    def execute(self):
        """Executes the fetch operation. This is different to the DB API as it returns an iterable. Of course we could
        model that API more precisely in future.

        :return: An iterable of the records fetched
        """

        return self.__do_execute(self.sql)

    def __do_execute(self, sql):
        """ Executes the defined cursor using the given SQL

        :param sql: The sql statement to execute against S3
        :return: An iterable of the records fetched
        """
        s3 = boto3.client('s3')

        # print("Executing select_object_content")

        # Note:
        #
        # CSV files use | as a delimiter and have a trailing delimiter so record delimiter is |\n
        #
        # NOTE: As responses are chunked the file headers are only returned in the first chunk. We ignore them for now
        # just because its simpler. It does mean the records are returned as a list instead of a dict though (can change
        # in future).
        #
        response = s3.select_object_content(
            Bucket=util.constants.S3_BUCKET_NAME,
            Key=self.key,
            ExpressionType='SQL',
            Expression=sql,
            InputSerialization={'CSV': {'FileHeaderInfo': 'Use', 'RecordDelimiter': '|\n', 'FieldDelimiter': '|'}},
            OutputSerialization={'CSV': {}}
        )

        self.event_stream = response['Payload']

        return self.parse_event_stream()

    def parse_event_stream(self):
        """Generator that hands out records from the event stream lazily

        """

        prev_record_str = None

        # first_record = True

        for event in self.event_stream:

            if 'Records' in event:

                records_str = event['Records']['Payload'].decode('utf-8')

                # print("Records Event {}".format(records_str))

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

                        yield record

                    else:
                        # It's an incomplete record, save for next iteration
                        # print("Incomplete: {}".format(record_str))
                        prev_record_str = record_str

            elif 'Stats' in event:
                pass
                # bytes_scanned = event['Stats']['Details']['BytesScanned']
                # bytes_processed = event['Stats']['Details']['BytesProcessed']
                # print("Stats Event: bytes scanned: {}, bytes processed: {}".format(bytes_scanned, bytes_processed))

            elif 'Progress' in event:
                pass
                # print("Progress Event")

            elif 'End' in event:
                pass
                # print("End Event")

            elif 'Cont' in event:
                pass
                # print("Cont Event")

    def close(self):
        if self.event_stream:
            self.event_stream.close()
