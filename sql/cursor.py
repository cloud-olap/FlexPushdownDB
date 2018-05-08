# -*- coding: utf-8 -*-
"""Cursor support

"""
import boto3
import csv
import util.constants
import itertools
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
        self.lim = 0
        self.op = ''
        self.key = ''
        self.limit_strategy = LimitStrategy.NONE
        self.event_stream = None

    def select(self, key):
        """Creates a select cursor

        :param key: the s3 key to select against
        :return: the cursor
        """
        self.op = Operator.SELECT
        self.key = key

        return self

    def limit(self, num, strategy):
        """Defines a limit clause

        :param num: Number of records to limit a select to
        :param strategy: Which limit strategy to use
        :return: the cursor
        """
        self.lim = num
        self.limit_strategy = strategy

        return self

    def execute(self):
        """Executes the fetch operation. This is different to the DB API as it returns an interable. Of course we could
        model that API more precisely in future.

        :return: An iterable of the records fetched
        """
        if self.op == Operator.SELECT:
            if self.limit_strategy == LimitStrategy.OP:
                rows = self.do_execute("select * from s3object s limit {};".format(self.lim + 1))
                return rows
            if self.limit_strategy == LimitStrategy.NONE:
                rows = self.do_execute("select * from s3object s;")
                return rows
            else:
                raise Exception("Unrecognized limit strategy {}".format(self.limit_strategy))
        else:
            raise Exception("Unrecognized operator {}".format(self.op))

    def do_execute(self, sql):
        """ Executes the defined cursor using the given SQL

        :param key:
        :param sql:
        :return:
        """
        s3 = boto3.client('s3')

        print("Executing select_object_content")
        response = s3.select_object_content(
            Bucket=util.constants.S3_BUCKET_NAME,
            Key=self.key,
            ExpressionType='SQL',
            Expression=sql,
            InputSerialization={'CSV': {'FileHeaderInfo': 'Ignore', 'RecordDelimiter': '|\n', 'FieldDelimiter': '|'}},
            OutputSerialization={'CSV': {}}
        )

        self.event_stream = response['Payload']
        for event in self.event_stream:
            if 'Records' in event:
                print("Records Event")
                records_str = event['Records']['Payload'].decode('utf-8')
                records_rdr = csv.reader(io.StringIO(records_str))
                for r in records_rdr:
                    yield r

            elif 'Stats' in event:
                bytes_scanned = event['Stats']['Details']['BytesScanned']
                bytes_processed = event['Stats']['Details']['BytesProcessed']
                print("Stats Event: bytes scanned: {}, bytes processed: {}".format(bytes_scanned, bytes_processed))

            elif 'Progress' in event:
                print("Progress Event")

            elif 'End' in event:
                print("End Event")

            elif 'Cont' in event:
                print("Cont Event")

    def close(self):
        self.event_stream.close()


class LimitStrategy:
    OP = 'OP'
    NONE = 'NONE'

    def __init__(self):
        pass


class Operator:
    SELECT = 'SELECT'

    def __init__(self):
        pass
