# -*- coding: utf-8 -*-
"""Cursor support

"""

import boto3

from s3filter.util.timer import Timer


class S3Get(object):

    def __init__(self):
        self.s3key = "lineitem.csv"

        self.timer = Timer()

        # self.query_bytes = 0
        # self.bytes_scanned = 0
        # self.bytes_processed = 0
        self.bytes_returned = 0
        self.records_events = 0
        # self.rows_returned = 0

        self.timer = Timer()

        self.time_to_first_record_response = None
        self.time_to_last_record_response = None

    def run(self):

        s3 = boto3.client('s3')

        self.timer.start()

        response = s3.get_object(
            Bucket='s3filter',
            Key=self.s3key
        )

        streaming_body = response['Body']

        bytes_ = streaming_body.read(1024)
        self.time_to_first_record_response = self.timer.elapsed()
        while bytes_:
            self.records_events += 1
            self.bytes_returned += len(bytes_)
            bytes_ = streaming_body.read(1024)
            self.time_to_last_record_response = self.timer.elapsed()

        if streaming_body:
            streaming_body.close()

        elapsed_time = self.timer.elapsed()

        print("elapsed_time: {}".format(elapsed_time))
        print("time_to_first_record_response: {}".format(self.time_to_first_record_response))
        print("time_to_last_record_response: {}".format(self.time_to_last_record_response))
        print("bytes_returned: {}".format(self.bytes_returned))
        print("records_events: {}".format(self.records_events))

        bytes_sec = self.bytes_returned / elapsed_time

        print("bytes_sec: {} (KB {:f}, MB {:f})".format(bytes_sec, bytes_sec / 1000.0, bytes_sec / 1000000))


def main():
    s3_get = S3Get()
    s3_get.run()


if __name__ == "__main__":
    main()
