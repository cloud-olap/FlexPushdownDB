"""Cursor support

"""

import boto3
from s3filter.util.timer import Timer


class S3Select(object):

    def __init__(self):
        self.s3key = "lineitem.csv"
        self.s3sql = "select * from s3Object"
        self.event_stream = None

        self.timer = Timer()

        self.time_to_first_record_response = None
        self.time_to_last_record_response = None

        self.query_bytes = 0
        self.bytes_scanned = 0
        self.bytes_processed = 0
        self.bytes_returned = 0
        self.records_events = 0
        self.rows_returned = 0

        self.timer = Timer()

        self.prev_record_str = None

    def parse_event(self, event):
        if 'Records' in event:

            # print("Records")

            elapsed_time = self.timer.elapsed()
            if self.time_to_first_record_response is None:
                self.time_to_first_record_response = elapsed_time

            self.time_to_last_record_response = elapsed_time

            # records_str = event['Records']['Payload'].decode('utf-8')
            #
            # records_str_rdr = io.StringIO(records_str)
            #
            # for record_str in records_str_rdr:
            #
            #     # Check record ends with newline (excluding an escaped newline)
            #     if record_str.endswith('\n') and not record_str.endswith('\\n'):  # It's a complete record
            #
            #         if prev_record_str is not None:  # There was an incomplete record present in the last payload
            #             # Append the current record to the previous incomplete record
            #             # print("Appending: {} {}".format(prev_record_str, record_str))
            #             record_str = prev_record_str + record_str
            #             prev_record_str = None
            #
            #         # print("Complete: {}".format(record_str))
            #
            #         # Parse CSV
            #         record_rdr = csv.reader([record_str])
            #         if PYTHON_3:
            #             record = next(record_rdr)
            #         else:
            #             record = record_rdr.next()
            #
            #         # Do something with the record
            #         rows_returned += 1
            #
            #     else:
            #         # It's an incomplete record, save for next iteration
            #         # print("Incomplete: {}".format(record_str))
            #         prev_record_str = record_str
            self.records_events += 1

        elif 'Stats' in event:
            self.bytes_scanned += event['Stats']['Details']['BytesScanned']
            self.bytes_processed += event['Stats']['Details']['BytesProcessed']
            self.bytes_returned += event['Stats']['Details']['BytesReturned']
            print("Stats Event: bytes scanned: {}, bytes processed: {}, bytes returned: {}"
                  .format(self.bytes_scanned, self.bytes_processed, self.bytes_returned))

        elif 'Progress' in event:
            # pass
            print("Progress Event")

        elif 'End' in event:
            print("End Event")
            # pass

        elif 'Cont' in event:
            # pass
            print("Cont Event")

    def run(self):

        self.query_bytes = len(self.s3key.encode('utf-8')) + len(self.s3sql.encode('utf-8'))

        s3 = boto3.client('s3')

        self.timer.start()

        response = s3.select_object_content(
            Bucket='s3filter',
            Key=self.s3key,
            ExpressionType='SQL',
            Expression=self.s3sql,
            InputSerialization={'CSV': {'FileHeaderInfo': 'Use', 'RecordDelimiter': '|\n', 'FieldDelimiter': '|'}},
            OutputSerialization={'CSV': {}}
        )

        event_stream = response['Payload']

        map(self.parse_event, event_stream)

        if event_stream:
            event_stream.close()

        elapsed_time = self.timer.elapsed()

        print("elapsed_time: {}".format(elapsed_time))
        print("time_to_first_record_response: {}".format(self.time_to_first_record_response))
        print("time_to_last_record_response: {}".format(self.time_to_last_record_response))
        print("query_bytes: {}".format(self.query_bytes))
        print("bytes_scanned: {}".format(self.bytes_scanned))
        print("bytes_processed: {}".format(self.bytes_processed))
        print("bytes_returned: {}".format(self.bytes_returned))
        print("records_events: {}".format(self.records_events))
        print("rows_returned: {}".format(self.rows_returned))

        bytes_sec = self.bytes_returned / elapsed_time

        print("bytes_sec: {} (KB {:f}, MB {:f})".format(bytes_sec, bytes_sec / 1000.0, bytes_sec / 1000000))


def main():
    s3_select = S3Select()
    s3_select.run()


# if __name__ == "__main__":
main()

