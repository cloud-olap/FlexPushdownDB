# -*- coding: utf-8 -*-
"""Simple top k query

Performs a full "table" load and simply stops iterating through the response once the desired
record count limit has been reached. This is intended to demonstrate the worst case/most naive
method of loading of records.

"""

import boto3
import csv
import io
import util.constants

LIMIT = 10

s3 = boto3.client('s3')

# The file header info is set to none as it allows us to pick up the field names for the data set.
#
# Also note there is an extra | delimiter at the end of each record so need to use |\n as the record delimiter.

response = s3.select_object_content(
    Bucket=util.constants.S3_BUCKET_NAME,
    Key='nation.csv',
    ExpressionType='SQL',
    Expression="select * from s3object s;",
    InputSerialization={'CSV': {'FileHeaderInfo': 'None', 'RecordDelimiter': '|\n', 'FieldDelimiter': '|'}},
    OutputSerialization={'CSV': {}}
)

event_stream = response['Payload']
is_end_event_received = False
is_aborted = False
i = 0
for event in event_stream:

    if 'Records' in event:
        records_str = event['Records']['Payload'].decode('utf-8')
        records_rdr = csv.DictReader(io.StringIO(records_str))

        for r in records_rdr:
            i += 1
            print("Row Event {}: {}".format(i, r))

            if i >= LIMIT:
                is_aborted = True
                break

            # for k, v in row.items():
            #     print(k, v)

    elif 'Stats' in event:
        bytes_scanned = event['Stats']['Details']['BytesScanned']
        bytes_processed = event['Stats']['Details']['BytesProcessed']
        print("Stats Event: bytes scanned: {}, bytes processed: {}".format(bytes_scanned, bytes_processed))

    elif 'Progress' in event:
        print("Progress Event")

    elif 'End' in event:
        is_end_event_received = True
        print("End Event")

    elif 'Cont' in event:
        print("Cont Event")

    if is_aborted:
        break

event_stream.close()

if not is_aborted and not is_end_event_received:
    raise Exception("End event not received, request incomplete.")
