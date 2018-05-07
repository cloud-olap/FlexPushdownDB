# -*- coding: utf-8 -*-
"""Limit top k query

Performs a partial "table" load by using the LIMIT keyword in S3 select. Note that the documentation at
https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference-select.html#s3-glacier-select-sql-reference-limit
does not specify precisely what limit does. Experimentation indicates it returns a number of rows less than the number
specified in the limit clause.

"""

import boto3
import csv
import io
import util.constants

LIMIT = 10

s3 = boto3.client('s3')

# The file header info is set to none as it allows us to pick up the field names for the data set.
#
# Note there is an extra | delimiter at the end of each record so need to use |\n as the record delimiter.
#
# Also not that the limit is incremented by one as specifying a limit of 11 (for example) means at most 10
# records will be returned.

response = s3.select_object_content(
    Bucket=util.constants.S3_BUCKET_NAME,
    Key='nation.csv',
    ExpressionType='SQL',
    Expression="select * from s3object s limit {};".format(LIMIT + 1),
    InputSerialization={'CSV': {'FileHeaderInfo': 'None', 'RecordDelimiter': '|\n', 'FieldDelimiter': '|'}},
    OutputSerialization={'CSV': {}}
)

event_stream = response['Payload']
is_end_event_received = False
i = 0
for event in event_stream:

    if 'Records' in event:
        records_str = event['Records']['Payload'].decode('utf-8')
        records_rdr = csv.DictReader(io.StringIO(records_str))

        for r in records_rdr:
            i += 1
            print("Row Event {}: {}".format(i, r))

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

event_stream.close()

if not is_end_event_received:
    raise Exception("End event not received, request incomplete.")
