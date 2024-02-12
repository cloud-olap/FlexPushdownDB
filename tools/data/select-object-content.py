"""
Copies an object from one s3 bucket to another

"""
import os

import boto3 as boto3
import pyarrow
import pyarrow.csv
import pyarrow.parquet


def cat(src_bucket,
        src_object,
        sql,
        src_s3_client):
    response = src_s3_client.select_object_content(Bucket=src_bucket,
                                                   Key=src_object,
                                                   Expression=sql,
                                                   ExpressionType="SQL",
                                                   InputSerialization={
                                                       "CSV": {
                                                           "FileHeaderInfo": "USE",
                                                       }
                                                   },
                                                   OutputSerialization={
                                                       "CSV": {}
                                                   })
    buffer = b''
    event_stream = response['Payload']
    end_event_received = False
    # Iterate over events in the event stream as they come
    for event in event_stream:
        # If we received a records event, write the data to a file
        if 'Records' in event:
            chunk = event['Records']['Payload']
            buffer += chunk
        # If we received a progress event, print the details
        elif 'Progress' in event:
            print(event['Progress']['Details'])
        # End event indicates that the request finished successfully
        elif 'End' in event:
            end_event_received = True
            break
    if not end_event_received:
        raise Exception("End event not received, request incomplete.")

    table = pyarrow.csv.read_csv(pyarrow.py_buffer(buffer),
                                 read_options=pyarrow.csv.ReadOptions(autogenerate_column_names=True))

    print(f"Selected CSV (bucket: '{src_bucket}', object: '{src_object}', sql: '{sql}')")

    return table


def main():
    src_bucket = "flexpushdowndb"

    src_host = "localhost"
    src_port = 8010
    src_aws_access_key_id = "test-test"
    src_aws_secret_access_key = "test"

    src_session = boto3.Session(aws_access_key_id=src_aws_access_key_id,
                                aws_secret_access_key=src_aws_secret_access_key)
    src_s3_client = src_session.client('s3',
                                       endpoint_url=f"http://{src_host}:{src_port}/s3/test")

    table = cat(src_bucket, "ssb-sf1/csv/date.tbl", "SELECT d_datekey, d_year FROM s3Object WHERE cast(d_year as int) = 1992", src_s3_client)
    df = table.to_pandas()
    print(f"{os.linesep}{df}")

    table2 = cat(src_bucket, "ssb-sf1/csv/lineorder.tbl", "SELECT lo_orderdate, lo_extendedprice, lo_discount, lo_quantity FROM s3Object WHERE (cast(lo_discount as int) >= 1 and cast(lo_discount as int) <= 3) and (cast(lo_quantity as int) < 25)", src_s3_client)
    df2 = table2.to_pandas()
    print(f"{os.linesep}{df2}")


if __name__ == '__main__':
    main()
