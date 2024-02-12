"""
Copies an object from one s3 bucket to another

"""
import math
import pathlib
import tempfile
import time

import boto3 as boto3
import botocore.exceptions
from boto3.s3.transfer import TransferConfig

use_cache = True


def on_downloaded(bytes_downloaded):
    print(f"Downloaded {bytes_downloaded} bytes", flush=True)


def on_uploaded(bytes_uploaded):
    print(f"Uploaded {bytes_uploaded} bytes", flush=True)


def cp(src_bucket,
       src_object,
       dst_bucket,
       dst_object,
       src_s3_client,
       dst_s3_client):

    print(f"Copying file '{src_bucket}/{src_object}' to '{dst_bucket}/{dst_object}'")

    # tmp_root_dir = tempfile.mkdtemp()
    tmp_root_dir = f"{tempfile.gettempdir()}/fpdb/cp"
    tmp_file = pathlib.Path(tmp_root_dir) / src_bucket / src_object
    tmp_dir = tmp_file.parent
    tmp_dir.mkdir(parents=True, exist_ok=True)

    do_download = True
    if use_cache:
        if not tmp_file.exists():
            print(f"File not yet downloaded to cache")
            do_download = True
        else:
            now_ms = math.floor(time.time() * 1000)
            mtime_ms = math.floor(tmp_file.stat().st_mtime * 1000)
            if now_ms - mtime_ms > 15 * 60 * 1000:
                print("File downloaded to cache more than 15 minutes ago")
                do_download = True
            else:
                print("File downloaded to cache less than 15 minutes ago")
                do_download = False
    else:
        do_download = True

    if do_download:
        print("Downloading file...")
        src_s3_client.download_file(src_bucket, src_object, str(tmp_file), Callback=on_downloaded)
        print("Done")

    print("Uploading file...")
    try:
        dst_s3_client.head_bucket(Bucket=dst_bucket)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] != '404':
            raise e
        else:
            print(f"Creating bucket '{dst_bucket}'...")
            dst_s3_client.create_bucket(Bucket=dst_bucket)

    dst_s3_client.upload_file(str(tmp_file), dst_bucket, dst_object, Callback=on_uploaded,
                              Config=TransferConfig(multipart_threshold=5 * 1024 * 1024 * 1024, use_threads=False))
    print("Done")


def main():
    src_bucket = "flexpushdowndb"
    src_objects = ["ssb-sf1/csv/customer.tbl", "ssb-sf1/csv/date.tbl", "ssb-sf1/csv/lineorder.tbl",
                   "ssb-sf1/csv/part.tbl", "ssb-sf1/csv/supplier.tbl"]

    dst_host = "localhost"
    dst_port = 8010
    dst_bucket = "flexpushdowndb"
    dst_objects = src_objects
    dst_aws_access_key_id = "test-test"
    dst_aws_secret_access_key = "test"

    src_session = boto3.Session(profile_name='FlexPushdownDB')
    src_s3_client = src_session.client('s3', "us-east-2")

    dst_session = boto3.Session(aws_access_key_id=dst_aws_access_key_id,
                                aws_secret_access_key=dst_aws_secret_access_key)
    dst_s3_client = dst_session.client('s3',
                                       endpoint_url=f"http://{dst_host}:{dst_port}/s3/test")

    for src_object, dst_object in zip(src_objects, dst_objects):
        cp(src_bucket, src_object, dst_bucket, dst_object, src_s3_client, dst_s3_client)


if __name__ == '__main__':
    main()
