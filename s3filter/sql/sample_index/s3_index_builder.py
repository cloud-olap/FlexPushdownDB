"""
Build and retrieve byte range index of a CSV table on s3
"""

import boto3
import pandas as pd
from s3filter.util.filesystem_util import *
from s3filter.util.constants import *
from collections import defaultdict

__author__ = "Abdurrahman Ghanem <abghanem@qf.org.qa>"

BLOCK_SIZE = 100000


class IndexHandler:
    tables_sizes = defaultdict(int)

    def __init__(self, s3key):
        self.s3key = s3key
        self.table_local_file_path = None
        self.index_local_path = None
        self.s3_index_path = None
        self.s3_size_path = None
        self.s3_client = boto3.resource('s3')
        self.build_index()

    def build_index(self):
        """
        takes s3 table name, if does not exist, an index file mapping the position of the record within the table to
        the byte range of the corresponding row
        """
        proj_dir = '/Volumes/ghanemabdo/s3filter/'  # os.environ['PYTHONPATH'].split(":")[0]
        table_loc = os.path.join(proj_dir, TABLE_STORAGE_LOC)

        table_local_file_path = os.path.join(table_loc, self.s3key)

        index_loc = os.path.join(table_loc, 'indexes')
        index_loc = os.path.join(index_loc, self.s3key.split('.')[0] + '_index')
        create_dirs(index_loc)
        index_local_path = os.path.join(index_loc, os.path.basename(self.s3key).split('.')[0] + '_index')

        self.table_local_file_path = table_local_file_path
        self.index_local_path = index_local_path

        s3_dir = os.path.dirname(self.s3key)
        s3_name = os.path.basename(self.s3key)
        self.s3_index_path = '{}/index/seq_index_{}'.format(s3_dir, s3_name)
        self.s3_size_path = '{}/index/size_index_{}'.format(s3_dir, s3_name)
        if self.s3_index_exists(self.s3_index_path):
            self.get_table_size()
            return self.s3_index_path

        if not os.path.exists(table_loc):
            os.makedirs(table_loc)
        if not os.path.exists(table_local_file_path):
            create_file_dirs(table_local_file_path)
            s3_client = boto3.client('s3')
            s3_client.download_file(
                Bucket=S3_BUCKET_NAME,
                Key=self.s3key,
                Filename=table_local_file_path
            )

        if os.path.exists(table_local_file_path):
            got_header = False
            index = 0
            current_byte = 0
            with open(table_local_file_path, 'r') as table_file:
                create_file_dirs(index_loc)
                with open(index_local_path, 'w') as index_file:
                    index_file.write('serial|first_byte|last_byte|\n')
                    for row in table_file:
                        if not got_header:
                            got_header = True
                            current_byte += len(row)
                            continue

                        index_file.write('{}|{}|{}|\n'.format(index, current_byte, current_byte + len(row)))
                        current_byte += len(row)
                        index += 1

                    IndexHandler.tables_sizes[self.s3key] = index
                    self.s3_client.Object(S3_BUCKET_NAME, self.s3_size_path).put(Body=str(index).encode('utf-8'))
                    index_file.flush()
                    self.s3_client.Object(S3_BUCKET_NAME, self.s3_index_path).put(Body=open(index_local_path, 'rb'))

        return index_local_path

    def get_table_size(self):
        if self.s3key in IndexHandler.tables_sizes:
            return IndexHandler.tables_sizes[self.s3key]

        obj = self.s3_client.Object(S3_BUCKET_NAME, self.s3_size_path)
        IndexHandler.tables_sizes[self.s3key] = int(obj.get()['Body'].read().decode('utf-8'))
        return IndexHandler.tables_sizes[self.s3key]

    def s3_index_exists(self, key):
        bucket = self.s3_client.Bucket('s3filter')
        objs = list(bucket.objects.filter(Prefix=key))

        return len(objs) > 0 and objs[0].key == key

    def get_s3_index_path(self):
        return self.s3_index_path


if __name__ == "__main__":
    indx_hndl = IndexHandler('tpch-sf1/lineitem.csv')
    rec_ranges = indx_hndl.build_record_ranges([0, 100, 200000, 300000, 1000000, 5000000], 50)
    byte_ranges = indx_hndl.get_byte_ranges(rec_ranges)
    print(byte_ranges)
    proj_dir = '/Volumes/ghanemabdo/s3filter/'  # os.environ['PYTHONPATH'].split(":")[0]
    table_loc = os.path.join(proj_dir, TABLE_STORAGE_LOC)

    table_local_file_path = os.path.join(table_loc, 'tpch-sf1/lineitem.csv')

    with open(table_local_file_path, 'rb') as tbl:
        cur_offset = 0
        for rng in byte_ranges.values:
            tbl.seek(rng[0] - cur_offset, 1)
            data = tbl.read(rng[1] - rng[0])
            print(str(data))
            cur_offset = rng[1]
