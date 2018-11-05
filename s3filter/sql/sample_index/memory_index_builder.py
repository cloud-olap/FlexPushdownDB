"""
Build and retrieve byte range index of a CSV table on s3
"""

import boto3
import pandas as pd
from s3filter.util.filesystem_util import *
from s3filter.util.constants import *
import pandas as pd
import numpy as np

__author__ = "Abdurrahman Ghanem <abghanem@qf.org.qa>"

BLOCK_SIZE = 100000


class MemoryIndexHandler:

    loaded_indexes = dict()

    def __init__(self, s3key):
        self.s3key = s3key
        self.table_local_file_path = None
        self.index_local_path = None
        self.s3_index_path = None
        self.s3_size_path = None
        self.s3_client = boto3.resource('s3')
        self.index = []
        self.build_index()

    def build_index(self):
        """
        takes s3 table name, if does not exist, an index file mapping the position of the record within the table to
        the byte range of the corresponding row. The index of the whole table is loaded in the memory
        """
        proj_dir = os.environ['PYTHONPATH'].split(":")[0] #'/Volumes/ghanemabdo/s3filter/'
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
        if self.s3_index_exists(self.s3_index_path):
            self.get_table_size()
            self.load_index()
            return self.s3_index_path

        MemoryIndexHandler.download_key(self.s3key, table_loc)

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

                        row_rng = (current_byte, current_byte + len(row))
                        index_file.write('{}|{}|{}|\n'.format(index, row_rng[0], row_rng[1]))
                        self.index.append((row_rng[0], row_rng[1]))
                        current_byte += len(row)
                        index += 1

                    index_file.flush()
                    self.s3_client.Object(S3_BUCKET_NAME, self.s3_index_path).put(Body=open(index_local_path, 'rb'))

        return index_local_path

    @staticmethod
    def download_key(key, filepath):
        if not os.path.exists(filepath):
            os.makedirs(filepath)
            create_file_dirs(filepath)
            s3_client = boto3.client('s3')
            s3_client.download_file(
                Bucket=S3_BUCKET_NAME,
                Key=key,
                Filename=filepath
            )

    def load_index(self, delete_after_loading=False):

        if self.s3key in MemoryIndexHandler.loaded_indexes:
            self.index = MemoryIndexHandler.loaded_indexes[self.s3key]
            return

        print('loading into memory index {}'.format(self.index_local_path))
        MemoryIndexHandler.download_key(self.s3_index_path, self.index_local_path)
        index_df = pd.read_csv(self.index_local_path, delimiter='|',
                    header=None,
                    prefix='_', dtype=np.str,
                    engine='c', quotechar='"', na_filter=False, compression=None, low_memory=False,
                    skiprows=1, skip_blank_lines=True)
        index_df[index_df.columns[1:3]] = index_df[index_df.columns[1:3]].astype(np.int64)
        for row in index_df.values:
            self.index.append((row[1], row[2]))

        MemoryIndexHandler.loaded_indexes[self.s3key] = self.index

        if delete_after_loading:
            os.remove(self.index_local_path)

    def get_table_size(self):
        self.load_index()
        return len(self.index)

    def s3_index_exists(self, key):
        bucket = self.s3_client.Bucket('s3filter')
        objs = list(bucket.objects.filter(Prefix=key))

        return len(objs) > 0 and objs[0].key == key

    def get_s3_index_path(self):
        return self.s3_index_path

    def build_byte_ranges(self, record_ranges):
        byte_ranges = []

        for rng in record_ranges:
            first_rec_rng = self.index[rng[0]]
            second_rec_rng = self.index[rng[1]]
            byte_ranges.append((first_rec_rng[0], second_rec_rng[1] - 1))

        return byte_ranges
