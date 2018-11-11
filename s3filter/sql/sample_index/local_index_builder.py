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

    indicies = defaultdict(lambda: defaultdict(dict))
    tables_sizes = defaultdict(int)

    def __init__(self, s3key):
        self.s3key = s3key
        self.table_local_file_path = None
        self.index_local_path = None
        IndexHandler.indicies[self.s3key] = dict()
        self.s3_client = boto3.resource('s3')
        self.build_index()

    def build_index(self):
        """
        takes s3 table name, if does not exist, an index file mapping the position of the record within the table to
        the byte range of the corresponding row
        """
        proj_dir = '/Volumes/ghanemabdo/s3filter/'#os.environ['PYTHONPATH'].split(":")[0]
        table_loc = os.path.join(proj_dir, TABLE_STORAGE_LOC)

        table_local_file_path = os.path.join(table_loc, self.s3key)

        index_loc = os.path.join(table_loc, 'indexes')
        index_loc = os.path.join(index_loc, self.s3key.split('.')[0] + '_index')
        create_dirs(index_loc)
        index_local_path = os.path.join(index_loc, os.path.basename(self.s3key).split('.')[0] + '_index')

        self.table_local_file_path = table_local_file_path
        self.index_local_path = index_local_path

        if os.path.exists(index_local_path + '_0.indx'):
            return index_local_path

        s3_dir = os.path.dirname(self.s3key)
        s3_name = os.path.basename(self.s3key)
        s3_index_path = '{}/index/seq_index_{}'.format(s3_dir, s3_name)
        if self.index_exists(s3_index_path):
            return s3_index_path

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
                try:
                    index_file = open('{}_{}.indx'.format(index_local_path,
                                                          IndexHandler.get_index_block(index)), 'w')
                    # index_file.write('{}|first_byte|last_byte|\n'.format('|'.join(col_names)))
                    for row in table_file:
                        if not got_header:
                            got_header = True
                            current_byte += len(row)
                            continue

                        # index_file.write('{}|{}|{}|\n'.format(index, current_byte, current_byte + len(row)))
                        index_file.write('{}|{}|\n'.format(current_byte, current_byte + len(row)))
                        current_byte += len(row)
                        index += 1

                        if index % BLOCK_SIZE == 0:
                            index_file.close()
                            index_file = open('{}_{}.indx'.format(index_local_path,
                                                                  IndexHandler.get_index_block(index)), 'w')

                    index_file.close()

                    with open('{}_records_count.indx'.format(index_local_path,
                                                             IndexHandler.get_index_block(index)), 'w') as count_file:
                        count_file.write('{}|{}'.format(index, BLOCK_SIZE))

                    IndexHandler.tables_sizes[self.s3key] = index

                except IOError as ex:
                    raise Warning('Can not create table {} index. Error: {}'.format(self.s3key, ex.message))
                    return None

        return index_local_path

    def __load_index(self, index_ranges):
        """
        given the table name and the indexing columns, build and return the index map
        :param index_ranges: the ranges to retrieve by ranges for
        :return: hash table from filed values to their corresponding byte ranges
        """
        index_local_path = self.build_index()

        if os.path.exists(index_local_path + '_0.indx'):
            for in_rng in index_ranges:
                rng_blks = IndexHandler.get_range_blocks(in_rng)
                for i in rng_blks:
                    self.__load_index_block(index_local_path, i)

    def __load_index_block(self, index_path, block_index):
        if block_index not in IndexHandler.indicies[self.s3key]:
            with open('{}_{}.indx'.format(index_path, block_index), 'r') as index_file:
                block_items = []
                for record in index_file:
                    record_comps = record.split('|')[:-1]
                    block_items.append((int(record_comps[0]), int(record_comps[1])))

                IndexHandler.indicies[self.s3key][block_index] = block_items

    def build_record_ranges(self, values, fragment_size=1):
        """
        Retrieve the byte range of a records in a table given its location in the table. It also loads the index
        blocks needed to retrieve byte ranges for the given record ranges
        :param values: values to look up their byte ranges
        :param fragment_size: whether retrieve a number of following records to batch records fetching
        :return: list of record ranges
        """
        ranges = []
        for value in values:
            ranges.append((value, value + fragment_size - 1))

        self.__load_index(ranges)
        return ranges

    def get_byte_ranges(self, record_ranges):
        """
        Retrieve the byte range of a records in a table given its location in the table
        :param: ranges of records to obtain the byte ranges for
        :return: pandas dataframe of ranges to fetch
        """
        byte_ranges = []
        for rng in record_ranges:
            rng_start = self.get_byte_range_for_index(rng[0])
            rng_end = self.get_byte_range_for_index(rng[1])

            if rng_start is not None and rng_end is not None:
                byte_ranges.append((rng_start[0], rng_end[1]-2))

        return pd.DataFrame(data=byte_ranges, columns=['start', 'end'])

    def get_byte_range_for_index(self, index):
        block = IndexHandler.get_index_block(index)
        pos = IndexHandler.get_position_in_block(index)

        if block in IndexHandler.indicies[self.s3key]:
            block_list = IndexHandler.indicies[self.s3key][block]
            if pos < len(block_list):
                return block_list[pos]
            else:
                return block_list[len(block_list)-1]

    def get_table_size(self):
        if self.s3key not in IndexHandler.tables_sizes:
            self.__load_table_stats()
        return IndexHandler.tables_sizes[self.s3key]

    def __load_table_stats(self):
        with open('{}_records_count.indx'.format(self.index_local_path), 'r') as table_stats_file:
            stats = table_stats_file.read()
            IndexHandler.tables_sizes[self.s3key] = int(stats.split('|')[0])

    def index_exists(self, key):
        bucket = self.s3_client.Bucket('s3filter')
        objs = list(bucket.objects.filter(Prefix=key))

        return len(objs) > 0 and objs[0].key == key

    @staticmethod
    def get_index_block(index):
        return index / BLOCK_SIZE

    @staticmethod
    def get_position_in_block(index):
        return index % BLOCK_SIZE

    @staticmethod
    def get_range_blocks(rng):
        return [(x / BLOCK_SIZE) for x in range(rng[0], rng[1], BLOCK_SIZE)]


if __name__ == "__main__":
    indx_hndl = IndexHandler('tpch-sf1/lineitem.csv')
    rec_ranges = indx_hndl.build_record_ranges([0, 100, 200000, 300000, 1000000, 5000000], 50)
    byte_ranges = indx_hndl.get_byte_ranges(rec_ranges)
    print(byte_ranges)
    proj_dir = '/Volumes/ghanemabdo/s3filter/'#os.environ['PYTHONPATH'].split(":")[0]
    table_loc = os.path.join(proj_dir, TABLE_STORAGE_LOC)

    table_local_file_path = os.path.join(table_loc, 'tpch-sf1/lineitem.csv')

    with open(table_local_file_path, 'rb') as tbl:
        cur_offset = 0
        for rng in byte_ranges.values:
            tbl.seek(rng[0] - cur_offset, 1)
            data = tbl.read(rng[1] - rng[0])
            print(str(data))
            cur_offset = rng[1]
