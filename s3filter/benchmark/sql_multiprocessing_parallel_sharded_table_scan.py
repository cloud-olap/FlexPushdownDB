from s3filter.op.sql_multiprocessing_parallel_sharded_table_scan import SQLMultiprocessingParallelShardedTableScan
from s3filter.op.collate import Collate
import sys


def main(parts):
    parallel_scanner = SQLMultiprocessingParallelShardedTableScan('lineitem.csv', 'SELECT * FROM S3Object',
                                                           'sql_multiprocessing_parallel_sharded_table_scan',
                                                           parts, True)
    collate = Collate('parallel_scanner_collate', True)
    parallel_scanner.connect(collate)

    parallel_scanner.start()
    print('records count {}'.format(len(collate.tuples())))
    print(collate.tuples()[-10:-1])


if __name__ == "__main__":
    main(int(sys.argv[1]))
