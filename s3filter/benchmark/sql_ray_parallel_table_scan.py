from s3filter.op.sql_ray_parallel_table_scan import SQLRayParallelShardedTableScan
from s3filter.op.collate import Collate
import sys


def main(parts):
    parallel_scanner = SQLRayParallelShardedTableScan('lineitem.csv', 'SELECT * FROM S3Object',
                                                           'sql_ray_parallel_sharded_table_scan',
                                                           parts, True)
    collate = Collate('parallel_scanner_collate', True)
    parallel_scanner.connect(collate)

    parallel_scanner.start()
    print('records count {}'.format(len(collate.tuples())))
    collate.print_tuples()
    collate.write_to_file('tuples.txt')


if __name__ == "__main__":
    main(int(sys.argv[1]))
