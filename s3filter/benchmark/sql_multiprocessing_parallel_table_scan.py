from s3filter.op.sql_multiprocessing_parallel_table_scan import SQLMultiprocessingParallelTableScan
from s3filter.op.collate import Collate
import sys


# def main(parts):
#     parallel_scanner = SQLMultiprocessingParallelTableScan('lineitem.csv', 'SELECT * FROM S3Object',
#                                                            'sql_multiprocessing_parallel_table_scan',
#                                                            parts, 'l_orderkey', True)
#     collate = Collate('parallel_scanner_collate', True)
#     parallel_scanner.connect(collate)
#
#     parallel_scanner.start()
#     print('records count {}'.format(len(collate.tuples())))
#     print(collate.tuples()[-10:-1])
#
#
# if __name__ == "__main__":
#     main(int(sys.argv[1]))
