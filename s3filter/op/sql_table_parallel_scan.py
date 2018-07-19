import os, sys
from multiprocessing import Process, Array, Manager
from s3filter.op.message import TupleMessage
from s3filter.op.operator_base import Operator
from s3filter.op.tuple import Tuple, IndexedTuple
from s3filter.sql.cursor import Cursor
from s3filter.op.sql_table_scan import SQLTableScanMetrics
from s3filter.util import constants
__author__ = "Abdurrahman Ghanem<abghanem@qf.org.qa>"

BY_TO_MB = 1 / (1000.0 * 1000.0)
BY_TO_KB = 1 / 1000.0


class SQLTableParallelScan(Operator):
    """
    Scan S3 CSV table in parallel
    """

    def __init__(self, s3key, s3sql, name, parts, processes, log_enabled):
        """Creates a new Table Scan operator that executes the given query on the table given in s3key in parallel.
        The parallelism factor is passed in the parts parameter. The table partitioning will be based on the key
        passed in split_on_key parameter

        :param s3key: The object key to select against
        :param s3sql: The s3 select sql
        :param parts: The parallelism factor (number of downloading threads)
        """

        super(SQLTableParallelScan, self).__init__(name, SQLTableScanMetrics(), log_enabled)

        self.s3key = s3key
        self.s3sql = s3sql
        self.parts = parts
        self.processes = processes
        self.log_enabled = log_enabled
        self.ranges = []
        self.is_streamed = True

        self.records = []
    def start(self):
        self.op_metrics.timer_start()

        procs = [0] * self.processes
        for pid in range(self.processes):
            parts = [x for x in range(self.parts) if x % self.processes == pid]
            procs[pid] = Process(target=self.download_part, args=(pid, parts, self.records))
            procs[pid].start()

        for p in procs:
            p.join()

        print("All parts finished")

        self.complete()
        self.op_metrics.timer_stop()
        self.print_stats(to_file=self.s3key + '.' + str(self.parts) +'.stats.txt')

        self.records[:] = []

    def download_part(self, pid, parts, records_queue):
        bytes_scanned = 0
        bytes_processed = 0
        rows_returned = 0
        for part in parts:
            part_key = self.get_part_key('sf1000-lineitem', part) 
            print('Started do wnloading part {} key {}'.format(part, part_key))

            cur = Cursor().select(part_key, self.s3sql)

            tuples = cur.execute()

            for t in tuples:
                rows_returned += 1
                records_queue.append(t)

            del tuples
            bytes_scanned += cur.bytes_scanned
            bytes_processed += cur.bytes_processed
        print('pid={}, byte_scanned={}, bytes_processed={}'.format(pid, bytes_scanned, bytes_processed)   )

    def print_stats(self, to_file=None):
        stats_str = ''
        stats_header = 'PART\tSIZE(MB)\tTOTAL(SEC)\tDOWNLOAD_RATE(MB/S)'
        stats_lines = stats_header

        cum_download_speed = 0

        total_time = self.op_metrics.elapsed_time()
        print('total_time={}'.format(total_time))

    def on_producer_completed(self, _producer):
        """This event is overridden really just to indicate that it never fires.

        :param _producer: The completed producer
        :return: None
        """

        pass

    def get_part_key(self, parts_prefix, part):
        fname = os.path.basename(self.s3key)
        filename, ext = os.path.splitext(fname)
        return '{}/{}_{}{}'.format(parts_prefix, filename, part, ext)

    def __repr__(self):
        return {
            'elapsed_time': round(self.elapsed_time(), 5),
            'rows_returned': self.rows_returned,
            'query_bytes': self.query_bytes,
            'bytes_scanned': self.bytes_scanned,
            'bytes_processed': self.bytes_processed,
            'bytes_returned': self.bytes_returned,
            'time_to_first_response': round(self.time_to_first_response, 5),
            'time_to_first_record_response':
                None if self.time_to_first_record_response is None
                else round(self.time_to_first_record_response, 5),
            'time_to_last_record_response':
                None if self.time_to_last_record_response is None
                else round(self.time_to_last_record_response, 5)

        }.__repr__()
