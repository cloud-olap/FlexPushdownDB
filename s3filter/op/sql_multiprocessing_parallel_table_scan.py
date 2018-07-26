"""

"""
import os, sys
from multiprocessing import Process, Manager
from s3filter.op.message import TupleMessage
from s3filter.op.operator_base import Operator
from s3filter.op.tuple import Tuple, IndexedTuple
from s3filter.sql.cursor import Cursor
from s3filter.op.sql_table_scan import SQLTableScanMetrics

__author__ = "Abdurrahman Ghanem<abghanem@qf.org.qa>"

BY_TO_MB = 1 / (1000.0 * 1000.0)
BY_TO_KB = 1 / 1000.0

class SQLMultiprocessingParallelTableScan(Operator):
    """
    Scan S3 CSV table in parallel
    """

    def __init__(self, s3key, s3sql, name, parts, partitioning_key, query_plan, log_enabled):
        """Creates a new Table Scan operator that executes the given query on the table given in s3key in parallel.
        The parallelism factor is passed in the parts parameter. The table partitioning will be based on the key
        passed in split_on_key parameter

        :param s3key: The object key to select against
        :param s3sql: The s3 select sql
        :param parts: The parallelism factor (number of downloading threads)
        :param partitioning_key: The key used to partition the table records in order to distributed over workers
        """

        super(SQLMultiprocessingParallelTableScan, self).__init__(name, SQLTableScanMetrics(), query_plan, log_enabled)

        self.s3key = s3key
        self.s3sql = s3sql
        self.parts = parts
        self.partitioning_key = partitioning_key
        self.log_enabled = log_enabled
        self.ranges = []
        self.is_streamed = False

        # determining the range of the partitioning key values to be able to find the splitting intervals
        range_sql = '''
                    SELECT MIN(CAST({} AS int)) AS min_key, MAX(CAST({} AS int)) AS max_key \
                    FROM S3Object
        '''.format(self.partitioning_key, self.partitioning_key)

        self.min_key = 1        #l_orderkey min value
        self.max_key = 6000000  #l_orderkey max value
        self.ranges = []

        self.part_size = (self.max_key - self.min_key + 1) / self.parts
        for part in range(self.parts):
            start = part * self.part_size + 1
            end = start + self.part_size - 1
            self.ranges.append((start, end))
        self.ranges[-1] = (self.ranges[-1][0], self.max_key)

        manager = Manager()
        self.records = manager.list()
        self.worker_metrics = manager.dict()

    def start(self):
        self.op_metrics.timer_start()

        if self.parts == 1:
            self.records = []
            self.worker_metrics = {}
            self.download_part(0, self.records, self.worker_metrics)
        else:
            processes = []
            for part in range(self.parts):
                p = Process(target=self.download_part, args=(part, self.records, self.worker_metrics))
                p.start()
                processes.append(p)

            for p in processes:
                p.join()

        print("All parts finished with {} records".format(len(self.records)))

        first_tuple = True
        for msg in self.records:

            if first_tuple:
                # Create and send the record field names
                it = IndexedTuple.build_default(msg.tuple_)
                first_tuple = False

                if self.log_enabled:
                    print("{}('{}') | Sending field names: {}"
                          .format(self.__class__.__name__, self.name, it.field_names()))

                self.send(TupleMessage(Tuple(it.field_names())), self.consumers)

            self.send(msg, self.consumers)

        self.complete()
        self.op_metrics.timer_stop()
        self.print_stats(to_file=self.s3key + '.' + str(self.parts) +'.stats.txt')

        self.records[:] = []

    def download_part(self, part, records_queue, stats):
        print('Started downloading part {}'.format(part))
        part_range = self.ranges[part]
        part_sql = self.s3sql + ' and CAST({} AS int) >= {} AND CAST({} AS int) <= {}'.format(self.partitioning_key,
                                                                                                part_range[0],
                                                                                                self.partitioning_key,
                                                                                                part_range[1])

        op_metrics = SQLTableScanMetrics()
        op_metrics.timer_start()

        cur = Cursor(self.query_plan.s3).select(self.s3key, part_sql)

        tuples = cur.execute()

        op_metrics.query_bytes = cur.query_bytes
        op_metrics.time_to_first_response = op_metrics.elapsed_time()

        for t in tuples:
            op_metrics.rows_returned += 1
            tuple_msg = TupleMessage(Tuple(t))
            records_queue.append(tuple_msg)

        del tuples

        op_metrics.bytes_scanned = cur.bytes_scanned
        op_metrics.bytes_processed = cur.bytes_processed
        op_metrics.bytes_returned = cur.bytes_returned
        op_metrics.time_to_first_record_response = cur.time_to_first_record_response
        op_metrics.time_to_last_record_response = cur.time_to_last_record_response

        op_metrics.timer_stop()
        stats[part] = op_metrics

        print('Finished downloading part {} read {} records'.format(part, op_metrics.rows_returned))

    def print_stats(self, to_file=None):
        stats_str = ''
        stats_header = 'PART\tSIZE(MB)\tTOTAL(SEC)\tDOWNLOAD_RATE(MB/S)'
        stats_lines = stats_header

        cum_download_speed = 0
        for part in self.worker_metrics.keys():
            p_op_metrics = self.worker_metrics[part]
            size = p_op_metrics.bytes_returned
            self.op_metrics.bytes_returned += size
            elapsed_time = p_op_metrics.elapsed_time()
            part_size = size * BY_TO_MB
            download_speed = part_size / elapsed_time

            stats_str += '\n\n----- PART ' + str(part) + '----'
            stats_str += '\nSIZE: ' + str(part_size) + ' MB'
            stats_str += '\nTOTAL: ' + str(elapsed_time) + ' seconds'
            stats_str += '\nDOWNLOAD SPEED: ' + str(download_speed)

            cum_download_speed += download_speed

            stats_lines += '\n{}\t{}\t{}\t{}'.format(part, part_size, elapsed_time, download_speed)

        total_time = self.op_metrics.elapsed_time()
        total_speed = self.op_metrics.bytes_returned * BY_TO_MB / total_time
        stats_str += '\n\nTotal download time: ' + str(total_time)
        stats_str += '\nOverall Download Speed: ' + str(total_speed)
        stats_str += '\nCumulative download speed: ' + str(cum_download_speed)
        stats_lines += '\n\nTotal download time: ' + str(total_time)
        stats_lines += '\nOverall Download Speed: ' + str(total_speed)
        stats_lines += '\nCumulative Download Speed: ' + str(cum_download_speed)

        if to_file is not None:
            with open(to_file, 'w') as stats_file:
                stats_file.write(stats_lines)

        overall_stats_f = 'overall_stats_1.txt'
        overall_stats = ''
        if not os.path.exists(overall_stats_f):
            overall_stats += stats_header
        with open(overall_stats_f, 'a+') as overall_stats_f:
            overall_stats += '\n{}\t{}\t\t{}\t\t{}'.format(self.parts, self.op_metrics.bytes_returned * BY_TO_MB,
                                                            self.op_metrics.elapsed_time(), total_speed)
            overall_stats_f.write(overall_stats)

        print(stats_str)

    def on_producer_completed(self, _producer):
        """This event is overridden really just to indicate that it never fires.

        :param _producer: The completed producer
        :return: None
        """

        pass

    def __repr__(self):
        return {
            'elapsed_time': round(self.op_metrics.elapsed_time(), 5),
            'rows_returned': self.op_metrics.rows_returned,
            'query_bytes': self.op_metrics.query_bytes,
            'bytes_scanned': self.op_metrics.bytes_scanned,
            'bytes_processed': self.op_metrics.bytes_processed,
            'bytes_returned': self.op_metrics.bytes_returned,
            'time_to_first_response': round(self.op_metrics.time_to_first_response, 5),
            'time_to_first_record_response':
                None if self.op_metrics.time_to_first_record_response is None
                else round(self.op_metrics.time_to_first_record_response, 5),
            'time_to_last_record_response':
                None if self.op_metrics.time_to_last_record_response is None
                else round(self.op_metrics.time_to_last_record_response, 5)

        }.__repr__()
