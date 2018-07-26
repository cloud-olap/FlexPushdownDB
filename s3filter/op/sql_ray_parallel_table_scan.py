"""

"""
import os
import ray
import numpy as np
from s3filter.op.message import TupleMessage
from s3filter.op.operator_base import Operator
from s3filter.op.tuple import Tuple
from s3filter.sql.cursor import Cursor
from s3filter.op.sql_table_scan import SQLTableScanMetrics

__author__ = "Abdurrahman Ghanem<abghanem@qf.org.qa>"

BY_TO_MB = 1 / (1000.0 * 1000.0)
BY_TO_KB = 1 / 1000.0


class SQLRayParallelShardedTableScan(Operator):
    """
    Scan S3 CSV table in parallel
    """

    def __init__(self, s3key, s3sql, name, parts, log_enabled):
        """Creates a new Table Scan operator that executes the given query on the table given in s3key in parallel.
        The parallelism factor is passed in the parts parameter. The table partitioning will be based on the key
        passed in split_on_key parameter

        :param s3key: The object key to select against
        :param s3sql: The s3 select sql
        :param parts: The parallelism factor (number of downloading threads)
        """

        super(SQLRayParallelShardedTableScan, self).__init__(name, SQLTableScanMetrics(), log_enabled)

        self.s3key = s3key
        self.s3sql = s3sql
        self.parts = parts
        self.log_enabled = log_enabled
        self.ranges = []
        self.is_streamed = False
        self.records = []
        self.worker_metrics = {}

        ray.init()

    def start(self):
        self.op_metrics.timer_start()

        if self.parts == 1:
            self.records, part, part_op_metrics = download_part_local(self.s3sql, 0, self.s3key, self.records,
                                                                            self.worker_metrics)
            self.worker_metrics[part] = part_op_metrics
        else:
            result_ids = [download_part_remote.remote(self.s3sql, part, self.get_part_key('sf1000-lineitem', part))
                          for part in range(self.parts)]

            for result_id, part_id, part_op_metrics_id in result_ids:
                res_records = ray.get(result_id)
                part = ray.get(part_id)
                part_metrics = ray.get(part_op_metrics_id)
                self.worker_metrics[part] = part_metrics
                self.records.append(res_records)
                print('got {} records from part {}'.format(len(res_records), part))
                # self.send(msg, self.consumers)

        self.records = np.vstack(self.records)
        print("All parts finished")
        print('got {} records'.format(len(self.records)))

        for rec in self.records[0:10]:
            self.send(TupleMessage(Tuple(rec)), self.consumers)

        self.complete()
        self.op_metrics.timer_stop()
        self.print_stats(to_file=self.s3key + '.' + str(self.parts) +'.stats.txt')

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

            stats_str += '\n\n------- PART {} ------'.format(part)
            stats_str += '\nSIZE: {} MB'.format(part_size)
            stats_str += '\nTOTAL Time: {} seconds'.format(elapsed_time)
            stats_str += '\nDOWNLOAD SPEED: {} MB/Sec'.format(download_speed)

            cum_download_speed += download_speed

            stats_lines += '\n{}\t{}\t{}\t{}'.format(part, part_size, elapsed_time, download_speed)

        total_time = self.op_metrics.elapsed_time()
        total_speed = self.op_metrics.bytes_returned * BY_TO_MB / total_time

        stats_str += '\n\nTotal download time: {} Sec'.format(total_time)
        stats_str += '\nOverall Download Speed: {} MB/Sec'.format(total_speed)
        stats_str += '\nCumulative download speed: {} MB/Sec'.format(cum_download_speed)
        stats_str += '\nAVG download speed: {} MB/Sec'.format(cum_download_speed / self.parts)

        stats_lines += '\n\nTotal download time: {} Sec'.format(total_time)
        stats_lines += '\nOverall Download Speed: {} MB/Sec'.format(total_speed)
        stats_lines += '\nCumulative Download Speed: {} MB/Sec'.format(cum_download_speed)
        stats_lines += '\nAVG download speed: {} MB/Sec'.format(cum_download_speed / self.parts)

        if to_file is not None:
            with open(to_file, 'w') as stats_file:
                stats_file.write(stats_lines)

        overall_stats_f = 'overall_stats_ray_collecting_records.txt'
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


@ray.remote(num_return_vals=3)
def download_part_remote(s3sql, part, part_key):
    return download_part_local(s3sql, part, part_key)


def download_part_local(s3sql, part, part_key):
    print('Started downloading part {} key {}'.format(part, part_key))

    op_metrics = SQLTableScanMetrics()
    op_metrics.timer_start()

    cur = Cursor().select(part_key, s3sql)

    tuples = cur.execute()

    op_metrics.query_bytes = cur.query_bytes
    op_metrics.time_to_first_response = op_metrics.elapsed_time()

    records = []
    for t in tuples:
        op_metrics.rows_returned += 1
        tpl = Tuple(t)
        # tuple_msg = TupleMessage(tpl)
        # records.append(tuple_msg)
        records.append(tpl)

    del tuples

    op_metrics.bytes_scanned = cur.bytes_scanned
    op_metrics.bytes_processed = cur.bytes_processed
    op_metrics.bytes_returned = cur.bytes_returned
    op_metrics.time_to_first_record_response = cur.time_to_first_record_response
    op_metrics.time_to_last_record_response = cur.time_to_last_record_response

    op_metrics.timer_stop()

    print('Finished downloading part {} read {} records'.format(part, op_metrics.rows_returned))

    return np.array(records), part, op_metrics
