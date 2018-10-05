# -*- coding: utf-8 -*-
"""

"""
from boto3 import Session
from botocore.config import Config

from s3filter.hash.sliced_sql_bloom_filter import SlicedSQLBloomFilter
from s3filter.op.operator_base import Operator
from s3filter.op.message import TupleMessage, BloomMessage
from s3filter.op.sql_table_scan import SQLTableScanMetrics, SQLTableScan
from s3filter.op.tuple import Tuple, IndexedTuple
# noinspection PyCompatibility,PyPep8Naming
import cPickle as pickle
# import scan

class SQLTableScanBloomUse(Operator):
    """Performs a table scan using a received bloom filter.

    """

    def __init__(self, s3key, s3sql, bloom_filter_field_name, use_pandas, secure, use_native, name, query_plan, log_enabled, fn=None):
        """

        :param s3key: The s3 key to select against
        :param s3sql:  The s3 sql to use
        :param bloom_filter_field_name: The field name to apply to the bloom filter predicate
        :param name: The name of the operator
        :param log_enabled: Whether logging is enabled
        """

        super(SQLTableScanBloomUse, self).__init__(name, SQLTableScanMetrics(), query_plan, log_enabled)

        self.s3key = s3key
        self.s3sql = s3sql
        self.fn = fn

        if not use_native:
            if secure:
                cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10)
                session = Session()
                self.s3 = session.client('s3', config=cfg)
            else:
                cfg = Config(region_name="us-east-1", parameter_validation=False, max_pool_connections=10,
                             s3={'payload_signing_enabled': False})
                session = Session()
                self.s3 = session.client('s3', use_ssl=False, verify=False, config=cfg)
        # else :
        #     self.fast_s3 = scan

        self.use_native = use_native
        self.use_pandas = use_pandas

        self.__field_names = None
        self.__tuples = []

        self.__bloom_filters = []

        if type(bloom_filter_field_name) is str:
            self.__bloom_filter_field_name = bloom_filter_field_name
        else:
            raise Exception("Bloom filter field name is of type {}. Field name must be of type str to be "
                            "used in SQL predicate".format(type(bloom_filter_field_name)))

    def on_producer_completed(self, producer_name):
        """This event is overridden because we don't want the normal operator completion procedure to run. We want this
        operator to complete when all the tuples have been retrieved or consumers indicate they need no more tuples.

        :param producer_name: The completed producer
        :return: None
        """

        if producer_name in self.producer_completions.keys():
            self.producer_completions[producer_name] = True

        if all(self.producer_completions.values()):
            self.start()

        Operator.on_producer_completed(self, producer_name)

    def on_receive(self, ms, producer_name):
        """Handles the event of receiving a new message from a producer.

        :param ms: The received messages
        :param producer_name: The producer of the message
        :return: None
        """

        for m in ms:
            if type(m) is BloomMessage:
                self.on_receive_bloom_filter(m, producer_name)
            else:
                raise Exception("Unrecognized message {}".format(m))

    def on_receive_bloom_filter(self, m, _producer_name):
        self.__bloom_filters.append(SlicedSQLBloomFilter(m.bloom_filter))

    def start(self):
        """Executes the query and sends the tuples to consumers.

        :return: None
        """

        # Append the bloom filter predicate either using where... or and...
        bloom_filter_sql_predicates = []
        for bf in self.__bloom_filters:
            bloom_filter_sql_predicate = bf.build_bit_array_string_sql_predicate(self.__bloom_filter_field_name)
            bloom_filter_sql_predicates.append(bloom_filter_sql_predicate)

        sql_suffix = self.__build_sql_suffix(self.s3sql, bloom_filter_sql_predicates)
        self.s3sql = self.s3sql + sql_suffix

        if self.log_enabled:
            print("{}('{}') | {}".format(self.__class__.__name__, self.name, self.s3sql))

        if self.use_pandas:
            cur = SQLTableScan.execute_pandas_query(self)
        else:
            cur = SQLTableScan.execute_py_query(self)

        self.op_metrics.bytes_scanned = cur.bytes_scanned
        self.op_metrics.bytes_processed = cur.bytes_processed
        self.op_metrics.bytes_returned = cur.bytes_returned

        self.op_metrics.time_to_first_record_response = cur.time_to_first_record_response
        self.op_metrics.time_to_last_record_response = cur.time_to_last_record_response

        if not self.is_completed():
            self.complete()

    @staticmethod
    def __build_sql_suffix(sql, bloom_filter_sql_predicates):
        """Creates the bloom filter sql predicate. Basically determines whether the sql suffix should start with 'and'
        or 'where'.

        :param sql: The sql to create the suffix for
        :param bloom_filter_sql_predicates: The sql predicates from the bloom filters
        :return: The sql suffix
        """

        stripped_sql = sql.strip()
        if stripped_sql.endswith(';'):
            stripped_sql = stripped_sql[:-1].strip()

        split_sql = stripped_sql.split()
        is_predicate_present = split_sql[-1].lower() != "s3object"

        p_iter = iter(bloom_filter_sql_predicates)
        next_p = p_iter.next()

        bloom_filter_sql_predicate = " (" + next_p + ") "
        for next_p in p_iter:
            bloom_filter_sql_predicate += " or (" + next_p + ")"

        if is_predicate_present:
            return " and ({}) ".format(bloom_filter_sql_predicate)
        else:
            return " where ({}) ".format(bloom_filter_sql_predicate)

    def send_field_values(self, tuple_):
        """Sends a field values tuple

        :param tuple_: The tuple
        :return: None
        """

        if self.log_enabled:
            print("{}('{}') | Sending field values [{}]".format(self.__class__.__name__, self.name, {'data': tuple_}))

        self.send(TupleMessage(Tuple(tuple_)), self.consumers)

    def send_field_names(self, tuple_):
        """Sends the field names tuple

        :param tuple_: The tuple
        :return: None
        """

        # Create and send the record field names
        lt = IndexedTuple.build_default(tuple_)
        labels = Tuple(lt.field_names())

        if self.log_enabled:
            print("{}('{}') | Sending field names [{}]".format(
                self.__class__.__name__,
                self.name,
                {'field_names': labels}))

        self.send(TupleMessage(labels), self.consumers)

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, {'name': self.name, 's3key': self.s3key})
