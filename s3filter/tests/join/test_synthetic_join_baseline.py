# -*- coding: utf-8 -*-
"""

    select
        l_orderkey,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        o_orderdate,
        o_shippriority
    from
        customer,
        orders,
        lineitem
    where
        c_mktsegment = CAST('BUILDING' AS char(10))
        and c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate < date '1995-03-01'
        and l_shipdate > date '1995-03-01'
    group by
        l_orderkey,
        o_orderdate,
        o_shippriority
    order by
        revenue desc,
        o_orderdate
    limit 10;

"""

import os
from collections import OrderedDict
from datetime import datetime

import numpy

from s3filter import ROOT_DIR
from s3filter.op.aggregate import Aggregate
from s3filter.op.aggregate_expression import AggregateExpression
from s3filter.op.collate import Collate
from s3filter.op.hash_join_build import HashJoinBuild
from s3filter.op.hash_join_probe import HashJoinProbe
from s3filter.op.join_expression import JoinExpression
from s3filter.op.map import Map
from s3filter.op.operator_connector import connect_many_to_many, connect_all_to_all, connect_many_to_one, \
    connect_one_to_one
from s3filter.op.project import Project, ProjectExpression
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.plan.query_plan import QueryPlan
from s3filter.query import tpch_q19
from s3filter.query.join import synthetic_join_baseline
from s3filter.query.join.synthetic_join_settings import SyntheticBaselineJoinSettings
from s3filter.query.tpch import get_file_key
from s3filter.query.tpch_q19 import get_sql_suffix
from s3filter.util.test_util import gen_test_id
import s3filter.util.constants
import pandas as pd


def test():
    max_orderdate = datetime.strptime('1995-03-01', '%Y-%m-%d')
    min_shipdate = datetime.strptime('1995-03-01', '%Y-%m-%d')

    run(SyntheticBaselineJoinSettings(
        parallel=True, use_pandas=True, secure=False, use_native=False, buffer_size=0,
        table_A_key='customer',
        table_A_parts=8,
        table_A_sharded=False,
        table_A_field_names=['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment',
                             'c_comment'],
        table_A_filter_fn=lambda df: df['c_mktsegment'] == 'BUILDING',
        table_A_AB_join_key='c_custkey',
        table_B_key='orders',
        table_B_parts=8,
        table_B_sharded=False,
        table_B_field_names=['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate',
                             'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment'],
        table_B_filter_fn=lambda df: pd.to_datetime(df['o_orderdate']) < max_orderdate,
        table_B_AB_join_key='o_custkey',
        table_B_BC_join_key='o_orderkey',
        table_C_key='lineitem',
        table_C_parts=8,
        table_C_sharded=False,
        table_C_field_names=['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                             'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                             'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment'],
        table_C_filter_fn=lambda df: pd.to_datetime(df['l_shipdate']) > min_shipdate,
        table_C_BC_join_key='l_orderkey',
        table_C_detail_field_name='l_extendedprice'),
        expected_total_balance=1171288505.15)


def run(settings, expected_total_balance):
    """

    :return: None
    """

    query_plan = synthetic_join_baseline.query_plan(settings)

    collate = query_plan.get_operator('collate')

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    query_plan.execute()

    tuples = collate.tuples()

    collate.print_tuples(tuples)

    # Write the metrics
    query_plan.print_metrics()

    # Shut everything down
    query_plan.stop()

    field_names = ['total_balance']

    assert len(tuples) == 1 + 1

    assert tuples[0] == field_names

    numpy.testing.assert_approx_equal(tuples[1][0], expected_total_balance)
