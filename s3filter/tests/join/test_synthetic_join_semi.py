# -*- coding: utf-8 -*-
"""

"""

import os
from collections import OrderedDict

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
from s3filter.query.join import synthetic_join_baseline, synthetic_join_filtered, synthetic_join_bloom, \
    synthetic_join_semi
from s3filter.query.join.synthetic_join_settings import SyntheticFilteredJoinSettings, SyntheticBloomJoinSettings, \
    SyntheticSemiJoinSettings
from s3filter.query.tpch import get_file_key
from s3filter.query.tpch_q19 import get_sql_suffix
from s3filter.util.test_util import gen_test_id
import s3filter.util.constants


def test():
    run(SyntheticSemiJoinSettings(
        parallel=False, use_pandas=True, secure=False, use_native=False, buffer_size=0,
        table_A_key='region',
        table_A_parts=2,
        table_A_sharded=False,
        table_A_field_names=['r_regionkey'],
        table_A_filter_sql='r_name = \'ASIA\'',
        table_A_AB_join_key='r_regionkey',
        table_B_key='nation',
        table_B_parts=2,
        table_B_sharded=False,
        table_B_field_names=['n_nationkey', 'n_regionkey'],
        table_B_AB_join_key='n_regionkey',
        table_B_BC_join_key='n_nationkey',
        table_C_key='supplier',
        table_C_parts=2,
        table_C_sharded=False,
        table_C_field_names=['s_suppkey', 's_nationkey'],
        table_C_BC_join_key='s_nationkey',
        table_C_primary_key='s_suppkey',
        table_C_detail_field_name='s_acctbal'),
        expected_total_balance=8975843.09)


def run(settings,
        expected_total_balance):
    """

    :return: None
    """

    query_plan = synthetic_join_semi.query_plan(settings)

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
