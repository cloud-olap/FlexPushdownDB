# -*- coding: utf-8 -*-
"""Synthetic Baseline Benchmarks

"""
import inspect
import os
import subprocess
import sys
from datetime import datetime

from s3filter import ROOT_DIR
from s3filter.benchmark.join import runner
from s3filter.benchmark.join.join_result import SF1_JOIN_2_RESULT
from s3filter.query.join import synthetic_join_baseline
from s3filter.query.join.synthetic_join_settings import SyntheticBaselineJoinSettings
import pandas as pd

from s3filter.util import filesystem_util
from s3filter.util.test_util import gen_test_id
import numpy as np


def main(sf, parts, sharded, table_a_filter_fn, table_b_filter_fn, expected_result):

    settings = SyntheticBaselineJoinSettings(
        parallel=True, use_pandas=True, secure=False, use_native=False, buffer_size=0,
        use_shared_mem=False, shared_memory_size=-1, sf=sf,
        table_A_key='customer',
        table_A_parts=parts,
        table_A_sharded=sharded,
        table_A_field_names=['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal',
                             'c_mktsegment',
                             'c_comment'],
        table_A_filter_fn=table_a_filter_fn,
        table_A_AB_join_key='c_custkey',
        table_B_key='orders',
        table_B_parts=parts,
        table_B_sharded=sharded,
        table_B_field_names=['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate',
                             'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment'],
        table_B_filter_fn=table_b_filter_fn,
        table_B_AB_join_key='o_custkey',
        table_B_BC_join_key=None,
        table_B_detail_field_name='o_totalprice',
        table_C_key=None,
        table_C_parts=None,
        table_C_sharded=None,
        table_C_field_names=None,
        table_C_filter_fn=None,
        table_C_BC_join_key=None,
        table_C_detail_field_name=None)

    path = os.path.join(ROOT_DIR, "../aws-exps/join")
    filesystem_util.create_dirs(path)
    sys.stdout = open(os.path.join(path, "synthetic_join_2_baseline_sf{}.txt".format(sf)), "w+")

    print("--- TEST: {} ---".format(gen_test_id()))
    print("--- SCALE FACTOR: {} ---".format(sf))
    print("--- CUSTOMER FILTER: {} ---".format(inspect.getsource(table_a_filter_fn)))
    print("--- ORDER FILTER: {} ---".format(inspect.getsource(table_b_filter_fn)))

    query_plan = synthetic_join_baseline.query_plan(settings)

    runner.run(query_plan, expected_result=expected_result, test_id=gen_test_id())

    sys.stdout.close()

    subprocess.call(['cat', os.path.join(path, "synthetic_join_2_baseline_sf{}.txt".format(sf))])


if __name__ == "__main__":
    main(1, 2, False, SF1_JOIN_2_RESULT)
