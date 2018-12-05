# -*- coding: utf-8 -*-
"""Synthetic Bloom Join Benchmarks

"""
import os
import subprocess
import sys

from s3filter import ROOT_DIR
from s3filter.benchmark.join import runner
from s3filter.benchmark.join.join_result import SF1_JOIN_2_RESULT
from s3filter.query.join import synthetic_join_bloom
from s3filter.query.join.synthetic_join_settings import SyntheticBloomJoinSettings
from s3filter.util import filesystem_util
from s3filter.util.test_util import gen_test_id


def main(sf, parts, sharded, fp_rate, table_a_filter_val, table_b_filter_val, expected_result, trial):
    table_a_filter_sql, _, table_b_filter_sql, _ = runner.build_filters(table_a_filter_val, table_b_filter_val)

    settings = SyntheticBloomJoinSettings(
        parallel=True, use_pandas=True, secure=False, use_native=False, buffer_size=0,
        use_shared_mem=False, shared_memory_size=-1, sf=sf, fp_rate=fp_rate,
        table_A_key='customer',
        table_A_parts=parts,
        table_A_sharded=sharded,
        table_A_field_names=['c_custkey'],
        table_A_filter_sql=table_a_filter_sql,
        table_A_AB_join_key='c_custkey',
        table_B_key='orders',
        table_B_parts=parts,
        table_B_sharded=sharded,
        table_B_field_names=['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate',
                             'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment'],
        table_B_filter_sql=table_b_filter_sql,
        table_B_AB_join_key='o_custkey',
        table_B_BC_join_key=None,
        table_B_detail_field_name='o_totalprice',
        table_C_key=None,
        table_C_parts=None,
        table_C_sharded=None,
        table_C_field_names=None,
        table_C_filter_sql=None,
        table_C_BC_join_key=None,
        table_C_detail_field_name=None)

    path = os.path.join(ROOT_DIR, "../aws-exps/join")
    filesystem_util.create_dirs(path)
    out_file = "synthetic_join_2_bloom_sf{}_aval{}_bval{}_fp{}_trial{}.txt" \
        .format(sf, table_a_filter_val, table_b_filter_val, fp_rate, trial)
    sys.stdout = open(os.path.join(path, out_file), "w+")

    print("--- TEST: {} ---".format(gen_test_id()))
    print("--- SCALE FACTOR: {} ---".format(sf))
    print("--- FALSE POSITIVE RATE: {} ---".format(fp_rate))
    print("--- CUSTOMER FILTER: {} ---".format(table_a_filter_sql))
    print("--- ORDER FILTER: {} ---".format(table_b_filter_sql))

    query_plan = synthetic_join_bloom.query_plan(settings)

    runner.run(query_plan, expected_result=expected_result, test_id=gen_test_id())

    sys.stdout.close()

    subprocess.call(['cat', os.path.join(path, out_file)])


if __name__ == "__main__":
    # main(1, 1, False, 0.001, 'cast(c_acctbal as float) <= -999.0',
    #      'cast(o_orderdate as timestamp) < cast(\'1998-01-01\' as timestamp)', SF1_JOIN_2_RESULT)
    main(10, 2, False, 0.001, 2000.0, None, SF1_JOIN_2_RESULT, 2)
    # main(1, 4, False, 0.001, 'cast(c_acctbal as float) <= -999.0',
    #      'cast(o_orderdate as timestamp) < cast(\'1998-01-01\' as timestamp)', SF1_JOIN_2_RESULT)
    # main(1, 8, False, 0.001, 'cast(c_acctbal as float) <= -999.0',
    #      'cast(o_orderdate as timestamp) < cast(\'1998-01-01\' as timestamp)', SF1_JOIN_2_RESULT)
    # main(1, 32, True, 0.001, 'cast(c_acctbal as float) <= -999.0',
    #      'cast(o_orderdate as timestamp) < cast(\'1998-01-01\' as timestamp)', SF1_JOIN_2_RESULT)
