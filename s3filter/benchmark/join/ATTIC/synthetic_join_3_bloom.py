# -*- coding: utf-8 -*-
"""Synthetic Bloom Join Benchmarks

"""
from s3filter.benchmark.join import runner
from s3filter.benchmark.join.join_result import SF1_JOIN_3_RESULT, SF10_JOIN_3_RESULT
from s3filter.query.join import synthetic_join_bloom
from s3filter.query.join.synthetic_join_settings import SyntheticBloomJoinSettings
from s3filter.util.test_util import gen_test_id


def main(sf, parts, sharded, fp_rate, expected_result):
    settings = SyntheticBloomJoinSettings(
        parallel=True, use_pandas=True, secure=False, use_native=False, buffer_size=0,
        use_shared_mem=False, shared_memory_size=-1, sf=sf, fp_rate=fp_rate,
        table_A_key='customer',
        table_A_parts=parts,
        table_A_sharded=sharded,
        table_A_field_names=['c_custkey'],
        table_A_filter_sql='cast(c_acctbal as float) <= -999.0',
        table_A_AB_join_key='c_custkey',
        table_B_key='orders',
        table_B_parts=parts,
        table_B_sharded=sharded,
        table_B_field_names=['o_orderkey', 'o_custkey'],
        table_B_filter_sql='cast(o_orderdate as timestamp) < cast(\'1995-01-01\' as timestamp)',
        table_B_AB_join_key='o_custkey',
        table_B_BC_join_key='o_orderkey',
        table_B_detail_field_name=None,
        table_C_key='lineitem',
        table_C_parts=parts,
        table_C_sharded=sharded,
        table_C_field_names=['l_orderkey', 'l_extendedprice', 'l_discount'],
        table_C_filter_sql='cast(l_shipdate as timestamp) < cast(\'1995-01-01\' as timestamp)',
        table_C_BC_join_key='l_orderkey',
        table_C_detail_field_name='l_extendedprice')

    print("--- TEST: {} ---".format(gen_test_id()))
    print("--- SCALE FACTOR: {} ---".format(sf))
    print("--- FALSE POSITIVE RATE: {} ---".format(fp_rate))

    query_plan = synthetic_join_bloom.query_plan(settings)

    runner.run(query_plan, expected_result=expected_result, test_id=gen_test_id())


if __name__ == "__main__":
    # main(1, 4, False, 0.0001, SF1_JOIN_3_RESULT)
    main(10, 4, False, 0.0001, SF10_JOIN_3_RESULT)
