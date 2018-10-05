# -*- coding: utf-8 -*-
"""Synthetic Bloom Join Benchmarks

"""
from s3filter.benchmark.join import runner
from s3filter.query.join import synthetic_join_bloom
from s3filter.query.join.synthetic_join_settings import SyntheticBloomJoinSettings
from s3filter.util.test_util import gen_test_id


def main():
    settings = SyntheticBloomJoinSettings(
        parallel=True, use_pandas=True, secure=False, use_native=False, buffer_size=0,
        use_shared_mem=True, shared_memory_size=2 * 1024 * 1024,
        table_A_key='customer',
        table_A_parts=8,
        table_A_sharded=False,
        table_A_field_names=['c_custkey'],
        table_A_filter_sql='c_mktsegment = \'BUILDING\'',
        table_A_AB_join_key='c_custkey',
        table_B_key='orders',
        table_B_parts=8,
        table_B_sharded=False,
        table_B_field_names=['o_orderkey', 'o_custkey', 'o_orderdate', 'o_shippriority'],
        table_B_filter_sql='cast(o_orderdate as timestamp) < cast(\'1995-03-01\' as timestamp)',
        table_B_AB_join_key='o_custkey',
        table_B_BC_join_key='o_orderkey',
        table_C_key='lineitem',
        table_C_parts=8,
        table_C_sharded=False,
        table_C_field_names=['l_orderkey', 'l_extendedprice', 'l_discount'],
        table_C_filter_sql='cast(l_shipdate as timestamp) > cast(\'1995-03-01\' as timestamp)',
        table_C_BC_join_key='l_orderkey',
        table_C_detail_field_name='l_extendedprice')

    query_plan = synthetic_join_bloom.query_plan(settings)

    runner.run(query_plan, expected_total_balance=1171288505.15, test_id=gen_test_id())


if __name__ == "__main__":
    main()
