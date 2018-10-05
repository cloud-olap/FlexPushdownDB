# -*- coding: utf-8 -*-
"""Synthetic Baseline Benchmarks

"""

from datetime import datetime

from s3filter.benchmark.join import runner
from s3filter.query.join import synthetic_join_baseline_opt
from s3filter.query.join.synthetic_join_settings import SyntheticBaselineJoinSettings
import pandas as pd

from s3filter.util.test_util import gen_test_id


def main():
    max_orderdate = datetime.strptime('1995-03-01', '%Y-%m-%d')
    min_shipdate = datetime.strptime('1995-03-01', '%Y-%m-%d')
    
    shards = 2
    settings = SyntheticBaselineJoinSettings(
        parallel=True, use_pandas=True, secure=False, use_native=False, buffer_size=0,
        table_A_key='customer',
        table_A_parts=shards,
        table_A_sharded=True,
        table_A_field_names=['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal',
                             'c_mktsegment',
                             'c_comment'],
        table_A_filter_fn=lambda df: df['c_mktsegment'] == 'BUILDING',
        table_A_AB_join_key='c_custkey',
        table_B_key='orders',
        table_B_parts=shards,
        table_B_sharded=True,
        table_B_field_names=['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate',
                             'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment'],
        table_B_filter_fn=lambda df: pd.to_datetime(df['o_orderdate']) < max_orderdate,
        table_B_AB_join_key='o_custkey',
        table_B_BC_join_key='o_orderkey',
        table_C_key='lineitem',
        table_C_parts=shards,
        table_C_sharded=True,
        table_C_field_names=['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity',
                             'l_extendedprice',
                             'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                             'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment'],
        table_C_filter_fn=lambda df: pd.to_datetime(df['l_shipdate']) > min_shipdate,
        table_C_BC_join_key='l_orderkey',
        table_C_detail_field_name='l_extendedprice')

    query_plan = synthetic_join_baseline_opt.query_plan(settings)

    runner.run(query_plan, expected_total_balance=1171288505.15, test_id=gen_test_id())


if __name__ == "__main__":
    main()
