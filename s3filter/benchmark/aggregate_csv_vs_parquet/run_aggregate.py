# -*- coding: utf-8 -*-
"""Groupby Benchmarks

"""
import os

import sys

from s3filter.benchmark.aggregate_csv_vs_parquet import csv_aggregate, parquet_aggregate
from s3filter.sql.format import Format

trials = ['1']

outputDir = "csv_vs_parquet_aggregate_1"
csv_path = 'tpch-sf10/lineitem_sharded'
parquet_path = 'parquet/tpch-sf10/lineitem_sharded_rg-256m'
first_part = 1
num_parts = 96


queried_columns_1 = ['sum(cast(l_quantity as int))']
queried_aliases_1 =['agg_1']
castable_aliases_1 = ['agg_1']
aggregate_column_1 = 'agg_1'
filter_1 = ""
key_1 = 1


queried_columns_2 = ['sum(cast(l_quantity as int))', 'sum(cast(l_extendedprice as float))', 'sum(cast(l_discount as float))']
queried_aliases_2 = ['agg_1', 'agg_2', 'agg_3']
castable_aliases_2 = ['agg_1', 'agg_2', 'agg_3']
aggregate_column_2 = 'agg_1'
filter_2 = ""
key_2 = 2

queried_columns_3 = ['sum(cast(l_quantity as int))', 'sum(cast(l_extendedprice as float))', 'sum(cast(l_discount as float))', 'sum(cast(l_tax as float))', 'avg(cast(l_quantity as int))']
queried_aliases_3 = ['agg_1', 'agg_2', 'agg_3', 'agg_4', 'agg_5']
castable_aliases_3 = ['agg_1', 'agg_2', 'agg_3', 'agg_4', 'agg_5']
aggregate_column_3 = 'agg_1'
filter_3 = ""
key_3 = 3

queried_columns_4 = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                      'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate']
queried_aliases_4 =  ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                      'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate']
castable_aliases_4 = []
aggregate_column_4 = None
filter_4 = ""
key_4 = 4

queried_columns_5 = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                      'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate']
queried_aliases_5 = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice',
                      'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate']
castable_aliases_5 = []
aggregate_column_5 = None
filter_5 = "where 1!=1"
key_5 = 5

queried_columns_list = [ queried_columns_1, queried_columns_2, queried_columns_3, queried_columns_4, queried_columns_5]
queried_aliases_list = [  queried_aliases_1, queried_aliases_2, queried_aliases_3, queried_aliases_4,queried_aliases_5]
castable_aliases_list =  [   castable_aliases_1, castable_aliases_2, castable_aliases_3, castable_aliases_4, castable_aliases_5]
aggregate_column_list =  [  aggregate_column_1, aggregate_column_2,aggregate_column_3, aggregate_column_4, aggregate_column_5]
filters_list = [filter_1, filter_2, filter_3, filter_4, filter_5]
keys_list = [key_1, key_2, key_3, key_4, key_5]

for trial in trials:
    for tuple in zip(queried_columns_list, queried_aliases_list, castable_aliases_list, aggregate_column_list, filters_list, keys_list):
        queried_columns = tuple[0]
        select_str = ", ".join(tuple[0])
        queried_aliases = tuple[1]
        castable_aliases = tuple[2]
        aggregate_column = tuple[3]
        filter_str = tuple[4]
        key = tuple[5]

        # CSV
        if not os.path.exists("benchmark-output/{}".format(outputDir)):
            os.makedirs("benchmark-output/{}".format(outputDir))
        sys.stdout = open("benchmark-output/{}/csv_{}_trial{}.txt".format(outputDir, key, trial), "w+")
        csv_aggregate.run(True, True, 0, table_first_part=first_part, table_parts=num_parts, queried_columns=queried_columns,
                          queried_aliases=queried_aliases, castable_aliases=castable_aliases, select_str=select_str,
                          aggregate_column=aggregate_column, filter_str=filter_str, path=csv_path, format_=Format.CSV)
        sys.stdout.close()

        # Parquet
        sys.stdout = open("benchmark-output/{}/parquet_{}_trial{}.txt".format(outputDir, key, trial), "w+")
        parquet_aggregate.run(True, True, 0, table_first_part=first_part, table_parts=num_parts, queried_columns=queried_columns,
                              queried_aliases=queried_aliases, castable_aliases=castable_aliases, select_str=select_str,
                              aggregate_column=aggregate_column, filter_str=filter_str, path=parquet_path, format_=Format.PARQUET)
        sys.stdout.close()
