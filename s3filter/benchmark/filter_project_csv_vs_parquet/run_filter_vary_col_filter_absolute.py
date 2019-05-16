# -*- coding: utf-8 -*-
"""Groupby Benchmarks

"""
import os

import s3filter.benchmark.filter_project_csv_vs_parquet.csv_filter_all_or_none as csv_filter_all_or_none
import s3filter.benchmark.filter_project_csv_vs_parquet.parquet_filter_all_or_none as parquet_filter_all_or_none
import sys

from s3filter.sql.format import Format

trials = ['1']

outputDir = "csv_vs_parquet_filter_absolute_vary_col_1"
csv_path = 'tpch-sf10/lineitem_sharded'
parquet_path = 'parquet/tpch-sf10/lineitem_sharded_rg-256m'
first_part = 1
num_parts = 96

all_columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber',
               'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax',
               'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
               'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']


for trial in trials:
    for num_cols in range(1, len(all_columns)):
        queried_columns = all_columns[0:num_cols]
        select_columns = ", ".join(queried_columns)
        if len(queried_columns) == 16:
            select_columns = "*"
	col_file_name = str(num_cols) + "col"
        # CSV Filter Out Nothing
        if not os.path.exists("benchmark-output/{}".format(outputDir)):
            os.makedirs("benchmark-output/{}".format(outputDir))
        sys.stdout = open("benchmark-output/{}/csv_{}_FilterOutNothing_trial{}.txt".format(outputDir, col_file_name, trial), "w+")
        csv_filter_all_or_none.run(True, True, 0, table_first_part=first_part, table_parts=num_parts, queried_columns=queried_columns,
                                   select_columns=select_columns, filterOutNothing=True, path=csv_path, format_=Format.CSV)
        sys.stdout.close()

        # CSV Filter Out Everything
        sys.stdout = open("benchmark-output/{}/csv_{}_FilterOutEverything_trial{}.txt".format(outputDir, col_file_name, trial), "w+")
        csv_filter_all_or_none.run(True, True, 0, table_first_part=first_part, table_parts=num_parts, queried_columns=queried_columns,
                                   select_columns=select_columns, filterOutNothing=False, path=csv_path, format_=Format.CSV)
        sys.stdout.close()

        # Parquet Filter Out Nothing
        sys.stdout = open("benchmark-output/{}/parquet_{}_FilterOutNothing_trial{}.txt".format(outputDir, col_file_name, trial), "w+")
        parquet_filter_all_or_none.run(True, True, 0, table_first_part=first_part, table_parts=num_parts, queried_columns=queried_columns,
                                   select_columns=select_columns, filterOutNothing=True, path=parquet_path, format_=Format.PARQUET)
        sys.stdout.close()

        # Parquet Filter Out Everything
        sys.stdout = open("benchmark-output/{}/parquet_{}_FilterOutEverything_trial{}.txt".format(outputDir, col_file_name, trial), "w+")
        parquet_filter_all_or_none.run(True, True, 0, table_first_part=first_part, table_parts=num_parts, queried_columns=queried_columns,
                                   select_columns=select_columns, filterOutNothing=False, path=parquet_path, format_=Format.PARQUET)
        sys.stdout.close()












