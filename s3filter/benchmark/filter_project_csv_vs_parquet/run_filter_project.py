# -*- coding: utf-8 -*-
"""Groupby Benchmarks

"""
import os

import s3filter.benchmark.filter_project_csv_vs_parquet.csv_filter_project as csv_filter_project
import s3filter.benchmark.filter_project_csv_vs_parquet.parquet_filter_project as parquet_filter_project
import sys

from s3filter.sql.format import Format

trials = ['1']

outputDir = "csv_vs_parquet_filter_project_1"
csv_path = 'tpch-sf10/lineitem_sharded'
parquet_path = 'parquet/tpch-sf10/lineitem_sharded1RG'
first_part = 1
num_parts = 96

# two columns
queried_columns_2 = ['l_orderkey', 'l_partkey']
# five columns
queried_columns_5 = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity']
# eight columns
queried_columns_8 = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax']
# eleven columns
queried_columns_11 = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 
                      'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate']


num_cols_to_scan = ["2col", "5col", "8col", "11col"]
columns_to_fields = {"2col": queried_columns_2, "5col": queried_columns_5, "8col": queried_columns_8, "11col": queried_columns_11}

selectivities = ["0.00001", "0.0001", "0.001", "0.01", "0.1"]
selectivity_to_lower_upper_vals = {"0.00001": (0, 600),
                                   "0.0001" : (0, 6000),
                                   "0.001"  : (0, 60000),
                                   "0.01"   : (0, 600000),
                                   "0.1"    : (0, 6000000)}

for trial in trials:
    for num_cols in num_cols_to_scan:
        queried_columns = columns_to_fields[num_cols]
        select_columns = ", ".join(queried_columns)
        for selectivity in selectivities:
            lower, upper = selectivity_to_lower_upper_vals[selectivity]
            # CSV Filter + Project
            sys.stdout = open("benchmark-output/{}/csv_{}_{}_trial{}.txt".format(outputDir, num_cols, selectivity, trial), "w+")
            csv_filter_project.run(True, True, 0, table_first_part=first_part, table_parts=num_parts, queried_columns=queried_columns, 
                                   select_columns=select_columns, lower=lower, upper=upper, path=csv_path, format_=Format.CSV)
            sys.stdout.close()

            # Parquet Filter + Project
            sys.stdout = open("benchmark-output/{}/parquet_{}_{}_trial{}.txt".format(outputDir, num_cols, selectivity, trial), "w+")
            parquet_filter_project.run(True, True, 0, table_first_part=first_part, table_parts=num_parts, queried_columns=queried_columns, 
                                       select_columns=select_columns, lower=lower, upper=upper, path=parquet_path, format_=Format.PARQUET)
            sys.stdout.close()
























        
