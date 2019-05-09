# -*- coding: utf-8 -*-
"""Groupby Benchmarks

"""
import os

import s3filter.benchmark.filter_project_csv_vs_parquet.csv_parquet_filter_single_file as csv_parquet_filter_single_file
import sys

from s3filter.sql.format import Format

outputDir = "csv_vs_parquet_filter_1"
directory_path = 'parquet/csv_vs_parquet_10MB'
csv_path = 'tpch-sf10/lineitem_sharded'

min_cols = 1
max_cols = 10

for num_cols in range(min_cols, max_cols + 1):
    csv_file_path = directory_path + "/{}_column_10MB.csv".format(num_cols)
    parquet_file_path = directory_path + "/{}_column_10MB_uncompressed_untyped.parquet".format(num_cols)

    # CSV Filter Selectivity=1
    sys.stdout = open("benchmark-output/{}/csv_{}col_selectivity1.txt".format(num_cols), "w+")
    csv_parquet_filter_single_file.run(True, True, 0, selectivity1not0=True, path=csv_file_path, format_=Format.CSV)
    sys.stdout.close()

    # CSV Filter Selectivity=0
    sys.stdout = open("benchmark-output/{}/csv_{}col_selectivity0.txt".format(num_cols), "w+")
    csv_parquet_filter_single_file.run(True, True, 0, selectivity1not0=False, path=csv_file_path, format_=Format.CSV)
    sys.stdout.close()

    # Parquet Filter Selectivity=1
    sys.stdout = open("benchmark-output/{}/parquet_{}col_selectivity1.txt".format(num_cols), "w+")
    csv_parquet_filter_single_file.run(True, True, 0, selectivity1not0=True, path=parquet_file_path, format_=Format.PARQUET)
    sys.stdout.close()

    # Parquet Filter Selectivity=1
    sys.stdout = open("benchmark-output/{}/parquet_{}col_selectivity0.txt".format(num_cols), "w+")
    csv_parquet_filter_single_file.run(True, True, 0, selectivity1not0=False, path=parquet_file_path, format_=Format.PARQUET)
    sys.stdout.close()















