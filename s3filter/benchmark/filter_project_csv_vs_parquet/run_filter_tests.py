# -*- coding: utf-8 -*-
"""Groupby Benchmarks

"""
import os

import s3filter.benchmark.filter_project_csv_vs_parquet.csv_parquet_filter_single_file as csv_parquet_filter_single_file
import sys

from s3filter.sql.format import Format

outputDir = "csv_vs_parquet_filter_10_trials"
directory_path = 'parquet/csv_vs_parquet_10MB/100MB_per_col_typed'

min_cols = 1
max_cols = 10

trials = [str(i) for i in range(1, 11)] # 1 - 10

for trial in trials:
    if not os.path.exists("benchmark-output/{}".format(outputDir)):
        os.makedirs("benchmark-output/{}".format(outputDir))

    for num_cols in range(min_cols, max_cols + 1):
        csv_file_path = directory_path + "/{}_column_100MB_per_Col.csv".format(num_cols)
        parquet_file_path = directory_path + "/{}_column_100MB_per_Col_uncompressed_typed.parquet".format(num_cols)

        #run(parallel, use_pandas, buffer_size, selectivity1not0, typed, path, format_):
        # CSV Filter Selectivity=1
        sys.stdout = open("benchmark-output/{}/csv_{}col_selectivity1_trial{}.txt".format(outputDir, num_cols, trial), "w+")
        csv_parquet_filter_single_file.run(True, True, 0, selectivity1not0=True, typed=False, path=csv_file_path, format_=Format.CSV)
        sys.stdout.close()

        # CSV Filter Selectivity=0
        sys.stdout = open("benchmark-output/{}/csv_{}col_selectivity0_trial{}.txt".format(outputDir, num_cols, trial), "w+")
        csv_parquet_filter_single_file.run(True, True, 0, selectivity1not0=False, typed=False, path=csv_file_path, format_=Format.CSV)
        sys.stdout.close()

        # Parquet Filter Selectivity=1
        sys.stdout = open("benchmark-output/{}/parquet_{}col_selectivity1_trial{}.txt".format(outputDir, num_cols, trial), "w+")
        csv_parquet_filter_single_file.run(True, True, 0, selectivity1not0=True, typed=True, path=parquet_file_path, format_=Format.PARQUET)
        sys.stdout.close()

        # Parquet Filter Selectivity=1
        sys.stdout = open("benchmark-output/{}/parquet_{}col_selectivity0_trial{}.txt".format(outputDir, num_cols, trial), "w+")
        csv_parquet_filter_single_file.run(True, True, 0, selectivity1not0=False, typed=True, path=parquet_file_path, format_=Format.PARQUET)
        sys.stdout.close()















