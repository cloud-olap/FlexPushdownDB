# -*- coding: utf-8 -*-
"""Topk Benchmarks

"""
import os

import s3filter.benchmark.topk_parquet.topk_baseline as topk_baseline
import s3filter.benchmark.topk_parquet.topk_sample as topk_sample
import s3filter.benchmark.topk_parquet.topk_columnscan as topk_columnscan
import sys

from s3filter.sql.format import Format

names = ['Baseline', 'Sample', 'Columnscan']
trials = ['1']

# sweep
#sample_sizes = [ x * 1000 for x in [5, 10, 50, 100, 200, 500, 1000, 2000] ]
path = 'tpch-parquet/tpch-sf1/lineitem_sharded'
num_parts = 32
k = 1000

for trial in trials:
   # for k in [10, 100, 1000]:
        # Baseline
        #sys.stdout = open("benchmark-output/topk/Baseline_k{}_trial{}.txt".format(k, trial), "w+")
        topk_baseline.run('l_extendedprice', k, True, True, 'ASC', buffer_size=0, table_parts=num_parts, path=path, format_=Format.PARQUET)
        #sys.stdout.close()

        #for sample_size in sample_sizes:
            #sys.stdout = open("benchmark-output/topk/Sample_k{}_s{}k_trial{}.txt".format(k, sample_size/1000, trial), "w+")
        topk_sample.run('l_extendedprice', 100, sample_size=10000, parallel=True, use_pandas=True,
                        sort_order='ASC', buffer_size=0, table_parts=num_parts, path=path, format_= Format.PARQUET)
            #sys.stdout.close()

        # Columnscan
        #sys.stdout = open("benchmark-output/topk/Columnscan_k{}_trial{}.txt".format(k, trial), "w+")
        topk_columnscan.run('l_extendedprice', k, parallel=True, use_pandas=True,
                            sort_order='ASC', buffer_size=0, table_parts=num_parts, path=path, format_= Format.PARQUET)
        #sys.stdout.close()
