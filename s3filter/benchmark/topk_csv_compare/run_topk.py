# -*- coding: utf-8 -*-
"""Groupby Benchmarks

"""
import os

import s3filter.benchmark.topk_csv_compare.topk_baseline as topk_baseline
import s3filter.benchmark.topk_csv_compare.topk_sample as topk_sample
import sys

from s3filter.sql.format import Format

names = ['Baseline', 'Filtered', 'Sample']
trials = ['1']

outputDir = "csv16Col"
path = 'tpch-sf10/lineitem_sharded'
first_part = 1
num_parts = 96
#         1  10^1 10^2  10^3   10^4    10^5
k_vals = [1,   10, 100, 1000, 10000, 100000 ]

queried_columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber',
                  'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax',
                 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']

# two columns
#queried_columns = ['l_orderkey', 'l_extendedprice']
# five columns
#queried_columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_extendedprice']
# eight columns
#queried_columns = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax']

if len(queried_columns) == 16:
    select_columns = "*"
else:
    select_columns = ", ".join(queried_columns)
fields_queried = len(queried_columns)
alpha = 1.0 / fields_queried # approximating alpha as 1/num fields
N = 59986052
# getting these values as optimal sample size is sqrt(KN/alpha)


for trial in trials:
    for k in k_vals:
        # Baseline
        sys.stdout = open("benchmark-output/{}/Baseline_k{}_trial{}.txt".format(outputDir, k, trial), "w+")
        topk_baseline.run('l_extendedprice', k, True, True, 'ASC', buffer_size=0, table_first_part=first_part, table_parts=num_parts, queried_columns=queried_columns,
           select_columns=select_columns, path=path, format_=Format.CSV)
        sys.stdout.close()
        
	opt_sample_size = (float(k*N)/(alpha))**0.5
	sample_sizes = [int(opt_sample_size / 10.),int(opt_sample_size), int(opt_sample_size * 10.)]
	# Sample
        for sample_size in sample_sizes:
            sys.stdout = open("benchmark-output/{}/Sample_k{}_s{}_trial{}.txt".format(outputDir, k, sample_size, trial), "w+")
            topk_sample.run('l_extendedprice', k, sample_size=sample_size, parallel=True, use_pandas=True,
                            sort_order='ASC', buffer_size=0, table_first_part=first_part, queried_columns=queried_columns,
                            select_columns=select_columns, table_parts=num_parts, path=path, format_= Format.CSV)
            sys.stdout.close()
