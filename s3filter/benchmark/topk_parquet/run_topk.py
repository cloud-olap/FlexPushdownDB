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

path = 'parquet/tpch-sf10/lineitem_sharded1RG'
#path = 'parquet/lineitem.610000RGS.TPCH-sf1.uncompressed.parquet'
#path = 'parquet/lineitemSF10/lineitemSF10.typed.100MBRowGroup.parquet'
#path = 'parquet/lineitemSF1.typed.100MBRowGroup.parquet'
outputDir = "typed1RG"
first_part = 1
num_parts = 97 # off by one error as really 96, but starting at 1
#         1  10^1 10^2  10^3   10^4    10^5
k_vals = [1,   10, 100, 1000, 10000, 100000 ]
# sample_sizes = [ k**.5 * 2000 for k in k_vals ]

for trial in trials:
    for k in k_vals:
        # Baseline
	#print("On baseline k = " + str(k))
        sys.stdout = open("benchmark-output/" + outputDir + "/Baseline_k{}_trial{}.txt".format(k, trial), "w+")
        topk_baseline.run('l_extendedprice', k, True, True, 'ASC', buffer_size=0, table_first_part=first_part, table_parts=num_parts, path=path, format_=Format.PARQUET)
        sys.stdout.close()

        sample_sizes = [int(k**.5 * 2000 / 10),int(k**.5 * 2000), int(k**.5 * 2000 * 10)]
        for sample_size in sample_sizes:
            #print("On sample k = " + str(k) + " s=" + str(sample_size))
            sys.stdout = open("benchmark-output/" + outputDir + "/Sample_k{}_s{}k_trial{}.txt".format(k,int(sample_size), trial), "w+")
            topk_sample.run('l_extendedprice', k, sample_size=sample_size, parallel=True, use_pandas=True,
                        sort_order='ASC', buffer_size=0, table_first_part=first_part, table_parts=num_parts, path=path, format_= Format.PARQUET)
            sys.stdout.close()

        # Columnscan
        #print("On columnscan k = " + str(k))
        sys.stdout = open("benchmark-output/" + outputDir + "/Columnscan_k{}_trial{}.txt".format(k, trial), "w+")
        topk_columnscan.run('l_extendedprice', k, parallel=True, use_pandas=True,
                            sort_order='ASC', buffer_size=0, table_first_part=first_part, table_parts=num_parts, path=path, format_= Format.PARQUET)
        sys.stdout.close()

        
