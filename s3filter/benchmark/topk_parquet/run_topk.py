# -*- coding: utf-8 -*-
"""Topk Benchmarks

"""
import os

import s3filter.benchmark.topk_parquet.topk_baseline as topk_baseline
import s3filter.benchmark.topk_parquet.topk_sample as topk_sample
import s3filter.benchmark.topk_parquet.topk_columnscan as topk_columnscan
import s3filter.benchmark.topk_parquet.topk_column_sample_and_scan as topk_columnsampleandscan
import sys

from s3filter.sql.format import Format

names = ['Baseline', 'Sample', 'Columnscan']
trials = ['1']

# sweep

path = 'parquet/tpch-sf10/lineitem_sharded1RG'
#path = 'parquet/lineitem.610000RGS.TPCH-sf1.uncompressed.parquet'
#path = 'parquet/lineitemSF10/lineitemSF10.typed.100MBRowGroup.parquet'
#path = 'parquet/lineitemSF1.typed.100MBRowGroup.parquet'
outputDir = "typed1RG16Col"
first_part = 1
num_parts = 96
#         1  10^1 10^2  10^3   10^4    10^5
k_vals = [1,   10, 100, 1000, 10000, 100000 ]

field_sizes = {'l_orderkey':8, 'l_partkey':8, 'l_suppkey':8, 'l_linenumber':4,
                 'l_quantity':4, 'l_extendedprice':4, 'l_discount':4, 'l_tax':4,
                 'l_shipdate':8, 'l_commitdate':8, 'l_receiptdate':8}

# all of the columns
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
    bytes_per_record_returned = 120
else:
    select_columns = ", ".join(queried_columns)
    bytes_per_record_returned = sum(field_sizes[x] for x in queried_columns)
alpha = field_sizes['l_extendedprice'] * 1.0 / bytes_per_record_returned
N = 59986052
# getting these values as optimal sample size is sqrt(KN/alpha)

for trial in trials:
    for k in k_vals:
        # Baseline
	#print("On baseline k = " + str(k))
        sys.stdout = open("benchmark-output/" + outputDir + "/Baseline_k{}_trial{}.txt".format(k, trial), "w+")
        topk_baseline.run('l_extendedprice', k, True, True, 'ASC', buffer_size=0, table_first_part=first_part, table_parts=num_parts,
                          queried_columns=queried_columns, select_columns=select_columns, path=path, format_=Format.PARQUET)
        sys.stdout.close()
	opt_sample_size = ((k*N)/(alpha))**0.5
        sample_sizes = [int(opt_sample_size / 10.),int(opt_sample_size), int(opt_sample_size * 10.)]
        for sample_size in sample_sizes:
            #print("On sample k = " + str(k) + " s=" + str(sample_size))
            sys.stdout = open("benchmark-output/" + outputDir + "/Sample_k{}_s{}_trial{}.txt".format(k,int(sample_size), trial), "w+")
            topk_sample.run('l_extendedprice', k, sample_size=sample_size, parallel=True, use_pandas=True,
                        sort_order='ASC', buffer_size=0, table_first_part=first_part, table_parts=num_parts,
                        queried_columns=queried_columns, select_columns=select_columns, path=path, format_= Format.PARQUET)
            sys.stdout.close()

            sys.stdout = open("benchmark-output/" + outputDir + "/Sample_Scan_k{}_s{}_trial{}.txt".format(k,int(sample_size), trial), "w+")
            topk_columnsampleandscan.run('l_extendedprice', k, sample_size=sample_size, parallel=True, use_pandas=True,
                        sort_order='ASC', buffer_size=0, table_first_part=first_part, table_parts=num_parts,
                        queried_columns=queried_columns, select_columns=select_columns, path=path, format_= Format.PARQUET)
            sys.stdout.close()

        # Columnscan
        #print("On columnscan k = " + str(k))
        sys.stdout = open("benchmark-output/" + outputDir + "/Columnscan_k{}_trial{}.txt".format(k, trial), "w+")
        topk_columnscan.run('l_extendedprice', k, parallel=True, use_pandas=True,
                            sort_order='ASC', buffer_size=0, table_first_part=first_part, table_parts=num_parts,
                            queried_columns=queried_columns, select_columns=select_columns, path=path, format_= Format.PARQUET)
        sys.stdout.close()

        
