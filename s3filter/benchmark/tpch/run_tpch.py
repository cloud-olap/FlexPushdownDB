# -*- coding: utf-8 -*-
"""Run TPCH Benchmarks

"""

from s3filter.benchmark.tpch import tpch_q14_baseline_join
from s3filter.benchmark.tpch import tpch_q14_filtered_join
from s3filter.benchmark.tpch import tpch_q14_bloom_join
from s3filter.benchmark.tpch import tpch_q17_baseline_join
from s3filter.benchmark.tpch import tpch_q17_filtered_join
from s3filter.benchmark.tpch import tpch_q17_bloom_join
from s3filter.benchmark.tpch import tpch_q19_baseline_join
from s3filter.benchmark.tpch import tpch_q19_filtered_join
from s3filter.benchmark.tpch import tpch_q19_bloom_join
import sys

def get_config(query, alg): 
    exec("config = tpch_{}_{}_join".format(query, alg))
    return config
    
def main():
    for query in ['q14', 'q17', 'q19']:
        for alg in ['baseline', 'filtered', 'bloom']:
            for trial in [1, 2, 3]:
                config = get_config(query, alg)
                sys.stdout = open("benchmark-output/join/tpch_{}_{}_SF10_trial{}.txt".format(query, alg, trial), "w+")
                #config.run(parallel=True, use_pandas=True, buffer_size=0, lineitem_parts=96, part_parts=4)
                config.run(parallel=True, use_pandas=True, buffer_size=0, lineitem_parts=2, part_parts=2)
                sys.stdout.close()

if __name__ == "__main__":
    main()
