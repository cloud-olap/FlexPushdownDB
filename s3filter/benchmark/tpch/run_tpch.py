# -*- coding: utf-8 -*-
"""Run TPCH Benchmarks

"""
import os
import subprocess

from s3filter import ROOT_DIR
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

from s3filter.util import filesystem_util


def get_config(query, alg):
    exec("config = tpch_{}_{}_join".format(query, alg))
    return config

def end_capture(out_file, path):
    sys.stdout.close()
    subprocess.call(['cat', os.path.join(path, out_file)])


def start_capture(query, sf, approach, trial):
    path = os.path.join(ROOT_DIR, "../aws-exps/tpch")
    filesystem_util.create_dirs(path)
    out_file = "{}_sf{}_{}_trial{}.txt".format(query, sf, approach, trial)
    sys.stdout = open(os.path.join(path, out_file), "w+")
    return out_file, path
    
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
