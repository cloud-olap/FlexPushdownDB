# -*- coding: utf-8 -*-
"""Groupby Benchmarks

"""
import os

import s3filter.benchmark.topk.topk_baseline as topk_baseline
import s3filter.benchmark.topk.topk_filtered as topk_filtered
import s3filter.benchmark.topk.topk_sample as topk_sample
import sys

names = ['Baseline', 'Filtered', 'Sample']
trials = ['1']

# sweep    
sample_sizes = [ x * 1000 for x in [5, 10, 50, 100, 200, 500, 1000, 2000] ]
path = 'topk_benchmark/10GB-100shards'
num_parts = 1

for trial in trials:
    for k in [10, 100, 1000]:
        # Baseline
        sys.stdout = open("benchmark-output/topk/Baseline_k{}_trial{}.txt".format(k, trial), "w+")
        topk_baseline.run('F0', k, True, True, 'ASC', 0, num_parts, path)
        sys.stdout.close()
        
        # Filtered
        sys.stdout = open("benchmark-output/topk/Filtered_k{}_trial{}.txt".format(k, trial), "w+")
        topk_filtered.run('F0', k, True, True, 'ASC', 0, num_parts, path)
        sys.stdout.close()
        
        # Sample
        for sample_size in sample_sizes:
            sys.stdout = open("benchmark-output/topk/Sample_k{}_s{}k_trial{}.txt".format(k, sample_size/1000, trial), "w+")
            topk_sample.run('F0', k, sample_size, True, True, 'ASC', 0, num_parts, path)
            sys.stdout.close()

os.system('aws ses send-email --from yxy@mit.edu --to yxy@mit.edu --subject "setup core done" --text "EOM"')            
