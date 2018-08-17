# -*- coding: utf-8 -*-
"""Groupby Benchmarks

"""

import s3filter.benchmark.groupby.groupby_baseline as groupby_baseline
import s3filter.benchmark.groupby.groupby_filtered as groupby_filtered
import s3filter.benchmark.groupby.groupby_2phase as groupby_2phase
import s3filter.benchmark.groupby.groupby_hybrid as groupby_hybrid
import sys

names = ['Baseline', 'Filtered', 'TwoPhase', 'Hybrid']
trials = ['1']

""" 
##################################### 
# Experiments for uniform group size.
##################################### 
for trial in trials:
    for n, config in enumerate([groupby_baseline, groupby_filtered, groupby_2phase, groupby_hybrid]):
        table_parts = 100
        
        file_format = 'groupby_benchmark/shards-10GB/groupby_data_{}.csv'
        
        # sweep the number of groups
        for group in ['G0', 'G1', 'G2', 'G3', 'G4']:
            # This is a hack to print to a file.
            sys.stdout = open("benchmark-output/groupby/{}_{}_10_trial{}.txt".format(names[n], group, trial), "w+")
            config.run([group], ['F{}'.format(x) for x in range(0, 10)], parallel=True, use_pandas=True, buffer_size=0, table_parts=table_parts)
            sys.stdout.close()
    
        # sweep the number of aggregated fields
        for num_fields in [1, 2, 4]:
            sys.stdout = open("benchmark-output/groupby/{}_G0_{}_trial{}.txt".format(names[n], num_fields, trial), "w+")
            config.run(['G0'], ['F{}'.format(x) for x in range(0, num_fields)], parallel=True, use_pandas=True, buffer_size=0, table_parts=table_parts)
            sys.stdout.close()
""" 
        
##################################### 
# Experiments for skewed group size
##################################### 
       
file_format = 'groupby_benchmark/shards-zipf-10GB/groupby_powerlaw_data_{}.csv'
table_parts = 100
       
for trial in trials:
    """
    # sweep the nlargest for groupby_hybrid
    for nlargest in [1, 4, 6, 8, 10, 12]:
        sys.stdout = open("benchmark-output/groupby/skew_{}_G2_4fields_nlargest{}_trial{}.txt".format('Hybrid', nlargest, trial), "w+")
        groupby_hybrid.run(['G2'], ['F0', 'F1', 'F2', 'F3'], parallel=True, 
                           use_pandas=True, buffer_size=0, table_parts=table_parts, 
                           files=file_format, nlargest=nlargest)
        sys.stdout.close()
    """
    for n, config in enumerate([groupby_baseline, groupby_filtered, groupby_hybrid]):
        # sweep the number of groups
        for group in ['G6', 'G7', 'G2', 'G8', 'G9']:
            sys.stdout = open("benchmark-output/groupby/skew_{}_{}_4fields_trial{}.txt".format(names[n], group, trial), "w+")
            config.run([group], ['F0', 'F1', 'F2', 'F3'], parallel=True, 
                       use_pandas=True, buffer_size=0, table_parts=table_parts,
                       files=file_format)
            sys.stdout.close()
        
        # sweep the skew factor
        for group in ['G0', 'G1', 'G2', 'G3', 'G4', 'G5']:
            sys.stdout = open("benchmark-output/groupby/skew_{}_{}_4fields_trial{}.txt".format(names[n], group, trial), "w+")
            config.run([group], ['F0', 'F1', 'F2', 'F3'], parallel=True, 
                       use_pandas=True, buffer_size=0, table_parts=table_parts,
                       files=file_format)
            sys.stdout.close()
       
        # sweep the number of aggregated fields
        group = 'G2'
        for num_fields in [1, 2, 4, 10]:
            sys.stdout = open("benchmark-output/groupby/skew_{}_{}_{}fields_trial{}.txt".format(names[n], group, num_fields, trial), "w+")
            config.run([group], ['F{}'.format(x) for x in range(0, num_fields)], parallel=True, 
                       use_pandas=True, buffer_size=0, table_parts=table_parts,
                       files=file_format)
            sys.stdout.close()
