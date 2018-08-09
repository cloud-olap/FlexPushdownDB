# -*- coding: utf-8 -*-
"""Groupby Benchmarks

"""

import s3filter.benchmark.groupby.groupby_baseline as groupby_baseline
import s3filter.benchmark.groupby.groupby_filtered as groupby_filtered
import s3filter.benchmark.groupby.groupby_2phase as groupby_2phase
import sys

names = ['Baseline', 'Filtered', 'TwoPhase']
trials = ['1']

for trial in trials:
    for n, config in enumerate([groupby_baseline, groupby_filtered, groupby_2phase]):
        table_parts = 100
    
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

