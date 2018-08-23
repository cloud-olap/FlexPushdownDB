# -*- coding: utf-8 -*-
"""Access Method Benchmarks

"""

import s3filter.benchmark.access_method.index as index
import s3filter.benchmark.access_method.filtered_scan as scan
import sys

trials = [1]
nshards = 100
for trial in trials:

    # sweep nthreads for indexing
    name = 'index'
    """
    for selectivity in [0.001, 0.01]:
        for nthreads in [4, 16, 64, 256]:
            sys.stdout = open("benchmark-output/access_method/{}_perc{}_nthreads{}_trial{}.txt".format(name, selectivity, nthreads, trial), "w+")
            path = 'access_method_benchmark/shards-10GB'
            index.run(True, True, 0, nshards, selectivity, path, nthreads) 
            sys.stdout.close()       
    """
    """
    # sweep selectivity for scan and index
    for name, config in [ ['index', index], ['scan', scan] ]:
        for selectivity in [0.0001, 0.001, 0.01, 0.1]:  
            sys.stdout = open("benchmark-output/access_method/{}_perc{}_trial{}.txt".format(name, selectivity, trial), "w+")
            path = 'access_method_benchmark/shards-10GB'
            config.run(True, True, 0, nshards, selectivity, path) 
            sys.stdout.close()
    """
    for npart in [1, 10, 100]:
        for name, config in [ ['index', index], ['scan', scan] ]:
            selectivity = 0.00001
            sys.stdout = open("benchmark-output/access_method/{}_perc{}_npart{}_trial{}.txt".format(name, selectivity, npart, trial), "w+")
            if npart == 100:
                fname = 'shards-10GB'
            else: 
                fname = '{}-shards-10GB'.format(npart)
            path = 'access_method_benchmark/{}'.format(fname)
            config.run(True, True, 0, nshards, selectivity, path) 
            sys.stdout.close()

