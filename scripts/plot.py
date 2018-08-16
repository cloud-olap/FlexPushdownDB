import os
import matplotlib.pyplot as plt
import csv
import re

def parse(fname):
    total_time = 0
    total_bytes_returned = 0
    total_bytes_scanned = 0
    with open(fname, 'r') as f:
        lines = ''.join(f.readlines())
        
        items = re.findall(r"total_elapsed_time: \d*\.?\d+", lines)
        total_time = float(items[0].split(':')[1])
        
        items = re.findall(r"bytes_returned': \d*\.?\d+", lines)
        total_bytes_returned = sum( [ float(x.split(':')[1]) for x in items ] )
        
        items = re.findall(r"bytes_scanned': \d*\.?\d+", lines)
        total_bytes_scanned = sum( [ float(x.split(':')[1]) for x in items ] )
    return total_time, total_bytes_returned, total_bytes_scanned

path = 'aws-exps/groupby/'
## runtime. sweep # of groups
fig, ax = plt.subplots(figsize=(10, 5))
algs = ['Baseline', 'Filtered', 'TwoPhase']
for alg in algs:
    data = [0] * 5
    for n in range(5):
        data[n] = parse(path + '{}_G{}_10_trial1.txt'.format(alg, n))[0]
    ax.plot(range(5), data, label=alg)
ax.set_xticks(range(5))
ax.set_xticklabels(['2', '4', '8', '16', '32'])
ax.legend(loc='best')
ax.set_xlabel('Number of Groups')
ax.set_ylabel('Runtime (sec)')
plt.savefig('figs/thr_group_num.png')

## bytes returned. sweep # of groups
fig, ax = plt.subplots(figsize=(10, 5))
algs = ['Baseline', 'Filtered', 'TwoPhase']
for alg in algs:
    data = [0] * 5
    for n in range(5):
        data[n] = parse(path + '{}_G{}_10_trial1.txt'.format(alg, n))[1] / 1024 / 1024 / 1024
    ax.plot(range(5), data, label=alg)
ax.set_xticks(range(5))
ax.set_xticklabels(['2', '4', '8', '16', '32'])
ax.legend(loc='best')
ax.set_xlabel('Number of Groups')
ax.set_ylabel('Bytes returned (GB)')
plt.savefig('figs/bytes_returned_group_num.png')

## runtime. sweep # of aggregated fields
fig, ax = plt.subplots(figsize=(10, 5))

algs = ['Baseline', 'Filtered', 'TwoPhase']
for alg in algs:
    data = []
    for n in [1, 2, 4, 10]:
        data.append( parse(path + '{}_G0_{}_trial1.txt'.format(alg, n))[0] )
    ax.plot(data, label=alg)
ax.set_xticks(range(4))
ax.set_xticklabels(['1', '2', '4', '10'])
ax.legend(loc='best')
ax.set_xlabel('Number of Aggregated Fields')
ax.set_ylabel('Runtime (sec)')
plt.savefig('figs/thr_field_num.png')

## bytes returned. sweep # of aggregated fields
fig, ax = plt.subplots(figsize=(10, 5))

algs = ['Baseline', 'Filtered', 'TwoPhase']
for alg in algs:
    data = []
    for n in [1, 2, 4, 10]:
        data.append( parse(path + '{}_G0_{}_trial1.txt'.format(alg, n))[1] / 1024 / 1024 / 1024 )
    ax.plot(data, label=alg)
ax.set_xticks(range(4))
ax.set_xticklabels(['1', '2', '4', '10'])
ax.legend(loc='best')
ax.set_xlabel('Number of Aggregated Fields')
ax.set_ylabel('Bytes returned (GB)')
plt.savefig('figs/bytes_returned_field_num.png')

