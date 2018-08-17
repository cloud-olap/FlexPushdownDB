import os
import matplotlib.pyplot as plt
import csv
import re

colors = ['#00cccc', '#f08080', '#d7b8ff', '#fff68f', '#00e595']

def parse(fname):
    total_time = 0
    total_bytes_returned = 0
    total_bytes_scanned = 0
    phase1_time = 0
    phase2_local = 0
    phase2_remote = 0
    with open(fname, 'r') as f:
        lines = ''.join(f.readlines())
        
        items = re.findall(r"total_elapsed_time: \d*\.?\d+", lines)
        total_time = float(items[0].split(':')[1])
        
        items = re.findall(r"bytes_returned': '\d*\.?\d+", lines)
        total_bytes_returned = sum( [ float(x.split(": '")[1]) for x in items ] )
        
        items = re.findall(r"bytes_scanned': \d*\.?\d+", lines)
        total_bytes_scanned = sum( [ float(x.split(':')[1]) for x in items ] )
        try:
            items = re.findall(r"groupby_filter_build: \d*\.?\d+", lines)
            phase1_time = float(items[0].split(':')[1])

            items = re.findall(r"groupby_reduce_phase2_local: \d*\.?\d+", lines)
            phase2_local = float(items[0].split(':')[1])
        
            items = re.findall(r"groupby_reduce_phase2_remote: \d*\.?\d+", lines)
            phase2_remote = float(items[0].split(':')[1])
        except:
            pass

    return total_time, total_bytes_returned, total_bytes_scanned, phase1_time, phase2_local, phase2_remote

width = 0.2
path = 'aws-exps/groupby/'
"""
##############################
## runtime. sweep # of groups
##############################
fig, ax = plt.subplots(figsize=(10, 5))
algs = ['Baseline', 'Filtered', 'TwoPhase']
for (aid, alg) in enumerate(algs):
    data = [0] * 5
    for n in range(5):
        data[n] = parse(path + '{}_G{}_10_trial1.txt'.format(alg, n))[0]
    ax.bar([x + aid * width for x in range(5)], data, label=alg, width=width, color=colors[aid])
    #ax.plot(range(5), data, label=alg)
ax.set_xticks([x + 0.3 for x in range(5)])
ax.set_xticklabels(['2', '4', '8', '16', '32'])
ax.legend(loc='best')
ax.set_xlim([-0.2, 4.8])
ax.set_xlabel('Number of Groups')
ax.set_ylabel('Runtime (sec)')
plt.savefig('figs/thr_group_num.png')

##############################
## bytes returned. sweep # of groups
##############################
fig, ax = plt.subplots(figsize=(10, 5))
algs = ['Baseline', 'Filtered', 'TwoPhase']

for (aid, alg) in enumerate(algs):
    data = [0] * 5
    for n in range(5):
        data[n] = parse(path + '{}_G{}_10_trial1.txt'.format(alg, n))[1] / 1024 / 1024 / 1024
    ax.bar([x + aid*width for x in range(5)], data, label=alg, width=width, color=colors[aid])
ax.set_xticks([ x + 0.3 for x in range(5) ])
ax.set_xticklabels(['2', '4', '8', '16', '32'])
ax.legend(loc='center right')
ax.set_xlim([-0.2, 4.8])
ax.set_xlabel('Number of Groups')
ax.set_ylabel('Bytes Transfered (GB)')
plt.savefig('figs/bytes_returned_group_num.png')

#################################################
## runtime. sweep # of aggregated fields
#################################################
fig, ax = plt.subplots(figsize=(10, 5))

algs = ['Baseline', 'Filtered', 'TwoPhase']
for (aid, alg) in enumerate(algs):
    data = []
    for n in [1, 2, 4, 10]:
        data.append( parse(path + '{}_G0_{}_trial1.txt'.format(alg, n))[0] )
    #ax.plot(data, label=alg)
    ax.bar([x + aid * width for x in range(4)], data, label=alg, width=width, color=colors[aid])
ax.set_xticks(range(4))
ax.set_xticklabels(['1', '2', '4', '10'])
ax.legend(loc='best')
ax.set_xlim([-0.2, 3.8])
ax.set_xlabel('Number of Aggregated Fields')
ax.set_ylabel('Runtime (sec)')
plt.savefig('figs/thr_field_num.png')

#################################################
## bytes returned. sweep # of aggregated fields
#################################################
fig, ax = plt.subplots(figsize=(10, 5))

algs = ['Baseline', 'Filtered', 'TwoPhase']
for (aid, alg) in enumerate(algs):
    data = []
    for n in [1, 2, 4, 10]:
        data.append( parse(path + '{}_G0_{}_trial1.txt'.format(alg, n))[1] / 1024 / 1024 / 1024 )
    #ax.plot(data, label=alg)
    ax.bar([x + aid * width for x in range(4)], data, label=alg, width=width, color=colors[aid])
ax.set_xticks(range(4))
ax.set_xticklabels(['1', '2', '4', '10'])
ax.legend(loc='center right')
ax.set_xlim([-0.2, 3.8])
ax.set_xlabel('Number of Aggregated Fields')
ax.set_ylabel('Bytes Returned (GB)')
plt.savefig('figs/bytes_returned_field_num.png')
"""

##############################
## Skewed data set
##############################

# sweep the level of skew
# -----------------------
fig, ax = plt.subplots(figsize=(10, 5))
algs = ['Baseline', 'Filtered', 'Hybrid']
groups = ['G0', 'G1', 'G2', 'G4', 'G5']
thetas = [0, 0.6, 0.9, 1.1, 1.3]
for (aid, alg) in enumerate(algs):
    data = []
    for group in groups:
        r = parse(path + 'skew_{}_{}_4fields_trial1.txt'.format(alg, group))
        runtime = r[0]
        data.append(runtime)
    #ax.plot(thetas, data, label=alg, marker='o')
    ax.bar([x + aid * width for x in range(5)], data, label=alg, width=width, color=colors[aid])
    #ax.plot(range(5), data, label=alg)
ax.set_xticks([x + 0.3 for x in range(5)])
ax.set_xticklabels(thetas)
#ax.legend(ncol=3, bbox_to_anchor=[0.64, 0.16])
ax.set_xlim([-0.2, 4.8])
ax.set_ylim([0, 130])
ax.set_xlabel('Skew Factor')
ax.set_ylabel('Runtime (sec)')
ax.legend(loc='best', ncol=3)
plt.savefig('figs/skew_thr_theta.png')

# sweep the number of groups
# --------------------------
fig, ax = plt.subplots(figsize=(10, 5))
algs = ['Baseline', 'Filtered', 'Hybrid']
groups = ['G7', 'G2', 'G8', 'G9']
num_groups = [10, 100, 1000, 10000]
for (aid, alg) in enumerate(algs):
    data = []
    for group in groups:
        runtime = parse(path + 'skew_{}_{}_4fields_trial1.txt'.format(alg, group))[0]
        data.append(runtime)
    ax.bar([x + aid * width for x in range(4)], data, label=alg, width=width, color=colors[aid])
ax.set_xticks([x + 0.3 for x in range(4)])
ax.set_xticklabels(num_groups)
ax.legend(loc='best')
#ax.set_xlim([-0.2, 4.8])
ax.set_xlabel('Number of groups')
ax.set_ylabel('Runtime (sec)')
plt.savefig('figs/skew_thr_ngroups.png')

# sweep nlargest
# --------------
path = 'aws-exps/groupby/'
fig, ax = plt.subplots(figsize=(10, 5))
nlargests = [1, 2, 4, 6, 8, 10]
data = []
for nlargest in nlargests:
    runtime = parse(path + 'skew_Hybrid_G2_4fields_nlargest{}_trial1.txt'.format(nlargest))[0]
    data.append(runtime)
ax.plot(range(6), data) 
ax.set_xticklabels(['1', '2', '4', '6', '8', '10'])
#ax.legend(loc='best')
#ax.set_xlim([-0.2, 4.8])
ax.set_xlabel('Number of remote aggregate groups')
ax.set_ylabel('Runtime (sec)')
plt.savefig('figs/skew_thr_nlargest.png')

# sweep nlargest. local vs. remote
# --------------------------------
path = 'aws-exps/groupby/'
width = 0.3
fig, ax = plt.subplots(figsize=(10, 5))
nlargests = [1, 2, 4, 6, 8, 10]
total = []
phase1 = []
phase2_local = []
phase2_remote = []
transfer = [] 
for nlargest in nlargests:
    items = parse(path + 'skew_Hybrid_G2_4fields_nlargest{}_trial1.txt'.format(nlargest))    
    phase1.append(items[3])
    phase2_local.append(items[4])
    phase2_remote.append(items[5])
    transfer.append(items[1] / 1024.0 / 1024 / 1024)
    total.append(items[0])

# total_time, total_bytes_returned, total_bytes_scanned, phase1_time, phase2_local, phase2_remote
local = phase2_local
remote = phase2_remote

ax.bar(range(6), local, color=colors[0], width=width, label='Local Aggregation Time')
ax.bar([x + width for x in range(6)], remote, color=colors[1], width=width, label='Remote Aggregation Time')
ax2 = ax.twinx()
ax2.plot([x+0.3 for x in range(6)], transfer, color=colors[2], marker='o', label='Bytes Returned') 

ax.set_xticks([x + 0.3 for x in range(6)])
ax.set_xticklabels(nlargests)

ax.legend(loc='best') #, bbox_to_anchor=[0.98, 0.4])
ax.set_xlim([-0.2, 5.8])
ax.set_xlabel('Number of Groups using Remote Aggregation')
ax2.set_ylabel('Bytes Returned (GB)')
ax.set_ylabel('Runtime (sec)')
plt.savefig('figs/skew_thr_stack.png')

