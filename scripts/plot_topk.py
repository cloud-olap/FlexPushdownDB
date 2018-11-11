import os
import matplotlib.pyplot as plt
import csv
import re
plt.style.use('classic')
plt.rcParams.update({'font.size': 16, 'font.family' : 'serif'})

colors = ['#00cccc', '#f08080', '#d7b8ff', '#fff68f', '#00e595']

def parse(fname):
    total_time = 0
    total_bytes_returned = 0
    total_bytes_scanned = 0
    phase1_time = 0
    phase2_time = 0
    with open(fname, 'r') as f:
        lines = ''.join(f.readlines())
        
        items = re.findall(r"total_elapsed_time: \d*\.?\d+", lines)
        total_time = float(items[0].split(':')[1])
        
        items = re.findall(r"sql_returned_bytes: \d+", lines)
        total_bytes_returned = sum( [ float(x.split(":")[1]) for x in items ] )
        
        items = re.findall(r"sql_scanned_bytes: \d+", lines)
        total_bytes_scanned = sum( [ float(x.split(':')[1]) for x in items ] )
        try:
            items = re.findall(r"sql_gen: \d*\.?\d+", lines)
            phase1_time = float(items[0].split(':')[1])
            phase2_time = total_time - phase1_time
        except:
            pass

    return total_time, total_bytes_returned, total_bytes_scanned, phase1_time, phase2_time

def calculate_cost(res):
    data_cost = res[2] / 1024.0**3 * 0.002 + res[1] / 1024**3 * 0.0007 
    comp_cost = res[0] / 3600.0 * 2.128 
    total_cost = data_cost + comp_cost
    return total_cost, data_cost, comp_cost

##############################
## Sample Size: runtime and data returned 
##############################
fig, ax = plt.subplots(figsize=(8, 4))
k = 100
#sample_sizes = [ 10**2, 10**3, 10**4, 10**5, 10**6, 10**7]
sample_sizes = [ 10**3, 10**4, 10**5, 10**6, 10**7]

trials = [1, 2, 3, 4, 5, 6, 7, 8]
data = []
traffic = []
phase1 = []
phase2 = []
width=0.6
for sample_size in sample_sizes:
    rts = []
    ph1 = []
    ph2 = []
    returned = []
    for trial in trials:
        #return total_time, total_bytes_returned, total_bytes_scanned, phase1_time, phase2_time
        items = parse('aws-exps/topk/sample_k100_sample{}_trial{}.txt'.format(sample_size, trial))
        #print items
        rts.append(items[0])
        returned.append(items[1])
        ph1.append(items[3])
        ph2.append(items[4])
    idx = rts.index(min(rts))
    data.append(rts[idx]) 
    traffic.append(returned[idx] / 1024**3)
    phase1.append(ph1[idx])
    phase2.append(ph2[idx])
ax.bar(range(len(sample_sizes)), phase1, label='Sample Phase', color=colors[1], width=width)
ax.bar(range(len(sample_sizes)), phase2, bottom=phase1, label='Scan Phase', color=colors[2], width=width)

ax2 = ax.twinx()
ax2.plot([x for x in range(len(sample_sizes))], traffic, color='gray', marker='o', label='Bytes Returned') 
#ax2.set_ylim([0, 1])
ax2.set_ylabel('S3 Data Returned (GB)')

#ax.set_ylim([0, 10])
ax.set_xticks([x for x in range(len(sample_sizes))])
ax.set_xlim([-0.6, 4.6])
ax.legend(ncol=2, loc='best', fontsize=14)
plt.subplots_adjust(left=0.08, right=0.9, bottom=0.15, top=0.92)

#ax.set_xticklabels(['$10^2$', '$10^3$', '$10^4$', '$10^5$', '$10^6$', '$10^7$'])
ax.set_xticklabels(['$10^3$', '$10^4$', '$10^5$', '$10^6$', '$10^7$'])
ax.set_xlabel('Sample Size')
ax.set_ylabel('Runtime (sec)')
plt.savefig('figs/runtime_topk.png')
plt.savefig('figs/pdf/topk-sample-rt.pdf')

##############################
## Sample Size: Cost 
##############################
fig, ax = plt.subplots(figsize=(8, 4))
k = 100
#sample_sizes = [ 10**2, 10**3, 10**4, 10**5, 10**6, 10**7]
sample_sizes = [ 10**3, 10**4, 10**5, 10**6, 10**7]

trials = [1, 2, 3]
data_cost = []
compute_cost = []
width=0.6
for sample_size in sample_sizes:
    dc = []
    cc = []
    rts = []
    for trial in trials:
        #return total_time, total_bytes_returned, total_bytes_scanned, phase1_time, phase2_time
        items = parse('aws-exps/topk/sample_k100_sample{}_trial{}.txt'.format(sample_size, trial))
        costs = calculate_cost(items)
        dc.append(costs[1])
        cc.append(costs[2])
        rts.append(items[0])
    idx = rts.index(min(rts))
    data_cost.append(dc[idx])
    compute_cost.append(cc[idx])

ax.bar(range(len(sample_sizes)), data_cost, width=width, color=colors[0], hatch='//')
ax.bar(range(len(sample_sizes)), compute_cost, bottom=data_cost, width=width, color=colors[1])

ax.set_xticks([x for x in range(len(sample_sizes))])
ax.set_xlim([-0.6, 4.6])
plt.subplots_adjust(left=0.14, right=0.98, bottom=0.15, top=0.92)

ax.set_xticklabels(['$10^3$', '$10^4$', '$10^5$', '$10^6$', '$10^7$'])
ax.set_xlabel('Sample Size')
ax.set_ylabel('Cost (\$)')
#plt.savefig('figs/runtime_topk.png')
plt.savefig('figs/pdf/topk-sample-cost.pdf')



##############################
## Runtime (sweep K)
##############################

fig, ax = plt.subplots(figsize=(8, 4))
k = 100
ks = [1, 10, 100, 1000, 10000, 100000] 

trials = [1, 2, 3]
runtime = {}
width=0.3
for n, config in enumerate(['baseline', 'sample']):
    runtime[config] = []
    for k in ks: 
        rts = []
        for trial in trials:
            items = parse('aws-exps/topk/{}_k{}_trial{}.txt'.format(config, k, trial))
            #print items
            rts.append(items[0])
        runtime[config].append(min(rts))

ax.bar(range(len(ks)), runtime['baseline'], label='Server-Side Top-K', color=colors[0], width=width)
ax.bar([x + width for x in range(len(ks))], runtime['sample'], label='Sampled Top-K', color=colors[1], width=width)

ax.set_xticks([x + width/2 for x in range(len(ks))])
ax.set_xlim([-0.45, 5.75])
ax.legend(ncol=2, loc='best', fontsize=14)

plt.subplots_adjust(left=0.12, right=0.98, bottom=0.15, top=0.92)

#ax.set_xticklabels(['$10^2$', '$10^3$', '$10^4$', '$10^5$', '$10^6$', '$10^7$'])
ax.set_xticklabels(['$1$', '$10$', '$10^2$', '$10^3$', '$10^4$', '$10^5$'])
ax.set_xlabel('K')
ax.set_ylabel('Runtime (sec)')
plt.savefig('figs/pdf/topk-k-rt.pdf')


##############################
## Cost (sweep K)
##############################

fig, ax = plt.subplots(figsize=(8, 4))
k = 100
ks = [1, 10, 100, 1000, 10000, 100000] 

trials = [1, 2, 3]
width=0.3
for n, config in enumerate(['baseline', 'sample']):
    data_cost = []
    compute_cost = []
    for k in ks: 
        rts = []
        costs = []
        dc = []
        cc = []
        for trial in trials:
            items = parse('aws-exps/topk/{}_k{}_trial{}.txt'.format(config, k, trial))
            costs = calculate_cost(items)
            dc.append(costs[1])
            cc.append(costs[2])
            rts.append(items[0])
        idx = rts.index(min(rts))
        data_cost.append(dc[idx])
        compute_cost.append(cc[idx])
    label = 'Server-Side Top-K' if n == 0 else 'Sampled Top-K'
    ax.bar([x + n*width for x in range(len(ks))], data_cost, width=width, color=colors[n], hatch='//')
    ax.bar([x + n*width for x in range(len(ks))], compute_cost, label=label, bottom=data_cost, width=width, color=colors[n])

ax.set_xticks([x + width/2 for x in range(len(ks))])
ax.set_xlim([-0.45, 5.75])
ax.legend(ncol=2, loc='best', fontsize=14)
plt.subplots_adjust(left=0.12, right=0.98, bottom=0.15, top=0.92)

#ax.set_xticklabels(['$10^2$', '$10^3$', '$10^4$', '$10^5$', '$10^6$', '$10^7$'])
ax.set_xticklabels(['$1$', '$10$', '$10^2$', '$10^3$', '$10^4$', '$10^5$'])
ax.set_xlabel('K')
ax.set_ylabel('Cost (\$)')
plt.savefig('figs/pdf/topk-k-cost.pdf')

