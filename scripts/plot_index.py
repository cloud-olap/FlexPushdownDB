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
    
    sql_scanned_bytes = 0
    sql_returned_bytes = 0
    returned_bytes = 0
    num_http_get_requests = 0
    
    phase1_time = 0
    phase2_time = 0
    with open(fname, 'r') as f:
        lines = ''.join(f.readlines())
        
        items = re.findall(r"total_elapsed_time: \d*\.?\d+", lines)
        total_time = float(items[0].split(':')[1])
        
        items = re.findall(r"sql_scanned_bytes: \d*\.?\d+", lines)
        sql_scanned_bytes = float(items[0].split(':')[1])

        items = re.findall(r"sql_returned_bytes: \d*\.?\d+", lines)
        sql_returned_bytes = float(items[0].split(':')[1])
        
        items = re.findall(r"[^_]returned_bytes: \d*\.?\d+", lines)
        returned_bytes = float(items[0].split(':')[1])
        
        items = re.findall(r"num_http_get_requests: \d*\.?\d+", lines)
        num_http_get_requests = float(items[0].split(':')[1])
        
        items = re.findall(r"bytes_returned': '\d*\.?\d+", lines)
        total_bytes_returned = sum( [ float(x.split(": '")[1]) for x in items ] )
        
        items = re.findall(r"bytes_scanned': \d*\.?\d+", lines)
        total_bytes_scanned = sum( [ float(x.split(':')[1]) for x in items ] )
        
        try:
            items = re.findall(r"index_scan_0: \d*\.?\d+", lines)
            phase1_time = float(items[0].split(':')[1])

            phase2_time = total_time - phase1_time
        except:
            pass

    return total_time, total_bytes_returned, total_bytes_scanned, \
           sql_scanned_bytes, sql_returned_bytes, returned_bytes, num_http_get_requests, \
           phase1_time, phase2_time

def calculate_cost(res):
    transfer_cost = res[4] / 1024**3 * 0.0007
    scan_cost = res[3] / 1024.0**3 * 0.002
    request_cost = res[6] * 0.0000004    
    comp_cost = res[0] / 3600.0 * 2.128 
    total_cost = transfer_cost + scan_cost + request_cost + comp_cost
    return total_cost, transfer_cost, scan_cost, request_cost, comp_cost

width = 0.3
path = 'aws-exps/access_method'

sels = [1e-7, 1e-6, 1e-5, 1e-4, 1e-3, 1e-2]
# for SF=1
#actual_sel = [ x / 6001216.0 for x in [1, 13, 35, 140, 866, 3375, 13122] ] 
#actual_sel = [ x / 60000000.0 for x in [7, 95, 309, 1245, 8431, 32893, 130260] ] 
names = ['scan', 'filtered', 'index']
labels = ['Server-Side Filter', 'S3-Side Filter', 'Indexing']
trials = [1, 2, 3]
#sfs = [1, 10, 100]

##############################
## runtime. 
##############################
fig, ax = plt.subplots(figsize=(8, 4))
width  = 0.2
for cid, name in enumerate(names):
    data = []
    for sel in sels:
        rts = []
        for trial in trials: 
            t = parse('{}/{}_sf10_sel{}_trial{}.txt'.format(path, name, sel, trial))[0]
            rts.append(t)
        #data.append(sum(rts) / len(rts))
        data.append(min(rts))
    ax.bar([x + width * cid for x in range(len(sels))], data, width=width, color=colors[cid], label=labels[cid])
    #ax.semilogx(sels, data, label=name, color=colors[cid])
ax.set_xticks([x + width for x in range(6)])
ax.set_xlim([-1.5*width, 6-1.5*width])
#ax.legend(loc='best')
ax.legend(ncol=3, bbox_to_anchor=[0.99, 1.18], fontsize=14)
plt.subplots_adjust(left=0.12, right=0.99, bottom=0.15, top=0.88)

ax.set_xticklabels(['$10^{-7}$', '$10^{-6}$', '$10^{-5}$', '$10^{-4}$', '$10^{-3}$', '$10^{-2}$'])
ax.set_xlabel('Filter Selectivity')
ax.set_ylabel('Runtime (sec)')
plt.savefig('figs/filter-rt.png')
plt.savefig('figs/pdf/filter-rt.pdf')

##############################
## Cost
##############################
fig, ax = plt.subplots(figsize=(8, 4))
width  = 0.2
bars = [0] * len(names)
for cid, name in enumerate(names):
    transfer_cost = []
    scan_cost = []
    request_cost = []
    comp_cost = []
    for sel in sels:
        vals = []
        rts = []
        for trial in trials: 
            t = parse('{}/{}_sf10_sel{}_trial{}.txt'.format(path, name, sel, trial))
            rts.append(t[0])
            val = calculate_cost( t )
            vals.append(val)
        total_cost = [ x[0] for x in vals ]
        min_index = rts.index( min(rts) )
        transfer_cost.append(   vals[min_index][1])
        scan_cost.append(       vals[min_index][2])
        request_cost.append(    vals[min_index][3])
        comp_cost.append(       vals[min_index][4])
    bottom = [0] * len(comp_cost)
    # plot legend
    b_compute = ax.bar([x + cid * width for x in range(len(sels))], bottom, width=width, color='w', label='Compute Cost')
    b_request = ax.bar([x + cid * width for x in range(len(sels))], bottom, width=width, color='w', hatch='xxx', label='Request Cost')
    b_scan = ax.bar([x + cid * width for x in range(len(sels))], bottom, width=width, color='w', hatch='\\\\\\', label='Scan Cost')
    b_transfer = ax.bar([x + cid * width for x in range(len(sels))], bottom, width=width, color='w', hatch='////', label='Transfer Cost')
    
    # plot bars 
    ax.bar([x + cid * width for x in range(len(sels))], transfer_cost, width=width, color=colors[cid], hatch='////')
    bottom = [x + y for x, y in zip(bottom, transfer_cost)]
    ax.bar([x + cid * width for x in range(len(sels))], scan_cost, bottom=bottom, width=width, color=colors[cid], hatch='\\\\\\')
    bottom = [x + y for x, y in zip(bottom, scan_cost)]
    ax.bar([x + cid * width for x in range(len(sels))], request_cost, bottom=bottom, width=width, color=colors[cid], hatch='xxx')
    bottom = [x + y for x, y in zip(bottom, request_cost)]
    b = ax.bar([x + cid * width for x in range(len(sels))], comp_cost, bottom=bottom, label=labels[cid], width=width, color=colors[cid])
    bars[cid] = b[0]

ax.set_xticks([x + width for x in range(6)])
ax.set_xlim([-1.5*width, 6-1.5*width])

#ax.legend(ncol=3, bbox_to_anchor=[0.99, 1.18], fontsize=14)
lg1 = ax.legend(bars, labels, ncol=3, bbox_to_anchor=[0.99, 1.18], fontsize=14)
lg2 = ax.legend([b_compute[0], b_request[0], b_scan[0], b_transfer[0]], ['Compute Cost', 'Request Cost', 'Scan Cost', 'Transfer Cost'], 
        ncol=1, bbox_to_anchor=[0.35, 1.02], fontsize=14)
plt.gca().add_artist(lg1)
plt.gca().add_artist(lg2)
plt.subplots_adjust(left=0.12, right=0.99, bottom=0.15, top=0.88)

ax.set_xticklabels(['$10^{-7}$', '$10^{-6}$', '$10^{-5}$', '$10^{-4}$', '$10^{-3}$', '$10^{-2}$'], fontsize=16)
ax.text(4.8, 0.092, '0.30', fontsize=16)
ax.set_ylim([0, 0.1])
ax.set_xlabel('Filter Selectivity', fontsize=16)
ax.set_ylabel('Cost ($)', fontsize=16)
plt.savefig('figs/filter-cost.png')
plt.savefig('figs/pdf/filter-cost.pdf')

##############################
## Bytes returned 
##############################
names = ['scan', 'filtered', 'index']
fig, ax = plt.subplots(figsize=(10, 5))
for cid, name in enumerate(names):
    data = []
    for sel in sels:
        res2 = parse('{}/{}_sf10_sel{}_trial2.txt'.format(path, name, sel))[1]
        data.append(res2)
    ax.semilogx(sels, data, label=name, color=colors[cid])
ax.legend(loc='best')
ax.set_xlabel('Selectivity')
ax.set_ylabel('Bytes Returned')
plt.savefig('figs/byteret_index.png')


