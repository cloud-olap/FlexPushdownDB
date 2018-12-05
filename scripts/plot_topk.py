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

        """
        items = re.findall(r"total_elapsed_time: \d*\.?\d+", lines)
        total_time = float(items[0].split(':')[1])
        
        items = re.findall(r"sql_returned_bytes: \d+", lines)
        total_bytes_returned = sum( [ float(x.split(":")[1]) for x in items ] )
        
        items = re.findall(r"sql_scanned_bytes: \d+", lines)
        total_bytes_scanned = sum( [ float(x.split(':')[1]) for x in items ] )
        """
        try:
            items = re.findall(r"sql_gen: \d*\.?\d+", lines)
            phase1_time = float(items[0].split(':')[1])
            phase2_time = total_time - phase1_time
        except:
            pass
    return total_time, total_bytes_returned, total_bytes_scanned, \
           sql_scanned_bytes, sql_returned_bytes, returned_bytes, num_http_get_requests, \
           phase1_time, phase2_time
    #return total_time, total_bytes_returned, total_bytes_scanned, phase1_time, phase2_time
def calculate_cost(res):
    transfer_cost = res[4] / 1024**3 * 0.0007
    scan_cost = res[3] / 1024.0**3 * 0.002
    request_cost = res[6] * 0.0000004    
    comp_cost = res[0] / 3600.0 * 2.128 
    total_cost = transfer_cost + scan_cost + request_cost + comp_cost
    return total_cost, transfer_cost, scan_cost, request_cost, comp_cost

#def calculate_cost(res):
#    data_cost = res[2] / 1024.0**3 * 0.002 + res[1] / 1024**3 * 0.0007 
#    comp_cost = res[0] / 3600.0 * 2.128 
#    total_cost = data_cost + comp_cost
#    return total_cost, data_cost, comp_cost

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
        ph1.append(items[7])
        ph2.append(items[8])
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
width=0.6
transfer_cost = []
scan_cost = []
request_cost = []
comp_cost = []
for sample_size in sample_sizes:
    rts = []
    vals = []
    for trial in trials:
        #return total_time, total_bytes_returned, total_bytes_scanned, phase1_time, phase2_time
        items = parse('aws-exps/topk/sample_k100_sample{}_trial{}.txt'.format(sample_size, trial))
        val = calculate_cost(items)
        vals.append(val)
        rts.append(items[0])
    min_index = rts.index( min(rts) )
    transfer_cost.append(   vals[min_index][1])
    scan_cost.append(       vals[min_index][2])
    request_cost.append(    vals[min_index][3])
    comp_cost.append(       vals[min_index][4])
print (transfer_cost)

bottom = [0] * len(comp_cost)
# plot legend
b_compute = ax.bar([x for x in range(5)], bottom, width=width, color=colors[3], label='Compute Cost')
b_request = ax.bar([x for x in range(5)], bottom, width=width, color=colors[2], hatch='xxx', label='Request Cost')
b_scan = ax.bar([x    for x in range(5)], bottom, width=width, color=colors[1], hatch='\\\\\\', label='Scan Cost')
b_transfer = ax.bar([x for x in range(5)], bottom, width=width, color=colors[0], hatch='////', label='Transfer Cost')

# plot bars 
ax.bar([x for x in range(5)], transfer_cost, width=width, color=colors[0], hatch='////')
bottom = [x + y for x, y in zip(bottom, transfer_cost)]
ax.bar([x for x in range(5)], scan_cost, bottom=bottom, width=width, color=colors[1], hatch='\\\\\\')
bottom = [x + y for x, y in zip(bottom, scan_cost)]
ax.bar([x for x in range(5)], request_cost, bottom=bottom, width=width, color=colors[2], hatch='xxx')
bottom = [x + y for x, y in zip(bottom, request_cost)]
ax.bar([x for x in range(5)], comp_cost, bottom=bottom, width=width, color=colors[3])

#ax.bar(range(len(sample_sizes)), data_cost, width=width, color=colors[0], hatch='//')
#ax.bar(range(len(sample_sizes)), compute_cost, bottom=data_cost, width=width, color=colors[1])

ax.set_xticks([x for x in range(len(sample_sizes))])
ax.set_xlim([-0.6, 4.6])
plt.subplots_adjust(left=0.14, right=0.98, bottom=0.15, top=0.92)

ax.legend([b_compute[0], b_request[0], b_scan[0], b_transfer[0]], 
    ['Compute Cost', 'Request Cost', 'Scan Cost', 'Transfer Cost'], 
    ncol=1, bbox_to_anchor=[0.98, 0.6], fontsize=14)

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
bars = [0] * 2
labels = ['Server-Side Top-K', 'Sampled Top-K']
for cid, config in enumerate(['baseline', 'sample']):
    transfer_cost = []
    scan_cost = []
    request_cost = []
    comp_cost = []
    for k in ks:
        rts = []
        vals = []
        #dc = []
        #cc = []
        for trial in trials:
            items = parse('aws-exps/topk/{}_k{}_trial{}.txt'.format(config, k, trial))
            c = calculate_cost(items)
            rts.append(items[0])
            vals.append(c)
        min_index = rts.index(min(rts))
        transfer_cost.append(   vals[min_index][1])
        scan_cost.append(       vals[min_index][2])
        request_cost.append(    vals[min_index][3])
        comp_cost.append(       vals[min_index][4])

    bottom = [0] * len(comp_cost)
    # plot legend
    
    b_compute = ax.bar([x + cid * width for x in range(6)], bottom, width=width, color='w', label='Compute Cost')
    b_request = ax.bar([x + cid * width for x in range(6)], bottom, width=width, color='w', hatch='xxx', label='Request Cost')
    b_scan = ax.bar([x + cid * width for x in range(6)], bottom, width=width, color='w', hatch='\\\\\\', label='Scan Cost')
    b_transfer = ax.bar([x + cid * width for x in range(6)], bottom, width=width, color='w', hatch='////', label='Transfer Cost')

    # plot bars 
    ax.bar([x + cid * width for x in range(6)], transfer_cost, width=width, color=colors[cid], hatch='////')
    bottom = [x + y for x, y in zip(bottom, transfer_cost)]
    ax.bar([x + cid * width for x in range(6)], scan_cost, bottom=bottom, width=width, color=colors[cid], hatch='\\\\\\')
    bottom = [x + y for x, y in zip(bottom, scan_cost)]
    ax.bar([x + cid * width for x in range(6)], request_cost, bottom=bottom, width=width, color=colors[cid], hatch='xxx')
    bottom = [x + y for x, y in zip(bottom, request_cost)]
    b = ax.bar([x + cid * width for x in range(6)], comp_cost, bottom=bottom, label=labels[cid], width=width, color=colors[cid])
    bars[cid] = b[0]


ax.set_xticks([x + width/2 for x in range(len(ks))])
ax.set_xlim([-0.45, 5.75])

lg1 = ax.legend(bars, labels, ncol=3, bbox_to_anchor=[0.99, 1.18], fontsize=14)
lg2 = ax.legend([b_compute[0], b_request[0], b_scan[0], b_transfer[0]], ['Compute Cost', 'Request Cost', 'Scan Cost', 'Transfer Cost'], 
        ncol=1, bbox_to_anchor=[0.35, 1.02], fontsize=14)
plt.gca().add_artist(lg1)
plt.gca().add_artist(lg2)

#ax.legend(ncol=2, loc='best', fontsize=14)
plt.subplots_adjust(left=0.12, right=0.98, bottom=0.15, top=0.88)
#ax.set_xticklabels(['$10^2$', '$10^3$', '$10^4$', '$10^5$', '$10^6$', '$10^7$'])
ax.set_xticklabels(['$1$', '$10$', '$10^2$', '$10^3$', '$10^4$', '$10^5$'])
ax.set_xlabel('K')
ax.set_ylabel('Cost (\$)')
plt.savefig('figs/pdf/topk-k-cost.pdf')

