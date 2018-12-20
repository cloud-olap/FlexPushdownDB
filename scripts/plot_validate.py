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
        
        """
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
        """

    return total_time, sql_scanned_bytes, sql_returned_bytes, returned_bytes, num_http_get_requests, 

def calculate_cost(res):
    transfer_cost = res[2] / 1024**3 * 0.0007
    scan_cost = res[1] / 1024.0**3 * 0.002
    request_cost = res[4] * 0.0000004    
    comp_cost = res[0] / 3600.0 * 2.128 
    total_cost = transfer_cost + scan_cost + request_cost + comp_cost
    return total_cost, transfer_cost, scan_cost, request_cost, comp_cost

width = 0.3
#names = ['scan', 'filtered', 'index']
#labels = ['Server-Side Filter', 'S3-Side Filter', 'Indexing']
trials = [1, 2, 3]

configs = ['Presto', 'NescoDB (Baseline)', 'NescoDB (Optimized)']
cost_types = ['transfer', 'scan', 'request', 'compute']
# TODO Join is missing. 
#queries = ['Filter', 'Groupby', 'TopK', 'Join', 'TPCH Q1', 'TPCH Q3', 'TPCH Q6', 'TPCH Q14', 'TPCH Q17', 'TPCH Q19']
queries = ['Filter', 'Groupby', 'TopK', 'Join', 'TPCH Q1', 'TPCH Q3', 'TPCH Q6', 'TPCH Q14', 'TPCH Q17', 'TPCH Q19']

# runtime[ config ] = [filter, groupby, topk, ...]
# cost[ config ][ cost_type ] = [filter, groupby, topk, ...]
runtime = {}
cost = {}
for config in configs:
    runtime[config] = []
    cost[config] = {}
    for t in cost_types:
        cost[config][t] = []

def get_file_name(query, config, trial):
    if query == 'Filter':
        if config == configs[1]:
            return 'aws-exps/access_method/{}_sf10_sel{}_trial{}.txt'.format('scan', 1e-7, trial)
        elif config == configs[2]:
            return 'aws-exps/access_method/{}_sf10_sel{}_trial{}.txt'.format('index', 1e-7, trial)
    elif query == 'Groupby':
        if config == configs[1]:
            return 'aws-exps/groupby/skew_baseline_{}_4fields_trial{}.txt'.format('G5', trial)
        if config == configs[2]:
            return 'aws-exps/groupby/skew_hybrid_{}_4fields_trial{}.txt'.format('G5', trial)
    elif query == 'TopK':
        if config == configs[1]:
            return 'aws-exps/topk/baseline_k1_trial{}.txt'.format(trial)
        if config == configs[2]:
            return 'aws-exps/topk/sample_k1_trial{}.txt'.format(trial)
    elif query.endswith('Q1'): 
        if config == configs[1]:
            return 'aws-exps/tpch/tpch_q1_baseline_trial{}.txt'.format(trial)
        if config == configs[2]:
            return 'aws-exps/tpch/tpch_q1_s3_trial{}.txt'.format(trial)
    elif query.endswith('Q6'):
        if config == configs[1]:
            return 'aws-exps/tpch/tpch_q6_baseline_trial{}.txt'.format(trial)
        if config == configs[2]:
            return 'aws-exps/tpch/tpch_q6_filtered_trial{}.txt'.format(trial)

# collect NescoDB results
for query in queries:
    if query in ['Join', 'TPCH Q3', 'TPCH Q14', 'TPCH Q17', 'TPCH Q19']:
        if query == 'Join':
            runtime[ configs[1] ].append(25.70661)
            runtime[ configs[2] ].append(4.13324)
        elif query == 'TPCH Q3':
            runtime[ configs[1] ].append(293.23974)
            runtime[ configs[2] ].append(52.02051)
        elif query == 'TPCH Q14':
            runtime[ configs[1] ].append(95.82822)
            runtime[ configs[2] ].append(66.86879)
        elif query == 'TPCH Q17':
            runtime[ configs[1] ].append(99.48379)
            runtime[ configs[2] ].append(9.78955)
        elif query == 'TPCH Q19':
            runtime[ configs[1] ].append(106.73188)
            runtime[ configs[2] ].append(6.27914)
        for c, t in enumerate(cost_types):
            cost[ configs[1] ][t].append( 0 )
            cost[ configs[2] ][t].append( 0 )
        continue
    for config in configs[1:]:
        rts = []
        costs = []

        for trial in trials:
            fname = get_file_name(query, config, trial)
            t = parse(fname)
            rts.append(t[0])
            costs.append( calculate_cost(t) )
    
        min_index = rts.index( min(rts) )
        rt = rts[min_index]
        runtime[ config ].append(rt)
        for c, t in enumerate(cost_types):
            cost[config][t].append(costs[min_index][c + 1]) 

# collect Presto results
def get_presto_runtime(fname, category, query, trial):
    f = open(fname, 'r')
    lines = f.readlines()
    for i, l in enumerate(lines):
        if category in l and query in l and "trial {}".format(trial) in l:
            assert(lines[i+2].startswith('real'))
            time = lines[i+2].split()[1]
            m = time.split('m')[0]
            s = time.split('m')[1].rstrip('s')
            return int(m) * 60 + float(s)
        else:
            pass
    assert(False)

for query in queries:
    #queries = ['Filter', 'Groupby', 'TopK', 'TPCH Q1', 'TPCH Q6']
    rts = []
    for trial in trials:
        if 'TPCH' in query:
            fname = "../presto-exp/presto-validation/results/tpch_queries_output.txt"
            t = get_presto_runtime(fname, 'tpch', query.split()[1], trial)
        elif query == 'Filter':
            fname = "../presto-exp/presto-validation/results/filter_queries_output.txt"
            t = get_presto_runtime(fname, 'filter', 'Q1', trial)
        elif query == 'Groupby':
            fname = "../presto-exp/presto-validation/results/groupby_queries_output.txt"
            t = get_presto_runtime(fname, 'groupby', 'S5', trial)
        elif query == 'TopK':
            fname = "../presto-exp/presto-validation/results/topk_queries_output.txt"
            t = get_presto_runtime(fname, 'topk', 'Q1', trial)
        elif query == 'Join':
            fname = "../presto-exp/presto-validation/results/join_queries_output.txt"
            t = get_presto_runtime(fname, 'join', 'J1', trial)
        rts.append(t)

    min_index = rts.index( min(rts) )
    rt = rts[min_index]
    config = configs[0]
    runtime[ config ].append(rt)
    cost[ config ][ cost_types[0] ].append(0)
    cost[ config ][ cost_types[1] ].append(0)
    cost[ config ][ cost_types[2] ].append(0)
    cost[ config ][ cost_types[3] ].append( rt / 3600.0 * 2.218 )
       
# Plot runtime figure 
#####################
fig, ax = plt.subplots(figsize=(16, 4))
width  = 0.2
for cid, config in enumerate(configs):
    acc = 1 
    for x in runtime[config]:
        acc *= x
    geomean = acc ** (1 / len(runtime[config]))
    ax.bar([x + width * cid for x in range(len(queries)+1)], runtime[config] + [geomean], width=width, color=colors[cid], label=config)

ax.fill_between([-1.5*width, 2.7], 0, 200, facecolor='green', alpha=0.1)
ax.fill_between([2.7, 9.7], 0, 200, facecolor='yellow', alpha=0.1)
ax.fill_between([9.7, 11], 0, 200, facecolor='#0077FF', alpha=0.1)

ax.set_xticks([x + width for x in range(len(queries)+1)])
ax.set_xlim([-1.5*width, len(queries)+1-1.5*width])
ax.set_ylim([0, 120])
ax.text(4.7, 110, '293', fontsize=16)
ax.legend(ncol=3, bbox_to_anchor=[0.99, 1.17], fontsize=14)
plt.subplots_adjust(left=0.06, right=0.99, bottom=0.1, top=0.88)

ax.set_xticklabels(queries + ['Geo-Mean'])
ax.set_ylabel('Runtime (sec)')
plt.savefig('figs/pdf/validation-rt.pdf')

# Plot cost figure
##############################
## Cost
##############################
fig, ax = plt.subplots(figsize=(16, 4))
width  = 0.2
bars = [0] * len(configs)
for cid, config in enumerate(configs):
    bottom = [0] * len(queries)
    # plot legend
    b_compute = ax.bar([x + cid * width for x in range(len(queries))], bottom, width=width, color='w', label='Compute Cost')
    b_request = ax.bar([x + cid * width for x in range(len(queries))], bottom, width=width, color='w', hatch='xxx', label='Request Cost')
    b_scan = ax.bar([x + cid * width for x in range(len(queries))], bottom, width=width, color='w', hatch='\\\\\\', label='Scan Cost')
    b_transfer = ax.bar([x + cid * width for x in range(len(queries))], bottom, width=width, color='w', hatch='////', label='Transfer Cost')
    
    # plot bars 
    ax.bar([x + cid * width for x in range(len(queries))], cost[config]['transfer'], width=width, color=colors[cid], hatch='////')
    bottom = [x + y for x, y in zip(bottom, cost[config]['transfer'])]
    ax.bar([x + cid * width for x in range(len(queries))], cost[config]['scan'], bottom=bottom, width=width, color=colors[cid], hatch='\\\\\\')
    bottom = [x + y for x, y in zip(bottom, cost[config]['scan'])]
    ax.bar([x + cid * width for x in range(len(queries))], cost[config]['request'], bottom=bottom, width=width, color=colors[cid], hatch='xxx')
    bottom = [x + y for x, y in zip(bottom, cost[config]['request'])]
    b = ax.bar([x + cid * width for x in range(len(queries))], cost[config]['compute'], bottom=bottom, label=configs[cid], width=width, color=colors[cid])
    bars[cid] = b[0]

ax.set_xticks([x + width for x in range(len(queries))])
ax.set_xlim([-1.5*width, len(queries)-1.5*width])

#ax.legend(ncol=3, bbox_to_anchor=[0.99, 1.18], fontsize=14)
lg1 = ax.legend(bars, configs, ncol=3, bbox_to_anchor=[0.99, 1.17], fontsize=14)
lg2 = ax.legend([b_compute[0], b_request[0], b_scan[0], b_transfer[0]], ['Compute Cost', 'Request Cost', 'Scan Cost', 'Transfer Cost'], 
        ncol=1, bbox_to_anchor=[0.35, 1.02], fontsize=14)
plt.gca().add_artist(lg1)
plt.gca().add_artist(lg2)
plt.subplots_adjust(left=0.06, right=0.99, bottom=0.1, top=0.88)

ax.set_xticklabels(queries, fontsize=16)
#ax.text(4.8, 0.092, '0.30', fontsize=16)
#ax.set_ylim([0, 0.1])
#ax.set_xlabel('Filter Selectivity', fontsize=16)
ax.set_ylabel('Cost ($)', fontsize=16)
plt.savefig('figs/pdf/validation-cost.pdf')


