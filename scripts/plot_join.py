import os

import matplotlib
from matplotlib import ticker

matplotlib.use('Agg')
import matplotlib.pyplot as plt
import csv
import re

from s3filter import ROOT_DIR
from s3filter.util import filesystem_util

plt.style.use('classic')
plt.rcParams.update({'font.size': 16, 'font.family': 'serif'})

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
        total_bytes_returned = sum([float(x.split(": '")[1]) for x in items])

        items = re.findall(r"bytes_scanned': \d*\.?\d+", lines)
        total_bytes_scanned = sum([float(x.split(':')[1]) for x in items])

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
    transfer_cost = res[4] / 1024 ** 3 * 0.0007
    scan_cost = res[3] / 1024.0 ** 3 * 0.002
    request_cost = res[6] * 0.0000004
    comp_cost = res[0] / 3600.0 * 2.128
    total_cost = transfer_cost + scan_cost + request_cost + comp_cost
    return total_cost, transfer_cost, scan_cost, request_cost, comp_cost

def calculate_download_cost(res):
    transfer_cost = 0
    scan_cost = 0
    request_cost = res[6] * 0.0000004
    comp_cost = res[0] / 3600.0 * 2.128
    total_cost = transfer_cost + scan_cost + request_cost + comp_cost
    return total_cost, transfer_cost, scan_cost, request_cost, comp_cost

sf =10
width = 0.3
path = os.path.join(ROOT_DIR, "../aws-exps/join/sf{}/bench-03".format(sf))
filesystem_util.create_dirs(os.path.join(path, "figs"))
filesystem_util.create_dirs(os.path.join(path, "figs/pdf"))

avals = [-950, -850, -750, -650, -550, -450]
bvals = ['1992-03-01', '1992-06-01', '1993-01-01', '1994-01-01', '1995-01-01', None]
# for SF=1
# actual_sel = [ x / 6001216.0 for x in [1, 13, 35, 140, 866, 3375, 13122] ]
# actual_sel = [ x / 60000000.0 for x in [7, 95, 309, 1245, 8431, 32893, 130260] ]
names = ['baseline', 'filtered', 'bloom']
fp_rates = [0.0001, 0.001, 0.01, 0.1, 0.3, 0.5]
labels = ['Baseline Join', 'Filtered Join', 'Bloom Join']
trials = [1, 2, 3]
# sfs = [1, 10, 100]

FIXED_A_VAL_IDX = 0
FIXED_B_VAL_IDX = 5
FIXED_FP_RATE_IDX = 2




fp_rate = fp_rates[FIXED_FP_RATE_IDX]
bval = bvals[FIXED_B_VAL_IDX]
labels = ['Baseline Join', 'Filtered Join', 'Bloom Join']
fig_name = 'join-aval-rt'

fig, ax = plt.subplots(figsize=(8, 4))
width = 0.2
for cid, name in enumerate(names):
    data = []
    for aval in avals:
        rts = []
        for trial in trials:
            if name != 'bloom':
                t = parse('{}/synthetic_join_2_{}_sf{}_aval{}_bval{}_trial{}.txt'
                          .format(path, name, sf, aval, bval, trial))[0]
            else:
                t = parse('{}/synthetic_join_2_{}_sf{}_aval{}_bval{}_fp{}_trial{}.txt'
                          .format(path, name, sf, aval, bval, fp_rate, trial))[0]
            rts.append(t)
        data.append(min(rts))
    pos = [x + width + width * cid for x in range(len(avals))]
    ax.bar(pos, data, width=width, color=colors[cid], label=labels[cid])
    # ax.bar(pos, data, width=width)
    # ax.semilogx(sels, data, label=name, color=colors[cid])
ax.set_xticks([x + width + width / 2.0 + width for x in range(len(avals))])
# ax.set_xlim([-1.5 * width, 12 - 1.5 * width])
# ax.legend(loc='best')
ax.legend(ncol=3, bbox_to_anchor=[0.99, 1.18], fontsize=14)
plt.subplots_adjust(left=0.15, right=0.99, bottom=0.15, top=0.88)
# ax.set_xticklabels(['$10^{-7}$', '$10^{-6}$', '$10^{-5}$', '$10^{-4}$', '$10^{-3}$', '$10^{-2}$'])
ticklabels = []
for aval in avals:
    ticklabels.append("{}".format(aval))
ax.set_xticklabels(ticklabels, fontsize=14)
ax.set_xlabel('Customer Filter Selectivity (c_acctbal <= ?)')
ax.set_ylabel('Runtime (sec)')
plt.savefig(os.path.join(path, 'figs/{}.png'.format(fig_name)))
plt.savefig(os.path.join(path, 'figs/pdf/{}.pdf'.format(fig_name)))

fp_rate = fp_rates[FIXED_FP_RATE_IDX]
aval = avals[FIXED_A_VAL_IDX]
labels = ['Baseline Join', 'Filtered Join', 'Bloom Join']
fig_name = 'join-bval-rt'

fig, ax = plt.subplots(figsize=(8, 4))
width = 0.2
for cid, name in enumerate(names):
    data = []
    for bval in bvals:
        rts = []
        for trial in trials:
            if name != 'bloom':
                t = parse('{}/synthetic_join_2_{}_sf{}_aval{}_bval{}_trial{}.txt'
                          .format(path, name, sf, aval, bval, trial))[0]
            else:
                t = parse('{}/synthetic_join_2_{}_sf{}_aval{}_bval{}_fp{}_trial{}.txt'
                          .format(path, name, sf, aval, bval, fp_rate, trial))[0]
            rts.append(t)
        data.append(min(rts))
    pos = [x + width + width * cid for x in range(len(bvals))]
    ax.bar(pos, data, width=width, color=colors[cid], label=labels[cid])
    # ax.bar(pos, data, width=width)
    # ax.semilogx(sels, data, label=name, color=colors[cid])
ax.set_xticks([x + width + width / 2.0 + width for x in range(len(bvals))])
# ax.set_xlim([-1.5 * width, 12 - 1.5 * width])
# ax.legend(loc='best')
ax.legend(ncol=3, bbox_to_anchor=[0.99, 1.25], fontsize=14)
plt.subplots_adjust(left=0.15, right=0.99, bottom=0.35, top=0.88)
# ax.set_xticklabels(['$10^{-7}$', '$10^{-6}$', '$10^{-5}$', '$10^{-4}$', '$10^{-3}$', '$10^{-2}$'])
ticklabels = []
for bval in bvals:
    ticklabels.append("{}".format(bval))
ax.set_xticklabels(ticklabels, fontsize=14, rotation=45)
ax.set_xlabel('Order Filter Selectivity (o_orderdate < ?)')
ax.set_ylabel('Runtime (sec)')
plt.savefig(os.path.join(path, 'figs/{}.png'.format(fig_name)))
plt.savefig(os.path.join(path, 'figs/pdf/{}.pdf'.format(fig_name)))




aval = avals[FIXED_A_VAL_IDX]
bval = bvals[FIXED_B_VAL_IDX]
labels = ['Baseline Join', 'Filtered Join', 'Bloom Join']
fig_name = 'join-fp_rate-rt'

fig, ax = plt.subplots(figsize=(8, 4))
width = 0.4
for cid, name in enumerate(names):
    data = []
    if name != 'bloom':
        rts = []
        for trial in trials:
            t = parse('{}/synthetic_join_2_{}_sf{}_aval{}_bval{}_trial{}.txt'
                      .format(path, name, sf, aval, bval, trial))[0]
            rts.append(t)
        data.append(min(rts))
        pos = [cid + width]
        ax.bar(pos, data, width=width, color=colors[cid], label=labels[cid])
    else:
        for fp_rate in fp_rates:
            rts = []
            for trial in trials:
                if name != 'bloom':
                    t = parse('{}/synthetic_join_2_{}_sf{}_aval{}_bval{}_trial{}.txt'
                              .format(path, name, sf, aval, bval, trial))[0]
                else:
                    t = parse('{}/synthetic_join_2_{}_sf{}_aval{}_bval{}_fp{}_trial{}.txt'
                              .format(path, name, sf, aval, bval, fp_rate, trial))[0]
                rts.append(t)
            data.append(min(rts))
        pos = [width + x for x in range(2, len(fp_rates) + 2)]
        ax.bar(pos, data, width=width, color=colors[cid], label=labels[cid])
    # ax.bar(pos, data, width=width)
    # ax.semilogx(sels, data, label=name, color=colors[cid])
ax.set_xticks([x + width + width / 2.0 for x in range(len(fp_rates) + 2)])
ax.set_xlim([-0.1* width, 8 + width -0.2])
# ax.legend(loc='best')
ax.legend(ncol=3, bbox_to_anchor=[0.99, 1.21], fontsize=14)
plt.subplots_adjust(left=0.15, right=0.99, bottom=0.25, top=0.88)
# ax.set_xticklabels(['$10^{-7}$', '$10^{-6}$', '$10^{-5}$', '$10^{-4}$', '$10^{-3}$', '$10^{-2}$'])
ticklabels = []
ticklabels.append('')
ticklabels.append('')
for fp_rate in fp_rates:
    ticklabels.append("{}".format(fp_rate))
ax.set_xticklabels(ticklabels, rotation='45', fontsize=14)
ax.set_xlabel('Bloom Filter False Positive Rate')
ax.set_ylabel('Runtime (sec)')
plt.savefig(os.path.join(path, 'figs/{}.png'.format(fig_name)))
plt.savefig(os.path.join(path, 'figs/pdf/{}.pdf'.format(fig_name)))


#########
# COST
############


fp_rate = fp_rates[FIXED_FP_RATE_IDX]
bval = bvals[FIXED_B_VAL_IDX]
labels = ['Baseline Join', 'Filtered Join', 'Bloom Join']
fig_name = 'join-aval-cost'

fig, ax = plt.subplots(figsize=(8, 4))
width = 0.2
bars = [0] * len(names)
for cid, name in enumerate(names):
    transfer_cost = []
    scan_cost = []
    request_cost = []
    comp_cost = []
    for aval in avals:
        vals = []
        rts = []
        for trial in trials:
            if name != 'bloom':
                t = parse('{}/synthetic_join_2_{}_sf{}_aval{}_bval{}_trial{}.txt'.format(path, name, sf, aval, bval, trial))
            else:
                t = parse('{}/synthetic_join_2_{}_sf{}_aval{}_bval{}_fp{}_trial{}.txt'.format(path, name, sf, aval, bval, fp_rate, trial))
            rts.append(t)
            if name == 'baseline':
                val = calculate_download_cost(t)
            else:
                val = calculate_cost(t)
            vals.append(val)
        total_cost = [x[0] for x in vals]
        min_index = rts.index(min(rts))
        transfer_cost.append(vals[min_index][1])
        scan_cost.append(vals[min_index][2])
        request_cost.append(vals[min_index][3])
        comp_cost.append(vals[min_index][4])
    bottom = [0] * len(comp_cost)
    # plot legend
    pos = [x + width + width * cid for x in range(len(avals))]
    b_compute = ax.bar(pos, bottom, width=width, color='w',
                       label='Compute Cost')
    b_request = ax.bar(pos, bottom, width=width, color='w', hatch='xxx',
                       label='Request Cost')
    b_scan = ax.bar(pos, bottom, width=width, color='w', hatch='\\\\\\',
                    label='Scan Cost')
    b_transfer = ax.bar(pos, bottom, width=width, color='w', hatch='////',
                        label='Transfer Cost')

    # plot bars
    ax.bar(pos, transfer_cost, width=width, color=colors[cid], hatch='////')
    bottom = [x + y for x, y in zip(bottom, transfer_cost)]
    ax.bar(pos, scan_cost, bottom=bottom, width=width, color=colors[cid],
           hatch='\\\\\\')
    bottom = [x + y for x, y in zip(bottom, scan_cost)]
    ax.bar(pos, request_cost, bottom=bottom, width=width, color=colors[cid],
           hatch='xxx')
    bottom = [x + y for x, y in zip(bottom, request_cost)]
    b = ax.bar(pos, comp_cost, bottom=bottom, label=labels[cid], width=width,
               color=colors[cid])
    bars[cid] = b[0]

ax.set_xticks([x + width + width / 2.0 + width for x in range(len(avals))])
# ax.set_xlim([-1.5 * width, 6 - 1.5 * width])

# ax.legend(ncol=3, bbox_to_anchor=[0.99, 1.18], fontsize=14)
lg1 = ax.legend(bars, labels, ncol=3, bbox_to_anchor=[0.99, 1.18], fontsize=14)
lg2 = ax.legend([b_compute[0], b_request[0], b_scan[0], b_transfer[0]],
                ['Compute Cost', 'Request Cost', 'Scan Cost', 'Transfer Cost'],
                ncol=2, bbox_to_anchor=[0.99, 0.99], fontsize=14)
plt.gca().add_artist(lg1)
plt.gca().add_artist(lg2)
# plt.subplots_adjust(left=0.12, right=0.99, bottom=0.15, top=0.88)
plt.subplots_adjust(left=0.15, right=0.99, bottom=0.15, top=0.88)

# ax.set_xticklabels(['$10^{-7}$', '$10^{-6}$', '$10^{-5}$', '$10^{-4}$', '$10^{-3}$', '$10^{-2}$'], fontsize=16)
ticklabels = []
for aval in avals:
    ticklabels.append("{}".format(aval))
ax.set_xticklabels(ticklabels, fontsize=14)
# ax.text(4.8, 0.092, '0.30', fontsize=16)
ax.set_ylim([0, 0.02])
# ax.set_xlabel('Filter Selectivity', fontsize=16)
ax.set_xlabel('Customer Filter Selectivity (c_acctbal <= ?)')
ax.set_ylabel('Cost ($)', fontsize=16)
plt.savefig(os.path.join(path, 'figs/{}.png'.format(fig_name)))
plt.savefig(os.path.join(path, 'figs/pdf/{}.pdf'.format(fig_name)))






fp_rate = fp_rates[FIXED_FP_RATE_IDX]
aval = avals[FIXED_A_VAL_IDX]
labels = ['Baseline Join', 'Filtered Join', 'Bloom Join']
fig_name = 'join-bval-cost'

fig, ax = plt.subplots(figsize=(8, 4))
width = 0.2
bars = [0] * len(names)
for cid, name in enumerate(names):
    transfer_cost = []
    scan_cost = []
    request_cost = []
    comp_cost = []
    for bval in bvals:
        vals = []
        rts = []
        for trial in trials:
            if name != 'bloom':
                t = parse('{}/synthetic_join_2_{}_sf{}_aval{}_bval{}_trial{}.txt'.format(path, name, sf, aval, bval, trial))
            else:
                t = parse('{}/synthetic_join_2_{}_sf{}_aval{}_bval{}_fp{}_trial{}.txt'.format(path, name, sf, aval, bval, fp_rate, trial))
            rts.append(t)
            if name == 'baseline':
                val = calculate_download_cost(t)
            else:
                val = calculate_cost(t)
            vals.append(val)
        total_cost = [x[0] for x in vals]
        min_index = rts.index(min(rts))
        transfer_cost.append(vals[min_index][1])
        scan_cost.append(vals[min_index][2])
        request_cost.append(vals[min_index][3])
        comp_cost.append(vals[min_index][4])
    bottom = [0] * len(comp_cost)
    # plot legend
    pos = [x + width + width * cid for x in range(len(bvals))]
    b_compute = ax.bar(pos, bottom, width=width, color='w',
                       label='Compute Cost')
    b_request = ax.bar(pos, bottom, width=width, color='w', hatch='xxx',
                       label='Request Cost')
    b_scan = ax.bar(pos, bottom, width=width, color='w', hatch='\\\\\\',
                    label='Scan Cost')
    b_transfer = ax.bar(pos, bottom, width=width, color='w', hatch='////',
                        label='Transfer Cost')

    # plot bars
    ax.bar(pos, transfer_cost, width=width, color=colors[cid], hatch='////')
    bottom = [x + y for x, y in zip(bottom, transfer_cost)]
    ax.bar(pos, scan_cost, bottom=bottom, width=width, color=colors[cid],
           hatch='\\\\\\')
    bottom = [x + y for x, y in zip(bottom, scan_cost)]
    ax.bar(pos, request_cost, bottom=bottom, width=width, color=colors[cid],
           hatch='xxx')
    bottom = [x + y for x, y in zip(bottom, request_cost)]
    b = ax.bar(pos, comp_cost, bottom=bottom, label=labels[cid], width=width,
               color=colors[cid])
    bars[cid] = b[0]

ax.set_xticks([x + width + width / 2.0 + width for x in range(len(bvals))])
# ax.set_xlim([-1.5 * width, 6 - 1.5 * width])

# ax.legend(ncol=3, bbox_to_anchor=[0.99, 1.18], fontsize=14)
lg1 = ax.legend(bars, labels, ncol=3, bbox_to_anchor=[0.99, 1.25], fontsize=14)
lg2 = ax.legend([b_compute[0], b_request[0], b_scan[0], b_transfer[0]],
                ['Compute Cost', 'Request Cost', 'Scan Cost', 'Transfer Cost'],
                ncol=2, bbox_to_anchor=[0.99, 0.99], fontsize=14)
plt.gca().add_artist(lg1)
plt.gca().add_artist(lg2)
# plt.subplots_adjust(left=0.12, right=0.99, bottom=0.15, top=0.88)
plt.subplots_adjust(left=0.15, right=0.99, bottom=0.35, top=0.88)

# ax.set_xticklabels(['$10^{-7}$', '$10^{-6}$', '$10^{-5}$', '$10^{-4}$', '$10^{-3}$', '$10^{-2}$'], fontsize=16)
ticklabels = []
for bval in bvals:
    ticklabels.append("{}".format(bval))
ax.set_xticklabels(ticklabels, fontsize=14, rotation=45)
# ax.text(4.8, 0.092, '0.30', fontsize=16)
ax.set_ylim([0, 0.025])
# ax.set_xlabel('Filter Selectivity', fontsize=16)
ax.set_xlabel('Order Filter Selectivity (o_orderdate < ?)')
ax.set_ylabel('Cost ($)', fontsize=16)
plt.savefig(os.path.join(path, 'figs/{}.png'.format(fig_name)))
plt.savefig(os.path.join(path, 'figs/pdf/{}.pdf'.format(fig_name)))





aval = avals[FIXED_A_VAL_IDX]
bval = bvals[FIXED_B_VAL_IDX]
labels = ['Baseline Join', 'Filtered Join', 'Bloom Join']
fig_name = 'join-fp_rate-cost'

fig, ax = plt.subplots(figsize=(8, 4))
width = 0.4
bars = [0] * len(names)
for cid, name in enumerate(names):
    transfer_cost = []
    scan_cost = []
    request_cost = []
    comp_cost = []
    if name != 'bloom':
        vals = []
        rts = []
        for trial in trials:
            t = parse('{}/synthetic_join_2_{}_sf{}_aval{}_bval{}_trial{}.txt'
                      .format(path, name, sf, aval, bval, trial))
            rts.append(t)
            if name == 'baseline':
                val = calculate_download_cost(t)
            else:
                val = calculate_cost(t)
            vals.append(val)
        total_cost = [x[0] for x in vals]
        min_index = rts.index(min(rts))
        transfer_cost.append(vals[min_index][1])
        scan_cost.append(vals[min_index][2])
        request_cost.append(vals[min_index][3])
        comp_cost.append(vals[min_index][4])

        bottom = [0] * len(comp_cost)
        pos = [cid + width]
        # plot legend
        b_compute = ax.bar(pos, bottom, width=width, color='w',
                           label='Compute Cost')
        b_request = ax.bar(pos, bottom, width=width, color='w', hatch='xxx',
                           label='Request Cost')
        b_scan = ax.bar(pos, bottom, width=width, color='w', hatch='\\\\\\',
                        label='Scan Cost')
        b_transfer = ax.bar(pos, bottom, width=width, color='w',
                            hatch='////',
                            label='Transfer Cost')

        # plot bars
        ax.bar(pos, transfer_cost, width=width, color=colors[cid],
               hatch='////')
        bottom = [x + y for x, y in zip(bottom, transfer_cost)]
        ax.bar(pos, scan_cost, bottom=bottom, width=width,
               color=colors[cid],
               hatch='\\\\\\')
        bottom = [x + y for x, y in zip(bottom, scan_cost)]
        ax.bar(pos, request_cost, bottom=bottom, width=width,
               color=colors[cid],
               hatch='xxx')
        bottom = [x + y for x, y in zip(bottom, request_cost)]
        b = ax.bar(pos, comp_cost, bottom=bottom, label=labels[cid],
                   width=width,
                   color=colors[cid])
        bars[cid] = b[0]
    else:
        for fp_rate in fp_rates:
            vals = []
            rts = []
            for trial in trials:
                if name != 'bloom':
                    t = parse('{}/synthetic_join_2_{}_sf{}_aval{}_bval{}_trial{}.txt'.format(path, name, sf, aval, bval, trial))
                else:
                    t = parse('{}/synthetic_join_2_{}_sf{}_aval{}_bval{}_fp{}_trial{}.txt'.format(path, name, sf, aval, bval, fp_rate, trial))
                rts.append(t)
                val = calculate_cost(t)
                vals.append(val)
            total_cost = [x[0] for x in vals]
            min_index = rts.index(min(rts))
            transfer_cost.append(vals[min_index][1])
            scan_cost.append(vals[min_index][2])
            request_cost.append(vals[min_index][3])
            comp_cost.append(vals[min_index][4])
        bottom = [0] * len(comp_cost)
        # plot legend
        pos = [width * 4.0 + x + cid * width for x in range(len(fp_rates))]
        b_compute = ax.bar(pos, bottom, width=width, color='w',
                           label='Compute Cost')
        b_request = ax.bar(pos, bottom, width=width, color='w', hatch='xxx',
                           label='Request Cost')
        b_scan = ax.bar(pos, bottom, width=width, color='w', hatch='\\\\\\',
                        label='Scan Cost')
        b_transfer = ax.bar(pos, bottom, width=width, color='w', hatch='////',
                            label='Transfer Cost')

        # plot bars
        ax.bar(pos, transfer_cost, width=width, color=colors[cid], hatch='////')
        bottom = [x + y for x, y in zip(bottom, transfer_cost)]
        ax.bar(pos, scan_cost, bottom=bottom, width=width, color=colors[cid],
               hatch='\\\\\\')
        bottom = [x + y for x, y in zip(bottom, scan_cost)]
        ax.bar(pos, request_cost, bottom=bottom, width=width, color=colors[cid],
               hatch='xxx')
        bottom = [x + y for x, y in zip(bottom, request_cost)]
        b = ax.bar(pos, comp_cost, bottom=bottom, label=labels[cid], width=width,
                   color=colors[cid])
        bars[cid] = b[0]

ax.set_xticks([x + width + width / 2.0 for x in range(len(fp_rates) + 2)])
ax.set_xlim([-0.1* width, 8 + width -0.2])

# ax.legend(ncol=3, bbox_to_anchor=[0.99, 1.18], fontsize=14)
lg1 = ax.legend(bars, labels, ncol=3, bbox_to_anchor=[0.99, 1.21], fontsize=14)
lg2 = ax.legend([b_compute[0], b_request[0], b_scan[0], b_transfer[0]],
                ['Compute Cost', 'Request Cost', 'Scan Cost', 'Transfer Cost'],
                ncol=2, bbox_to_anchor=[0.99, 0.99], fontsize=14)
plt.gca().add_artist(lg1)
plt.gca().add_artist(lg2)
# plt.subplots_adjust(left=0.12, right=0.99, bottom=0.15, top=0.88)
plt.subplots_adjust(left=0.15, right=0.99, bottom=0.25, top=0.88)

# ax.set_xticklabels(['$10^{-7}$', '$10^{-6}$', '$10^{-5}$', '$10^{-4}$', '$10^{-3}$', '$10^{-2}$'], fontsize=16)
ticklabels = []
ticklabels.append('')
ticklabels.append('')
for fp_rate in fp_rates:
    ticklabels.append("{}".format(fp_rate))
ax.set_xticklabels(ticklabels, fontsize=14, rotation=45)
# ax.text(4.8, 0.092, '0.30', fontsize=16)
ax.set_ylim([0, 0.02])
# ax.set_xlabel('Filter Selectivity', fontsize=16)
ax.set_xlabel('Bloom Filter False Positive Rate')
ax.set_ylabel('Cost ($)', fontsize=16)
plt.savefig(os.path.join(path, 'figs/{}.png'.format(fig_name)))
plt.savefig(os.path.join(path, 'figs/pdf/{}.pdf'.format(fig_name)))



##############################
## Bytes returned
##############################
fp_rate = fp_rates[FIXED_FP_RATE_IDX]
bval = bvals[FIXED_B_VAL_IDX]
fig_name = 'join-aval-byteret'
trial = 1

fig, ax = plt.subplots(figsize=(8, 4))
width = 0.2
for cid, name in enumerate(names):
    data = []
    for aval in avals:
        if name != 'bloom':
            res2 = parse('{}/synthetic_join_2_{}_sf{}_aval{}_bval{}_trial{}.txt'.format(path, name, sf, aval, bval, trial))[1]
            data.append(res2)
        else:
            res2 = parse('{}/synthetic_join_2_{}_sf{}_aval{}_bval{}_fp{}_trial{}.txt'.format(path, name, sf, aval, bval, fp_rate, trial))[1]
            data.append(res2)
    pos = [x + width + width * cid for x in range(len(avals))]
    gb = map(lambda x: x / 1024.0 / 1024.0 / 1024.0, data)
    ax.bar(pos, gb, width=width, color=colors[cid], label=labels[cid])
    # ax.semilogx(avals, data, label=name, color=colors[cid])
ax.set_xticks([x + width + width / 2.0 + width for x in range(len(avals))])
ax.legend(ncol=3, bbox_to_anchor=[0.99, 1.18], fontsize=14)
plt.subplots_adjust(left=0.15, right=0.99, bottom=0.15, top=0.88)
ticklabels = []
for aval in avals:
    ticklabels.append("{}".format(aval))
ax.set_xticklabels(ticklabels, fontsize=14)
# ax.set_xlabel('Selectivity')
ax.set_xlabel('Customer Filter Selectivity (c_acctbal <= ?)')
ax.set_ylabel('Bytes Returned (GB)')
plt.savefig(os.path.join(path, 'figs/{}.png'.format(fig_name)))
plt.savefig(os.path.join(path, 'figs/pdf/{}.pdf'.format(fig_name)))



fp_rate = fp_rates[2]
aval = avals[FIXED_A_VAL_IDX]
fig_name = 'join-bval-byteret'
trial = 1

fig, ax = plt.subplots(figsize=(8, 4))
width = 0.2
for cid, name in enumerate(names):
    data = []
    for bval in bvals:
        if name != 'bloom':
            res2 = parse('{}/synthetic_join_2_{}_sf{}_aval{}_bval{}_trial{}.txt'.format(path, name, sf, aval, bval, trial))[1]
            data.append(res2)
        else:
            res2 = parse('{}/synthetic_join_2_{}_sf{}_aval{}_bval{}_fp{}_trial{}.txt'.format(path, name, sf, aval, bval, fp_rate, trial))[1]
            data.append(res2)
    pos = [x + width + width * cid for x in range(len(bvals))]
    gb = map(lambda x: x / 1024.0 / 1024.0 / 1024.0, data)
    ax.bar(pos, gb, width=width, color=colors[cid], label=labels[cid])
    # ax.semilogx(avals, data, label=name, color=colors[cid])
ax.set_xticks([x + width + width / 2.0 + width for x in range(len(bvals))])
ax.legend(ncol=3, bbox_to_anchor=[0.99, 1.25], fontsize=14)
plt.subplots_adjust(left=0.15, right=0.99, bottom=0.35, top=0.88)
ticklabels = []
for bval in bvals:
    ticklabels.append("{}".format(bval))
ax.set_xticklabels(ticklabels, fontsize=14, rotation=45)
# ax.set_xlabel('Selectivity')
ax.set_xlabel('Order Filter Selectivity (o_orderdate < ?)')
ax.set_ylabel('Bytes Returned (GB)')
plt.savefig(os.path.join(path, 'figs/{}.png'.format(fig_name)))
plt.savefig(os.path.join(path, 'figs/pdf/{}.pdf'.format(fig_name)))




bval = bvals[FIXED_B_VAL_IDX]
aval = avals[FIXED_A_VAL_IDX]
fig_name = 'join-fp_rate-byteret'
trial = 1

fig, ax = plt.subplots(figsize=(8, 4))
width = 0.4
for cid, name in enumerate(names):
    data = []
    if name != 'bloom':
        res2 = parse('{}/synthetic_join_2_{}_sf{}_aval{}_bval{}_trial{}.txt'.format(path, name, sf, aval, bval, trial))[1]
        data.append(res2)
        pos = [cid + width]
        gb = map(lambda x: x / 1024.0 / 1024.0 / 1024.0, data)
        ax.bar(pos, gb, width=width, color=colors[cid], label=labels[cid])
    else:
        for fp_rate in fp_rates:
            res2 = parse('{}/synthetic_join_2_{}_sf{}_aval{}_bval{}_fp{}_trial{}.txt'.format(path, name, sf, aval, bval, fp_rate, trial))[1]
            data.append(res2)
            pos = [width * 4.0 + x + cid * width for x in range(len(fp_rates))]
        gb = map(lambda x: x / 1024.0 / 1024.0 / 1024.0, data)
        ax.bar(pos, gb, width=width, color=colors[cid], label=labels[cid])
        # ax.semilogx(avals, data, label=name, color=colors[cid])
ax.set_xticks([x + width + width / 2.0 for x in range(len(fp_rates) + 2)])
ax.set_xlim([-0.1* width, 8 + width -0.2])
ax.legend(ncol=3, bbox_to_anchor=[0.99, 1.21], fontsize=14)
plt.subplots_adjust(left=0.15, right=0.99, bottom=0.25, top=0.88)
ticklabels = []
ticklabels.append('')
ticklabels.append('')
for fp_rate in fp_rates:
    ticklabels.append("{}".format(fp_rate))
ax.set_xticklabels(ticklabels, fontsize=14, rotation=45)
# ax.set_xlabel('Selectivity')
ax.set_xlabel('Bloom Filter False Positive Rate')
ax.set_ylabel('Bytes Returned (GB)')
plt.savefig(os.path.join(path, 'figs/{}.png'.format(fig_name)))
plt.savefig(os.path.join(path, 'figs/pdf/{}.pdf'.format(fig_name)))

# ##############################
# ## runtime.
# ##############################
# fig, ax = plt.subplots(figsize=(8, 5))
# width = 0.2
# for cid, name in enumerate(names):
#     data = []
#     for aval in avals:
#         for bval in bvals:
#             for fp_rate in fp_rates:
#                 if name != 'bloom':
#                     rts = []
#                     for trial in trials:
#                         t = parse('{}/synthetic_join_2_{}_sf{}_aval{}_bval{}_trial{}.txt'
#                                   .format(path, name, sf, aval, bval, trial))[0]
#                         rts.append(t)
#                     data.append(min(rts))
#                 else:
#                     data.append(0)
#     pos = [x + width * cid for x in range(len(avals) * len(bvals) * len(fp_rates))]
#     ax.bar(pos, data, width=width, color=colors[cid], label=labels[cid])
#     # ax.bar(pos, data, width=width)
#     # ax.semilogx(sels, data, label=name, color=colors[cid])
# ax.set_xticks([x + width for x in range(len(avals) * len(bvals) * len(fp_rates))])
# # ax.set_xlim([-1.5 * width, 12 - 1.5 * width])
# # ax.legend(loc='best')
# ax.legend(ncol=3, bbox_to_anchor=[0.99, 1.18], fontsize=14)
# plt.subplots_adjust(left=0.12, right=0.99, bottom=0.35, top=0.88)
#
# # ax.set_xticklabels(['$10^{-7}$', '$10^{-6}$', '$10^{-5}$', '$10^{-4}$', '$10^{-3}$', '$10^{-2}$'])
# ticklabels = []
# for aval in avals:
#     for bval in bvals:
#         for fp_rate in fp_rates:
#             ticklabels.append("a: {}\nb: {}\n: fp: {}".format(aval, bval, fp_rate))
# ax.set_xticklabels(ticklabels, rotation='45', fontsize=10)
# ax.set_xlabel('Filter Selectivity')
# ax.set_ylabel('Runtime (sec)')
# plt.savefig(os.path.join(path, 'figs/filter-rt.png'))
# plt.savefig(os.path.join(path, 'figs/pdf/filter-rt.pdf'))
#
#
#
# ##############################
# ## Cost
# ##############################
# fig, ax = plt.subplots(figsize=(8, 4))
# width = 0.2
# bars = [0] * len(names)
# for cid, name in enumerate(names):
#     transfer_cost = []
#     scan_cost = []
#     request_cost = []
#     comp_cost = []
#     for aval in avals:
#         vals = []
#         rts = []
#         for bval in bvals:
#             if name != 'bloom':
#                 t = \
#                     parse('{}/synthetic_join_2_{}_sf{}_aval{}_bval{}.txt'.format(path, name, sf, aval, bval))
#                 rts.append(t)
#                 val = calculate_cost(t)
#                 vals.append(val)
#             else:
#                 for fp_rate in fp_rates:
#                     t = parse('{}/synthetic_join_2_{}_sf{}_aval{}_bval{}_fp{}.txt'.format(path, name, sf, aval, bval, fp_rate))
#                     rts.append(t)
#                     val = calculate_cost(t)
#                     vals.append(val)
#         total_cost = [x[0] for x in vals]
#         min_index = rts.index(min(rts))
#         transfer_cost.append(vals[min_index][1])
#         scan_cost.append(vals[min_index][2])
#         request_cost.append(vals[min_index][3])
#         comp_cost.append(vals[min_index][4])
#     bottom = [0] * len(comp_cost)
#     # plot legend
#     b_compute = ax.bar([x + cid * width for x in range(len(avals))], bottom, width=width, color='w',
#                        label='Compute Cost')
#     b_request = ax.bar([x + cid * width for x in range(len(avals))], bottom, width=width, color='w', hatch='xxx',
#                        label='Request Cost')
#     b_scan = ax.bar([x + cid * width for x in range(len(avals))], bottom, width=width, color='w', hatch='\\\\\\',
#                     label='Scan Cost')
#     b_transfer = ax.bar([x + cid * width for x in range(len(avals))], bottom, width=width, color='w', hatch='////',
#                         label='Transfer Cost')
#
#     # plot bars
#     ax.bar([x + cid * width for x in range(len(avals))], transfer_cost, width=width, color=colors[cid], hatch='////')
#     bottom = [x + y for x, y in zip(bottom, transfer_cost)]
#     ax.bar([x + cid * width for x in range(len(avals))], scan_cost, bottom=bottom, width=width, color=colors[cid],
#            hatch='\\\\\\')
#     bottom = [x + y for x, y in zip(bottom, scan_cost)]
#     ax.bar([x + cid * width for x in range(len(avals))], request_cost, bottom=bottom, width=width, color=colors[cid],
#            hatch='xxx')
#     bottom = [x + y for x, y in zip(bottom, request_cost)]
#     b = ax.bar([x + cid * width for x in range(len(avals))], comp_cost, bottom=bottom, label=labels[cid], width=width,
#                color=colors[cid])
#     bars[cid] = b[0]
#
# ax.set_xticks([x + width for x in range(6)])
# ax.set_xlim([-1.5 * width, 6 - 1.5 * width])
#
# # ax.legend(ncol=3, bbox_to_anchor=[0.99, 1.18], fontsize=14)
# lg1 = ax.legend(bars, labels, ncol=3, bbox_to_anchor=[0.99, 1.18], fontsize=14)
# lg2 = ax.legend([b_compute[0], b_request[0], b_scan[0], b_transfer[0]],
#                 ['Compute Cost', 'Request Cost', 'Scan Cost', 'Transfer Cost'],
#                 ncol=1, bbox_to_anchor=[0.35, 1.02], fontsize=14)
# plt.gca().add_artist(lg1)
# plt.gca().add_artist(lg2)
# plt.subplots_adjust(left=0.12, right=0.99, bottom=0.15, top=0.88)
#
# ax.set_xticklabels(['$10^{-7}$', '$10^{-6}$', '$10^{-5}$', '$10^{-4}$', '$10^{-3}$', '$10^{-2}$'], fontsize=16)
# ax.text(4.8, 0.092, '0.30', fontsize=16)
# ax.set_ylim([0, 0.1])
# ax.set_xlabel('Filter Selectivity', fontsize=16)
# ax.set_ylabel('Cost ($)', fontsize=16)
# plt.savefig(os.path.join(path, 'figs/filter-cost.png'))
# plt.savefig(os.path.join(path, 'figs/pdf/filter-cost.pdf'))

##############################
## Bytes returned
##############################
# names = ['baseline', 'filtered', 'bloom']
# fig, ax = plt.subplots(figsize=(10, 5))
# for cid, name in enumerate(names):
#     data = []
#     for aval in avals:
#         for bval in bvals:
#             if name != 'bloom':
#                 res2 = parse('{}/synthetic_join_2_{}_sf{}_aval{}_bval{}.txt'.format(path, name, sf, aval, bval))[1]
#                 data.append(res2)
#             else:
#                 for fp_rate in fp_rates:
#                     res2 = parse('{}/synthetic_join_2_{}_sf{}_aval{}_bval{}_fp{}.txt'.format(path, name, sf, aval, bval, fp_rate))[1]
#                     data.append(res2)
#     ax.semilogx(avals, data, label=name, color=colors[cid])
# ax.legend(loc='best')
# ax.set_xlabel('Selectivity')
# ax.set_ylabel('Bytes Returned')
# plt.savefig(os.path.join(path, 'figs/byteret_index.png'))


