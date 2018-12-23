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
path = os.path.join(ROOT_DIR, "../aws-exps/tpch/sf{}/bench-06".format(sf))
filesystem_util.create_dirs(os.path.join(path, "figs"))
filesystem_util.create_dirs(os.path.join(path, "figs/pdf"))

# avals = [-950, -850, -750, -650, -550, -450]
# bvals = ['1992-03-01', '1992-06-01', '1993-01-01', '1994-01-01', '1995-01-01', None]
# for SF=1
# actual_sel = [ x / 6001216.0 for x in [1, 13, 35, 140, 866, 3375, 13122] ]
# actual_sel = [ x / 60000000.0 for x in [7, 95, 309, 1245, 8431, 32893, 130260] ]
qs = ['1', '3', '14', '17', '19']
names = ['baseline', 'filtered', 'bloom']
labels = ['Baseline Join', 'Filtered Join', 'Bloom Join']
trials = [1, 2, 3]
# sfs = [1, 10, 100]

# FIXED_A_VAL_IDX = 0
# FIXED_B_VAL_IDX = 5
# FIXED_FP_RATE_IDX = 2
#
#


# fp_rate = fp_rates[FIXED_FP_RATE_IDX]
# bval = bvals[FIXED_B_VAL_IDX]
# labels = ['Baseline Join', 'Filtered Join', 'Bloom Join']
fig_name = 'tpch-rt'

fig, ax = plt.subplots(figsize=(8, 4))
# width = 0.2
for cid, name in enumerate(names):
    data = []
    for q in qs:
        rts = []
        for trial in trials:
            if q == '1' and name == 'bloom':
                t = 0
            else:
                t = parse('{}/tpch_q{}_sf{}_{}_trial{}.txt'.format(path, q, sf, name, trial))[0]
            rts.append(t)
        data.append(min(rts))
    pos = [x + width * cid for x in range(len(qs))]
    ax.bar(pos, data, width=width, label=name)
    # ax.bar(pos, data, width=width)
    # ax.semilogx(sels, data, label=name, color=colors[cid])
ax.set_xticks([x + width for x in range(len(qs))])
# ax.set_xlim([-1.5 * width, 12 - 1.5 * width])
ax.legend(loc='best')
# ax.legend(ncol=3, bbox_to_anchor=[0.99, 1.18], fontsize=14)
# plt.subplots_adjust(left=0.15, right=0.99, bottom=0.15, top=0.88)
# ax.set_xticklabels(['$10^{-7}$', '$10^{-6}$', '$10^{-5}$', '$10^{-4}$', '$10^{-3}$', '$10^{-2}$'])
ticklabels = []
for q in qs:
    ticklabels.append("{}".format(q))
ax.set_xticklabels(ticklabels, fontsize=14)
# ax.set_xlabel('Customer Filter Selectivity (c_acctbal <= ?)')
# ax.set_ylabel('Runtime (sec)')
plt.savefig(os.path.join(path, 'figs/{}.png'.format(fig_name)))
plt.savefig(os.path.join(path, 'figs/pdf/{}.pdf'.format(fig_name)))

exit(0)

