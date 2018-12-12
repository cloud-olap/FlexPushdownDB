import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

lines = []
file = '/Users/ghanemabdo/Work/DS/s3filter/indexed_sampling_topk_stats_avg.csv'
bar_width = 0.35
print_labels = False


def import_data(file_path):
    with open(file_path, 'r') as stats_file:
        is_header = True
        for line in stats_file:
            if is_header:
                is_header = False
                continue
            line_components = line.strip().split(',')
            lines.append(line_components[4:])


def autolabel(ax, rects, datatype=float, heights=None, vmargin=0.0, hmargin=0.0):
    """
    Attach a text label above each bar displaying its height
    """
    ymax = ax.get_ylim()[1]
    for idx, rect in enumerate(rects):
        height = rect.get_height() if heights is None else heights[idx]
        height_loc = ymax if height > ymax else height
        ax.text((1.0 + hmargin) * (rect.get_x() + rect.get_width()/2.), (1.05 + vmargin) * height_loc,
                '{0:.2f}'.format(datatype(height)) if datatype is float else '{}'.format(height),
                ha='center', va='bottom')


def plt_topk_runtime():
    fig, ax = plt.subplots()
    bar_width = 0.3
    index = np.arange(len(topk_ks))

    rects1 = plt.bar(index, baseline_runtimes, bar_width,
                     color='b',
                     label='Baseline',
                     edgecolor='black')

    rects2 = plt.bar(index + bar_width, headtable_runtimes, bar_width,
                     color='r',
                     label='Table_beginning_sampling',
                     edgecolor='black')

    rects3 = plt.bar(index + 2 * bar_width, indexedrange_runtimes, bar_width,
                     color='g',
                     label='Indexed_range_sampling',
                     edgecolor='black')

    plt.xlabel('K')
    plt.ylabel('Runtime')
    # plt.title('TopK Algorithms Runtime', y=1.0)
    # plt.text(0.0, -50, 'TopK Algorithms Runtime')
    plt.xticks(index + bar_width, topk_ks, rotation=45)
    plt.ylim(top=200)
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=3, fancybox=True, prop={'size': 9})

    if print_labels:
        autolabel(ax, rects1, datatype=float)
        autolabel(ax, rects2, datatype=float)
        autolabel(ax, rects3, datatype=float)

    autolabel(ax, [rects1[-1]], datatype=float, hmargin=-0.09, vmargin=-0.12)

    plt.tight_layout()
    # plt.show()
    fig.savefig('topk_runtime.pdf')
    plt.close(fig)


def plt_topk_cost():
    fig, ax = plt.subplots()
    bar_width = 0.3
    index = np.arange(len(topk_ks))

    baseline_datacost_plt = plt.bar(index, baseline_datacost, bar_width,
                                    color='b',
                                    label='Baseline',
                                    hatch='////',
                                    edgecolor='black')
    baseline_computecost_plt = plt.bar(index, baseline_computecost, bar_width, bottom=baseline_datacost,
                                       color='b',
                                       edgecolor='black')

    headtable_datacost_plt = plt.bar(index + bar_width, headtable_datacost, bar_width,
                                     color='r',
                                     label='Table_beginning_sampling',
                                     hatch='////',
                                     edgecolor='black')
    headtable_computecost_plt = plt.bar(index + bar_width, headtable_computecost, bar_width, bottom=headtable_datacost,
                                        color='r',
                                        edgecolor='black')

    indexedrange_datacost_plt = plt.bar(index + 2 * bar_width, indexedrange_datacost, bar_width,
                                        color='g',
                                        label='Indexed_range_sampling',
                                        hatch='////',
                                        edgecolor='black')
    indexedrange_computecost_plt = plt.bar(index + 2 * bar_width, indexedrange_computecost, bar_width,
                                           bottom=indexedrange_datacost,
                                           color='g',
                                           edgecolor='black')

    plt.xlabel('K')
    plt.ylabel('Cost (USD)')
    # plt.title('TopK Algorithms Cost')
    plt.xticks(index + bar_width, topk_ks, rotation=45)

    baseline_patch = mpatches.Patch(color='b', label='Baseline')
    table_beginning_patch = mpatches.Patch(color='r', label='Table Beginning')
    indexed_range_patch = mpatches.Patch(color='g', label='Indexed Range')
    first_legend = plt.legend(handles=[baseline_patch, table_beginning_patch, indexed_range_patch],
                              loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=3, fancybox=True, prop={'size': 10})
    ax = plt.gca().add_artist(first_legend)

    data_cost_patch = mpatches.Patch(facecolor='white', edgecolor='black', label='Data Cost', hatch='////')
    compute_cost_patch = mpatches.Patch(facecolor='white', edgecolor='black', label='Compute Cost')
    plt.legend(handles=[data_cost_patch, compute_cost_patch])

    if print_labels:
        autolabel(ax, baseline_computecost_plt, datatype=float,
                  heights=[baseline_datacost[i] + baseline_computecost[i] for i in range(len(baseline_datacost))])
        autolabel(ax, headtable_computecost_plt, datatype=float,
                  heights=[headtable_datacost[i] + headtable_computecost[i] for i in range(len(headtable_datacost))],
                  margin=0.05)
        autolabel(ax, indexedrange_computecost_plt, datatype=float,
                  heights=[indexedrange_datacost[i] + indexedrange_computecost[i] for i in
                           range(len(indexedrange_datacost))],
                  margin=0.1)

    plt.tight_layout()
    # plt.show()
    fig.savefig('topk_cost.pdf')
    plt.close(fig)


def plt_indexed_range_parallelism_runtime():
    fig, ax = plt.subplots()
    bar_width = 0.35
    index = np.arange(len(parallelism_degree))

    rects1 = plt.bar(index, k100_runtime, bar_width,
                     color='b',
                     label='K=100',
                     edgecolor='black')

    rects2 = plt.bar(index + bar_width, k10000_runtime, bar_width,
                     color='r',
                     label='K=10,000',
                     edgecolor='black')

    plt.xlabel('Number of Parallel Fetches')
    plt.ylabel('Runtime')
    # plt.title('Indexed Range Random Sampling TopK Runtime')
    plt.xticks(index + bar_width, parallelism_degree, rotation=45)
    # plt.ylim(top=200)
    plt.legend()

    if print_labels:
        autolabel(ax, rects1, datatype=float)
        autolabel(ax, rects2, datatype=float)

    plt.tight_layout()
    # plt.show()
    # fig = plt.figure()
    fig.savefig('indexed_topk_parallelism_runtime.pdf')
    plt.close(fig)


def plt_indexed_range_parallelism_cost():
    fig, ax = plt.subplots()
    bar_width = 0.35
    index = np.arange(len(parallelism_degree))

    k100_datacost_plt = plt.bar(index, k100_datacost, bar_width,
                                    color='b',
                                    label='K=100 data cost',
                                    hatch='////',
                                    edgecolor='black')
    k100_computecost_plt = plt.bar(index, k100_computecost, bar_width, bottom=k100_datacost,
                                    color='b',
                                    label='K=100 compute cost',
                                    edgecolor='black')

    k10000_datacost_plt = plt.bar(index + bar_width, k10000_datacost, bar_width,
                                     color='r',
                                     label='K=10,000 data cost',
                                     hatch='////',
                                     edgecolor='black')
    k10000_computecost_plt = plt.bar(index + bar_width, k10000_computecost, bar_width, bottom=k10000_datacost,
                                        color='r',
                                        label='K=10,000 compute cost',
                                        edgecolor='black')

    plt.xlabel('Number of Parallel Fetches')
    plt.ylabel('Cost (USD)')
    # plt.title('Indexed Range Random Sampling TopK Cost')
    plt.xticks(index + bar_width, parallelism_degree, rotation=45)

    k100_patch = mpatches.Patch(color='b', label='K=100')
    k10000_patch = mpatches.Patch(color='r', label='K=10,000')
    first_legend = plt.legend(handles=[k100_patch, k10000_patch],
                              loc='upper center', bbox_to_anchor=(0.5, 1.15), ncol=3, fancybox=True, prop={'size': 12})
    ax = plt.gca().add_artist(first_legend)

    data_cost_patch = mpatches.Patch(facecolor='white', edgecolor='black', label='Data Cost', hatch='////')
    compute_cost_patch = mpatches.Patch(facecolor='white', edgecolor='black', label='Compute Cost')
    plt.legend(handles=[data_cost_patch, compute_cost_patch])

    # if print_labels:
    # autolabel(ax, k100_datacost_plt, datatype=float,
    #           heights=[k100_datacost[i] + k100_computecost[i] for i in range(len(k100_datacost))])
    # autolabel(ax, k10000_datacost_plt, datatype=float,
    #           heights=[k10000_datacost[i] + k10000_computecost[i] for i in range(len(k10000_datacost))],
    #           margin=0.05)

    plt.tight_layout()
    # plt.show()
    fig.savefig('indexed_topk_parallelism_cost.pdf')
    plt.close(fig)


def plt_indexed_range_sample_size_runtime():
    fig, ax = plt.subplots()
    index = np.arange(len(sample_ratios))

    k100_runtime_plt = plt.plot(index, k100_sample_runtime, 'r', label='K=100')
    k10000_runtime_plt = plt.plot(index, k10000_sample_runtime, 'b', label='K=10,000')

    plt.xlabel('Sample Size Ratio From Optimal Sample Size')
    plt.ylabel('Runtime')
    # plt.title('Impact of Sample Size on TopK Runtime')
    plt.xticks(index + bar_width, sample_ratios)
    plt.legend()

    plt.tight_layout()
    # plt.show()
    fig.savefig('indexed_topk_samplesize_change_runtime.pdf')
    plt.close(fig)


def plt_indexed_range_sample_size_cost():
    fig, ax = plt.subplots()
    index = np.arange(len(sample_ratios))

    k100_cost_plt = plt.plot(index,
                             [k100_sample_datacost[i] + k100_sample_computecost[i] for i in
                              range(len(k100_sample_datacost))],
                             'r', label='K=100')
    k10000_cost_plt = plt.plot(index,
                               [k10000_sample_datacost[i] + k10000_sample_computecost[i] for i in
                                range(len(k10000_sample_datacost))],
                               'b', label='K=10,000')

    plt.xlabel('Sample Size Ratio From Optimal Sample Size')
    plt.ylabel('Cost (USD)')
    # plt.title('Impact of Sample Size on TopK Cost')
    plt.xticks(index + bar_width, sample_ratios)
    plt.legend()

    plt.tight_layout()
    # plt.show()
    fig.savefig('indexed_topk_samplesize_change_cost.pdf')
    plt.close(fig)


import_data(file)

lines = np.array(lines)
ks = lines[:, 0].astype(int)
sample_sizes = lines[:, 1].astype(int)
batch_sizes = lines[:, 2].astype(int)
runtimes = lines[:, 6].astype(float)
data_costs = lines[:, 10].astype(float)
compute_costs = lines[:, 11].astype(float)
total_costs = lines[:, 12].astype(float)

# comparing the three topk algorithms in terms of runtime and cost
topk_ks = ks[0:9]

baseline_runtimes = runtimes[0:9]
headtable_runtimes = runtimes[9:18]
indexedrange_runtimes = runtimes[18:27]

baseline_datacost = data_costs[0:9]
headtable_datacost = data_costs[9:18]
indexedrange_datacost = data_costs[18:27]

baseline_computecost = compute_costs[0:9]
headtable_computecost = compute_costs[9:18]
indexedrange_computecost = compute_costs[18:27]

plt_topk_runtime()
plt_topk_cost()

# testing parallesim effect on indexed range random sampling
parallelism_degree = [100, 200, 300, 400, 500, 1000, 5000, 10000, 50000, 1000000]

k100_runtime = runtimes[27:37]
k10000_runtime = runtimes[37:47]

k100_datacost = data_costs[27:37]
k10000_datacost = data_costs[37:47]

k100_computecost = compute_costs[27:37]
k10000_computecost = compute_costs[37:47]

plt_indexed_range_parallelism_runtime()
plt_indexed_range_parallelism_cost()

# testing the effect of sample size on indexed range random sampling with topk
sample_ratios = [0.1, 0.5, 1.0, 1.1, 1.5, 2.0]

k100_sample_runtime = runtimes[47:53]
k10000_sample_runtime = runtimes[53:59]

k100_sample_datacost = data_costs[47:53]
k10000_sample_datacost = data_costs[53:59]

k100_sample_computecost = compute_costs[47:53]
k10000_sample_computecost = compute_costs[53:59]

k100_sample_sizes = sample_sizes[47:53]
k10000_sample_sizes = sample_sizes[53:59]

plt_indexed_range_sample_size_runtime()
plt_indexed_range_sample_size_cost()
