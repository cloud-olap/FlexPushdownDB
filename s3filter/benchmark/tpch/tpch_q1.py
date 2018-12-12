# -*- coding: utf-8 -*-
"""TPCH Q3 Benchmarks

"""

from s3filter.benchmark.tpch import tpch_results, tpch_q1_baseline_groupby, tpch_q1_filtered_groupby
from s3filter.benchmark.tpch.run_tpch import start_capture, end_capture


def main(sf, lineitem_parts, lineitem_sharded, expected_result, trial):

    out_file, path = start_capture('tpch_q1', sf, 'baseline', trial)

    tpch_q1_baseline_groupby.main(sf, lineitem_parts, lineitem_sharded)

    end_capture(out_file, path)
    out_file, path = start_capture('tpch_q1', sf, 'filtered', trial)

    tpch_q1_filtered_groupby.main(sf, lineitem_parts, lineitem_sharded)

    end_capture(out_file, path)


if __name__ == "__main__":
    main(1, 4, False, tpch_results.q1_sf1_expected_result, 1)
