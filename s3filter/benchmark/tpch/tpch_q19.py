# -*- coding: utf-8 -*-
"""TPCH Q19 Benchmarks

"""

from s3filter.benchmark.tpch import tpch_q19_filtered_join, tpch_q19_baseline_join, tpch_q19_bloom_join, tpch_results
from s3filter.benchmark.tpch.run_tpch import start_capture, end_capture


def main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, other_parts, fp_rate, expected_result, trial):

    out_file, path = start_capture('tpch_q19', sf, 'baseline', trial)

    tpch_q19_baseline_join.main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, other_parts, expected_result)

    end_capture(out_file, path)
    out_file, path = start_capture('tpch_q19', sf, 'filtered', trial)

    tpch_q19_filtered_join.main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, other_parts, expected_result)

    end_capture(out_file, path)
    out_file, path = start_capture('tpch_q19', sf, 'filtered', trial)

    tpch_q19_bloom_join.main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, other_parts, fp_rate, expected_result)

    end_capture(out_file, path)


if __name__ == "__main__":
    main(1, 4, False, 4, False, 2, 0.01, tpch_results.q19_sf1_expected_result, 1)
