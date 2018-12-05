# -*- coding: utf-8 -*-
"""TPCH Q14 Benchmarks

"""

from s3filter.benchmark.tpch import tpch_q14_baseline_join, tpch_q14_filtered_join, tpch_q14_bloom_join, tpch_results, \
    tpch_q14_baseline_join_opt


def main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, fp_rate, expected_result):
    # tpch_q14_baseline_join.main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, expected_result)
    tpch_q14_baseline_join_opt.main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, 2, expected_result)
    # tpch_q14_filtered_join.main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, expected_result)
    # tpch_q14_bloom_join.main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, fp_rate, expected_result)


if __name__ == "__main__":
    main(1, 2, False, 2, False, 0.1, tpch_results.q14_sf1_expected_result)
