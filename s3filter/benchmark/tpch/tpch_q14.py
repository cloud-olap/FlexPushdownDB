# -*- coding: utf-8 -*-
"""TPCH Q14 Benchmarks

"""

from s3filter.benchmark.tpch import tpch_q14_baseline_join, tpch_results


def main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, other_parts, fp_rate, expected_result):
    # tpch_q14_baseline_join.main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, other_parts, expected_result)
    tpch_q14_baseline_join.main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, other_parts, expected_result)
    # tpch_q14_filtered_join.main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, other_parts, expected_result)
    # tpch_q14_bloom_join.main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, other_parts, fp_rate, expected_result)


if __name__ == "__main__":
    main(1, 4, False, 4, False, 2, 0.1, tpch_results.q14_sf1_expected_result)
