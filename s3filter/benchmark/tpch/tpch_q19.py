# -*- coding: utf-8 -*-
"""TPCH Q19 Benchmarks

"""

from s3filter.benchmark.tpch import tpch_q19_filtered_join, tpch_q19_baseline_join, tpch_q19_bloom_join, tpch_results


def main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, other_parts, fp_rate, expected_result):
    tpch_q19_baseline_join.main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, other_parts, expected_result)
    tpch_q19_filtered_join.main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, other_parts, expected_result)
    tpch_q19_bloom_join.main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, other_parts, fp_rate, expected_result)


if __name__ == "__main__":
    main(1, 2, False, 2, False, 0.1, tpch_results.q19_sf1_expected_result)
