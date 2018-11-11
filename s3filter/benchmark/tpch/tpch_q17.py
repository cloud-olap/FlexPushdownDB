# -*- coding: utf-8 -*-
"""TPCH Q17 Benchmarks

"""

from s3filter.benchmark.tpch import tpch_q17_baseline_join, tpch_q17_filtered_join, tpch_q17_bloom_join


def main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, fp_rate, expected_result):
    # tpch_q17_baseline_join.main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, expected_result)
    # tpch_q17_filtered_join.main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, expected_result)
    tpch_q17_bloom_join.main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, fp_rate, expected_result)


if __name__ == "__main__":
    main()
