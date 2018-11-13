# -*- coding: utf-8 -*-
"""TPCH Q14 Benchmarks

"""

from s3filter.benchmark.tpch import tpch_q14_baseline_join, tpch_q14_filtered_join, tpch_q14_bloom_join, tpch_results, \
    tpch_q3_baseline_join, tpch_q3_filtered_join, tpch_q3_bloom_join


def main(sf, customer_parts, customer_sharded, order_parts, order_sharded, lineitem_parts, lineitem_sharded, fp_rate,
         expected_result):
    # tpch_q3_baseline_join.main(sf, customer_parts, customer_sharded, order_parts, order_sharded, lineitem_parts,
    #                            lineitem_sharded, expected_result)
    # tpch_q3_filtered_join.main(sf, customer_parts, customer_sharded, order_parts, order_sharded, lineitem_parts,
    #                            lineitem_sharded, expected_result)
    tpch_q3_bloom_join.main(sf, customer_parts, customer_sharded, order_parts, order_sharded, lineitem_parts,
                            lineitem_sharded, fp_rate, expected_result)


if __name__ == "__main__":
    main(1, 2, False, 2, False, 2, False, 0.1, tpch_results.q3_sf1_testing_expected_result)
