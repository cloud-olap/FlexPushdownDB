# -*- coding: utf-8 -*-
"""TPCH Benchmarks

"""
from s3filter.benchmark.tpch import tpch_q14, tpch_q17, tpch_q19, tpch_results, tpch_q3


def main():
    sf = 1
    lineitem_parts = 2
    part_parts = 2
    lineitem_sharded = False
    part_sharded = False
    fp_rate = 0.1
    q14_expected_result = tpch_results.q14_sf1_expected_result
    q17_expected_result = tpch_results.q17_sf1_expected_result
    q19_expected_result = tpch_results.q19_sf1_expected_result

    # sf = 10
    # lineitem_parts = 96
    # part_parts = 96
    # customer_parts = 96
    # order_parts = 96
    # lineitem_sharded = True
    # part_sharded = True
    # customer_sharded = True
    # order_sharded = True
    # fp_rate = 0.3
    # q3_expected_result = tpch_results.q3_sf10_testing_expected_result
    # q14_expected_result = tpch_results.q14_sf10_expected_result
    # q17_expected_result = tpch_results.q17_sf10_expected_result
    # q19_expected_result = tpch_results.q19_sf10_expected_result

    print("--- TPCH START ---")

    # tpch_q3.main(sf, customer_parts, customer_sharded, order_parts, order_sharded, lineitem_parts, lineitem_sharded,
    #              fp_rate, q3_expected_result)
    tpch_q14.main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, fp_rate, q14_expected_result)
    # tpch_q17.main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, fp_rate, q17_expected_result)
    # tpch_q19.main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, fp_rate, q19_expected_result)

    print("--- TPCH END ---")


if __name__ == "__main__":
    main()
