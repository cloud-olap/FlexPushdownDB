# -*- coding: utf-8 -*-
"""TPCH Benchmarks

"""
from s3filter.benchmark.tpch import tpch_q14, tpch_q17, tpch_q19, tpch_results, tpch_q3
from s3filter.sql.format import Format


def main():
    # sf = 1
    # lineitem_parts = 4
    # part_parts = 4
    # customer_parts = 4
    # order_parts = 4
    # other_parts = 2
    # lineitem_sharded = False
    # part_sharded = False
    # customer_sharded = False
    # order_sharded = False
    # fp_rate = 0.01
    # q3_expected_result = tpch_results.q3_sf1_expected_result
    # q14_expected_result = tpch_results.q14_sf1_expected_result
    # q17_expected_result = tpch_results.q17_sf1_expected_result
    # q19_expected_result = tpch_results.q19_sf1_expected_result

    sf = 10
    lineitem_parts = 96
    part_parts = 96
    customer_parts = 96
    order_parts = 96
    lineitem_sharded = True
    part_sharded = True
    customer_sharded = True
    order_sharded = True
    other_parts = 32
    fp_rate = 0.001
    q3_expected_result = tpch_results.q3_sf10_testing_expected_result
    q14_expected_result = tpch_results.q14_sf10_expected_result
    q17_expected_result = tpch_results.q17_sf10_expected_result
    q19_expected_result = tpch_results.q19_sf10_expected_result

    print("--- TPCH START ---")

    trials = [1]
    formats = [Format.CSV, Format.PARQUET]

    # for trial in trials:
    #     tpch_q1.main(sf,
    #                  lineitem_parts, lineitem_sharded,
    #                  None, trial)
    for format_ in formats:
        for trial in trials:
            tpch_q3.main(sf, customer_parts, customer_sharded, order_parts, order_sharded, lineitem_parts,
                         lineitem_sharded,
                         other_parts, fp_rate, q3_expected_result, trial, format_)

    for format_ in formats:
        for trial in trials:
            tpch_q14.main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, other_parts, fp_rate,
                          q14_expected_result, trial, format_)
    for format_ in formats:
        for trial in trials:
            tpch_q17.main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, other_parts, fp_rate,
                          q17_expected_result, trial, format_)
    for format_ in formats:
        for trial in trials:
            tpch_q19.main(sf, lineitem_parts, lineitem_sharded, part_parts, part_sharded, other_parts, fp_rate,
                          q19_expected_result, trial, format_)

    print("--- TPCH END ---")


if __name__ == "__main__":
    main()
