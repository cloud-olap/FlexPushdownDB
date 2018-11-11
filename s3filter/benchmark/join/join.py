# -*- coding: utf-8 -*-
"""Join Benchmarks

"""

from s3filter.benchmark.join import synthetic_join_3_baseline, synthetic_join_2_filtered, synthetic_join_3_bloom, \
    synthetic_join_2_semi, synthetic_join_2_baseline, synthetic_join_3_filtered, synthetic_join_2_bloom, \
    synthetic_join_3_semi
from s3filter.benchmark.join.join_result import SF1_JOIN_2_RESULT, SF1_JOIN_3_RESULT, SF10_JOIN_2_RESULT, \
    SF10_JOIN_3_RESULT


def main():
    print("--- SYNTHETIC JOIN START ---")

    # sf = 1
    # expected_2_result = SF1_JOIN_2_RESULT
    # expected_3_result = SF1_JOIN_3_RESULT
    # parts = 32
    # sharded = True

    sf = 10
    expected_2_result = SF10_JOIN_2_RESULT
    # expected_3_result = SF10_JOIN_3_RESULT
    parts = 96
    sharded = True

    # parts = 2
    # sharded = False

    # fp_rate = 0.0001
    # fp_rate = 0.001
    # fp_rate = 0.01
    # fp_rate = 0.1
    fp_rate = 0.3
    # fp_rate = 0.5

    # 2 way join
    # synthetic_join_2_baseline.main(sf, parts, sharded, expected_result=expected_2_result)
    # synthetic_join_2_filtered.main(sf, parts, sharded, expected_result=expected_2_result)
    synthetic_join_2_bloom.main(sf, parts, sharded, fp_rate=fp_rate, expected_result=expected_2_result)
    # synthetic_join_2_semi.main(sf, parts, sharded, fp_rate=fp_rate, expected_result=expected_2_result)

    # 3 way join
    # synthetic_join_3_baseline.main(sf, parts, sharded, expected_result=expected_3_result)
    # synthetic_join_3_filtered.main(sf, parts, sharded, expected_result=expected_3_result)

    # synthetic_join_3_bloom.main(sf, parts, sharded, fp_rate=fp_rate, expected_result=expected_3_result)
    # synthetic_join_3_semi.main(sf, parts, sharded, fp_rate=fp_rate, expected_result=expected_3_result)

    print("--- SYNTHETIC JOIN END ---")


if __name__ == "__main__":
    main()
