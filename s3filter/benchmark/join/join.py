# -*- coding: utf-8 -*-
"""Join Benchmarks

"""

from s3filter.benchmark.join import synthetic_join_3_baseline, synthetic_join_2_filtered, synthetic_join_3_bloom, \
    synthetic_join_2_semi, synthetic_join_2_baseline, synthetic_join_3_filtered, synthetic_join_2_bloom, \
    synthetic_join_3_semi
from s3filter.benchmark.join.join_result import JOIN_2_RESULT, JOIN_3_RESULT


class ScaleSetting(object):

    def __init__(self, sf=1, parts=32, sharded=True):
        self.sf = sf
        self.parts = parts
        self.sharded = sharded


def main():
    print("--- SYNTHETIC JOIN START ---")

    sf = 1
    # sf = 10

    parts = 2
    # parts = 32
    # parts = 96

    # sharded = True
    sharded = False

    # fp_rate = 0.001
    # fp_rate = 0.01
    # fp_rate = 0.1
    fp_rate = 0.3
    # fp_rate = 0.5

    # 2 way join
    synthetic_join_2_baseline.main(sf, parts, sharded, expected_result=JOIN_2_RESULT)
    synthetic_join_2_filtered.main(sf, parts, sharded, expected_result=JOIN_2_RESULT)

    synthetic_join_2_bloom.main(sf, parts, sharded, fp_rate=fp_rate, expected_result=JOIN_2_RESULT)
    synthetic_join_2_semi.main(sf, parts, sharded, fp_rate=fp_rate, expected_result=JOIN_2_RESULT)

    # 3 way join
    synthetic_join_3_baseline.main(sf, parts, sharded, expected_result=JOIN_3_RESULT)
    synthetic_join_3_filtered.main(sf, parts, sharded, expected_result=JOIN_3_RESULT)

    synthetic_join_3_bloom.main(sf, parts, sharded, fp_rate=fp_rate, expected_result=JOIN_3_RESULT)
    synthetic_join_3_semi.main(sf, parts, sharded, fp_rate=fp_rate, expected_result=JOIN_3_RESULT)

    print("--- SYNTHETIC JOIN END ---")


if __name__ == "__main__":
    main()
