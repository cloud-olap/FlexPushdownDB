# -*- coding: utf-8 -*-
"""Join Benchmarks

"""
from sys import stdout

from s3filter.benchmark.join import synthetic_join_2_filtered, synthetic_join_2_baseline, synthetic_join_2_bloom
from s3filter.benchmark.join.join_result import SF10_JOIN_2_RESULT


def main():
    print("--- SYNTHETIC JOIN START ---")

    # sf = 1
    # expected_2_result = SF1_JOIN_2_RESULT
    # expected_3_result = SF1_JOIN_3_RESULT
    # parts = 32
    # sharded = True

    sf = 10
    expected_2_result = SF10_JOIN_2_RESULT
    # # expected_3_result = SF10_JOIN_3_RESULT
    parts = 96
    sharded = True

    # parts = 4
    # sharded = False

    trials = [1, 2, 3]
    fp_rates = [0.0001, 0.001, 0.01, 0.1, 0.3, 0.5]
    # table_a_filter_vals = [-950, -500, -250, 500, 2000, 4000, None]
    table_a_filter_vals = [-850,-750,-650,-550, -450]
    # table_b_filter_vals = ['1992-03-01', '1992-06-01', '1993-01-01', '1994-01-01', '1995-01-01', None]
    table_b_filter_vals = [None]

    # 2 way join
    for table_a_filter_val in table_a_filter_vals:
        for table_b_filter_val in table_b_filter_vals:
            for trial in trials:
                stdout.write('baseline_{}_{}_{}\n'.format(table_a_filter_val, table_b_filter_val, trial))
                synthetic_join_2_baseline.main(sf, parts, sharded,
                                               table_a_filter_val=table_a_filter_val,
                                               table_b_filter_val=table_b_filter_val,
                                               expected_result=expected_2_result, trial=trial)
                stdout.write('filtered_{}_{}_{}\n'.format(table_a_filter_val, table_b_filter_val, trial))
                synthetic_join_2_filtered.main(sf, parts, sharded,
                                               table_a_filter_val=table_a_filter_val,
                                               table_b_filter_val=table_b_filter_val,
                                               expected_result=expected_2_result, trial=trial)
                for fp_rate in fp_rates:
                    stdout.write('bloom_{}_{}_{}_{}\n'.format(table_a_filter_val, table_b_filter_val, fp_rate, trial))
                    synthetic_join_2_bloom.main(sf, parts, sharded, fp_rate=fp_rate,
                                                table_a_filter_val=table_a_filter_val,
                                                table_b_filter_val=table_b_filter_val,
                                                expected_result=expected_2_result, trial=trial)

    # synthetic_join_2_semi.main(sf, parts, sharded, fp_rate=fp_rate,
    #                            table_a_filter_val=table_a_filter_val,
    #                            table_b_filter_val=table_b_filter_val,
    #                            expected_result=expected_2_result)
    # 3 way join
    # synthetic_join_3_baseline.main(sf, parts, sharded, expected_result=expected_3_result)
    # synthetic_join_3_filtered.main(sf, parts, sharded, expected_result=expected_3_result)
    # synthetic_join_3_bloom.main(sf, parts, sharded, fp_rate=fp_rate, expected_result=expected_3_result)
    # synthetic_join_3_semi.main(sf, parts, sharded, fp_rate=fp_rate, expected_result=expected_3_result)

    print("--- SYNTHETIC JOIN END ---")


if __name__ == "__main__":
    main()
