# -*- coding: utf-8 -*-
"""Join Benchmarks

"""

from s3filter.benchmark.join import synthetic_join_3_baseline, synthetic_join_2_filtered, synthetic_join_bloom, \
    synthetic_join_semi, synthetic_join_2_baseline, synthetic_join_3_filtered


def main():
    synthetic_join_2_baseline.main()
    synthetic_join_3_baseline.main()
    synthetic_join_2_filtered.main()
    synthetic_join_3_filtered.main()
    synthetic_join_bloom.main()
    synthetic_join_semi.main()


if __name__ == "__main__":
    main()
