# -*- coding: utf-8 -*-
"""Join Benchmarks

"""

from s3filter.benchmark.join import synthetic_join_baseline, synthetic_join_filtered, synthetic_join_bloom, \
    synthetic_join_semi


def main():
    synthetic_join_baseline.main()
    synthetic_join_filtered.main()
    synthetic_join_bloom.main()
    synthetic_join_semi.main()


if __name__ == "__main__":
    main()
