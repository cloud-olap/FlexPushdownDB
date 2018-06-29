# -*- coding: utf-8 -*-
"""TPCH Q19 Benchmarks

"""

from s3filter.benchmark.tpch import tpch_q19_filtered_join, tpch_q19_baseline_join, tpch_q19_bloom_join


def main():
    tpch_q19_baseline_join.main()
    tpch_q19_filtered_join.main()
    tpch_q19_bloom_join.main()


if __name__ == "__main__":
    main()
