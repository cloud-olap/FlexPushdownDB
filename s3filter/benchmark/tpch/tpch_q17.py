# -*- coding: utf-8 -*-
"""TPCH Q17 Benchmarks

"""

from s3filter.benchmark.tpch import tpch_q17_baseline_join, tpch_q17_filtered_join, tpch_q17_bloom_join


def main():
    tpch_q17_baseline_join.main()
    tpch_q17_filtered_join.main()
    tpch_q17_bloom_join.main()


if __name__ == "__main__":
    main()
