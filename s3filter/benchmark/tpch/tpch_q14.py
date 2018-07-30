# -*- coding: utf-8 -*-
"""TPCH Q14 Benchmarks

"""

from s3filter.benchmark.tpch import tpch_q14_baseline_join


def main():
    # tpch_q14_baseline_join.main()
    # tpch_q14_filtered_join.main()
    # tpch_q14_bloom_join.main()
    # tpch_q14_partitioned_filtered_join.main()
    tpch_q14_baseline_join.main()
    # tpch_q14_sharded_filtered_join.main()
    # tpch_q14_partitioned_bloom_join.main()
    # tpch_q14_semi_join.main()


if __name__ == "__main__":
    main()
