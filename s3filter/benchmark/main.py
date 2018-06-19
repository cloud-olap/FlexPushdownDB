# -*- coding: utf-8 -*-
"""Join Benchmarks

"""

from s3filter.benchmark import tpch_q14, tpch_q17, tpch_q19


def main():
    # tpch_q14.main()
    tpch_q17.main()
    tpch_q19.main()


if __name__ == "__main__":
    main()
