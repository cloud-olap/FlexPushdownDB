# -*- coding: utf-8 -*-
"""Join Benchmarks

"""

from s3filter.benchmark.sql_table_scan import sql_table_scan
from s3filter.benchmark.tpch import tpch


def main():

    # tpch.main()
    sql_table_scan.main()


if __name__ == "__main__":
    main()
