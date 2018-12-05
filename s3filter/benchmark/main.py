# -*- coding: utf-8 -*-
"""Join Benchmarks

"""

from s3filter.benchmark.access_method import tpch_scan_ipc
from s3filter.benchmark.cursor import cursor
from s3filter.benchmark.join import join
from s3filter.benchmark.sql_table_scan import sql_table_scan
from s3filter.benchmark.tpch import tpch


def main():

    tpch.main()
    # sql_table_scan.main()
    # cursor.main()
    # join.main()
    # tpch_scan_ipc.main()


if __name__ == "__main__":
    main()
