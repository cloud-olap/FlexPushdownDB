# -*- coding: utf-8 -*-
"""Join Benchmarks

"""

from s3filter.benchmark.tpch import tpch


def main():

    tpch.main()
    # sql_table_scan.main()
    # cursor.main()
    # join.main()
    # tpch_scan_ipc.main()


if __name__ == "__main__":
    main()
