# -*- coding: utf-8 -*-
"""Table scan benchmark

"""

import os
import pstats

from s3filter import ROOT_DIR
from s3filter.op.collate import Collate
from s3filter.op.null import Null
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.plan.query_plan import QueryPlan
from s3filter.util.test_util import gen_test_id


def main():
    # run(use_pandas=False, secure=True, use_native=False)
    # run(use_pandas=False, secure=False, use_native=False)
    # run(use_pandas=True, secure=True, use_native=False)
    # run(use_pandas=True, secure=False, use_native=False)
    run(use_pandas=True, secure=True, use_native=True)


def run(use_pandas, secure, use_native):
    profile_file_name = os.path.join(ROOT_DIR, "../benchmark-output/" + gen_test_id() + ".prof")
    os.remove(profile_file_name) if os.path.exists(profile_file_name) else None

    print("SQL Table Scan | Settings {}".format({'use_pandas': use_pandas, 'secure': secure, 'use_native': use_native}))

    query_plan = QueryPlan(is_async=True, buffer_size=0)

    # Query plan
    scan = query_plan.add_operator(
        SQLTableScan('lineitem.csv',
                     "select "
                     "  * "
                     "from "
                     "  S3Object limit 100000",
                     use_pandas,
                     secure,
                     use_native,
                     'scan',
                     query_plan,
                     True))

    # scan.set_profiled(True, profile_file_name)

    null = query_plan.add_operator(
        Null('null', query_plan, True))

    scan.connect(null)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../benchmark-output"), gen_test_id())

    # Start the query
    query_plan.execute()

    query_plan.print_metrics()

    query_plan.stop()

    # Write the profile
    # s = pstats.Stats(profile_file_name)
    # s.strip_dirs().sort_stats("time").print_stats()

    print("SQL Table Scan | Done")


if __name__ == "__main__":
    main()
