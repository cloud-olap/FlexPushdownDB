# -*- coding: utf-8 -*-
"""Table scan benchmark

"""

import os
import pstats

from s3filter import ROOT_DIR
from s3filter.op.collate import Collate
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.plan.query_plan import QueryPlan
from s3filter.util.test_util import gen_test_id


def main():

    profile_file_name = os.path.join(ROOT_DIR, "../benchmark-output/" + gen_test_id() + ".prof")

    print('')
    print("SQL Table Scan")
    print("--------------")

    query_plan = QueryPlan(None, False)

    # Query plan
    scan = query_plan.add_operator(
        SQLTableScan('part.csv',
                     "select "
                     "  * "
                     "from "
                     "  S3Object",
                     'scan',
                     False))

    scan.set_profiled(True, profile_file_name)

    collate = query_plan.add_operator(
        Collate('collate', False))

    scan.connect(collate)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../benchmark-output"), gen_test_id())

    # Start the query
    query_plan.execute()

    # collate.print_tuples()

    # Write the metrics
    s = pstats.Stats(profile_file_name)
    s.strip_dirs().sort_stats("time").print_stats()
    query_plan.print_metrics()

    field_names = ['_0', '_1', '_2', '_3', '_4', '_5', '_6', '_7', '_8']

    assert len(collate.tuples()) == 200000 + 1

    assert collate.tuples()[0] == field_names


if __name__ == "__main__":
    main()
