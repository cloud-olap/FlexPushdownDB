# -*- coding: utf-8 -*-
"""Random table scan tests

"""

import os
from datetime import datetime
from s3filter import ROOT_DIR
from s3filter.op.collate import Collate
from s3filter.op.random_table_scan import RandomIntColumnDef, RandomTableScan, RandomStringColumnDef, \
    RandomDateColumnDef
from s3filter.plan.query_plan import QueryPlan
from s3filter.util.test_util import gen_test_id


def test_random_scan_simple():
    """Executes a random scan. The results are then collated.

    :return: None
    """

    num_rows = 0

    query_plan = QueryPlan()

    # Query plan
    random_col_defs = [
        RandomIntColumnDef(0, 9),
        RandomStringColumnDef(10, 20),
        RandomDateColumnDef(datetime.strptime('2017-01-01', '%Y-%m-%d'),
                            datetime.strptime('2018-01-01', '%Y-%m-%d'))
    ]

    random_table_scan = query_plan.add_operator(
        RandomTableScan(10,
                        random_col_defs,
                        'random_table_scan',
                        False))

    collate = query_plan.add_operator(
        Collate('collate', False))

    random_table_scan.connect(collate)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    random_table_scan.start()

    collate.print_tuples()

    # Write the metrics
    query_plan.print_metrics()

    # Assert the results
    assert len(collate.tuples()) == 10 + 1

    assert collate.tuples()[0] == ['_0', '_1', '_2']

    # TODO: Should assert the types
