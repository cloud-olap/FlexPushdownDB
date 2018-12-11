import os
from datetime import datetime

import numpy as np
import pandas as pd

from s3filter import ROOT_DIR


def run(query_plan, expected_result, test_id):
    """

    :return: None
    """

    collate = query_plan.get_operator('collate')

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../benchmark-output"), test_id)

    # Start the query
    query_plan.execute()

    tuples = collate.tuples()

    collate.print_tuples(tuples)

    # Write the metrics
    query_plan.print_metrics()

    # Shut everything down
    query_plan.stop()

    field_names = ['total_balance']

    assert len(tuples) == 1 + 1

    # assert tuples[0] == field_names
    #
    # np.testing.assert_approx_equal(tuples[1][0], expected_result)


def build_filters(table_a_filter_val, table_b_filter_val):

    if table_a_filter_val is not None:
        table_a_filter_sql = 'cast(c_acctbal as float) <= {}'.format(table_a_filter_val)

        def table_a_filter_fn(df):
            return df['c_acctbal'].astype(np.float) <= table_a_filter_val
    else:
        table_a_filter_sql = None

        table_a_filter_fn = None

    if table_b_filter_val is not None:
        table_b_filter_sql = 'cast(o_orderdate as timestamp) < cast(\'{}\' as timestamp)'.format(table_b_filter_val)

        max_orderdate = datetime.strptime(table_b_filter_val, '%Y-%m-%d')

        def table_b_filter_fn(df):
            return pd.to_datetime(df['o_orderdate']) < max_orderdate
    else:
        table_b_filter_sql = None

        table_b_filter_fn = None

    return table_a_filter_sql, table_a_filter_fn, table_b_filter_sql, table_b_filter_fn
