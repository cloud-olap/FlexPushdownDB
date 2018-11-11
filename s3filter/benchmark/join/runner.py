import os

import numpy as np

from s3filter import ROOT_DIR
from s3filter.util.test_util import gen_test_id


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

    assert tuples[0] == field_names

    np.testing.assert_approx_equal(tuples[1][0], expected_result)
