# -*- coding: utf-8 -*-
"""
Test utility functions

"""

import inspect
import itertools
import os
from datetime import datetime, date

import pandas as pd
import numpy as np

def gen_test_id():
    """Generates a string of the calling function script name and the calling functions name. Useful for generating
    tst output files during a tst run that are named after the tst.

    :return: <calling script name>-<calling function name>
    """
    test_id = os.path.basename(inspect.stack()[1][1]).split('.')[:-1][0] + "-" + inspect.stack()[1][3]
    return test_id


def assert_tuples(expected_result, tuples):
    for (tuple, expected_tuple) in itertools.izip(tuples[1:], expected_result):
        for (field, expected_field) in itertools.izip(tuple, expected_tuple):
            if isinstance(expected_field, str):
                assert field == expected_field
            elif isinstance(expected_field, int):
                assert int(field) == expected_field
            elif isinstance(expected_field, float):
                np.testing.assert_approx_equal(float(field), expected_field)
            elif isinstance(expected_field, date):
                if isinstance(field, pd.Timestamp):
                    assert field.date() == expected_field
                else:
                    assert datetime.strptime(field, "%Y-%m-%d").date() == expected_field
