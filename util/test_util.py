# -*- coding: utf-8 -*-
"""
Test utility functions

"""

import inspect
import os


def gen_test_id():
    """Generates a string of the calling function script name and the calling functions name. Useful for generating
    test output files during a test run that are named after the test.

    :return: <calling script name>-<calling function name>
    """
    test_id = os.path.basename(inspect.stack()[1][1]).split('.')[:-1][0] + "-" + inspect.stack()[1][3]
    return test_id
