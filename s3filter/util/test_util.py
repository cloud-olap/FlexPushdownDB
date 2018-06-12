# -*- coding: utf-8 -*-
"""
Test utility functions

"""

import inspect
import os


def gen_test_id():
    """Generates a string of the calling function script name and the calling functions name. Useful for generating
    tst output files during a tst run that are named after the tst.

    :return: <calling script name>-<calling function name>
    """
    test_id = os.path.basename(inspect.stack()[1][1]).split('.')[:-1][0] + "-" + inspect.stack()[1][3]
    return test_id
