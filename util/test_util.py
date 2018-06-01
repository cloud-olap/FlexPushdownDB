# -*- coding: utf-8 -*-
"""
Test utility functions

"""

import inspect
import os


def gen_test_id():
    test_id = os.path.basename(inspect.stack()[1][1]).split('.')[:-1][0] + "-" + inspect.stack()[1][3]
    return test_id
