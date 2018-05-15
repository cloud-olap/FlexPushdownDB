# -*- coding: utf-8 -*-
"""

"""
from op.operator import Operator


class Log(Operator):
    """This operator simply prints out each tuple, useful for debugging.

    """

    def __init__(self):
        Operator.__init__(self)

    def emit(self, t):
        print("Print | {}".format(t))
