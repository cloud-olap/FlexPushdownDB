# -*- coding: utf-8 -*-
"""

"""
from op.operator_base import Operator


class Log(Operator):
    """This operator simply prints out each tuple, useful for debugging.

    """

    def __init__(self):
        Operator.__init__(self)

    def on_emit(self, t, producer=None):
        print("Print | {}".format(t))
        self.do_emit(t)

    # TODO: These should may be on the base class as defaults that can be overridden
    def on_done(self):
        self.do_done()

    def on_stop(self):
        self.do_stop()
