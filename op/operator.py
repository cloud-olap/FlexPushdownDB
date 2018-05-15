# -*- coding: utf-8 -*-
"""

"""


class Operator(object):
    """Base class for an operator. Not much here now, but eventually common things can be pulled up into this class.

    """

    def __init__(self):
        self.consumer = None

    def connect(self, consumer):
        """Simply sets this operators consumer to the given operator and the given operators producer to this operator.

        :param consumer: The operator that will consume the results of this operator.
        :return:
        """
        self.consumer = consumer
        consumer.producer = self
