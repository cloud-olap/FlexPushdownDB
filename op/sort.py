# -*- coding: utf-8 -*-
"""

"""
from heapq import heappush, heappop

from op.operator import Operator


class Sort(Operator):
    """A sorting operator to sort the tuples emitted by the producing operator. Being a sort it needs to block until
    all tuples have been received before it can then emit those to its consuming operator. Uses the Python heap
    algorithm for sorting (which is a min/max heap sort).

    """

    def __init__(self, col_index, col_type, sort_order):
        """Creates a new Sort operator.

        :param col_index: The column to sort on.
        :param col_type: The type of the sorting column (use the Python casting operators such as 'float'
        :param sort_order: The sort order. Can be ASC or DESC.
        """
        Operator.__init__(self)

        self.col_index = col_index
        self.col_type = col_type
        self.sort_order = sort_order

        self.heap = []

        self.producer = None

        self.running = True

    def set_producer(self, operator):
        self.producer = operator

    def set_consumer(self, operator):
        self.consumer = operator

    def emit(self, t, producer=None):
        """ Collects tuples into a heap.

        :param t: The received tuple.
        :param producer: The producer that emitted the tuple
        :return: None
        """
        # print("Sort Emit | {}".format(t))
        t = HeapSortableTuple(t, self.col_index, self.col_type, self.sort_order)
        heappush(self.heap, t)

    def stop(self):
        """This allows consuming producers to indicate that the operator can stop.

        TODO: Need to verify that this is actually useful.

        :return: None
        """

        # print("Sort Stop | ")
        self.running = False
        self.producer.stop()

    def done(self):
        """When this operator receives a done it emits the sorted tuples.

        """
        # print("Sort Done | ")
        while self.heap:
            if self.running:
                t = heappop(self.heap).tuple
                self.consumer.emit(t)
            else:
                break


class HeapSortableTuple:
    """Pythons heap algorithm requires a tuple where the first element is comparable. This class represents that tuple
    with comparing functions (lt, gt and eq) defined.

    """

    def __init__(self, t, sort_key_index, sort_key_type, sort_order):
        self.tuple = t
        self.sort_key_index = sort_key_index
        self.sort_key_type = sort_key_type
        self.sort_order = sort_order

    def __lt__(self, o):

        v1 = self.sort_key_type(self.tuple[self.sort_key_index])
        v2 = self.sort_key_type(o[self.sort_key_index])

        if self.sort_order == 'ASC':
            return v1 < v2
        elif self.sort_order == 'DESC':
            return v1 > v2
        else:
            raise Exception("Unrecognised sort order {}".format(self.sort_order))

    def __eq__(self, o):

        v1 = self.sort_key_type(self.tuple[self.sort_key_index])
        v2 = self.sort_key_type(o[self.sort_key_index])

        return v1 == v2

    def __gt__(self, o):

        v1 = self.sort_key_type(self.tuple[self.sort_key_index])
        v2 = self.sort_key_type(o[self.sort_key_index])

        if self.sort_order == 'ASC':
            return v1 > v2
        elif self.sort_order == 'DESC':
            return v1 < v2
        else:
            raise Exception("Unrecognised sort order {}".format(self.sort_order))

    def __getitem__(self, k):
        return float(self.tuple[k])
