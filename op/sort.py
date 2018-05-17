# -*- coding: utf-8 -*-
"""Order by support

"""

from heapq import heappush, heappop
from op.operator_base import Operator


class SortExpression:
    """Represents an order by clause expression.

    """

    def __init__(self, col_index, col_type, sort_order):
        """Create a new order by expression

        :param col_index: column to sort on
        :param col_type: columns type
        :param sort_order: sort order, can be 'ASC' or 'DESC'
        """
        self.col_index = col_index
        self.col_type = col_type
        self.sort_order = sort_order


class Sort(Operator):
    """A sorting operator to sort the tuples emitted by the producing operator. Being a sort it needs to block until
    all tuples have been received before it can then emit those to its consuming operator. Uses the Python heap
    algorithm for sorting (which is a min/max heap sort).

    """

    def __init__(self, sort_expressions):
        """Creates a new Sort operator.

        :param col_index: The column to sort on.
        :param col_type: The type of the sorting column (use the Python casting operators such as 'float'
        :param sort_order: The sort order. Can be ASC or DESC.
        """
        Operator.__init__(self)

        self.sort_expressions = sort_expressions

        self.heap = []

        self.running = True

    def on_emit(self, t, producer=None):
        """ Collects tuples into a heap.

        :param t: The received tuple.
        :param producer: The producer that emitted the tuple
        :return: None
        """
        # print("Sort Emit | {}".format(t))
        sortable_t = HeapSortableTuple(t, self.sort_expressions)
        heappush(self.heap, sortable_t)

    def on_stop(self):
        """This allows consuming producers to indicate that the operator can stop.

        TODO: Need to verify that this is actually useful.

        :return: None
        """

        # print("Sort Stop | ")
        self.running = False
        self.do_stop()

    def on_done(self):
        """When this operator receives a done it emits the sorted tuples.

        """
        # print("Sort Done | ")
        while self.heap:
            if self.running:
                t = heappop(self.heap).tuple
                self.do_emit(t)
            else:
                break


class HeapSortableTuple:
    """Pythons heap algorithm requires a tuple where the first element is comparable. This class represents that tuple
    with comparing functions (lt, gt and eq) defined.

    """

    def __init__(self, t, sort_expressions):
        self.tuple = t
        self.sort_expressions = sort_expressions

    def __lt__(self, o):

        # Iterate through the sorting expressions and apply them to matching values in each tuple
        for ex in self.sort_expressions:

            v1 = ex.col_type(self.tuple[ex.col_index])
            v2 = ex.col_type(o.tuple[ex.col_index])

            if v1 == v2:
                pass
            else:
                if ex.sort_order == 'ASC':
                    return v1 < v2
                elif ex.sort_order == 'DESC':
                    return v1 > v2
                else:
                    raise Exception("Unrecognised sort order {}".format(ex.sort_order))
