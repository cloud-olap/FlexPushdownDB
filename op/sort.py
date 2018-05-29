# -*- coding: utf-8 -*-
"""Order by support

"""

from heapq import heappush, heappop

from metric.op_metrics import OpMetrics
from op.operator_base import Operator
from op.message import TupleMessage


class SortExpression(object):
    """Represents an order by clause expression.

    """

    def __init__(self, col_index, col_type, sort_order):
        """Create a new sorting expression

        :param col_index: column to sort on
        :param col_type: column type
        :param sort_order: sort order, can be 'ASC' or 'DESC'
        """

        self.col_index = col_index
        self.col_type = col_type
        self.sort_order = sort_order

        self._first_tuple = True


class Sort(Operator):
    """A sorting operator to sort the tuples emitted by the producing operator. Being a sort it needs to block until
    all tuples have been received before it can then emit those to its consuming operator. Uses the Python heap
    algorithm for sorting (which is a min/max heap sort).

    """

    def __init__(self, sort_expressions, name, log_enabled):
        """Creates a new Sort operator.

        :param sort_expressions: The sort expressions to apply to each tuple
        """

        super(Sort, self).__init__(name, OpMetrics(), log_enabled)

        self.sort_expressions = sort_expressions

        self.heap = []

        self.field_names = None

    def on_receive(self, m, _producer):
        """ Collects tuples into a heap.

        :param m: The received tuple.
        :param _producer: The producer that emitted the tuple
        :return: None
        """
        # print("Sort Emit | {}".format(t))
        if type(m) is TupleMessage:
            self.on_receive_tuple(m.tuple_)
        else:
            raise Exception("Unrecognized message {}".format(m))

    def on_receive_tuple(self, tuple_):
        if not self.field_names:
            # Collect and send field names through
            self.field_names = tuple_
            self.send(TupleMessage(tuple_), self.consumers)
        else:
            sortable_t = HeapSortableTuple(tuple_, self.field_names, self.sort_expressions)
            heappush(self.heap, sortable_t)

    def on_producer_completed(self, producer):
        """When this operator receives a done it emits the sorted tuples.

        """
        # print("Sort Done | ")
        while self.heap:
            if not self.is_completed():
                t = heappop(self.heap).tuple
                self.send(TupleMessage(t), self.consumers)
            else:
                break

        Operator.on_producer_completed(self, producer)


class HeapSortableTuple:
    """Pythons heap algorithm requires a tuple where the first element is comparable. This class represents that tuple
    with comparing functions (lt) defined.

    """

    def __init__(self, t, field_names, sort_expressions):
        self.tuple = t
        self.field_names = field_names
        self.sort_expressions = sort_expressions

    def __lt__(self, o):

        # Iterate through the sorting expressions and apply them to matching values in each tuple
        for ex in self.sort_expressions:

            field_index = self.field_names.index(ex.col_index)
            v1 = ex.col_type(self.tuple[field_index])
            v2 = ex.col_type(o.tuple[field_index])

            if v1 == v2:
                pass
            else:
                if ex.sort_order == 'ASC':
                    return v1 < v2
                elif ex.sort_order == 'DESC':
                    return v1 > v2
                else:
                    raise Exception("Unrecognised sort order {}".format(ex.sort_order))
