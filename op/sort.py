# -*- coding: utf-8 -*-
"""Order by support

"""

from heapq import heappush, heappop

from plan.op_metrics import OpMetrics
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

        if sort_order is not 'ASC' and sort_order is not 'DESC':
            raise Exception("Illegal sort order '{}'. Sort order must be '{}' or '{}'"
                            .format(sort_order, 'ASC', 'DESC'))

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

        :param sort_expressions: The sort expressions to apply to the tuples
        :param name: The name of the operator
        :param log_enabled: Whether logging is enabled
        """

        super(Sort, self).__init__(name, OpMetrics(), log_enabled)

        self.sort_expressions = sort_expressions

        self.heap = []

        self.field_names = None

    def on_receive(self, m, _producer):
        """ Handles a new message from a producer.

        :param m: The received message.
        :param _producer: The producer that emitted the message
        :return: None
        """

        # print("Sort Emit | {}".format(t))
        if type(m) is TupleMessage:
            self.on_receive_tuple(m.tuple_)
        else:
            raise Exception("Unrecognized message {}".format(m))

    def on_receive_tuple(self, tuple_):
        """Handles receipt of a tuple. Field names are stored and sent. Field values are placed into a sorted heap
        using the sort expressions to define the sort order.

        :param tuple_: The received tuple
        :return: None
        """

        if not self.field_names:
            # Collect and send field names through
            self.field_names = tuple_
            self.send(TupleMessage(tuple_), self.consumers)
        else:
            # Store the tuple in the sorted heap
            sortable_t = HeapSortableTuple(tuple_, self.field_names, self.sort_expressions)
            heappush(self.heap, sortable_t)

    def on_producer_completed(self, producer):
        """Handles the event when a producer completes. When this happens the sorted tuples are emitted.

        :param producer: The producer that completed
        :return: None
        """

        # print("Sort Done | ")
        while self.heap:

            if self.is_completed():
                break

            t = heappop(self.heap).tuple
            self.send(TupleMessage(t), self.consumers)

        Operator.on_producer_completed(self, producer)


class HeapSortableTuple:
    """Pythons heap algorithm requires a tuple where the first element is comparable. This class represents that tuple
    with comparing functions (lt) defined using the given sort expressions.

    """

    def __init__(self, t, field_names, sort_expressions):
        self.tuple = t
        self.field_names = field_names
        self.sort_expressions = sort_expressions

    def __lt__(self, o):
        """Whether this tuple is "less than" and hence prior to the given other tuple in the sorted heap.

        :param o: The other tuple
        :return: True if the tuple is "less than" the other tuple
        """

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
