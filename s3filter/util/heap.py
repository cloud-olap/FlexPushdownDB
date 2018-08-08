from heapq import heappush, heappop
from s3filter.op.tuple import Tuple
import sys

__author__ = "Abdurrahman Ghanem <abghanem@qf.org.qa>"


class HeapTuple(object):
    """Pythons heap algorithm requires a tuple where the first element is comparable. This class represents that tuple
    with comparing functions (lt) defined using the given sort expressions.

    """

    def __init__(self, t, field_names, sort_expression):
        self.tuple = t
        self.field_names = field_names
        self.sort_expression = sort_expression

    def __lt__(self, o):
        """Whether this tuple is "less than" and hence prior to the given other tuple in the sorted heap.

        :param o: The other tuple
        :return: True if the tuple is "less than" the other tuple
        """

        field_index = self.field_names.index(self.sort_expression.col_index)
        v1 = self.sort_expression.col_type(self.tuple[field_index])
        v2 = self.sort_expression.col_type(o.tuple[field_index])

        return v1 < v2

    def __gt__(self, o):
        """Whether this tuple is "greater than" and hence prior to the given other tuple in the sorted heap.

        :param o: The other tuple
        :return: True if the tuple is "greater than" the other tuple
        """

        field_index = self.field_names.index(self.sort_expression.col_index)
        v1 = self.sort_expression.col_type(self.tuple[field_index])
        v2 = self.sort_expression.col_type(o.tuple[field_index])

        return v1 > v2


class MaxHeapSortableTuple(HeapTuple):
    """
    Special Tuple class that implements comparison operations between two tuples. Based on Min or Max Heap,
    the comparison result is calculated. This class inherits from the HeapSortTuple mainly to flip the
    default comparison operators lt and gt to support Max Heap implementation.
    """

    def __lt__(self, other):
        return super(MaxHeapSortableTuple, self).__gt__(other)

    def __gt__(self, other):
        return not super(MaxHeapSortableTuple, self).__lt__(other)

    @staticmethod
    def convert_to_max_heap_tuple(min_heap_tuple):
        return MaxHeapSortableTuple(min_heap_tuple.tuple,
                                    min_heap_tuple.field_names,
                                    min_heap_tuple.sort_expression)


class MinHeapSortableTuple(HeapTuple):
    """
    Same as HeapSortableTuple ... just to honor the name :)
    """

    @staticmethod
    def convert_to_min_heap_tuple(heap_tuple):
        return MinHeapSortableTuple(heap_tuple.tuple,
                                    heap_tuple.field_names,
                                    heap_tuple.sort_expression)


class Heap(object):
    """
    Base class to represent the Heap data structure. It could be extended to have min or max heaps.
     This is mainly because the python's implementation of heap (heapq) supports only min heap.
    """

    def __init__(self, max_size=sys.maxint):
        self.heap_list = []
        self.max_size = max_size

    def __len__(self):
        return len(self.heap_list)

    def push(self, tup):
        if self.size() < self.max_size:
            heappush(self.heap_list, tup)

    def pop(self):
        return heappop(self.heap_list)

    def top(self):
        return self.heap_list[0]

    def get_topk(self, k=1, sort=False):
        k = min([self.size(), k, self.max_size])
        top_tuples = [self.pop() for _ in range(k)]

        if sort:
            top_tuples = sorted(top_tuples, reverse=True)

        return top_tuples

    def get_all_items(self, sort=False):
        return self.get_topk(len(self.heap_list), sort)

    def is_empty(self):
        return len(self.heap_list) == 0

    def clear(self):
        self.heap_list = []

    def size(self):
        return self.__len__()

    def convert_to_my_tuple(self, tup):
        pass


class MinHeap(Heap):
    """
    Heap data structure that maintains the minimum values on top of the heap
    """
    def push(self, tup, field_names=None, sort_exp=None):

        min_tup = tup

        if type(tup) is HeapTuple:
            min_tup = MinHeapSortableTuple.convert_to_min_heap_tuple(tup)
        elif type(tup) is Tuple:
            min_tup = MinHeapSortableTuple(tup, field_names, sort_exp)

        if self.size() < self.max_size:
            super(MinHeap, self).push(min_tup)
        elif tup > self.top():
            self.pop()
            self.push(min_tup)
            # print('swapping\n{} with\n{}'.format(t.tuple, min_tup.tuple))

    def convert_to_my_tuple(self, tup):
        return MinHeapSortableTuple.convert_to_min_heap_tuple(tup)


class MaxHeap(Heap):
    """
    Heap data structure that maintains the maximum values on top of the heap. This requires inverting the sign of
    inserted items to flip the result of comparison operation of the inserted items
    """
    def push(self, tup, field_names=None, sort_exp=None):
        max_tup = tup

        if type(tup) is HeapTuple:
            max_tup = MaxHeapSortableTuple.convert_to_max_heap_tuple(tup)
        elif type(tup) is Tuple:
            max_tup = MaxHeapSortableTuple(tup, field_names, sort_exp)

        if self.size() < self.max_size:
            super(MaxHeap, self).push(max_tup)
        elif tup > self.top():
            self.pop()
            self.push(max_tup)
            # print('swapping\n{} with\n{}'.format(t.tuple, max_tup.tuple))

    def convert_to_my_tuple(self, tup):
        return MaxHeapSortableTuple.convert_to_max_heap_tuple(tup)
