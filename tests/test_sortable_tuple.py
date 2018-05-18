# -*- coding: utf-8 -*-
"""

"""

from heapq import heappush, heappop

from op.sort import HeapSortableTuple, SortExpression


def test_sort_asc_asc():

    sort_expressions = [
        SortExpression(0, float, 'ASC'),
        SortExpression(1, float, 'ASC')
    ]

    heap = []

    heappush(heap, HeapSortableTuple((1, 1), sort_expressions))
    heappush(heap, HeapSortableTuple((1, 2), sort_expressions))
    heappush(heap, HeapSortableTuple((2, 1), sort_expressions))
    heappush(heap, HeapSortableTuple((2, 2), sort_expressions))

    popped_t1 = heappop(heap).tuple
    popped_t2 = heappop(heap).tuple
    popped_t3 = heappop(heap).tuple
    popped_t4 = heappop(heap).tuple

    assert popped_t1 == (1, 1)
    assert popped_t2 == (1, 2)
    assert popped_t3 == (2, 1)
    assert popped_t4 == (2, 2)


def test_sort_asc_desc():
    sort_expressions = [
        SortExpression(0, float, 'ASC'),
        SortExpression(1, float, 'DESC')
    ]

    heap = []

    heappush(heap, HeapSortableTuple((1, 1), sort_expressions))
    heappush(heap, HeapSortableTuple((1, 2), sort_expressions))
    heappush(heap, HeapSortableTuple((2, 1), sort_expressions))
    heappush(heap, HeapSortableTuple((2, 2), sort_expressions))

    popped_t1 = heappop(heap).tuple
    popped_t2 = heappop(heap).tuple
    popped_t3 = heappop(heap).tuple
    popped_t4 = heappop(heap).tuple

    assert popped_t1 == (1, 2)
    assert popped_t2 == (1, 1)
    assert popped_t3 == (2, 2)
    assert popped_t4 == (2, 1)


def test_sort_desc_asc():
    sort_expressions = [
        SortExpression(0, float, 'DESC'),
        SortExpression(1, float, 'ASC')
    ]

    heap = []

    heappush(heap, HeapSortableTuple((1, 1), sort_expressions))
    heappush(heap, HeapSortableTuple((1, 2), sort_expressions))
    heappush(heap, HeapSortableTuple((2, 1), sort_expressions))
    heappush(heap, HeapSortableTuple((2, 2), sort_expressions))

    popped_t1 = heappop(heap).tuple
    popped_t2 = heappop(heap).tuple
    popped_t3 = heappop(heap).tuple
    popped_t4 = heappop(heap).tuple

    assert popped_t1 == (2, 1)
    assert popped_t2 == (2, 2)
    assert popped_t3 == (1, 1)
    assert popped_t4 == (1, 2)


def test_sort_desc_desc():
    sort_expressions = [
        SortExpression(0, float, 'DESC'),
        SortExpression(1, float, 'DESC')
    ]

    heap = []

    heappush(heap, HeapSortableTuple((1, 1), sort_expressions))
    heappush(heap, HeapSortableTuple((1, 2), sort_expressions))
    heappush(heap, HeapSortableTuple((2, 1), sort_expressions))
    heappush(heap, HeapSortableTuple((2, 2), sort_expressions))

    popped_t1 = heappop(heap).tuple
    popped_t2 = heappop(heap).tuple
    popped_t3 = heappop(heap).tuple
    popped_t4 = heappop(heap).tuple

    assert popped_t1 == (2, 2)
    assert popped_t2 == (2, 1)
    assert popped_t3 == (1, 2)
    assert popped_t4 == (1, 1)
