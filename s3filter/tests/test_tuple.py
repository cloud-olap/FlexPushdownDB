# -*- coding: utf-8 -*-
"""

"""
from s3filter.op.tuple import Tuple, IndexedTuple


def test_tuple():

    t = Tuple(['A', 'B'])

    assert t[0] == 'A'
    assert t[1] == 'B'


def test_labelled_tuple():

    field_names = ['one', 'two']

    t1 = IndexedTuple.build_default(['A', 'B'])

    assert t1['_0'] == 'A'
    assert t1['_1'] == 'B'

    t2 = IndexedTuple.build(['A', 'B'], field_names)

    assert t2['one'] == 'A'
    assert t2['two'] == 'B'
