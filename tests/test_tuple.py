# -*- coding: utf-8 -*-
"""

"""

from op.tuple import Tuple, LabelledTuple


def test_tuple():

    t = Tuple(['A', 'B'])

    assert t[0] == 'A'
    assert t[1] == 'B'


def test_labelled_tuple():

    field_names = ['one', 'two']

    t1 = LabelledTuple(['A', 'B'])

    assert t1['_0'] == 'A'
    assert t1['_1'] == 'B'

    t2 = LabelledTuple(['A', 'B'], field_names)

    assert t2['one'] == 'A'
    assert t2['two'] == 'B'
