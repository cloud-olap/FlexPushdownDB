# -*- coding: utf-8 -*-
"""

"""
import timeit

from s3filter.op.tuple import Tuple, IndexedTuple
import pandas as pd


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


def test_list_tuple_build():
    start_time = timeit.default_timer()

    ts = build_tuple_list()

    end_time = timeit.default_timer()

    head = ts[0]
    print(head)
    print("Elapsed: {}".format(end_time - start_time))


def build_tuple_list():
    t = ['A1', 'B123', 'C123456', 'D1234567890']
    ts = []

    for i in range(0, 100000):
        nt = list(t)
        ts.append(nt)

    return ts


def test_pandas_tuple_build():
    start_time = timeit.default_timer()

    df = build_tuples_dataframe()

    end_time = timeit.default_timer()

    head = df.iloc[0]
    print(head)
    print("Elapsed: {}".format(end_time - start_time))


def build_tuples_dataframe():
    t = ['A1', 'B123', 'C123456', 'D1234567890']
    ts = []
    for i in range(0, 100000):
        ts.append(t)

    cols = map(lambda i: '_' + str(i), range(0, len(ts[0])))
    # cols = None

    df = pd.DataFrame(ts, dtype='object', columns=cols)
    return df


def test_list_tuple_modify():
    ts = build_tuple_list()

    start_time = timeit.default_timer()

    map(lambda t: t.append("TupleMessage"), ts)

    end_time = timeit.default_timer()

    head = ts[0]
    print(head)
    print("Elapsed: {}".format(end_time - start_time))


def test_pandas_tuple_modify():
    df = build_tuples_dataframe()

    start_time = timeit.default_timer()

    df['message_type'] = "TupleMessage"

    end_time = timeit.default_timer()

    head = df.iloc[0]
    print(head)
    print("Elapsed: {}".format(end_time - start_time))
