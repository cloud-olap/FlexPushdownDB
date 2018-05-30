# -*- coding: utf-8 -*-
"""Filter query tests

"""

from op.collate import Collate
from op.filter import Filter
from op.predicate_expression import PredicateExpression
from op.table_scan import TableScan
from op.tuple import LabelledTuple
from sql.function import timestamp, cast


def test_filter():

    # Query plan
    ts = TableScan('lineitem.csv', 'select * from S3Object limit 3;', 'ts', False)
    f = Filter(PredicateExpression(lambda t_: cast(t_['_10'], timestamp) >= cast('1996-03-01', timestamp)), 'f', False)
    c = Collate('c', False)

    ts.connect(f)
    f.connect(c)

    # Start the query
    ts.start()

    # Assert the results
    num_rows = 0
    for t in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    assert len(c.tuples()) == 2 + 1

    assert c.tuples()[0] == ['_0', '_1', '_2', '_3', '_4', '_5', '_6', '_7', '_8', '_9', '_10', '_11', '_12', '_13', '_14', '_15']

    assert LabelledTuple(c.tuples()[1], c.tuples()[0]) == \
           ['1', '155190', '7706', '1', '17', '21168.23', '0.04', '0.02', 'N', 'O', '1996-03-13', '1996-02-12',
            '1996-03-22', 'DELIVER IN PERSON', 'TRUCK', 'egular courts above the']

    assert LabelledTuple(c.tuples()[2], c.tuples()[0]) == \
           ['1', '67310', '7311', '2', '36', '45983.16', '0.09', '0.06', 'N', 'O', '1996-04-12', '1996-02-28',
            '1996-04-20', 'TAKE BACK RETURN', 'MAIL', 'ly final dependencies: slyly bold ']
