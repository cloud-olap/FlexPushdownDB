# -*- coding: utf-8 -*-
"""Project operator query tests

"""

from op.collate import Collate
from op.project import Project, ProjectExpr
from op.table_scan import TableScan
from op.tuple import LabelledTuple


def test_project():
    """Tests a projection

    :return: None
    """

    num_rows = 0

    # Query plan
    ts = TableScan('nation.csv',
                   'select * from S3Object '
                   'limit 3;')
    p = Project([
        ProjectExpr('_2', 'n_regionkey'),
        ProjectExpr('_0', 'n_nationkey'),
        ProjectExpr('_3', 'n_comment')
    ])
    c = Collate()

    ts.connect(p)
    p.connect(c)

    # Start the query
    ts.start()

    # Assert the results
    for t in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    assert len(c.tuples()) == 3 + 1

    assert c.tuples()[0] == ['n_regionkey', 'n_nationkey', 'n_comment']

    assert LabelledTuple(c.tuples()[1], c.tuples()[0]) == \
           ['0', '0', ' haggle. carefully final deposits detect slyly agai']
    assert LabelledTuple(c.tuples()[2], c.tuples()[0]) == \
           ['1', '1', 'al foxes promise slyly according to the regular accounts. bold requests alon']
    assert LabelledTuple(c.tuples()[3], c.tuples()[0]) == \
           ['1', '2',
            'y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special ']
