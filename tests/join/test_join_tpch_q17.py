# -*- coding: utf-8 -*-
"""Join experiments on TPC-H query 17

These queries test the performance of several approaches to running the TCP-H Q17 query.

TPCH Query 17

    select
        sum(l_extendedprice) / 7.0 as avg_yearly
    from
        lineitem,
        part
    where
        p_partkey = l_partkey
        and p_brand = cast('Brand#41' as char(10))
        and p_container = cast('SM PACK' as char(10))
        and l_quantity < (
            select
                0.2 * avg(l_quantity)
            from
                lineitem
            where
                l_partkey = p_partkey
        );



"""

from metric.op_metrics import OpMetrics
from op.aggregate import AggregateExpression
from op.collate import Collate
from op.group import Group
from op.join import Join, JoinExpression
from op.project import Project, ProjectExpr
from op.table_scan import TableScan
from sql.function import avg_fn


def test_join_baseline():
    """The baseline test uses nested loop joins with no projection and no filtering pushed down to s3.

    :return: None
    """

    print("Baseline Join")
    part_scan = TableScan('part.csv',
                          "select "
                          "  p_partkey from S3Object "
                          "where "
                          "  p_brand = cast('Brand#41' as char(10)) and "
                          "  p_container = cast('SM PACK' as char(10)) "
                          " ",
                          'part_scan',
                          False)
    lineitem_scan = TableScan('lineitem.csv',
                              "select "
                              "  * from S3Object "
                              "where "
                              "  (l_orderkey = '38502' and l_partkey = '99483') or "
                              "  (l_orderkey = '69316' and l_partkey = '99483') or "
                              "  (l_orderkey = '83873' and l_partkey = '99744') or "
                              "  (l_orderkey = '127303' and l_partkey = '99744') "
                              " ",
                              'lineitem_scan',
                              False)
    join = Join(JoinExpression(part_scan.name, '_0', lineitem_scan.name, '_1'), 'join',
                False)  # p_partkey and l_partkey
    lineitem_group = Group([lineitem_scan.name + '._1'],  # l_partkey
                           [
                               # avg(l_quantity)
                               AggregateExpression(lambda t_, ctx: avg_fn(float(t_[lineitem_scan.name + '._4']), ctx))
                           ],
                           'lineitem_group',
                           False)
    lineitem_project = Project(
        [
            # l_partkey
            ProjectExpr(lambda t_: t_['_0'], 'l_partkey'),
            # 0.2 * avg
            ProjectExpr(lambda t_: 0.2 * t_['_1'], 'l_computed00')
        ],
        'lineitem_project',
        False)
    collate = Collate('collate', False)

    part_scan.connect(join)
    lineitem_scan.connect(join)
    join.connect(lineitem_group)
    lineitem_group.connect(lineitem_project)
    lineitem_project.connect(collate)

    # Start the query
    part_scan.start()
    lineitem_scan.start()

    # Assert the results
    num_rows = 0
    for t in collate.tuples():
        num_rows += 1
        print("{}:{}".format(num_rows, t))

    # OpMetrics.print_metrics([
    #     part_scan,
    #     lineitem_scan,
    #     lineitem_group,
    #     lineitem_project,
    #     collate
    # ])
