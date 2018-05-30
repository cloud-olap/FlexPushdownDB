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
from op.aggregate import Aggregate
from op.aggregate_expression import AggregateExpression
from op.collate import Collate
from op.filter import Filter
from op.group import Group
from op.join import Join, JoinExpression
from op.predicate_expression import PredicateExpression
from op.project import Project, ProjectExpr
from op.table_scan import TableScan
from sql.function import avg_fn, sum_fn


def test_join_baseline():
    """The baseline test uses nested loop joins with no projection and no filtering pushed down to s3.

    :return: None
    """

    print("Baseline Join")

    # with part_scan as (select * from part)
    part_scan = TableScan('part.csv',
                          "select "
                          "  p_partkey "
                          "from "
                          "  S3Object "
                          "where "
                          "  p_brand = 'Brand#41' and "
                          "  p_container = 'SM PACK' "
                          " ",
                          'part_scan',
                          False)

    # with lineitem_scan as (select * from lineitem)
    lineitem_scan = TableScan('lineitem.csv',
                              "select "
                              "  * "
                              "from "
                              "  S3Object "
                              "where "
                              "  l_partkey = '182405' "
                              " ",
                              'lineitem_scan',
                              False)

    # with part_scan_project as (select _0 as p_partkey from part_scan)
    part_scan_project = Project(
        [
            ProjectExpr(lambda t_: t_['_0'], 'p_partkey')
        ],
        'part_scan_project',
        False)

    # with part_scan_project as (select _0 as p_partkey from part_scan)
    lineitem_scan_project = Project(
        [
            ProjectExpr(lambda t_: t_['_0'], 'l_orderkey'),
            ProjectExpr(lambda t_: t_['_1'], 'l_partkey'),
            ProjectExpr(lambda t_: t_['_4'], 'l_quantity'),
            ProjectExpr(lambda t_: t_['_5'], 'l_extendedprice')
        ],
        'lineitem_scan_project',
        False)

    # with part_lineitem_join as (select * from part_scan, lineitem_scan where p_partkey = l_partkey)
    part_lineitem_join = Join(
        JoinExpression('p_partkey', 'l_partkey'),
        'part_lineitem_join',
        False)  # p_partkey and l_partkey

    # with lineitem_part_avg_group as (select avg(l_quantity) from part_lineitem_join group by l_partkey)
    lineitem_part_avg_group = Group(
        ['l_partkey'],  # l_partkey
        [
            # avg(l_quantity)
            AggregateExpression(lambda t_, ctx: avg_fn(float(t_['l_quantity']), ctx))
        ],
        'lineitem_part_avg_group',
        False)

    # with lineitem_part_avg_group_project as (select l_partkey, 0.2 * avg(l_quantity) as l_quantity_computed00 from lineitem_part_avg_group)
    lineitem_part_avg_group_project = Project(
        [
            # l_partkey
            ProjectExpr(lambda t_: t_['_0'], 'l_partkey'),
            # 0.2 * avg
            ProjectExpr(lambda t_: 0.2 * t_['_1'], 'avg_l_quantity_computed00')
        ],
        'lineitem_part_avg_group_project',
        False)

    # with part_lineitem_join_avg_group_join as (select * from part_lineitem_join, lineitem_part_avg_group_project where p_partkey = l_partkey)
    part_lineitem_join_avg_group_join = Join(
        JoinExpression('l_partkey', 'p_partkey'),
        'part_lineitem_join_avg_group_join',
        False)  # l_partkey and l_partkey

    # with filter_join_2 as (select * from part_lineitem_join_avg_group_join where l_quantity < avg_l_quantity_computed00)
    lineitem_filter = Filter(
        PredicateExpression(lambda t_: float(t_['l_quantity']) < t_['avg_l_quantity_computed00']),
        'lineitem_filter',
        False)

    # with extendedprice_sum_aggregate as (select sum(l_extendedprice) from filter_join_2)
    extendedprice_sum_aggregate = Aggregate(
        [
            AggregateExpression(lambda t_, ctx: sum_fn(float(t_['l_extendedprice']), ctx))
        ],
        'extendedprice_sum_aggregate',
        False)

    extendedprice_sum_aggregate_project = Project(
        [
            ProjectExpr(lambda t_: t_['_0'] / 7.0, 'avg_yearly')
        ],
        'extendedprice_sum_aggregate_project',
        False)

    collate = Collate('collate', False)

    part_scan.connect(part_scan_project)
    lineitem_scan.connect(lineitem_scan_project)

    part_lineitem_join.connect_left_producer(part_scan_project)
    part_lineitem_join.connect_right_producer(lineitem_scan_project)

    part_lineitem_join.connect(lineitem_part_avg_group)
    lineitem_part_avg_group.connect(lineitem_part_avg_group_project)

    part_lineitem_join_avg_group_join.connect_left_producer(lineitem_part_avg_group_project)
    part_lineitem_join_avg_group_join.connect_right_producer(part_lineitem_join)

    part_lineitem_join_avg_group_join.connect(lineitem_filter)
    lineitem_filter.connect(extendedprice_sum_aggregate)
    extendedprice_sum_aggregate.connect(extendedprice_sum_aggregate_project)
    extendedprice_sum_aggregate_project.connect(collate)

    # Start the query
    part_scan.start()
    lineitem_scan.start()

    # Assert the results
    num_rows = 0
    for t in collate.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    field_names = ['avg_yearly']

    assert len(collate.tuples()) == 1 + 1

    assert collate.tuples()[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    assert collate.tuples()[1] == [1274.9142857142856]

    OpMetrics.print_metrics([
        part_scan,
        lineitem_scan,
        part_scan_project,
        lineitem_scan_project,
        part_lineitem_join,
        lineitem_part_avg_group,
        lineitem_part_avg_group_project,
        part_lineitem_join_avg_group_join,
        lineitem_filter,
        extendedprice_sum_aggregate,
        extendedprice_sum_aggregate_project
    ])
