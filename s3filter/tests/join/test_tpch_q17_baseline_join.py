# -*- coding: utf-8 -*-
"""Join experiments on TPC-H query 17

These queries tst the performance of several approaches to running the TCP-H Q17 query.

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
import os

from s3filter import ROOT_DIR
from s3filter.op.hash_join import HashJoin
from s3filter.plan.query_plan import QueryPlan
from s3filter.op.aggregate import Aggregate
from s3filter.op.aggregate_expression import AggregateExpression
from s3filter.op.collate import Collate
from s3filter.op.filter import Filter
from s3filter.op.group import Group
from s3filter.op.join_expression import JoinExpression
from s3filter.op.predicate_expression import PredicateExpression
from s3filter.op.project import Project, ProjectExpression
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.util.test_util import gen_test_id


def collate_op():
    return Collate('collate', False)


def extendedprice_sum_aggregate_project_op():
    """with extendedprice_sum_aggregate_project as (
    select l_extendedprice / 7.0 as avg_yearly from extendedprice_sum_aggregate
    )

    :return:
    """

    return Project(
        [
            ProjectExpression(lambda t_: t_['_0'] / 7.0, 'avg_yearly')
        ],
        'extendedprice_sum_aggregate_project',
        False)


# with extendedprice_sum_aggregate as (select sum(l_extendedprice) from filter_join_2)
def extendedprice_sum_aggregate_op():
    return Aggregate([AggregateExpression(AggregateExpression.SUM, lambda t_: float(t_['l_extendedprice']))],
                     'extendedprice_sum_aggregate', False)


# with filter_join_2 as (select * from part_lineitem_join_avg_group_join where l_quantity < avg_l_quantity_computed00)
def lineitem_filter_op():
    return Filter(PredicateExpression(lambda t_: float(t_['l_quantity']) < t_['avg_l_quantity_computed00']),
                  'lineitem_filter', False)


def part_lineitem_join_avg_group_op():
    """with part_lineitem_join_avg_group_join as (
    select * from part_lineitem_join, lineitem_part_avg_group_project where p_partkey = l_partkey
    )

    :return:
    """
    return HashJoin(JoinExpression('l_partkey', 'p_partkey'), 'part_lineitem_join_avg_group_join', False)


def lineitem_part_avg_group_project_op():
    """with lineitem_part_avg_group_project as (
    select l_partkey, 0.2 * avg(l_quantity) as l_quantity_computed00 from lineitem_part_avg_group
    )

    :return:
    """
    return Project(
        [
            # l_partkey
            ProjectExpression(lambda t_: t_['_0'], 'l_partkey'),
            # 0.2 * avg
            ProjectExpression(lambda t_: 0.2 * t_['_1'], 'avg_l_quantity_computed00')
        ],
        'lineitem_part_avg_group_project',
        False)


# with lineitem_part_avg_group as (select avg(l_quantity) from part_lineitem_join group by l_partkey)
def lineitem_avg_group_op():
    return Group(
        ['l_partkey'],  # l_partkey
        [
            # avg(l_quantity)
            AggregateExpression(AggregateExpression.AVG, lambda t_: float(t_['l_quantity']))
        ],
        'lineitem_part_avg_group',
        False)


# with part_lineitem_join as (select * from part_scan, lineitem_scan where p_partkey = l_partkey)
def part_line_item_join_op():
    return HashJoin(JoinExpression('p_partkey', 'l_partkey'), 'part_lineitem_join', False)


# with lineitem_scan as (select * from lineitem)
def lineitem_scan_op():
    return SQLTableScan('lineitem.csv',
                        "select "
                        "  l_orderkey, l_partkey, l_quantity, l_extendedprice "
                        "from "
                        "  S3Object "
                        "where "
                        "  l_partkey = '182405' ",
                        'lineitem_scan',
                        False)


# with part_scan as (select * from part)
def part_scan_op():
    return SQLTableScan('part.csv',
                        "select "
                        "  p_partkey "
                        "from "
                        "  S3Object "
                        "where "
                        "  p_brand = 'Brand#41' and "
                        "  p_container = 'SM PACK' ",
                        'part_scan',
                        False)


# with part_scan_project as (select _0 as p_partkey from part_scan)
def lineitem_project_op():
    return Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'l_orderkey'),
            ProjectExpression(lambda t_: t_['_1'], 'l_partkey'),
            ProjectExpression(lambda t_: t_['_2'], 'l_quantity'),
            ProjectExpression(lambda t_: t_['_3'], 'l_extendedprice')
        ],
        'lineitem_project',
        False)


# with part_scan_project as (select _0 as p_partkey from part_scan)
def part_project_op():
    return Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'p_partkey')
        ],
        'part_project',
        False)


def test():
    """The baseline tst uses nested loop joins with no projection and no filtering pushed down to s3.

    This works by:

    1. Scanning part and filtering on brand and container
    2. It then scans lineitem
    3. It then joins the two tables (essentially filtering out lineitems that dont include parts that we filtered out in
       step 1)
    4. It then computes the average of l_quantity from the joined table and groups the results by partkey
    5. It then joins these computed averages with the joined table in step 3
    6. It then filters out any rows where l_quantity is less than the computed average

    TODO: There are few ways this can be done, the above is just one.

    :return: None
    """

    print('')
    print("TPCH Q17 Baseline Join")
    print("----------------------")

    query_plan = QueryPlan()

    # Define the operators
    # with part_scan as (select * from part)
    part_scan = query_plan.add_operator(SQLTableScan('part.csv',
                                                     "select "
                                                     "  * "
                                                     "from "
                                                     "  S3Object "
                                                     "where "
                                                     "  p_brand = 'Brand#41' and "
                                                     "  p_container = 'SM PACK' ",
                                                     'part_scan',
                                                     False))

    # with lineitem_scan as (select * from lineitem)
    lineitem_scan = query_plan.add_operator(SQLTableScan('lineitem.csv',
                                                         "select "
                                                         "  * "
                                                         "from "
                                                         "  S3Object "
                                                         "where "
                                                         "  l_partkey = '182405' ",
                                                         'lineitem_scan',
                                                         False))

    # with part_project as (select _0 as p_partkey from part_scan)
    part_project = query_plan.add_operator(Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'p_partkey'),
            ProjectExpression(lambda t_: t_['_3'], 'p_brand'),
            ProjectExpression(lambda t_: t_['_6'], 'p_container')
        ],
        'part_project',
        False))

    part_filter = query_plan.add_operator(Filter(
        PredicateExpression(lambda t_: t_['p_brand'] == 'Brand#41' and t_['p_container'] == 'SM PACK'),
        'part_filter',
        False))

    # with part_project as (select _0 as p_partkey from part_scan)
    lineitem_project = query_plan.add_operator(Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'l_orderkey'),
            ProjectExpression(lambda t_: t_['_1'], 'l_partkey'),
            ProjectExpression(lambda t_: t_['_4'], 'l_quantity'),
            ProjectExpression(lambda t_: t_['_5'], 'l_extendedprice')
        ],
        'lineitem_project',
        False))

    part_lineitem_join = query_plan.add_operator(part_line_item_join_op())
    lineitem_part_avg_group = query_plan.add_operator(lineitem_avg_group_op())
    lineitem_part_avg_group_project = query_plan.add_operator(lineitem_part_avg_group_project_op())
    part_lineitem_join_avg_group_join = query_plan.add_operator(part_lineitem_join_avg_group_op())
    lineitem_filter = query_plan.add_operator(lineitem_filter_op())
    extendedprice_sum_aggregate = query_plan.add_operator(extendedprice_sum_aggregate_op())
    extendedprice_sum_aggregate_project = query_plan.add_operator(extendedprice_sum_aggregate_project_op())
    collate = query_plan.add_operator(collate_op())

    # Connect the operators
    part_scan.connect(part_project)
    lineitem_scan.connect(lineitem_project)

    part_project.connect(part_filter)

    part_lineitem_join.connect_left_producer(part_filter)
    part_lineitem_join.connect_right_producer(lineitem_project)

    part_lineitem_join.connect(lineitem_part_avg_group)
    lineitem_part_avg_group.connect(lineitem_part_avg_group_project)

    part_lineitem_join_avg_group_join.connect_left_producer(lineitem_part_avg_group_project)
    part_lineitem_join_avg_group_join.connect_right_producer(part_lineitem_join)

    part_lineitem_join_avg_group_join.connect(lineitem_filter)
    lineitem_filter.connect(extendedprice_sum_aggregate)
    extendedprice_sum_aggregate.connect(extendedprice_sum_aggregate_project)
    extendedprice_sum_aggregate_project.connect(collate)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    query_plan.execute()

    # Assert the results
    # num_rows = 0
    # for t in collate.tuples():
    #     num_rows += 1
    #     print("{}:{}".format(num_rows, t))

    collate.print_tuples()

    # Write the metrics and plan graph
    query_plan.print_metrics()

    field_names = ['avg_yearly']

    assert len(collate.tuples()) == 1 + 1

    assert collate.tuples()[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    assert collate.tuples()[1] == [1274.9142857142856]
