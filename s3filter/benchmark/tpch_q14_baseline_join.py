# -*- coding: utf-8 -*-
"""TPCH Q14 Baseline Benchmark

"""

import os
import re
from datetime import datetime, timedelta

from s3filter import ROOT_DIR
from s3filter.op.aggregate import Aggregate
from s3filter.op.aggregate_expression import AggregateExpression
from s3filter.op.collate import Collate
from s3filter.op.filter import Filter
from s3filter.op.join import JoinExpression, Join
from s3filter.op.predicate_expression import PredicateExpression
from s3filter.op.project import ProjectExpression, Project
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.plan.query_plan import QueryPlan
from s3filter.sql.function import timestamp, cast
from s3filter.util.test_util import gen_test_id


def main():
    """The baseline tst uses nested loop joins with no projection and no filtering pushed down to s3.

    :return: None
    """

    query_plan = QueryPlan("TPCH Q14 Baseline Join Test")

    # Query plan
    # This date is chosen because it triggers the filter to filter out 1 of the rows in the root data set.
    date = '1996-03-13'
    min_shipped_date = datetime.strptime(date, '%Y-%m-%d')
    max_shipped_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=30)

    lineitem_scan = query_plan.add_operator(SQLTableScan('lineitem.csv',
                                                         "select * from S3Object "
                                                         "where "
                                                         "(l_orderkey = '18436' and l_partkey = '164584') or "
                                                         "(l_orderkey = '18720' and l_partkey = '92764') or "
                                                         "(l_orderkey = '12482' and l_partkey = '117405') or "
                                                         "(l_orderkey = '27623' and l_partkey = '137010') or "

                                                         "(l_orderkey = '10407' and l_partkey = '43275') or "
                                                         "(l_orderkey = '17027' and l_partkey = '172729') or "
                                                         "(l_orderkey = '23302' and l_partkey = '18523') or "
                                                         "(l_orderkey = '27334' and l_partkey = '94308') or "

                                                         "(l_orderkey = '15427' and l_partkey = '125586') or "
                                                         "(l_orderkey = '11590' and l_partkey = '162359') or "
                                                         "(l_orderkey = '2945' and l_partkey = '126197') or "
                                                         "(l_orderkey = '15648' and l_partkey = '143904');",
                                                         'lineitem_scan',
                                                         False))

    lineitem_project = query_plan.add_operator(Project(
        [
            ProjectExpression(lambda t_: t_['_1'], 'l_partkey'),
            ProjectExpression(lambda t_: t_['_5'], 'l_extendedprice'),
            ProjectExpression(lambda t_: t_['_6'], 'l_discount'),
            ProjectExpression(lambda t_: t_['_10'], 'l_shipdate')
        ],
        'lineitem_project',
        False))

    part_scan = query_plan.add_operator(SQLTableScan('part.csv',
                                                     "select * from S3Object;",
                                                     'part_scan',
                                                     False))

    part_project = query_plan.add_operator(Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'p_partkey'),
            ProjectExpression(lambda t_: t_['_3'], 'p_brand'),
            ProjectExpression(lambda t_: t_['_4'], 'p_type')
        ],
        'part_project',
        False))

    lineitem_filter = query_plan.add_operator(Filter(
        PredicateExpression(lambda t_:
                            (cast(t_['l_shipdate'], timestamp) >= cast(min_shipped_date, timestamp)) and
                            (cast(t_['l_shipdate'], timestamp) < cast(max_shipped_date, timestamp))),
        'lineitem_filter',
        False))

    part_filter = query_plan.add_operator(Filter(
        PredicateExpression(lambda t_: t_['p_brand'] == 'Brand#12'),
        'part_filter',
        False))

    join = query_plan.add_operator(Join(
        JoinExpression('l_partkey', 'p_partkey'),
        'join',
        False))

    def ex1(t_):

        v1 = float(t_['l_extendedprice']) * (1.0 - float(t_['l_discount']))

        rx = re.compile('^PROMO.*$')

        if rx.search(t_['p_type']):  # p_type
            v2 = v1
        else:
            v2 = 0.0

        return v2

    def ex2(t_):

        v1 = float(t_['l_extendedprice']) * (1.0 - float(t_['l_discount']))

        return v1

    aggregate = query_plan.add_operator(
        Aggregate(
            [
                AggregateExpression(AggregateExpression.SUM, ex1),
                AggregateExpression(AggregateExpression.SUM, ex2)
            ],
            'aggregate',
            False))

    aggregate_project = query_plan.add_operator(Project(
        [
            ProjectExpression(lambda t_: 100 * t_['_0'] / t_['_1'], 'promo_revenue')
        ],
        'aggregate_project',
        False))

    collate = query_plan.add_operator(Collate('collate', False))

    lineitem_scan.connect(lineitem_project)
    lineitem_project.connect(lineitem_filter)
    join.connect_left_producer(lineitem_filter)
    part_scan.connect(part_project)
    part_project.connect(part_filter)
    join.connect_right_producer(part_filter)
    join.connect(aggregate)
    aggregate.connect(aggregate_project)
    aggregate_project.connect(collate)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../benchmark-output"), gen_test_id())

    # Start the query
    lineitem_scan.start()
    part_scan.start()

    # Assert the results
    num_rows = 0
    for t in collate.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    field_names = ['promo_revenue']

    assert len(collate.tuples()) == 1 + 1

    assert collate.tuples()[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    assert collate.tuples()[1] == [33.42623264199327]

    # Write the metrics
    query_plan.print_metrics()


if __name__ == "__main__":
    main()
