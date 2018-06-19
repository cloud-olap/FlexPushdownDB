import os
import re
from datetime import datetime, timedelta

from s3filter import ROOT_DIR
from s3filter.op.aggregate import Aggregate
from s3filter.op.aggregate_expression import AggregateExpression
from s3filter.op.collate import Collate
from s3filter.op.hash_join import HashJoin
from s3filter.op.nested_loop_join import JoinExpression
from s3filter.op.project import Project, ProjectExpression
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.plan.query_plan import QueryPlan
from s3filter.util.test_util import gen_test_id


def test_join_filtered():
    """The filtered tst uses nested loop joins but first projections and filtering is pushed down to s3.

    :return: None
    """

    query_plan = QueryPlan()

    # Query plan
    # TODO: DATE is the first day of a month randomly selected from a random year within [1993 .. 1997].
    date = '1996-03-13'
    min_shipped_date = datetime.strptime(date, '%Y-%m-%d')
    max_shipped_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=30)

    lineitem_scan = query_plan.add_operator(
        SQLTableScan('lineitem.csv',
                     "select l_partkey, l_extendedprice, l_discount from S3Object "
                     "where "
                     "cast(l_shipdate as timestamp) >= cast(\'{}\' as timestamp) and "
                     "cast(l_shipdate as timestamp) < cast(\'{}\' as timestamp) and "
                     "( "
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
                     "(l_orderkey = '15648' and l_partkey = '143904')"
                     ") "
                     ";".format(min_shipped_date.strftime('%Y-%m-%d'),
                                max_shipped_date.strftime('%Y-%m-%d')),
                     'lineitem_scan',
                     False))

    lineitem_project = query_plan.add_operator(Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'l_partkey'),
            ProjectExpression(lambda t_: t_['_1'], 'l_extendedprice'),
            ProjectExpression(lambda t_: t_['_2'], 'l_discount')
        ],
        'lineitem_project',
        False))

    part_scan = query_plan.add_operator(SQLTableScan('part.csv',
                                                     "select "
                                                     "  p_partkey, p_type from S3Object "
                                                     "where "
                                                     "  p_brand = 'Brand#12' "
                                                     " ",
                                                     'part_scan',
                                                     False))

    part_project = query_plan.add_operator(Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'p_partkey'),
            ProjectExpression(lambda t_: t_['_1'], 'p_type')
        ],
        'part_project',
        False))

    join = query_plan.add_operator(
        HashJoin(JoinExpression('l_partkey', 'p_partkey'), 'join', False))  # l_partkey and p_partkey

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
            'aggregate', False))

    aggregate_project = query_plan.add_operator(
        Project([ProjectExpression(lambda t_: 100 * t_['_0'] / t_['_1'], 'promo_revenue')], 'aggregate_project', False))

    collate = query_plan.add_operator(Collate('collate', False))

    lineitem_scan.connect(lineitem_project)
    part_scan.connect(part_project)
    join.connect_left_producer(lineitem_project)
    join.connect_right_producer(part_project)
    join.connect(aggregate)
    aggregate.connect(aggregate_project)
    aggregate_project.connect(collate)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    lineitem_scan.start()
    part_scan.start()

    # Assert the results
    num_rows = 0
    for t in collate.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    print('')
    collate.print_tuples()

    field_names = ['promo_revenue']

    assert len(collate.tuples()) == 1 + 1

    assert collate.tuples()[0] == field_names

    # NOTE: This result has been verified with the equivalent data and query on PostgreSQL
    assert collate.tuples()[1] == [33.42623264199327]

    # Write the metrics
    query_plan.print_metrics()
