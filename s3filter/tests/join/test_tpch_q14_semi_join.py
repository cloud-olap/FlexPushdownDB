import os
import re
from datetime import datetime, timedelta

from s3filter import ROOT_DIR
from s3filter.op.aggregate import Aggregate
from s3filter.op.aggregate_expression import AggregateExpression
from s3filter.op.bloom_create import BloomCreate
from s3filter.op.collate import Collate
from s3filter.op.hash_join import HashJoin
from s3filter.op.nested_loop_join import JoinExpression
from s3filter.op.project import Project, ProjectExpression
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.op.sql_table_scan_bloom_use import SQLTableScanBloomUse
from s3filter.plan.query_plan import QueryPlan
from s3filter.util.test_util import gen_test_id


def test_join_semi():
    """

    :return: None
    """

    query_plan = QueryPlan()

    # Query plan
    # TODO: DATE is the first day of a month randomly selected from a random year within [1993 .. 1997].
    date = '1996-03-13'
    min_shipped_date = datetime.strptime(date, '%Y-%m-%d')
    max_shipped_date = datetime.strptime(date, '%Y-%m-%d') + timedelta(days=30)

    part_scan_1 = query_plan.add_operator(SQLTableScan('part.csv',
                                                       "select "
                                                       "  p_partkey from S3Object "
                                                       "where "
                                                       "  p_brand = 'Brand#12' "
                                                       " ",
                                                       'part_table_scan_1',
                                                       False))

    part_scan_1_project = query_plan.add_operator(Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'p_partkey')
        ],
        'part_scan_1_project',
        False))

    part_bloom_create = query_plan.add_operator(
        BloomCreate('p_partkey', 'part_bloom_create', False))

    lineitem_scan_1 = query_plan.add_operator(
        SQLTableScanBloomUse('lineitem.csv',
                             "select "
                             "  l_partkey from S3Object "
                             "where "
                             "  cast(l_shipdate as timestamp) >= cast(\'{}\' as timestamp) and "
                             "  cast(l_shipdate as timestamp) < cast(\'{}\' as timestamp) and "
                             "  ( "
                             "      (l_orderkey = '18436' and l_partkey = '164584') or "
                             "      (l_orderkey = '18720' and l_partkey = '92764') or "
                             "      (l_orderkey = '12482' and l_partkey = '117405') or "
                             "      (l_orderkey = '27623' and l_partkey = '137010') or "

                             "      (l_orderkey = '10407' and l_partkey = '43275') or "
                             "      (l_orderkey = '17027' and l_partkey = '172729') or "
                             "      (l_orderkey = '23302' and l_partkey = '18523') or "
                             "      (l_orderkey = '27334' and l_partkey = '94308') or "

                             "      (l_orderkey = '15427' and l_partkey = '125586') or "
                             "      (l_orderkey = '11590' and l_partkey = '162359') or "
                             "      (l_orderkey = '2945' and l_partkey = '126197') or "
                             "      (l_orderkey = '15648' and l_partkey = '143904') "
                             "  ) "
                             " ".format(min_shipped_date.strftime('%Y-%m-%d'),
                                        max_shipped_date.strftime('%Y-%m-%d'))
                             ,
                             'l_partkey',
                             'lineitem_table_scan_1',
                             False))

    lineitem_scan_1_project = query_plan.add_operator(Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'l_partkey')
        ],
        'lineitem_scan_1_project',
        False))

    part_lineitem_join_1 = query_plan.add_operator(
        HashJoin(JoinExpression('p_partkey', 'l_partkey'), 'part_lineitem_join_1', False))

    join_bloom_create = query_plan.add_operator(
        BloomCreate('l_partkey', 'join_bloom_create', False))

    part_table_scan_2 = query_plan.add_operator(SQLTableScanBloomUse('part.csv',
                                                                     "select "
                                                                     "  p_partkey, p_type from S3Object "
                                                                     "where "
                                                                     "  p_brand = 'Brand#12' "
                                                                     " ",
                                                                     'p_partkey',
                                                                     'part_table_scan_2',
                                                                     False))

    part_scan_2_project = query_plan.add_operator(Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'p_partkey'),
            ProjectExpression(lambda t_: t_['_1'], 'p_type')
        ],
        'part_scan_2_project',
        False))

    lineitem_table_scan_2 = query_plan.add_operator(
        SQLTableScanBloomUse('lineitem.csv',
                             "select "
                             "  l_partkey, l_extendedprice, l_discount from S3Object "
                             "where "
                             "  cast(l_shipdate as timestamp) >= cast(\'{}\' as timestamp) and "
                             "  cast(l_shipdate as timestamp) < cast(\'{}\' as timestamp) and "
                             "  ( "
                             "      (l_orderkey = '18436' and l_partkey = '164584') or "
                             "      (l_orderkey = '18720' and l_partkey = '92764') or "
                             "      (l_orderkey = '12482' and l_partkey = '117405') or "
                             "      (l_orderkey = '27623' and l_partkey = '137010') or "

                             "      (l_orderkey = '10407' and l_partkey = '43275') or "
                             "      (l_orderkey = '17027' and l_partkey = '172729') or "
                             "      (l_orderkey = '23302' and l_partkey = '18523') or "
                             "      (l_orderkey = '27334' and l_partkey = '94308') or "

                             "      (l_orderkey = '15427' and l_partkey = '125586') or "
                             "      (l_orderkey = '11590' and l_partkey = '162359') or "
                             "      (l_orderkey = '2945' and l_partkey = '126197') or "
                             "      (l_orderkey = '15648' and l_partkey = '143904') "
                             "  ) "
                             " ".format(
                                 min_shipped_date.strftime('%Y-%m-%d'),
                                 max_shipped_date.strftime('%Y-%m-%d')),
                             'l_partkey',
                             'lineitem_table_scan_2',
                             False))

    lineitem_scan_2_project = query_plan.add_operator(Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'l_partkey'),
            ProjectExpression(lambda t_: t_['_1'], 'l_extendedprice'),
            ProjectExpression(lambda t_: t_['_2'], 'l_discount')
        ],
        'lineitem_scan_2_project',
        False))

    part_lineitem_join_2 = query_plan.add_operator(
        HashJoin(JoinExpression('p_partkey', 'l_partkey'),
                 'part_lineitem_join_2',
                 False))  # p_partkey and l_partkey

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
            [AggregateExpression(AggregateExpression.SUM, ex1), AggregateExpression(AggregateExpression.SUM, ex2)],
            'aggregate', False))

    project = query_plan.add_operator(
        Project([ProjectExpression(lambda t_: 100 * t_['_0'] / t_['_1'], 'promo_revenue')], 'project', False))
    collate = query_plan.add_operator(Collate('collate', False))

    part_scan_1.connect(part_scan_1_project)
    part_scan_1_project.connect(part_bloom_create)
    part_bloom_create.connect(lineitem_scan_1)
    part_lineitem_join_1.connect_left_producer(part_scan_1_project)
    lineitem_scan_1.connect(lineitem_scan_1_project)
    part_lineitem_join_1.connect_right_producer(lineitem_scan_1_project)
    part_lineitem_join_1.connect(join_bloom_create)
    join_bloom_create.connect(part_table_scan_2)
    join_bloom_create.connect(lineitem_table_scan_2)
    part_table_scan_2.connect(part_scan_2_project)
    part_lineitem_join_2.connect_left_producer(part_scan_2_project)
    lineitem_table_scan_2.connect(lineitem_scan_2_project)
    part_lineitem_join_2.connect_right_producer(lineitem_scan_2_project)
    part_lineitem_join_2.connect(aggregate)
    aggregate.connect(project)
    project.connect(collate)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    part_scan_1.start()

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
