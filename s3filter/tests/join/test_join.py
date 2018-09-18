# -*- coding: utf-8 -*-
"""Join query tests

"""
import os

from s3filter import ROOT_DIR
from s3filter.op.collate import Collate
from s3filter.op.hash_join import HashJoin
from s3filter.op.hash_join_build import HashJoinBuild
from s3filter.op.hash_join_probe import HashJoinProbe
from s3filter.op.join_expression import JoinExpression
from s3filter.op.project import Project, ProjectExpression
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.op.tuple import IndexedTuple
from s3filter.plan.query_plan import QueryPlan
from s3filter.util.test_util import gen_test_id


def test_join_baseline():
    """Tests a join

    :return: None
    """

    query_plan = QueryPlan(is_async=True, buffer_size=1024)

    # Query plan
    supplier_scan = query_plan.add_operator(
        SQLTableScan('region.csv', 'select * from S3Object;', False, 'supplier_scan', query_plan, True))

    supplier_project = query_plan.add_operator(
        Project([ProjectExpression(lambda t_: t_['_0'], 'r_regionkey')], 'supplier_project', query_plan, True))

    nation_scan = query_plan.add_operator(
        SQLTableScan('nation.csv', 'select * from S3Object;', False, 'nation_scan', query_plan, True))

    nation_project = query_plan.add_operator(
        Project([ProjectExpression(lambda t_: t_['_2'], 'n_regionkey')], 'nation_project', query_plan, True))

    supplier_nation_join = query_plan.add_operator(
        HashJoin(JoinExpression('r_regionkey', 'n_regionkey'), 'supplier_nation_join', query_plan, True))

    collate = query_plan.add_operator(Collate('collate', query_plan, True))

    supplier_scan.connect(supplier_project)
    nation_scan.connect(nation_project)
    supplier_nation_join.connect_left_producer(supplier_project)
    supplier_nation_join.connect_right_producer(nation_project)
    supplier_nation_join.connect(collate)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    query_plan.execute()

    tuples = collate.tuples()

    collate.print_tuples(tuples)

    # Write the metrics
    query_plan.print_metrics()

    # Shut everything down
    query_plan.stop()

    field_names = ['r_regionkey', 'n_regionkey']

    assert len(tuples) == 25 + 1

    assert tuples[0] == field_names

    num_rows = 0
    for t in tuples:
        num_rows += 1
        # Assert that the nation_key in table 1 has been joined with the record in table 2 with the same nation_key
        if num_rows > 1:
            lt = IndexedTuple.build(t, field_names)
            assert lt['r_regionkey'] == lt['n_regionkey']


def test_join_baseline_pandas():
    """Tests a join

    :return: None
    """

    query_plan = QueryPlan(is_async=True, buffer_size=1024)

    # Query plan
    supplier_scan = query_plan.add_operator(
        SQLTableScan('region.csv', 'select * from S3Object;', True, False, False, 'supplier_scan', query_plan, True))

    def supplier_project_fn(df):
        df = df.filter(['_0'], axis='columns')
        df = df.rename(columns={'_0': 'r_regionkey'})
        return df

    supplier_project = query_plan.add_operator(
        Project([ProjectExpression(lambda t_: t_['_0'], 'r_regionkey')], 'supplier_project', query_plan, True, supplier_project_fn))

    nation_scan = query_plan.add_operator(
        SQLTableScan('nation.csv', 'select * from S3Object;', True, False, False, 'nation_scan', query_plan, True))

    def nation_project_fn(df):
        df = df.filter(['_2'], axis='columns')
        df = df.rename(columns={'_2': 'n_regionkey'})
        return df

    nation_project = query_plan.add_operator(
        Project([ProjectExpression(lambda t_: t_['_2'], 'n_regionkey')], 'nation_project', query_plan, True, nation_project_fn))

    supplier_nation_join_build = query_plan.add_operator(
        HashJoinBuild('n_regionkey', 'supplier_nation_join_build', query_plan, True))

    supplier_nation_join_probe = query_plan.add_operator(
        HashJoinProbe(JoinExpression('n_regionkey', 'r_regionkey'), 'supplier_nation_join_probe', query_plan, True))

    collate = query_plan.add_operator(Collate('collate', query_plan, True))

    supplier_scan.connect(supplier_project)
    nation_scan.connect(nation_project)
    nation_project.connect(supplier_nation_join_build)
    supplier_nation_join_probe.connect_build_producer(supplier_nation_join_build)
    supplier_nation_join_probe.connect_tuple_producer(supplier_project)
    supplier_nation_join_probe.connect(collate)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    query_plan.execute()

    tuples = collate.tuples()

    collate.print_tuples(tuples)

    # Write the metrics
    query_plan.print_metrics()

    # Shut everything down
    query_plan.stop()

    field_names = ['n_regionkey', 'r_regionkey']

    assert len(tuples) == 25 + 1

    assert tuples[0] == field_names

    num_rows = 0
    for t in tuples:
        num_rows += 1
        # Assert that the nation_key in table 1 has been joined with the record in table 2 with the same nation_key
        if num_rows > 1:
            lt = IndexedTuple.build(t, field_names)
            assert lt['n_regionkey'] == lt['r_regionkey']


def test_r_to_l_join():
    """Tests a join

    :return: None
    """

    query_plan = QueryPlan()

    # Query plan
    supplier_scan = query_plan.add_operator(
        SQLTableScan('supplier.csv', 'select * from S3Object;', False, 'supplier_scan', query_plan, False))

    supplier_project = query_plan.add_operator(
        Project([ProjectExpression(lambda t_: t_['_3'], 's_nationkey')], 'supplier_project', query_plan, False))

    nation_scan = query_plan.add_operator(
        SQLTableScan('nation.csv', 'select * from S3Object;', False, 'nation_scan', query_plan, False))

    nation_project = query_plan.add_operator(
        Project([ProjectExpression(lambda t_: t_['_0'], 'n_nationkey')], 'nation_project', query_plan, False))

    supplier_nation_join = query_plan.add_operator(
        HashJoin(JoinExpression('n_nationkey', 's_nationkey'), 'supplier_nation_join', query_plan, False))

    collate = query_plan.add_operator(Collate('collate', query_plan, False))

    supplier_scan.connect(supplier_project)
    nation_scan.connect(nation_project)
    supplier_nation_join.connect_left_producer(nation_project)
    supplier_nation_join.connect_right_producer(supplier_project)
    supplier_nation_join.connect(collate)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    query_plan.execute()

    # Assert the results
    # num_rows = 0
    # for t in collate.tuples():
    #     num_rows += 1
    #     print("{}:{}".format(num_rows, t))

    # collate.print_tuples()

    field_names = ['n_nationkey', 's_nationkey']

    assert len(collate.tuples()) == 10000 + 1

    assert collate.tuples()[0] == field_names

    num_rows = 0
    for t in collate.tuples():
        num_rows += 1
        # Assert that the nation_key in table 1 has been joined with the record in table 2 with the same nation_key
        if num_rows > 1:
            lt = IndexedTuple.build(t, field_names)
            assert lt['s_nationkey'] == lt['n_nationkey']

    # Write the metrics
    query_plan.print_metrics()


def test_join_empty():
    """Executes a join where no records are returned. We tst this as it's somewhat peculiar with s3 select, in so much
    as s3 does not return column names when selecting data, meaning, unlike a traditional DBMS, no field names tuple
    should be present in the results.

    :return: None
    """

    query_plan = QueryPlan()

    # Query plan
    supplier_scan = query_plan.add_operator(
        SQLTableScan('supplier.csv', 'select * from S3Object limit 0;', False, 'supplier_scan', query_plan, False))

    supplier_project = query_plan.add_operator(
        Project([ProjectExpression(lambda t_: t_['_3'], 's_nationkey')], 'supplier_project', query_plan, False))

    nation_scan = query_plan.add_operator(
        SQLTableScan('nation.csv', 'select * from S3Object limit 0;', False, 'nation_scan', query_plan, False))

    nation_project = query_plan.add_operator(
        Project([ProjectExpression(lambda t_: t_['_0'], 'n_nationkey')], 'nation_project', query_plan, False))

    supplier_nation_join = query_plan.add_operator(
        HashJoin(JoinExpression('s_nationkey', 'n_nationkey'), 'supplier_nation_join', query_plan, False))

    collate = query_plan.add_operator(Collate('collate', query_plan, False))

    supplier_scan.connect(supplier_project)
    nation_scan.connect(nation_project)
    supplier_nation_join.connect_left_producer(supplier_project)
    supplier_nation_join.connect_right_producer(nation_project)
    supplier_nation_join.connect(collate)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    query_plan.execute()

    # Assert the results
    # num_rows = 0
    # for t in collate.tuples():
    #     num_rows += 1
    #     print("{}:{}".format(num_rows, t))

    assert len(collate.tuples()) == 0

    # Write the metrics
    query_plan.print_metrics()
