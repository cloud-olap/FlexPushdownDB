# -*- coding: utf-8 -*-
"""Join query tests

"""
import os

from s3filter import ROOT_DIR
from s3filter.op.collate import Collate
from s3filter.op.hash_join import HashJoin
from s3filter.op.nested_loop_join import NestedLoopJoin, JoinExpression
from s3filter.op.project import Project, ProjectExpression
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.op.tuple import IndexedTuple
from s3filter.plan.query_plan import QueryPlan
from s3filter.util.test_util import gen_test_id


def test_join_baseline():
    """Tests a join

    :return: None
    """

    query_plan = QueryPlan()

    # Query plan
    supplier_scan = query_plan.add_operator(
        SQLTableScan('supplier.csv', 'select * from S3Object;', 'supplier_scan', False))

    supplier_project = query_plan.add_operator(
        Project([ProjectExpression(lambda t_: t_['_3'], 's_nationkey')], 'supplier_project', False))

    nation_scan = query_plan.add_operator(
        SQLTableScan('nation.csv', 'select * from S3Object;', 'nation_scan', False))

    nation_project = query_plan.add_operator(
        Project([ProjectExpression(lambda t_: t_['_0'], 'n_nationkey')], 'nation_project', False))

    supplier_nation_join = query_plan.add_operator(
        HashJoin(JoinExpression('s_nationkey', 'n_nationkey'), 'supplier_nation_join', False))

    collate = query_plan.add_operator(Collate('collate', False))

    supplier_scan.connect(supplier_project)
    nation_scan.connect(nation_project)
    supplier_nation_join.connect_left_producer(supplier_project)
    supplier_nation_join.connect_right_producer(nation_project)
    supplier_nation_join.connect(collate)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    supplier_scan.start()
    nation_scan.start()

    # Assert the results
    num_rows = 0
    for t in collate.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    collate.print_tuples()

    field_names = ['s_nationkey', 'n_nationkey']

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


def test_r_to_l_join():
    """Tests a join

    :return: None
    """

    query_plan = QueryPlan()

    # Query plan
    supplier_scan = query_plan.add_operator(
        SQLTableScan('supplier.csv', 'select * from S3Object;', 'supplier_scan', False))

    supplier_project = query_plan.add_operator(
        Project([ProjectExpression(lambda t_: t_['_3'], 's_nationkey')], 'supplier_project', False))

    nation_scan = query_plan.add_operator(
        SQLTableScan('nation.csv', 'select * from S3Object;', 'nation_scan', False))

    nation_project = query_plan.add_operator(
        Project([ProjectExpression(lambda t_: t_['_0'], 'n_nationkey')], 'nation_project', False))

    supplier_nation_join = query_plan.add_operator(
        HashJoin(JoinExpression('n_nationkey', 's_nationkey'), 'supplier_nation_join', False))

    collate = query_plan.add_operator(Collate('collate', False))

    supplier_scan.connect(supplier_project)
    nation_scan.connect(nation_project)
    supplier_nation_join.connect_left_producer(nation_project)
    supplier_nation_join.connect_right_producer(supplier_project)
    supplier_nation_join.connect(collate)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    supplier_scan.start()
    nation_scan.start()

    # Assert the results
    num_rows = 0
    for t in collate.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    collate.print_tuples()

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
        SQLTableScan('supplier.csv', 'select * from S3Object limit 0;', 'supplier_scan', False))

    supplier_project = query_plan.add_operator(
        Project([ProjectExpression(lambda t_: t_['_3'], 's_nationkey')], 'supplier_project', False))

    nation_scan = query_plan.add_operator(
        SQLTableScan('nation.csv', 'select * from S3Object limit 0;', 'nation_scan', False))

    nation_project = query_plan.add_operator(
        Project([ProjectExpression(lambda t_: t_['_0'], 'n_nationkey')], 'nation_project', False))

    supplier_nation_join = query_plan.add_operator(
        NestedLoopJoin(JoinExpression('s_nationkey', 'n_nationkey'), 'supplier_nation_join', False))

    collate = query_plan.add_operator(Collate('collate', False))

    supplier_scan.connect(supplier_project)
    nation_scan.connect(nation_project)
    supplier_nation_join.connect_left_producer(supplier_project)
    supplier_nation_join.connect_right_producer(nation_project)
    supplier_nation_join.connect(collate)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    supplier_scan.start()
    nation_scan.start()

    # Assert the results
    num_rows = 0
    for t in collate.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    assert len(collate.tuples()) == 0

    # Write the metrics
    query_plan.print_metrics()
