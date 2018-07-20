# -*- coding: utf-8 -*-
"""Join query tests

"""
import math
import os

from s3filter import ROOT_DIR
from s3filter.op.collate import Collate
from s3filter.op.hash_join_build import HashJoinBuild
from s3filter.op.hash_join_probe import HashJoinProbe
from s3filter.op.join_expression import JoinExpression
from s3filter.op.merge import Merge
from s3filter.op.project import Project, ProjectExpression
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.plan.query_plan import QueryPlan
from s3filter.util.test_util import gen_test_id


def test_sharded_and_separate_build_and_probe_join():
    run(False, 8192)


def test_sharded_and_separate_build_and_probe_join_parallel():
    run(True, 8192)


def run(parallel, buffer_size):
    """Tests a with sharded operators and separate build and probe for join

    :return: None
    """

    query_plan = QueryPlan(is_async=parallel, buffer_size=buffer_size)

    # Query plan
    parts = 2

    collate = query_plan.add_operator(Collate('collate', query_plan, False))
    merge = query_plan.add_operator(Merge('merge', query_plan, False))

    hash_join_build_ops = []
    hash_join_probe_ops = []
    for p in range(1, parts + 1):
        r_region_key_lower = math.ceil((5.0 / float(parts)) * (p - 1))
        r_region_key_upper = math.ceil((5.0 / float(parts)) * (p))

        region_scan = query_plan.add_operator(
            SQLTableScan('region.csv',
                         'select * from S3Object where cast(r_regionkey as int) >= {} and cast(r_regionkey as int) < {};'
                         .format(r_region_key_lower, r_region_key_upper),
                         'region_scan' + '_' + str(p),
                         query_plan,
                         False))

        region_project = query_plan.add_operator(
            Project([
                ProjectExpression(lambda t_: t_['_0'], 'r_regionkey'),
                ProjectExpression(lambda t_: t_['_1'], 'r_name')
            ], 'region_project' + '_' + str(p), query_plan, False))

        n_nation_key_lower = math.ceil((25.0 / float(parts)) * (p - 1))
        n_nation_key_upper = math.ceil((25.0 / float(parts)) * (p))

        nation_scan = query_plan.add_operator(
            SQLTableScan('nation.csv',
                         'select * from S3Object where cast(n_nationkey as int) >= {} and cast(n_nationkey as int) < {};'
                         .format(n_nation_key_lower, n_nation_key_upper),
                         'nation_scan' + '_' + str(p),
                         query_plan,
                         False))

        nation_project = query_plan.add_operator(
            Project([
                ProjectExpression(lambda t_: t_['_0'], 'n_nationkey'),
                ProjectExpression(lambda t_: t_['_1'], 'n_name'),
                ProjectExpression(lambda t_: t_['_2'], 'n_regionkey')
            ], 'nation_project' + '_' + str(p), query_plan, False))

        region_hash_join_build = query_plan.add_operator(
            HashJoinBuild('r_regionkey', 'region_hash_join_build' + '_' + str(p), query_plan, False))
        hash_join_build_ops.append(region_hash_join_build)

        region_nation_join_probe = query_plan.add_operator(
            HashJoinProbe(JoinExpression('r_regionkey', 'n_regionkey'), 'region_nation_join_probe' + '_' + str(p),
                          query_plan, False))
        hash_join_probe_ops.append(region_nation_join_probe)

        # region_nation_join = query_plan.add_operator(
        #     HashJoin(JoinExpression('r_regionkey', 'n_regionkey'), 'region_nation_join' + '_' + str(p), query_plan, True))

        region_scan.connect(region_project)
        nation_scan.connect(nation_project)
        region_project.connect(region_hash_join_build)
        # region_nation_join.connect_left_producer(region_project)
        # region_nation_join.connect_right_producer(nation_project)
        # region_nation_join_probe.connect_build_producer(region_hash_join_build)
        region_nation_join_probe.connect_tuple_producer(nation_project)
        region_nation_join_probe.connect(merge)

    for probe_op in hash_join_probe_ops:
        for build_op in hash_join_build_ops:
            probe_op.connect_build_producer(build_op)

    merge.connect(collate)

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

    # field_names = ['r_regionkey', 'n_regionkey']
    #
    # assert len(tuples) == 25 + 1
    #
    # assert tuples[0] == field_names
    #
    # num_rows = 0
    # for t in tuples:
    #     num_rows += 1
    #     # Assert that the nation_key in table 1 has been joined with the record in table 2 with the same nation_key
    #     if num_rows > 1:
    #         lt = IndexedTuple.build(t, field_names)
    #         assert lt['r_regionkey'] == lt['n_regionkey']
