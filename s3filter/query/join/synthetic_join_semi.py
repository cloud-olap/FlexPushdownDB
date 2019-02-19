from collections import OrderedDict

from s3filter.multiprocessing.worker_system import WorkerSystem
from s3filter.op.aggregate import Aggregate
from s3filter.op.aggregate_expression import AggregateExpression
from s3filter.op.bloom_create import BloomCreate
from s3filter.op.collate import Collate
from s3filter.op.hash_join_build import HashJoinBuild
from s3filter.op.hash_join_probe import HashJoinProbe
from s3filter.op.join_expression import JoinExpression
from s3filter.op.map import Map
from s3filter.op.operator_connector import connect_many_to_many, connect_all_to_all, connect_many_to_one, \
    connect_one_to_one
from s3filter.op.project import ProjectExpression, Project
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.op.sql_table_scan_bloom_use import SQLTableScanBloomUse
from s3filter.plan.query_plan import QueryPlan
from s3filter.query.join.synthetic_join_settings import SyntheticSemiJoinSettings
from s3filter.query.tpch import get_file_key
from s3filter.query.tpch_q19 import get_sql_suffix
import pandas as pd
import numpy as np

def query_plan(settings):
    # type: (SyntheticSemiJoinSettings) -> QueryPlan
    """

    :return: None
    """

    if settings.use_shared_mem:
        system = WorkerSystem(settings.shared_memory_size)
    else:
        system = None

    query_plan = QueryPlan(system, is_async=settings.parallel, buffer_size=settings.buffer_size,
                           use_shared_mem=settings.use_shared_mem)

    # Define the operators
    scan_a = \
        map(lambda p:
            query_plan.add_operator(
                SQLTableScan(get_file_key(settings.table_A_key, settings.table_A_sharded, p, settings.sf),
                             "select "
                             "  {} "
                             "from "
                             "  S3Object "
                             "where "
                             "  {} "
                             "  {} "
                             .format(settings.table_A_AB_join_key,
                                     settings.table_A_filter_sql,
                                     get_sql_suffix(settings.table_A_key, settings.table_A_parts, p,
                                                    settings.table_A_sharded)), settings.format_,
                             settings.use_pandas,
                             settings.secure,
                             settings.use_native,
                             'scan_a' + '_{}'.format(p),
                             query_plan,
                             False)),
            range(0, settings.table_A_parts))

    field_names_map_a = OrderedDict(
        zip(['_{}'.format(i) for i, name in enumerate(settings.table_A_field_names)], settings.table_A_field_names))

    def project_fn_a(df):
        df = df.rename(columns=field_names_map_a, copy=False)
        return df

    project_a = map(lambda p:
                    query_plan.add_operator(Project(
                        [ProjectExpression(k, v) for k, v in field_names_map_a.iteritems()],
                        'project_a' + '_{}'.format(p),
                        query_plan,
                        False,
                        project_fn_a)),
                    range(0, settings.table_A_parts))

    bloom_create_ab_join_key = map(lambda p:
                                   query_plan.add_operator(BloomCreate(
                                       settings.table_A_AB_join_key, 'bloom_create_ab_join_key' + '_{}'.format(p),
                                       query_plan, False, fp_rate=settings.fp_rate)),
                                   range(0, settings.table_A_parts))

    scan_b_on_ab_join_key = \
        map(lambda p:
            query_plan.add_operator(
                SQLTableScanBloomUse(get_file_key(settings.table_B_key, settings.table_B_sharded, p, settings.sf),
                                     "select "
                                     "  {},{} "
                                     "from "
                                     "  S3Object "
                                     "where "
                                     "  {} "
                                     "  {} "
                                     .format(settings.table_B_BC_join_key,
                                             settings.table_B_AB_join_key,
                                             settings.table_B_filter_sql,
                                             get_sql_suffix(settings.table_B_key, settings.table_B_parts, p,
                                                            settings.table_B_sharded, add_where=False)), settings.format_,
                                     settings.table_B_AB_join_key,
                                     settings.use_pandas,
                                     settings.secure,
                                     settings.use_native,
                                     'scan_b_on_ab_join_key' + '_{}'.format(p),
                                     query_plan,
                                     False)),
            range(0, settings.table_B_parts))

    if settings.table_C_key is None:

        scan_b_detail_on_b_pk = \
            map(lambda p:
                query_plan.add_operator(
                    SQLTableScanBloomUse(get_file_key(settings.table_B_key, settings.table_B_sharded, p, settings.sf),
                                         "select "
                                         "  {},{} "
                                         "from "
                                         "  S3Object "
                                         "where "
                                         "  {} "
                                         "  {} "
                                         .format(settings.table_B_primary_key,
                                                 settings.table_B_detail_field_name,
                                                 settings.table_B_filter_sql,
                                                 get_sql_suffix(settings.table_B_key, settings.table_B_parts, p,
                                                                settings.table_B_sharded, add_where=False)), settings.format_,
                                         settings.table_B_primary_key,
                                         settings.use_pandas,
                                         settings.secure,
                                         settings.use_native,
                                         'scan_c_detail_on_b_pk' + '_{}'.format(p),
                                         query_plan,
                                         False)),
                range(0, settings.table_B_parts))

        field_names_map_b_detail = OrderedDict(
            [('_0', settings.table_B_primary_key), ('_1', settings.table_B_detail_field_name)])

        def project_fn_b_detail(df):
            df.rename(columns=field_names_map_b_detail, inplace=True)
            return df

        project_b_detail = map(lambda p:
                               query_plan.add_operator(Project(
                                   [ProjectExpression(k, v) for k, v in field_names_map_b_detail.iteritems()],
                                   'project_b_detail' + '_{}'.format(p),
                                   query_plan,
                                   False,
                                   project_fn_b_detail)),
                               range(0, settings.table_B_parts))

        map_b_pk_1 = map(lambda p:
                         query_plan.add_operator(
                             Map(settings.table_B_primary_key, 'map_b_pk_1' + '_{}'.format(p),
                                 query_plan, False)),
                         range(0, settings.table_B_parts))

        map_b_pk_2 = map(lambda p:
                         query_plan.add_operator(
                             Map(settings.table_B_primary_key, 'map_b_pk_2' + '_{}'.format(p),
                                 query_plan, False)),
                         range(0, settings.table_B_parts))

        bloom_create_b_pk = map(lambda p:
                                query_plan.add_operator(BloomCreate(
                                    settings.table_B_primary_key,
                                    'bloom_create_b_pk' + '_{}'.format(p), query_plan, False, fp_rate=settings.fp_rate)),
                                range(0, settings.table_B_parts))

        join_probe_ab_and_b_on_b_pk = map(lambda p:
                                           query_plan.add_operator(
                                               HashJoinProbe(JoinExpression(settings.table_B_primary_key,
                                                                            settings.table_B_primary_key),
                                                             'join_probe_ab_and_b_on_b_pk' + '_{}'.format(
                                                                 p),
                                                             query_plan, False)),
                                           range(0, settings.table_B_parts))

        join_build_ab_and_b_on_b_pk = map(lambda p:
                                                 query_plan.add_operator(
                                                     HashJoinBuild(settings.table_B_primary_key,
                                                                   'join_build_ab_and_b_on_b_pk' + '_{}'.format(
                                                                       p), query_plan,
                                                                   False)),
                                                 range(0, settings.table_B_parts))

    else:
        scan_c_on_bc_join_key = \
            map(lambda p:
                query_plan.add_operator(
                    SQLTableScanBloomUse(get_file_key(settings.table_C_key, settings.table_C_sharded, p, settings.sf),
                                         "select "
                                         "  {}, {} "
                                         "from "
                                         "  S3Object "
                                         "where "
                                         "  {} "
                                         "  {} "
                                         .format(settings.table_C_primary_key,
                                                 settings.table_C_BC_join_key,
                                                 settings.table_C_filter_sql,
                                                 get_sql_suffix(settings.table_C_key, settings.table_C_parts, p,
                                                                settings.table_C_sharded, add_where=False)),
                                         settings.table_C_BC_join_key, settings.format_,
                                         settings.use_pandas,
                                         settings.secure,
                                         settings.use_native,
                                         'scan_c_on_bc_join_key' + '_{}'.format(p),
                                         query_plan,
                                         False)),
                range(0, settings.table_C_parts))

        field_names_map_c = OrderedDict(
            zip(['_{}'.format(i) for i, name in enumerate(settings.table_C_field_names)], settings.table_C_field_names))

        def project_fn_c(df):
            df.rename(columns=field_names_map_c, inplace=True)
            return df

        project_c = map(lambda p:
                        query_plan.add_operator(Project(
                            [ProjectExpression(k, v) for k, v in field_names_map_c.iteritems()],
                            'project_c' + '_{}'.format(p),
                            query_plan,
                            False,
                            project_fn_c)),
                        range(0, settings.table_C_parts))

        scan_c_detail_on_c_pk = \
            map(lambda p:
                query_plan.add_operator(
                    SQLTableScanBloomUse(get_file_key(settings.table_C_key, settings.table_C_sharded, p, settings.sf),
                                         "select "
                                         "  {},{} "
                                         "from "
                                         "  S3Object "
                                         "where "
                                         "  {} "
                                         "  {} "
                                         .format(settings.table_C_primary_key,
                                                 settings.table_C_detail_field_name,
                                                 settings.table_C_filter_sql,
                                                 get_sql_suffix(settings.table_C_key, settings.table_C_parts, p,
                                                                settings.table_C_sharded, add_where=False)),
                                         settings.table_C_primary_key, settings.format_,
                                         settings.use_pandas,
                                         settings.secure,
                                         settings.use_native,
                                         'scan_c_detail_on_c_pk' + '_{}'.format(p),
                                         query_plan,
                                         False)),
                range(0, settings.table_C_parts))

        field_names_map_c_detail = OrderedDict(
            [('_0', settings.table_C_primary_key), ('_1', settings.table_C_detail_field_name)])

        def project_fn_c_detail(df):
            df.rename(columns=field_names_map_c_detail, inplace=True)
            return df

        project_c_detail = map(lambda p:
                               query_plan.add_operator(Project(
                                   [ProjectExpression(k, v) for k, v in field_names_map_c_detail.iteritems()],
                                   'project_c_detail' + '_{}'.format(p),
                                   query_plan,
                                   False,
                                   project_fn_c_detail)),
                               range(0, settings.table_C_parts))

        map_bc_b_join_key = map(lambda p:
                                query_plan.add_operator(
                                    Map(settings.table_B_BC_join_key, 'map_bc_b_join_key' + '_{}'.format(p),
                                        query_plan, False)),
                                range(0, settings.table_C_parts))

        map_c_pk_1 = map(lambda p:
                         query_plan.add_operator(
                             Map(settings.table_C_primary_key, 'map_c_pk_1' + '_{}'.format(p),
                                 query_plan, False)),
                         range(0, settings.table_C_parts))

        map_c_pk_2 = map(lambda p:
                         query_plan.add_operator(
                             Map(settings.table_C_primary_key, 'map_c_pk_2' + '_{}'.format(p),
                                 query_plan, False)),
                         range(0, settings.table_C_parts))

        bloom_create_c_pk = map(lambda p:
                                query_plan.add_operator(BloomCreate(
                                    settings.table_C_primary_key,
                                    'bloom_create_bc_b_to_c_join_key_{}'.format(p), query_plan, False, fp_rate=settings.fp_rate)),
                                range(0, settings.table_C_parts))

        join_build_ab_and_c_on_bc_join_key = map(lambda p:
                                                 query_plan.add_operator(
                                                     HashJoinBuild(settings.table_B_BC_join_key,
                                                                   'join_build_ab_and_c_on_bc_join_key' + '_{}'.format(
                                                                       p), query_plan,
                                                                   False)),
                                                 range(0, settings.table_C_parts))

        join_probe_ab_and_c_on_bc_join_key = map(lambda p:
                                                 query_plan.add_operator(
                                                     HashJoinProbe(
                                                         JoinExpression(settings.table_B_BC_join_key,
                                                                        settings.table_C_BC_join_key),
                                                         'join_probe_ab_and_c_on_bc_join_key' + '_{}'.format(p),
                                                         query_plan, False)),
                                                 range(0, settings.table_C_parts))

        join_build_abc_and_c_on_c_pk = map(lambda p:
                                           query_plan.add_operator(
                                               HashJoinBuild(settings.table_C_primary_key,
                                                             'join_build_abc_and_c_on_c_pk' + '_{}'.format(
                                                                 p),
                                                             query_plan,
                                                             False)),
                                           range(0, settings.table_C_parts))

        join_probe_abc_and_c_on_c_pk = map(lambda p:
                                           query_plan.add_operator(
                                               HashJoinProbe(JoinExpression(settings.table_C_primary_key,
                                                                            settings.table_C_primary_key),
                                                             'join_probe_abc_and_c_on_c_pk' + '_{}'.format(
                                                                 p),
                                                             query_plan, False)),
                                           range(0, settings.table_C_parts))

        bloom_create_bc_join_key = map(lambda p:
                                       query_plan.add_operator(BloomCreate(
                                           settings.table_B_BC_join_key,
                                           'bloom_create_bc_join_key' + '_{}'.format(p), query_plan, False)),
                                       range(0, settings.table_B_parts))

        map_bc_c_join_key = map(lambda p:
                                query_plan.add_operator(
                                    Map(settings.table_C_BC_join_key, 'map_bc_c_join_key' + '_{}'.format(p),
                                        query_plan, False)),
                                range(0, settings.table_B_parts))

    field_names_map_b = OrderedDict(
        zip(['_{}'.format(i) for i, name in enumerate(settings.table_B_field_names)], settings.table_B_field_names))

    def project_fn_b(df):
        df.rename(columns=field_names_map_b, inplace=True)
        return df

    project_b = map(lambda p:
                    query_plan.add_operator(Project(
                        [ProjectExpression(k, v) for k, v in field_names_map_b.iteritems()],
                        'project_b' + '_{}'.format(p),
                        query_plan,
                        False,
                        project_fn_b)),
                    range(0, settings.table_B_parts))





    map_ab_a_join_key = map(lambda p:
                            query_plan.add_operator(
                                Map(settings.table_A_AB_join_key, 'map_ab_a_join_key' + '_{}'.format(p), query_plan,
                                    False)),
                            range(0, settings.table_A_parts))

    map_ab_b_join_key = map(lambda p:
                            query_plan.add_operator(
                                Map(settings.table_B_AB_join_key, 'map_ab_b_join_key' + '_{}'.format(p),
                                    query_plan, False)),
                            range(0, settings.table_B_parts))







    join_build_a_and_b_on_ab_join_key = map(lambda p:
                                            query_plan.add_operator(
                                                HashJoinBuild(settings.table_A_AB_join_key,
                                                              'join_build_a_and_b_on_ab_join_key' + '_{}'.format(p),
                                                              query_plan,
                                                              False)),
                                            range(0, settings.table_B_parts))

    join_probe_a_and_b_on_ab_join_key = map(lambda p:
                                            query_plan.add_operator(
                                                HashJoinProbe(
                                                    JoinExpression(settings.table_A_AB_join_key,
                                                                   settings.table_B_AB_join_key),
                                                    'join_probe_a_and_b_on_ab_join_key' + '_{}'.format(p),
                                                    query_plan, False)),
                                            range(0, settings.table_B_parts))



    if settings.table_C_key is None:

        def part_aggregate_fn(df):
            sum_ = df[settings.table_B_detail_field_name].astype(np.float).sum()
            return pd.DataFrame({'_0': [sum_]})

        part_aggregate = map(lambda p:
                             query_plan.add_operator(Aggregate(
                                 [
                                     AggregateExpression(AggregateExpression.SUM,
                                                         lambda t: float(t[settings.table_B_detail_field_name]))
                                 ],
                                 settings.use_pandas,
                                 'part_aggregate_{}'.format(p), query_plan, False, part_aggregate_fn)),
                             range(0, settings.table_B_parts))

    else:
        def part_aggregate_fn(df):
            sum_ = df[settings.table_C_detail_field_name].astype(np.float).sum()
            return pd.DataFrame({'_0': [sum_]})

        part_aggregate = map(lambda p:
                             query_plan.add_operator(Aggregate(
                                 [
                                     AggregateExpression(AggregateExpression.SUM,
                                                         lambda t: float(t[settings.table_C_detail_field_name]))
                                 ],
                                 settings.use_pandas,
                                 'part_aggregate_{}'.format(p), query_plan, False, part_aggregate_fn)),
                             range(0, settings.table_C_parts))

    def aggregate_reduce_fn(df):
        sum_ = df['_0'].astype(np.float).sum()
        return pd.DataFrame({'_0': [sum_]})

    aggregate_reduce = query_plan.add_operator(Aggregate(
        [
            AggregateExpression(AggregateExpression.SUM, lambda t: float(t['_0']))
        ],
        settings.use_pandas,
        'aggregate_reduce', query_plan, False, aggregate_reduce_fn))

    aggregate_project = query_plan.add_operator(Project(
        [
            ProjectExpression(lambda t: t['_0'], 'total_balance')
        ],
        'aggregate_project', query_plan,
        False))

    collate = query_plan.add_operator(Collate('collate', query_plan, False))

    # Inline some of the operators
    map(lambda o: o.set_async(False), project_a)
    map(lambda o: o.set_async(False), project_b)
    map(lambda o: o.set_async(False), map_ab_a_join_key)
    map(lambda o: o.set_async(False), map_ab_b_join_key)
    if settings.table_C_key is None:
        map(lambda o: o.set_async(False), map_b_pk_1)
        map(lambda o: o.set_async(False), map_b_pk_2)
        map(lambda o: o.set_async(False), project_b_detail)
    else:
        map(lambda o: o.set_async(False), map_bc_b_join_key)
        map(lambda o: o.set_async(False), map_bc_c_join_key)
        map(lambda o: o.set_async(False), map_c_pk_1)
        map(lambda o: o.set_async(False), map_c_pk_2)
        map(lambda o: o.set_async(False), project_c)
        map(lambda o: o.set_async(False), project_c_detail)
    aggregate_project.set_async(False)

    # Connect the operators
    connect_many_to_many(scan_a, project_a)

    connect_many_to_many(project_a, map_ab_a_join_key)

    connect_all_to_all(map_ab_a_join_key, join_build_a_and_b_on_ab_join_key)
    connect_all_to_all(project_a, bloom_create_ab_join_key)
    # connect_all_to_all(map_A_to_B, join_build_a_and_b_on_ab_join_key)
    connect_many_to_many(join_build_a_and_b_on_ab_join_key, join_probe_a_and_b_on_ab_join_key)

    # connect_all_to_all(map_bloom_A_to_B, bloom_create_ab_join_key)
    connect_many_to_many(bloom_create_ab_join_key, scan_b_on_ab_join_key)
    connect_many_to_many(scan_b_on_ab_join_key, project_b)
    # connect_many_to_many(project_b, join_probe_a_and_b_on_ab_join_key)
    # connect_all_to_all(map_B_to_B, join_probe_a_and_b_on_ab_join_key)

    connect_many_to_many(project_b, map_ab_b_join_key)
    connect_all_to_all(map_ab_b_join_key, join_probe_a_and_b_on_ab_join_key)

    # connect_many_to_many(join_probe_a_and_b_on_ab_join_key, map_bloom_B_to_B)

    if settings.table_C_key is None:
        # connect_all_to_all(join_probe_a_and_b_on_ab_join_key, part_aggregate)
        connect_many_to_many(scan_b_detail_on_b_pk, project_b_detail)
        connect_many_to_many(project_b_detail, map_b_pk_2)
        connect_many_to_many(bloom_create_b_pk, scan_b_detail_on_b_pk)
        connect_all_to_all(join_probe_a_and_b_on_ab_join_key, bloom_create_b_pk)
        connect_all_to_all(map_b_pk_2, join_probe_ab_and_b_on_b_pk)
        connect_many_to_many(join_probe_ab_and_b_on_b_pk, part_aggregate)
        connect_many_to_many(join_build_ab_and_b_on_b_pk, join_probe_ab_and_b_on_b_pk)
        connect_many_to_many(join_probe_a_and_b_on_ab_join_key, map_b_pk_1)
        connect_all_to_all(map_b_pk_1, join_build_ab_and_b_on_b_pk)


    else:
        connect_all_to_all(join_probe_a_and_b_on_ab_join_key, bloom_create_bc_join_key)
        connect_many_to_many(bloom_create_bc_join_key, scan_c_on_bc_join_key)
        connect_many_to_many(scan_c_on_bc_join_key, project_c)
        # connect_many_to_many(project_c, join_probe_ab_and_c_on_bc_join_key)
        connect_all_to_all(map_bc_c_join_key, join_probe_ab_and_c_on_bc_join_key)
        # connect_many_to_many(join_probe_a_and_b_on_ab_join_key, join_build_ab_and_c_on_bc_join_key)
        connect_many_to_many(join_probe_a_and_b_on_ab_join_key, map_bc_b_join_key)
        connect_all_to_all(map_bc_b_join_key, join_build_ab_and_c_on_bc_join_key)
        connect_all_to_all(join_probe_ab_and_c_on_bc_join_key, bloom_create_c_pk)
        # connect_many_to_many(join_probe_ab_and_c_on_bc_join_key, join_build_abc_and_c_on_c_pk)
        connect_many_to_many(join_probe_ab_and_c_on_bc_join_key, map_c_pk_1)
        connect_all_to_all(map_c_pk_1, join_build_abc_and_c_on_c_pk)
        connect_many_to_many(bloom_create_c_pk, scan_c_detail_on_c_pk)
        # connect_all_to_all(bloom_create_bc_join_key, scan_c_detail_on_c_pk)
        connect_many_to_many(join_build_abc_and_c_on_c_pk, join_probe_abc_and_c_on_c_pk)
        # connect_many_to_many(join_probe_a_and_b_on_ab_join_key, map_B_to_C)
        # connect_all_to_all(join_probe_a_and_b_on_ab_join_key, join_build_abc_and_c_on_c_pk)
        connect_many_to_many(scan_c_detail_on_c_pk, project_c_detail)
        # connect_many_to_many(project_c_detail, map_C_to_C)
        # connect_all_to_all(project_c_detail, join_probe_abc_and_c_on_c_pk)
        connect_many_to_many(project_c_detail, map_c_pk_2)

        connect_many_to_many(project_c, map_bc_c_join_key)
        connect_many_to_many(join_build_ab_and_c_on_bc_join_key, join_probe_ab_and_c_on_bc_join_key)
        connect_all_to_all(map_c_pk_2, join_probe_abc_and_c_on_c_pk)

        connect_many_to_many(join_probe_abc_and_c_on_c_pk, part_aggregate)

    connect_many_to_one(part_aggregate, aggregate_reduce)
    connect_one_to_one(aggregate_reduce, aggregate_project)
    connect_one_to_one(aggregate_project, collate)

    return query_plan
