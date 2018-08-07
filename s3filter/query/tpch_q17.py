"""Reusable query elements for tpch q17

"""
import math

from s3filter.op.aggregate import Aggregate
from s3filter.op.aggregate_expression import AggregateExpression
from s3filter.op.bloom_create import BloomCreate
from s3filter.op.collate import Collate
from s3filter.op.filter import Filter
from s3filter.op.group import Group
from s3filter.op.hash_join import HashJoin
from s3filter.op.join_expression import JoinExpression
from s3filter.op.predicate_expression import PredicateExpression
from s3filter.op.project import Project, ProjectExpression
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.op.sql_table_scan_bloom_use import SQLTableScanBloomUse
from s3filter.query.tpch import get_file_key


def collate_op(name, query_plan):
    return Collate(name, query_plan, False)


def project_avg_yearly_op(name, query_plan):
    """with extendedprice_sum_aggregate_project as (
        select l_extendedprice / 7.0 as avg_yearly from extendedprice_sum_aggregate
    )

    :return:
    """

    return Project(
        [
            ProjectExpression(lambda t_: t_['_0'] / 7.0, 'avg_yearly')
        ],
        name, query_plan,
        False)


def project_orderkey_partkey_quantity_extendedprice_op(name, query_plan):
    return Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'l_orderkey'),
            ProjectExpression(lambda t_: t_['_1'], 'l_partkey'),
            ProjectExpression(lambda t_: t_['_2'], 'l_quantity'),
            ProjectExpression(lambda t_: t_['_3'], 'l_extendedprice')
        ],
        name, query_plan,
        False)


def project_partkey_op(name, query_plan):
    """with part_scan_project as (select _0 as p_partkey from part_scan)

    :param query_plan:
    :param name:
    :return:
    """

    def fn(df):
        # return df[['_0', '_1', '_2']]

        df = df.filter(items=['_0'], axis=1)

        df.rename(columns={'_0': 'p_partkey'}, inplace=True)

        return df

    return Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'p_partkey')
        ],
        name, query_plan,
        True, fn)


def project_lineitem_orderkey_partkey_quantity_extendedprice_op(name, query_plan):
    """with part_project as (select _0 as p_partkey from part_scan)

    :param query_plan:
    :param name:
    :return:
    """
    return Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'l_orderkey'),
            ProjectExpression(lambda t_: t_['_1'], 'l_partkey'),
            ProjectExpression(lambda t_: t_['_4'], 'l_quantity'),
            ProjectExpression(lambda t_: t_['_5'], 'l_extendedprice')
        ],
        name, query_plan,
        False)


def project_lineitem_filtered_orderkey_partkey_quantity_extendedprice_op(name, query_plan):
    """with part_project as (select _0 as p_partkey from part_scan)

    :param query_plan:
    :param name:
    :return:
    """

    def fn(df):
        # return df[['_0', '_1', '_2']]

        df = df.filter(items=['_0', '_1', '_2', '_3'], axis=1)

        df.rename(columns={'_0': 'l_orderkey', '_1': 'l_partkey', '_2': 'l_quantity', '_3': 'l_extendedprice'}, inplace=True)

        return df

    return Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'l_orderkey'),
            ProjectExpression(lambda t_: t_['_1'], 'l_partkey'),
            ProjectExpression(lambda t_: t_['_2'], 'l_quantity'),
            ProjectExpression(lambda t_: t_['_3'], 'l_extendedprice')
        ],
        name, query_plan,
        True, fn)


def project_partkey_brand_container_op(name, query_plan):
    """with part_project as (select _0 as p_partkey from part_scan)

    :param query_plan:
    :param name:
    :return:
    """
    return Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'p_partkey'),
            ProjectExpression(lambda t_: t_['_3'], 'p_brand'),
            ProjectExpression(lambda t_: t_['_6'], 'p_container')
        ],
        name, query_plan,
        False)


def project_partkey_avg_quantity_op(name, query_plan):
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
        name, query_plan,
        True)


def aggregate_sum_extendedprice_op(name, query_plan):
    """with extendedprice_sum_aggregate as (
        select sum(l_extendedprice) from filter_join_2
    )

    :param query_plan:
    :param name:
    :return:
    """
    return Aggregate([AggregateExpression(AggregateExpression.SUM, lambda t_: float(t_['l_extendedprice']))],
                     name, query_plan, True)


def filter_lineitem_quantity_op(name, query_plan):
    """with filter_join_2 as (select * from part_lineitem_join_avg_group_join where l_quantity < avg_l_quantity_computed00)

    :param query_plan:
    :param name:
    :return:
    """
    return Filter(PredicateExpression(lambda t_: float(t_['l_quantity']) < t_['avg_l_quantity_computed00']),
                  name, query_plan, False)


def filter_brand_container_op(name, query_plan):
    return Filter(
        PredicateExpression(lambda t_: t_['p_brand'] == 'Brand#41' and t_['p_container'] == 'SM PACK'),
        name, query_plan,
        False)


def join_l_partkey_p_partkey_op(name, query_plan):
    """with part_lineitem_join_avg_group_join as (
    select * from part_lineitem_join, lineitem_part_avg_group_project where p_partkey = l_partkey
    )

    :return:
    """
    return HashJoin(JoinExpression('l_partkey', 'p_partkey'), name, query_plan, False)


def join_p_partkey_l_partkey_op(name, query_plan):
    """with part_lineitem_join as (select * from part_scan, lineitem_scan where p_partkey = l_partkey)

    :param query_plan:
    :param name:
    :return:
    """
    return HashJoin(JoinExpression('p_partkey', 'l_partkey'), name, query_plan, False)


def group_partkey_avg_quantity_op(name, query_plan):
    """with lineitem_part_avg_group as (select avg(l_quantity) from part_lineitem_join group by l_partkey)

    :param query_plan:
    :param name:
    :return:
    """
    return Group(
        ['l_partkey'],  # l_partkey
        [
            # avg(l_quantity)
            # AggregateExpression(AggregateExpression.AVG, lambda t_: float(t_['l_quantity']))
            AggregateExpression(AggregateExpression.AVG, lambda t_: float(t_[3]))
        ],
        name, query_plan,
        True)


def sql_scan_lineitem_select_all_op(name, query_plan):
    return SQLTableScan('lineitem.csv',
                        "select "
                        "  * "
                        "from "
                        "  S3Object ", False,
                        name, query_plan,
                        False)


def sql_scan_part_select_all_op(name, query_plan):
    return SQLTableScan('part.csv',
                        "select "
                        "  * "
                        "from "
                        "  S3Object ", False,
                        name, query_plan,
                        False)


def sql_scan_lineitem_select_all_where_partkey_op(name, query_plan):
    """with lineitem_scan as (select * from lineitem)

    :param query_plan:
    :param name:
    :return:
    """
    return SQLTableScan('lineitem.csv',
                        "select "
                        "  * "
                        "from "
                        "  S3Object "
                        "where "
                        "  l_partkey = '182405' ", False,
                        name, query_plan,
                        False)


def sql_scan_part_select_all_where_brand_and_container_op(name, query_plan):
    """with part_scan as (select * from part)

    :param query_plan:
    :param name:
    :return:
    """
    return SQLTableScan('part.csv',
                        "select "
                        "  * "
                        "from "
                        "  S3Object "
                        "where "
                        "  p_brand = 'Brand#41' and "
                        "  p_container = 'SM PACK' ", False,
                        name, query_plan,
                        False)


def sql_scan_lineitem_select_orderkey_partkey_quantity_extendedprice_where_partkey_op(sharded, shard, num_shards, use_pandas, name, query_plan):
    """with lineitem_scan as (select * from lineitem)

    :param query_plan:
    :param name:
    :return:
    """
    return SQLTableScan(get_file_key('lineitem', sharded, shard),
                        "select "
                        "  l_orderkey, l_partkey, l_quantity, l_extendedprice "
                        "from "
                        "  S3Object "
                        "where "
                        "  l_partkey = '182405' ",
                        use_pandas,
                        name,
                        query_plan,
                        True)


def sql_scan_lineitem_select_orderkey_partkey_quantity_extendedprice(sharded, shard, num_shards, use_pandas, name,
                                                                     query_plan):
    """with lineitem_scan as (select * from lineitem)

    :param query_plan:
    :param name:
    :return:
    """

    return SQLTableScan(get_file_key('lineitem', sharded, shard),
                        "select "
                        "  l_orderkey, l_partkey, l_quantity, l_extendedprice "
                        "from "
                        "  S3Object ",
                        # "where "
                        # "  l_partkey = '182405' or l_partkey = '198'",
                        use_pandas,
                        name,
                        query_plan,
                        True)


def sql_scan_select_partkey_where_brand_container_op(sharded, shard, num_shards, use_pandas, name, query_plan):
    """with part_scan as (select * from part)

    :param query_plan:
    :param name:
    :return:
    """

    key_lower = math.ceil((200000.0 / float(num_shards)) * shard)
    key_upper = math.ceil((200000.0 / float(num_shards)) * (shard + 1))

    return SQLTableScan(get_file_key('part', sharded, shard),
                        "select "
                        "  p_partkey "
                        "from "
                        "  S3Object "
                        "where "
                        "  p_brand = 'Brand#41' and "
                        "  p_container = 'SM PACK' and "
                        "  cast(p_partkey as int) >= {} and cast(p_partkey as int) < {} "
                        .format(key_lower, key_upper),
                        use_pandas,
                        name, query_plan,
                        True)


def bloom_scan_lineitem_select_orderkey_partkey_quantity_extendedprice_where_partkey_bloom_partkey_op(use_pandas, name,
                                                                                                      query_plan):
    return SQLTableScanBloomUse('lineitem.csv',
                                "select "
                                "  l_orderkey, l_partkey, l_quantity, l_extendedprice "
                                "from "
                                "  S3Object "
                                "where "
                                "  l_partkey = '182405' ",
                                'l_partkey', use_pandas,
                                name, query_plan,
                                False)


def bloom_scan_lineitem_select_orderkey_partkey_quantity_extendedprice_bloom_partkey_op(name, query_plan):
    return SQLTableScanBloomUse('lineitem.csv',
                                "select "
                                "  l_orderkey, l_partkey, l_quantity, l_extendedprice "
                                "from "
                                "  S3Object ",
                                'l_partkey', False,
                                name, query_plan,
                                False)


def bloom_create_partkey_op(name, query_plan):
    return BloomCreate('p_partkey', name, query_plan, False)
