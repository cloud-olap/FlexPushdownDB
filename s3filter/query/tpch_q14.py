"""Reusable query elements for tpch q14

"""
import math
import re

from s3filter.op.aggregate import Aggregate
from s3filter.op.aggregate_expression import AggregateExpression
from s3filter.op.bloom_create import BloomCreate
from s3filter.op.collate import Collate
from s3filter.op.filter import Filter
from s3filter.op.hash_join import HashJoin
from s3filter.op.join_expression import JoinExpression
from s3filter.op.predicate_expression import PredicateExpression
from s3filter.op.project import Project, ProjectExpression
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.op.sql_table_scan_bloom_use import SQLTableScanBloomUse
from s3filter.plan.query_plan import QueryPlan
from s3filter.sql.function import cast, timestamp


def filter_brand12_operator_def(name, query_plan):
    # type: (str) -> Filter
    return Filter(
        PredicateExpression(lambda t_: t_['p_brand'] == 'Brand#12'),
        name, query_plan,
        False)


def join_lineitem_part_operator_def(name, query_plan):
    # type: (str) -> HashJoin
    return HashJoin(
        JoinExpression('l_partkey', 'p_partkey'),
        name, query_plan,
        False)


def join_part_lineitem_operator_def(name, query_plan):
    # type: (str) -> HashJoin
    return HashJoin(
        JoinExpression('p_partkey', 'l_partkey'),
        name, query_plan,
        False)


def aggregate_promo_revenue_operator_def(name, query_plan):
    # type: (str) -> Aggregate
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

    return Aggregate(
        [
            AggregateExpression(AggregateExpression.SUM, ex1),
            AggregateExpression(AggregateExpression.SUM, ex2)
        ],
        name, query_plan,
        False)


def project_partkey_type_operator_def(name, query_plan):
    # type: (str) -> Project
    return Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'p_partkey'),
            ProjectExpression(lambda t_: t_['_1'], 'p_type')
        ],
        name, query_plan,
        False)


def project_partkey_extendedprice_discount_operator_def(name, query_plan):
    # type: (str) -> Project
    return Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'l_partkey'),
            ProjectExpression(lambda t_: t_['_1'], 'l_extendedprice'),
            ProjectExpression(lambda t_: t_['_2'], 'l_discount')
        ],
        name, query_plan,
        False)


def project_promo_revenue_operator_def(name, query_plan):
    # type: (str) -> Project
    return Project(
        [
            ProjectExpression(lambda t_: 100 * t_['_0'] / t_['_1'], 'promo_revenue')
        ],
        name, query_plan,
        False)


def sql_scan_part_partkey_type_part_where_brand12_operator_def(name, query_plan):
    # type: (str) -> SQLTableScan
    return SQLTableScan('part.csv',
                        "select "
                        "  p_partkey, p_type "
                        "from "
                        "  S3Object "
                        "where "
                        "  p_brand = 'Brand#12' "
                        " ",
                        name, query_plan,
                        False)


def sql_scan_part_partkey_type_part_where_brand12_partitioned_operator_def(part, parts, name, query_plan):
    # type: (int, int, str, QueryPlan) -> SQLTableScan

    key_lower = math.ceil((200000.0 / float(parts)) * part)
    key_upper = math.ceil((200000.0 / float(parts)) * (part + 1))

    return SQLTableScan('part.csv',
                        "select "
                        "  p_partkey, p_type "
                        "from "
                        "  S3Object "
                        "where "
                        "  p_brand = 'Brand#12' and "
                        "  cast(p_partkey as int) >= {} and cast(p_partkey as int) < {} "
                        " ".format(key_lower, key_upper),
                        name, query_plan,
                        False)


def sql_scan_part_partkey_where_brand12_operator_def(name, query_plan):
    # type: (str) -> SQLTableScan
    return SQLTableScan('part.csv',
                        "select "
                        "  p_partkey "
                        "from "
                        "  S3Object "
                        "where "
                        "  p_brand = 'Brand#12' "
                        " ",
                        name, query_plan,
                        False)


def sql_scan_part_operator_def(name, query_plan):
    return SQLTableScan('part.csv',
                        "select * from S3Object;",
                        name, query_plan,
                        False)


def sql_scan_lineitem_operator_def(name, query_plan):
    return SQLTableScan('lineitem.csv',
                        "select * from S3Object;",
                        name, query_plan,
                        False)


def sql_scan_lineitem_extra_filtered_operator_def(name, query_plan):
    return SQLTableScan('lineitem.csv',
                        "select "
                        "  * "
                        "from "
                        "  S3Object "
                        "where "
                        "  (l_orderkey = '18436' and l_partkey = '164584') or "
                        "  (l_orderkey = '18720' and l_partkey = '92764') or "
                        "  (l_orderkey = '12482' and l_partkey = '117405') or "
                        "  (l_orderkey = '27623' and l_partkey = '137010') or "
                        "  (l_orderkey = '10407' and l_partkey = '43275') or "
                        "  (l_orderkey = '17027' and l_partkey = '172729') or "
                        "  (l_orderkey = '23302' and l_partkey = '18523') or "
                        "  (l_orderkey = '27334' and l_partkey = '94308') or "
                        "  (l_orderkey = '15427' and l_partkey = '125586') or "
                        "  (l_orderkey = '11590' and l_partkey = '162359') or "
                        "  (l_orderkey = '2945' and l_partkey = '126197') or "
                        "  (l_orderkey = '15648' and l_partkey = '143904');",
                        name, query_plan,
                        False)


def sql_scan_lineitem_partkey_extendedprice_discount_where_shipdate_operator_def(min_shipped_date,
                                                                                 max_shipped_date,
                                                                                 name, query_plan):
    return SQLTableScan('lineitem.csv',
                        "select "
                        "  l_partkey, l_extendedprice, l_discount "
                        "from "
                        "  S3Object "
                        "where "
                        "  cast(l_shipdate as timestamp) >= cast(\'{}\' as timestamp) and "
                        "  cast(l_shipdate as timestamp) < cast(\'{}\' as timestamp) "
                        ";".format(min_shipped_date.strftime('%Y-%m-%d'),
                                   max_shipped_date.strftime('%Y-%m-%d')),
                        name, query_plan,
                        True)


def sql_scan_lineitem_partkey_extendedprice_discount_where_shipdate_sharded_operator_def(min_shipped_date,
                                                                                         max_shipped_date,
                                                                                         shard,
                                                                                         name,
                                                                                         query_plan):
    return SQLTableScan('sf1000-lineitem/lineitem_{}.csv'.format(shard),
                        "select "
                        "  l_partkey, l_extendedprice, l_discount "
                        "from "
                        "  S3Object "
                        "where "
                        "  cast(l_shipdate as timestamp) >= cast(\'{}\' as timestamp) and "
                        "  cast(l_shipdate as timestamp) < cast(\'{}\' as timestamp) "
                        ";".format(min_shipped_date.strftime('%Y-%m-%d'),
                                   max_shipped_date.strftime('%Y-%m-%d')),
                        name, query_plan,
                        False)


def sql_scan_lineitem_partkey_extendedprice_discount_where_shipdate_partitioned_operator_def(min_shipped_date,
                                                                                             max_shipped_date, part,
                                                                                             parts,
                                                                                             name, query_plan):
    key_lower = math.ceil((6000000.0 / float(parts)) * part)
    key_upper = math.ceil((6000000.0 / float(parts)) * (part + 1))

    return SQLTableScan('lineitem.csv',
                        "select "
                        "  l_partkey, l_extendedprice, l_discount "
                        "from "
                        "  S3Object "
                        "where "
                        "  cast(l_shipdate as timestamp) >= cast(\'{}\' as timestamp) and "
                        "  cast(l_shipdate as timestamp) < cast(\'{}\' as timestamp) and "
                        "  cast(l_orderkey as int) >= {} and cast(l_orderkey as int) < {} "
                        ";".format(min_shipped_date.strftime('%Y-%m-%d'),
                                   max_shipped_date.strftime('%Y-%m-%d'),
                                   key_lower,
                                   key_upper),
                        name, query_plan,
                        False)


def project_partkey_brand_type_operator_def(name, query_plan):
    return Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'p_partkey'),
            ProjectExpression(lambda t_: t_['_3'], 'p_brand'),
            ProjectExpression(lambda t_: t_['_4'], 'p_type')
        ],
        name, query_plan,
        False)


def filter_shipdate_operator_def(min_shipped_date, max_shipped_date, name, query_plan):
    return Filter(
        PredicateExpression(lambda t_:
                            (cast(t_['l_shipdate'], timestamp) >= cast(min_shipped_date, timestamp)) and
                            (cast(t_['l_shipdate'], timestamp) < cast(max_shipped_date, timestamp))),
        name, query_plan,
        False)


def bloom_create_l_partkey_operator_def(name, query_plan):
    return BloomCreate('l_partkey', name, query_plan, False)


def bloom_create_p_partkey_operator_def(name, query_plan):
    return BloomCreate('p_partkey', name, query_plan, False)


def project_l_partkey_operator_def(name, query_plan):
    return Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'l_partkey')
        ],
        name, query_plan,
        False)


def project_p_partkey_operator_def(name, query_plan):
    return Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'p_partkey')
        ],
        name, query_plan,
        False)


def bloom_scan_part_partkey_type_brand12_operator_def(name, query_plan):
    return SQLTableScanBloomUse('part.csv',
                                "select "
                                "  p_partkey, p_type "
                                "from "
                                "  S3Object "
                                "where "
                                "  p_brand = 'Brand#12' "
                                " ",
                                'p_partkey',
                                name, query_plan,
                                False)


def bloom_scan_lineitem_where_shipdate_operator_def(min_shipped_date, max_shipped_date, name, query_plan):
    return SQLTableScanBloomUse('lineitem.csv',
                                "select "
                                "  l_partkey, l_extendedprice, l_discount "
                                "from "
                                "  S3Object "
                                "where "
                                "  cast(l_shipdate as timestamp) >= cast(\'{}\' as timestamp) and "
                                "  cast(l_shipdate as timestamp) < cast(\'{}\' as timestamp) "
                                " ".format(
                                    min_shipped_date.strftime('%Y-%m-%d'),
                                    max_shipped_date.strftime('%Y-%m-%d'))
                                ,
                                'l_partkey',
                                name, query_plan,
                                False)


def bloom_scan_lineitem_where_shipdate_sharded_operator_def(min_shipped_date, max_shipped_date, part, parts, name,
                                                            query_plan):
    key_lower = math.ceil((6000000.0 / float(parts)) * part)
    key_upper = math.ceil((6000000.0 / float(parts)) * (part + 1))

    return SQLTableScanBloomUse('lineitem.csv',
                                "select "
                                "  l_partkey, l_extendedprice, l_discount "
                                "from "
                                "  S3Object "
                                "where "
                                "  cast(l_shipdate as timestamp) >= cast(\'{}\' as timestamp) and "
                                "  cast(l_shipdate as timestamp) < cast(\'{}\' as timestamp) and "
                                "  cast(l_orderkey as int) >= {} and cast(l_orderkey as int) < {} "
                                " ".format(
                                    min_shipped_date.strftime('%Y-%m-%d'),
                                    max_shipped_date.strftime('%Y-%m-%d'),
                                    key_lower,
                                    key_upper)
                                ,
                                'l_partkey',
                                name, query_plan,
                                False)


def bloom_scan_lineitem_partkey_where_shipdate_operator_def(min_shipped_date, max_shipped_date, name, query_plan):
    return SQLTableScanBloomUse('lineitem.csv',
                                "select "
                                "  l_partkey "
                                "from "
                                "  S3Object "
                                "where "
                                "  cast(l_shipdate as timestamp) >= cast(\'{}\' as timestamp) and "
                                "  cast(l_shipdate as timestamp) < cast(\'{}\' as timestamp) "
                                " ".format(
                                    min_shipped_date.strftime('%Y-%m-%d'),
                                    max_shipped_date.strftime('%Y-%m-%d'))
                                ,
                                'l_partkey',
                                name, query_plan,
                                False)


def collate_operator_def(name, query_plan):
    return Collate(name, query_plan, False)


def project_partkey_extendedprice_discount_shipdate_operator_def(name, query_plan):
    return Project(
        [
            ProjectExpression(lambda t_: t_['_1'], 'l_partkey'),
            ProjectExpression(lambda t_: t_['_5'], 'l_extendedprice'),
            ProjectExpression(lambda t_: t_['_6'], 'l_discount'),
            ProjectExpression(lambda t_: t_['_10'], 'l_shipdate')
        ],
        name, query_plan,
        False)
