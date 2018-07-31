"""Reusable query elements for tpch q19

"""

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


def collate_op(query_plan):
    return Collate('collate', query_plan, False)


def aggregate_def(query_plan):
    return Aggregate(
        [
            AggregateExpression(
                AggregateExpression.SUM,
                lambda t_: float(t_['l_extendedprice']) * float((1 - float(t_['l_discount']))))
        ],
        'aggregate',
        query_plan, False)


def join_op(query_plan):
    return HashJoin(JoinExpression('l_partkey', 'p_partkey'), 'lineitem_part_join', query_plan, False)


def aggregate_project_def(query_plan):
    return Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'revenue')
        ],
        'aggregate_project',
        query_plan, False)


def filter_def(query_plan):
    return Filter(
        PredicateExpression(lambda t_:
                            (
                                    t_['p_brand'] == 'Brand#11' and
                                    t_['p_container'] in ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'] and
                                    3 <= int(t_['l_quantity']) <= 3 + 10 and
                                    1 <= int(t_['p_size']) <= 5 and
                                    t_['l_shipmode'] in ['AIR', 'AIR REG'] and
                                    t_['l_shipinstruct'] == 'DELIVER IN PERSON'
                            ) or (
                                    t_['p_brand'] == 'Brand#44' and
                                    t_['p_container'] in ['MED BAG', 'MED BOX', 'MED PACK', 'MED PKG'] and
                                    16 <= int(t_['l_quantity']) <= 16 + 10 and
                                    1 <= int(t_['p_size']) <= 10 and
                                    t_['l_shipmode'] in ['AIR', 'AIR REG'] and
                                    t_['l_shipinstruct'] == 'DELIVER IN PERSON'
                            ) or (
                                    t_['p_brand'] == 'Brand#53' and
                                    t_['p_container'] in ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'] and
                                    24 <= int(t_['l_quantity']) <= 24 + 10 and
                                    1 <= int(t_['p_size']) <= 15 and
                                    t_['l_shipmode'] in ['AIR', 'AIR REG'] and
                                    t_['l_shipinstruct'] == 'DELIVER IN PERSON'
                            )),
        'filter',
        query_plan, False)


def project_partkey_brand_size_container_op(query_plan):
    return Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'p_partkey'),
            ProjectExpression(lambda t_: t_['_3'], 'p_brand'),
            ProjectExpression(lambda t_: t_['_5'], 'p_size'),
            ProjectExpression(lambda t_: t_['_6'], 'p_container')
        ],
        'part_project',
        query_plan, False)


def project_partkey_quantity_extendedprice_discount_shipinstruct_shipmode_op(query_plan):
    return Project(
        [
            ProjectExpression(lambda t_: t_['_1'], 'l_partkey'),
            ProjectExpression(lambda t_: t_['_4'], 'l_quantity'),
            ProjectExpression(lambda t_: t_['_5'], 'l_extendedprice'),
            ProjectExpression(lambda t_: t_['_6'], 'l_discount'),
            ProjectExpression(lambda t_: t_['_13'], 'l_shipinstruct'),
            ProjectExpression(lambda t_: t_['_14'], 'l_shipmode')
        ],
        'lineitem_project',
        query_plan, False)


def sql_scan_part_select_all_where_partkey_op(query_plan):
    return SQLTableScan('part.csv',
                        "select "
                        "  * "
                        "from "
                        "  S3Object "
                        "where "
                        "  p_partkey = '103853' or "
                        "  p_partkey = '104277' or "
                        "  p_partkey = '104744' ",
                        'part_scan',
                        query_plan, False)


def sql_scan_lineitem_select_all_where_partkey_op(query_plan):
    return SQLTableScan('lineitem.csv',
                        "select "
                        "  * "
                        "from "
                        "  S3Object "
                        "where "
                        "  l_partkey = '103853' or "
                        "  l_partkey = '104277' or "
                        "  l_partkey = '104744' ",
                        'lineitem_scan',
                        query_plan, False)


def sql_scan_part_select_all_op(query_plan):
    return SQLTableScan('part.csv',
                        "select "
                        "  * "
                        "from "
                        "  S3Object ",
                        'part_scan',
                        query_plan, False)


def sql_scan_lineitem_select_all_op(query_plan):
    return SQLTableScan('lineitem.csv',
                        "select "
                        "  * "
                        "from "
                        "  S3Object ",
                        'lineitem_scan',
                        query_plan, False)


def bloom_scan_partkey_quantity_extendedprice_discount_shipinstruct_shipmode_where_extra_filtered_op(use_pandas, query_plan):
    return SQLTableScanBloomUse('lineitem.csv',
                                "select "
                                "  l_partkey, "
                                "  l_quantity, "
                                "  l_extendedprice, "
                                "  l_discount, "
                                "  l_shipinstruct, "
                                "  l_shipmode "
                                "from "
                                "  S3Object "
                                "where "
                                "  l_partkey = '103853' or "
                                "  l_partkey = '104277' or "
                                "  l_partkey = '104744' and "
                                "  ( "
                                "      ( "
                                "          cast(l_quantity as integer) >= 3 "
                                "          and cast(l_quantity as integer) <= 3 + 10 "
                                "          and l_shipmode in ('AIR', 'AIR REG') "
                                "          and l_shipinstruct = 'DELIVER IN PERSON' "
                                "      ) "
                                "      or "
                                "      ( "
                                "          cast(l_quantity as integer) >= 16 "
                                "          and cast(l_quantity as integer) <= 16 + 10 "
                                "          and l_shipmode in ('AIR', 'AIR REG') "
                                "          and l_shipinstruct = 'DELIVER IN PERSON' "
                                "      ) "
                                "      or "
                                "      ( "
                                "          cast(l_quantity as integer) >= 24 "
                                "          and cast(l_quantity as integer) <= 24 + 10 "
                                "          and l_shipmode in ('AIR', 'AIR REG') "
                                "          and l_shipinstruct = 'DELIVER IN PERSON' "
                                "      ) "
                                "  ) ",
                                'l_partkey', use_pandas,
                                'lineitem_bloom_use',
                                query_plan, False)


def bloom_scan_partkey_quantity_extendedprice_discount_shipinstruct_shipmode_where_filtered_op(query_plan):
    return SQLTableScanBloomUse('lineitem.csv',
                                "select "
                                "  l_partkey, "
                                "  l_quantity, "
                                "  l_extendedprice, "
                                "  l_discount, "
                                "  l_shipinstruct, "
                                "  l_shipmode "
                                "from "
                                "  S3Object "
                                "where "
                                "  ( "
                                "      ( "
                                "          cast(l_quantity as integer) >= 3 "
                                "          and cast(l_quantity as integer) <= 3 + 10 "
                                "          and l_shipmode in ('AIR', 'AIR REG') "
                                "          and l_shipinstruct = 'DELIVER IN PERSON' "
                                "      ) "
                                "      or "
                                "      ( "
                                "          cast(l_quantity as integer) >= 16 "
                                "          and cast(l_quantity as integer) <= 16 + 10 "
                                "          and l_shipmode in ('AIR', 'AIR REG') "
                                "          and l_shipinstruct = 'DELIVER IN PERSON' "
                                "      ) "
                                "      or "
                                "      ( "
                                "          cast(l_quantity as integer) >= 24 "
                                "          and cast(l_quantity as integer) <= 24 + 10 "
                                "          and l_shipmode in ('AIR', 'AIR REG') "
                                "          and l_shipinstruct = 'DELIVER IN PERSON' "
                                "      ) "
                                "  ) ",
                                'l_partkey',
                                'lineitem_bloom_use',
                                query_plan, False)


def sql_scan_lineitem_select_partkey_quantity_extendedprice_discount_shipinstruct_shipmode_where_extra_filtered_op(
        query_plan):
    return SQLTableScan('lineitem.csv',
                        "select "
                        "  l_partkey, "
                        "  l_quantity, "
                        "  l_extendedprice, "
                        "  l_discount, "
                        "  l_shipinstruct, "
                        "  l_shipmode "
                        "from "
                        "  S3Object "
                        "where "
                        "  l_partkey = '103853' or "
                        "  l_partkey = '104277' or "
                        "  l_partkey = '104744' and "
                        "  ( "
                        "      ( "
                        "          cast(l_quantity as integer) >= 3 and cast(l_quantity as integer) <= 3 + 10 "
                        "          and l_shipmode in ('AIR', 'AIR REG') "
                        "          and l_shipinstruct = 'DELIVER IN PERSON' "
                        "      ) "
                        "      or "
                        "      ( "
                        "          cast(l_quantity as integer) >= 16 and cast(l_quantity as integer) <= 16 + 10 "
                        "          and l_shipmode in ('AIR', 'AIR REG') "
                        "          and l_shipinstruct = 'DELIVER IN PERSON' "
                        "      ) "
                        "      or "
                        "      ( "
                        "          cast(l_quantity as integer) >= 24 and cast(l_quantity as integer) <= 24 + 10 "
                        "          and l_shipmode in ('AIR', 'AIR REG') "
                        "          and l_shipinstruct = 'DELIVER IN PERSON' "
                        "      ) "
                        "  ) ",
                        'lineitem_scan',
                        query_plan, False)


def sql_scan_lineitem_select_partkey_quantity_extendedprice_discount_shipinstruct_shipmode_where_filtered_op(
        query_plan):
    return SQLTableScan('lineitem.csv',
                        "select "
                        "  l_partkey, "
                        "  l_quantity, "
                        "  l_extendedprice, "
                        "  l_discount, "
                        "  l_shipinstruct, "
                        "  l_shipmode "
                        "from "
                        "  S3Object "
                        "where "
                        "  ( "
                        "      ( "
                        "          cast(l_quantity as integer) >= 3 and cast(l_quantity as integer) <= 3 + 10 "
                        "          and l_shipmode in ('AIR', 'AIR REG') "
                        "          and l_shipinstruct = 'DELIVER IN PERSON' "
                        "      ) "
                        "      or "
                        "      ( "
                        "          cast(l_quantity as integer) >= 16 and cast(l_quantity as integer) <= 16 + 10 "
                        "          and l_shipmode in ('AIR', 'AIR REG') "
                        "          and l_shipinstruct = 'DELIVER IN PERSON' "
                        "      ) "
                        "      or "
                        "      ( "
                        "          cast(l_quantity as integer) >= 24 and cast(l_quantity as integer) <= 24 + 10 "
                        "          and l_shipmode in ('AIR', 'AIR REG') "
                        "          and l_shipinstruct = 'DELIVER IN PERSON' "
                        "      ) "
                        "  ) ",
                        'lineitem_scan',
                        query_plan, False)


def bloom_create_partkey_op(query_plan):
    return BloomCreate('p_partkey', 'part_bloom_create', query_plan, False)


def project_partkey_brand_size_container_filtered_op(query_plan):
    return Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'p_partkey'),
            ProjectExpression(lambda t_: t_['_1'], 'p_brand'),
            ProjectExpression(lambda t_: t_['_2'], 'p_size'),
            ProjectExpression(lambda t_: t_['_3'], 'p_container')
        ],
        'part_project',
        query_plan, False)


def project_partkey_quantity_extendedprice_discount_shipinstruct_shipmode_filtered_op(query_plan):
    return Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'l_partkey'),
            ProjectExpression(lambda t_: t_['_1'], 'l_quantity'),
            ProjectExpression(lambda t_: t_['_2'], 'l_extendedprice'),
            ProjectExpression(lambda t_: t_['_3'], 'l_discount'),
            ProjectExpression(lambda t_: t_['_4'], 'l_shipinstruct'),
            ProjectExpression(lambda t_: t_['_5'], 'l_shipmode')
        ],
        'lineitem_project',
        query_plan, False)


def sql_scan_part_partkey_brand_size_container_where_filtered_op(query_plan):
    return SQLTableScan('part.csv',
                        "select "
                        "  p_partkey, "
                        "  p_brand, "
                        "  p_size, "
                        "  p_container "
                        "from "
                        "  S3Object "
                        "where "
                        "  ( "
                        "      ( "
                        "          p_brand = 'Brand#11' "
                        "          and p_container in ("
                        "              'SM CASE', "
                        "              'SM BOX', "
                        "              'SM PACK', "
                        "              'SM PKG'"
                        "          ) "
                        "          and cast(p_size as integer) between 1 and 5 "
                        "      ) "
                        "      or "
                        "      ( "
                        "          p_brand = 'Brand#44' "
                        "          and p_container in ("
                        "              'MED BAG', "
                        "              'MED BOX', "
                        "              'MED PKG', "
                        "              'MED PACK'"
                        "          ) "
                        "          and cast(p_size as integer) between 1 and 10 "
                        "      ) "
                        "      or "
                        "      ( "
                        "          p_brand = 'Brand#53' "
                        "          and p_container in ("
                        "              'LG CASE', "
                        "              'LG BOX', "
                        "              'LG PACK', "
                        "              'LG PKG'"
                        "          ) "
                        "          and cast(p_size as integer) between 1 and 15 "
                        "      ) "
                        "  ) ",
                        'part_scan',
                        query_plan, False)


def sql_scan_part_partkey_brand_size_container_where_extra_filtered_op(query_plan):
    return SQLTableScan('part.csv',
                        "select "
                        "  p_partkey, "
                        "  p_brand, "
                        "  p_size, "
                        "  p_container "
                        "from "
                        "  S3Object "
                        "where "
                        "  p_partkey = '103853' or "
                        "  p_partkey = '104277' or "
                        "  p_partkey = '104744' and "
                        "  ( "
                        "      ( "
                        "          p_brand = 'Brand#11' "
                        "          and p_container in ("
                        "              'SM CASE', "
                        "              'SM BOX', "
                        "              'SM PACK', "
                        "              'SM PKG'"
                        "          ) "
                        "          and cast(p_size as integer) between 1 and 5 "
                        "      ) "
                        "      or "
                        "      ( "
                        "          p_brand = 'Brand#44' "
                        "          and p_container in ("
                        "              'MED BAG', "
                        "              'MED BOX', "
                        "              'MED PKG', "
                        "              'MED PACK'"
                        "          ) "
                        "          and cast(p_size as integer) between 1 and 10 "
                        "      ) "
                        "      or "
                        "      ( "
                        "          p_brand = 'Brand#53' "
                        "          and p_container in ("
                        "              'LG CASE', "
                        "              'LG BOX', "
                        "              'LG PACK', "
                        "              'LG PKG'"
                        "          ) "
                        "          and cast(p_size as integer) between 1 and 15 "
                        "      ) "
                        "  ) ",
                        'part_scan',
                        query_plan, False)
