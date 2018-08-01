"""Reusable query elements for tpch q1

"""
import math
import re

from s3filter.op.aggregate import Aggregate
from s3filter.op.aggregate_expression import AggregateExpression
from s3filter.op.collate import Collate
from s3filter.op.filter import Filter
from s3filter.op.predicate_expression import PredicateExpression
from s3filter.op.project import Project, ProjectExpression
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.op.group import Group
from s3filter.plan.query_plan import QueryPlan
from s3filter.sql.function import cast, timestamp
from s3filter.query.tpch import get_file_key
import pandas as pd

def sql_scan_lineitem_operator_def(sharded, shard, use_pandas, name, query_plan):
    return SQLTableScan(get_file_key('lineitem', sharded, shard),
                        "select * from S3Object;", use_pandas,
                        name, query_plan,
                        True)

def project_lineitem_operator_def(name, query_plan):
    # type: (str, QueryPlan) -> Project
    def fn(df):
        df = df.filter(items=['_4', '_5', '_6', '_7', '_8', '_9', '_10'], axis=1)
        df.rename(columns={'_4': 'l_quantity', '_5': 'l_extendedprice', '_6': 'l_discount', '_7' : 'l_tax',
                           '_8': 'l_returnflag', '_9': 'l_linestatus', '_10': 'l_shipdate'}, inplace=True)
        return df

    return Project(
        [
    	    ProjectExpression(lambda t_: t_['_4'], 'l_quantity'),
            ProjectExpression(lambda t_: t_['_5'], 'l_extendedprice'),
      	    ProjectExpression(lambda t_: t_['_6'], 'l_discount'),
      	    ProjectExpression(lambda t_: t_['_7'], 'l_tax'),
    	    ProjectExpression(lambda t_: t_['_8'], 'l_returnflag'),
    	    ProjectExpression(lambda t_: t_['_9'], 'l_linestatus'),
      	    ProjectExpression(lambda t_: t_['_10'], 'l_shipdate'),
        ],
        name, query_plan, False, fn)

def filter_shipdate_operator_def(max_shipped_date, name, query_plan):
    def pd_expr(df):
        return pd.to_datetime(df['l_shipdate']) <= max_shipped_date
    
    return Filter(
        PredicateExpression(lambda t_:
                            (cast(t_['l_shipdate'], timestamp) <= cast(max_shipped_date, timestamp)),
                            pd_expr),
        name, query_plan,
        False)

def groupby_returnflag_linestatus_operator_def(name, query_plan):
    def fn(df):
        d = [
            pd.to_numeric(df['l_quantity']).sum(),
            pd.to_numeric(df['l_extendedprice']).sum(),
            (pd.to_numeric(df['l_extendedprice']) * (1 - pd.to_numeric(df['l_discount']))).sum(),
            (pd.to_numeric(df['l_extendedprice']) * (1-pd.to_numeric(df['l_quantity'])) * (1+pd.to_numeric(df['l_tax']))).sum(),
            pd.to_numeric(df['l_quantity']).sum(),
            pd.to_numeric(df['l_extendedprice']).sum(),
            pd.to_numeric(df['l_discount']).sum(),
            pd.to_numeric(df['__count']).sum(),
        ]
        names = [
            'sum_qty',
            'sum_base_price',
            'sum_disc_price',
            'sum_charge',
            'avg_qty',
            'avg_price',
            'avg_disc',
            'count_order'
        ]
        return pd.Series(d, index=names)

    return Group(
        [
	        'l_returnflag',
        	'l_linestatus'
        ],
        [
            #    0              1                  2             3        4               5               6
            #    ['l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate']
            # sum(l_quantity)
            AggregateExpression(AggregateExpression.SUM, lambda t_: float(t_[0]), 'sum_qty'),
            # sum(l_extendedprice) as sum_base_price
            AggregateExpression(AggregateExpression.SUM, lambda t_: float(t_[1]), 'sum_base_price'),
            # sum(l_extendedprice * (1 - l_discount)) as sum_disc_price
            AggregateExpression(AggregateExpression.SUM, lambda t_: float(t_[1]) * (1 - float(t_[2])), 'sum_disc_price'),
            # sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge
            AggregateExpression(AggregateExpression.SUM,
                                lambda t_: float(t_[1]) * (1 - float(t_[2])) * (1 + float(t_[3])), 'sum_charge'),
            # avg(l_quantity)
            AggregateExpression(AggregateExpression.AVG, lambda t_: float(t_[0]), 'avg_qty'),
            # avg(l_extendedprice)
            AggregateExpression(AggregateExpression.AVG, lambda t_: float(t_[1]), 'avg_price'),
            # avg(l_discount)
            AggregateExpression(AggregateExpression.AVG, lambda t_: float(t_[2]), 'avg_disc'),
            # count(*) as count_order
            AggregateExpression(AggregateExpression.COUNT, lambda t_: t_[4], 'count_order')
        ],
        name, query_plan, False, fn)

def groupby_reduce_returnflag_linestatus_operator_def(name, query_plan, use_pandas):
    def fn(df):
        d = [
            pd.to_numeric(df['sum_qty']).sum(),
            pd.to_numeric(df['sum_base_price']).sum(),
            pd.to_numeric(df['sum_disc_price']).sum(),
            pd.to_numeric(df['sum_charge']).sum(),
            pd.to_numeric(df['avg_qty']).sum(),
            pd.to_numeric(df['avg_price']).sum(),
            pd.to_numeric(df['avg_disc']).sum(),
            pd.to_numeric(df['count_order']).sum(),
        ]
        names = [
            'sum_qty',
            'sum_base_price',
            'sum_disc_price',
            'sum_charge',
            'avg_qty',
            'avg_price',
            'avg_disc',
            'count_order'
        ]
        return pd.Series(d, index=names)
    
    return Group(
	    ['l_returnflag', 'l_linestatus'] if use_pandas else ['_0', '_1'],
        [
            AggregateExpression(AggregateExpression.SUM, lambda t_: float(t_[2]), 'sum_qty'),
            AggregateExpression(AggregateExpression.SUM, lambda t_: float(t_[3]), 'sum_base_price'),
            AggregateExpression(AggregateExpression.SUM, lambda t_: float(t_[4]), 'sum_disc_price'),
            AggregateExpression(AggregateExpression.SUM, lambda t_: float(t_[5]), 'sum_charge'),
            AggregateExpression(AggregateExpression.SUM, lambda t_: float(t_[6]), 'avg_qty'),
            AggregateExpression(AggregateExpression.SUM, lambda t_: float(t_[7]), 'avg_price'),
            AggregateExpression(AggregateExpression.SUM, lambda t_: float(t_[8]), 'avg_disc'),
            AggregateExpression(AggregateExpression.SUM, lambda t_: float(t_[9]), '__count'),

        ],
        name, query_plan, False, fn)
"""
def project_output_operator_def(name, query_plan):
    # type: (str, QueryPlan) -> Project
    return Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'sum_qty'),
            ProjectExpression(lambda t_: t_['_1'], 'sum_base_price'),
            ProjectExpression(lambda t_: t_['_2'], 'sum_disc_price'),
            ProjectExpression(lambda t_: t_['_3'], 'sum_charge'),
            ProjectExpression(lambda t_: t_['_4'], 'avg_qty'),
            ProjectExpression(lambda t_: t_['_5'], 'avg_price'),
            ProjectExpression(lambda t_: t_['_6'], 'avg_disc'),
            ProjectExpression(lambda t_: t_['_7'], 'count_order'),
        ],
        name, query_plan,
        False)
"""
