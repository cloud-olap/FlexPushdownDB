# -*- coding: utf-8 -*-
"""TPC-H query tests

"""
import os
from datetime import datetime, timedelta

from s3filter import ROOT_DIR
from s3filter.op.aggregate_expression import AggregateExpression
from s3filter.op.collate import Collate
from s3filter.op.group import Group
from s3filter.op.sort import Sort, SortExpression
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.plan.query_plan import QueryPlan
from s3filter.util.test_util import gen_test_id


def test_group_baseline():
    """TPC-H Q1

    :return: None
    """

    # Query plan

    # select
    #   l_returnflag,
    #   l_linestatus,
    #   sum(l_quantity) as sum_qty,
    #   sum(l_extendedprice) as sum_base_price,
    #   sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    #   sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    #   avg(l_quantity) as avg_qty,
    #   avg(l_extendedprice) as avg_price,
    #   avg(l_discount) as avg_disc,
    #   count(*) as count_order
    # from
    #   lineitem
    # where
    #   l_shipdate <= date '1992-04-01' - interval '[DELTA]' day(3)
    # group by
    #   l_returnflag,
    #   l_linestatus
    # order by
    #   l_returnflag,
    #   l_linestatus;

    query_plan = QueryPlan()

    delta_days = 60  # TODO: This is supposed to be randomized I think
    shipped_date = datetime.strptime('1992-04-01', '%Y-%m-%d') - timedelta(days=delta_days)

    ts = query_plan.add_operator(SQLTableScan("lineitem.csv",
                                              "select * from S3Object "
                                              "where cast(l_shipdate as timestamp) <= cast(\'{}\' as timestamp)"
                                              .format(shipped_date.strftime('%Y-%m-%d')),
                                              'lineitem',
                                              False))
    g = query_plan.add_operator(Group(
        [
            '_8',  # l_returnflag
            '_9'  # l_linestatus
        ],
        [
            # sum(l_quantity)
            AggregateExpression(AggregateExpression.SUM, lambda t_: float(t_['_4'])),
            # sum(l_extendedprice) as sum_base_price
            AggregateExpression(AggregateExpression.SUM, lambda t_: float(t_['_5'])),
            # sum(l_extendedprice * (1 - l_discount)) as sum_disc_price
            AggregateExpression(AggregateExpression.SUM, lambda t_: float(t_['_5']) * (1 - float(t_['_6']))),
            # sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge
            AggregateExpression(AggregateExpression.SUM,
                                lambda t_: float(t_['_5']) * (1 - float(t_['_6'])) * (1 + float(t_['_7']))),
            # avg(l_quantity)
            AggregateExpression(AggregateExpression.AVG, lambda t_: float(t_['_4'])),
            # avg(l_extendedprice)
            AggregateExpression(AggregateExpression.AVG, lambda t_: float(t_['_5'])),
            # avg(l_discount)
            AggregateExpression(AggregateExpression.AVG, lambda t_: float(t_['_6'])),
            # count(*) as count_order
            AggregateExpression(AggregateExpression.COUNT, lambda t_: t_['_0'])
        ],
        'lineitem_grouped',
        False))
    s = query_plan.add_operator(Sort(
        [
            SortExpression('_0', str, 'ASC'),
            SortExpression('_1', str, 'ASC')
        ],
        'lineitem_group_sorted',
        False))
    c = query_plan.add_operator(Collate('collation', False))

    ts.connect(g)
    g.connect(s)
    s.connect(c)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    query_plan.execute()

    # Assert the results
    # num_rows = 0
    # for t in c.tuples():
    #     num_rows += 1
    #     print("{}:{}".format(num_rows, t))

    field_names = ['_0', '_1', '_2', '_3', '_4', '_5', '_6', '_7', '_8', '_9']

    assert c.tuples()[0] == field_names

    assert len(c.tuples()) == 2 + 1

    # These have been verified in Postgres
    assert c.tuples()[1] == ["A", "F", 129850, 194216048.19000033, 184525343.78730044, 191943492.96455324,
                             25.445816186556925,
                             38059.19031746035, 0.050005878894768374, 5103]
    assert c.tuples()[2] == ["R", "F", 129740, 193438367.33999985, 183701990.7670003, 191045646.36937532,
                             25.509241053873353,
                             38033.49731419579, 0.05061541486433399, 5086]

    # Write the metrics
    query_plan.print_metrics()
