# -*- coding: utf-8 -*-
"""TPC-H query tests

"""

from datetime import datetime, timedelta
from op.aggregate_expression import AggregateExpression
from op.collate import Collate
from op.group import Group
from op.sort import Sort, SortExpression
from op.sql_table_scan import SQLTableScan
from op.tuple import LabelledTuple
from plan.query_plan import QueryPlan
from sql.function import sum_fn, avg_fn, count_fn
from util.test_util import gen_test_id


def test_group_baseline():
    """TPC-H Q1

    :return: None
    """

    num_rows = 0

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

    query_plan = QueryPlan("TPCH Q1 Test")

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
    query_plan.write_graph(gen_test_id())

    # Start the query
    ts.start()

    # Assert the results
    for t in c.tuples():
        num_rows += 1
        # print("{}:{}".format(num_rows, t))

    field_names = ['_0', '_1', '_2', '_3', '_4', '_5', '_6', '_7', '_8', '_9']

    assert c.tuples()[0] == field_names

    assert len(c.tuples()) == 2 + 1

    # These have been verified in Postgres
    assert LabelledTuple(c.tuples()[1], field_names) == \
           ["A", "F", 129850, 194216048.19000033, 184525343.78730044, 191943492.96455324, 25.445816186556925,
            38059.19031746035, 0.050005878894768374, 5103]
    assert LabelledTuple(c.tuples()[2], field_names) == \
           ["R", "F", 129740, 193438367.33999985, 183701990.7670003, 191045646.36937532, 25.509241053873353,
            38033.49731419579, 0.05061541486433399, 5086]

    # Write the metrics
    query_plan.print_metrics()
