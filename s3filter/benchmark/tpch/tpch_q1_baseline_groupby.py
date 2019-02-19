# -*- coding: utf-8 -*-
"""TPCH Q1 Baseline Group By Benchmark

"""

import os
from datetime import datetime, timedelta

from s3filter import ROOT_DIR
from s3filter.op.collate import Collate
from s3filter.plan.query_plan import QueryPlan
from s3filter.query import tpch_q1
from s3filter.sql.format import Format
from s3filter.util.test_util import gen_test_id


def main(sf, lineitem_parts, sharded, format_):
    # if s3filter.util.constants.TPCH_SF == 10:
    #     run(parallel=True, use_pandas=True, secure=False, use_native=False, buffer_size=0, lineitem_parts=96,
    #         sharded=True)
    # elif s3filter.util.constants.TPCH_SF == 1:
    #     run(parallel=True, use_pandas=True, secure=False, use_native=False, buffer_size=0, lineitem_parts=4,
    #         sharded=True)
    run(sf, True, True, False, False, 0, lineitem_parts, sharded, format_)


def run(sf, parallel, use_pandas, secure, use_native, buffer_size, lineitem_parts, sharded, format_):
    """

    :return: None
    """

    print('')
    print("TPCH Q1 Baseline Group By")
    print("----------------------")

    query_plan = QueryPlan(is_async=parallel, buffer_size=buffer_size)

    # Query plan
    lineitem_scan = map(lambda p:
                        query_plan.add_operator(
                            tpch_q1.sql_scan_lineitem_operator_def(
                                sharded,
                                p,
                                sf,
                                use_pandas,
                                secure,
                                use_native,
                                'lineitem_scan' + '_' + str(p),
                                query_plan, format_)),
                        range(0, lineitem_parts))

    lineitem_project = map(lambda p:
                           query_plan.add_operator(
                               tpch_q1.project_lineitem_operator_def(
                                   'lineitem_project' + '_' + str(p),
                                   query_plan)),
                           range(0, lineitem_parts))

    date = '1998-12-01'
    max_shipped_date = datetime.strptime(date, '%Y-%m-%d') - timedelta(days=117)

    lineitem_filter = map(lambda p:
                          query_plan.add_operator(
                              tpch_q1.filter_shipdate_operator_def(
                                  max_shipped_date,
                                  'lineitem_filter' + '_' + str(p),
                                  query_plan)),
                          range(0, lineitem_parts))

    profile_file_name = os.path.join(ROOT_DIR, "../tests-output/" + gen_test_id() + ".prof")

    groupby = map(lambda p:
                  query_plan.add_operator(
                      tpch_q1.groupby_returnflag_linestatus_operator_def(
                          'groupby' + '_' + str(p),
                          query_plan)),
                  range(0, lineitem_parts))

    # groupby[0].set_profiled(True, profile_file_name)

    groupby_reduce = query_plan.add_operator(
        tpch_q1.groupby_reduce_returnflag_linestatus_operator_def(
            'groupby_reduce',
            query_plan, use_pandas))
    # lineitem_scan[0].set_profiled(True,
    #                               os.path.join(ROOT_DIR, "../benchmark-output/", gen_test_id() + "_scan_0" + ".prof"))
    # lineitem_project[0].set_profiled(True, os.path.join(ROOT_DIR, "../benchmark-output/",
    #                                                     gen_test_id() + "_project_0" + ".prof"))
    # groupby[0].set_profiled(True,
    #                         os.path.join(ROOT_DIR, "../benchmark-output/", gen_test_id() + "_groupby_0" + ".prof"))
    # groupby_reduce.set_profiled(True, os.path.join(ROOT_DIR, "../benchmark-output/",
    #                                                gen_test_id() + "_groupby_reduce" + ".prof"))

    collate = query_plan.add_operator(
        Collate('collate', query_plan, False))

    map(lambda o: o.set_async(False), lineitem_project)
    map(lambda o: o.set_async(False), lineitem_filter)
    # map(lambda o: o.set_async(False), groupby)

    map(lambda (p, o): o.connect(lineitem_project[p]), enumerate(lineitem_scan))
    map(lambda (p, o): o.connect(lineitem_filter[p]), enumerate(lineitem_project))
    map(lambda (p, o): o.connect(groupby[p]), enumerate(lineitem_filter))
    map(lambda (p, o): o.connect(groupby_reduce), enumerate(groupby))
    groupby_reduce.connect(collate)

    # Plan settings
    print('')
    print("Settings")
    print("--------")
    print('')
    print('use_pandas: {}'.format(use_pandas))
    print("lineitem parts: {}".format(lineitem_parts))
    print('')

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../benchmark-output"), gen_test_id() + "-" + str(lineitem_parts))

    # Start the query
    query_plan.execute()
    print('Done')
    tuples = collate.tuples()

    collate.print_tuples(tuples)

    # Write the metrics
    query_plan.print_metrics()

    # Shut everything down
    query_plan.stop()


if __name__ == "__main__":
    main(sf=1, lineitem_parts=2, sharded=False, format_=Format.CSV)
