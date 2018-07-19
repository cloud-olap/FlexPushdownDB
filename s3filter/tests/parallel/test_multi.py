# -*- coding: utf-8 -*-
"""Ray tests

"""
import cPickle
import os
import timeit
from multiprocessing import Process, Queue

import dill

from s3filter import ROOT_DIR
from s3filter.op.collate import Collate
from s3filter.op.operator_base import EvaluatedMessage, EvalMessage
from s3filter.op.project import Project, ProjectExpression
from s3filter.op.sql_table_scan import SQLTableScan
from s3filter.plan.query_plan import QueryPlan
from s3filter.util.test_util import gen_test_id

start_message_type = 0
test_message_type = 1
get_message_type = 2


class Envelope(object):

    @staticmethod
    def get_type(data):
        # return self._message[0]
        return data['type_']

    @staticmethod
    def get_message(data):
        return data[1:]

    @staticmethod
    def build_data(type_, message=None):
        # return list(type_).extend(message)
        if message is None:
            return dict({'type_': type_})
        else:
            return dict({'type_': type_}, **message)


def sender(num_messages_to_send, batch_size, q):
    try:

        num_messages_remaining = num_messages_to_send

        while num_messages_remaining > 0:
            num_messages_in_batch = min(num_messages_remaining, batch_size)
            buffer_ = [Envelope.build_data(test_message_type)] * num_messages_in_batch
            # print("send {}".format(num_messages_in_batch))
            q.put(buffer_, True)
            num_messages_remaining -= num_messages_in_batch
    except BaseException as e:
        print(e)
    finally:
        q.close()


def receiver(num_messages, q):
    try:
        c = 0
        done = False
        while not done:
            buffer_ = q.get(True)
            for _ in buffer_:
                # print("receive {}".format(m))
                c += 1
                if c >= num_messages:
                    done = True
    except BaseException as e:
        print(e)


def test_multi_message_throughput_batched():
    """Tests how many messages can be sent from one process to another with messages sent in batches

    :return:
    """

    num_messages = 10000000
    batch_size = 1024

    q = Queue()

    r = Process(target=receiver, args=(num_messages, q,))
    s = Process(target=sender, args=(num_messages, batch_size, q,))

    start = timeit.default_timer()

    r.start()
    s.start()

    r.join()

    finish = timeit.default_timer()

    r.terminate()
    s.terminate()

    elapsed = finish - start

    print("elapsed: {}, messages/sec: {}".format(elapsed, float(num_messages) / elapsed))


def test_multi_message_throughput():
    """Tests how many messages can be sent from one process to another with messages sent in batches

    NOTE: This is quite slow so not many messages are sent

    :return:
    """

    num_messages = 100000
    batch_size = 1

    q = Queue()

    r = Process(target=receiver, args=(num_messages, q,))
    s = Process(target=sender, args=(num_messages, batch_size, q,))

    start = timeit.default_timer()

    r.start()
    s.start()

    r.join()

    finish = timeit.default_timer()

    r.terminate()
    s.terminate()

    elapsed = finish - start

    print("elapsed: {}, messages/sec: {}".format(elapsed, float(num_messages) / elapsed))


def test_operators():

    query_plan = QueryPlan(is_async=True, buffer_size=64)

    # Query plan
    ts = query_plan.add_operator(SQLTableScan('nation.csv',
                                              'select * from S3Object '
                                              'limit 3;',
                                              'scan', query_plan,
                                              False))

    p = query_plan.add_operator(Project(
        [
            ProjectExpression(lambda t_: t_['_0'], 'n_nationkey'),
            ProjectExpression(lambda t_: t_['_1'], 'n_name'),
            ProjectExpression(lambda t_: t_['_2'], 'n_regionkey'),
            ProjectExpression(lambda t_: t_['_3'], 'n_comment')
        ],
        'project', query_plan,
        False))

    c = query_plan.add_operator(Collate('collate', query_plan, False))

    ts.connect(p)
    p.connect(c)

    # Write the plan graph
    query_plan.write_graph(os.path.join(ROOT_DIR, "../tests-output"), gen_test_id())

    # Start the query
    query_plan.execute()

    tuples = c.tuples()

    c.print_tuples(tuples)

    # Write the metrics
    query_plan.print_metrics()

    # Shut everything down
    query_plan.stop()

    # Assert the results
    # num_rows = 0
    # for t in c.tuples():
    #     num_rows += 1
    #     print("{}:{}".format(num_rows, t))

    field_names = ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']

    assert len(tuples) == 3 + 1

    assert tuples[0] == field_names

    assert tuples[1] == ['0', 'ALGERIA', '0', ' haggle. carefully final deposits detect slyly agai']
    assert tuples[2] == ['1', 'ARGENTINA', '1',
                         'al foxes promise slyly according to the regular accounts. bold requests alon']
    assert tuples[3] == ['2', 'BRAZIL', '1', 'y alongside of the pending deposits. carefully special packages '
                                             'are about the ironic forges. slyly special ']
