import StringIO
import cPickle
import cProfile
import pstats
import random
import timeit
from ctypes import c_char
from multiprocessing import Process, RawArray, Queue

import numpy as np
import pandas as pd
from enum import Enum

from s3filter.multiprocessing.message_base import MessageBase
from s3filter.multiprocessing.message_base_type import MessageBaseType
from s3filter.multiprocessing.worker import Worker

buffer_num_cols = 20
buffer_num_rows = 100
buffer_item_size = 100
num_iterations = 10


def random_dataframe():
    df = pd.DataFrame(np.random.randint(0, 10000, size=(buffer_num_rows, buffer_num_cols)))
    df = df.applymap(str)
    return df


def random_shaped_dataframe(field_value):
    num_cols = random.randint(10, 20)
    num_rows = random.randint(50, 100)

    row = [field_value] * num_cols
    cols = [row] * num_rows

    df = pd.DataFrame(cols)
    return df


# def test_sharedmem():
#     run(False)
#
#
# def test_pickle():
#     run(True)


# class Msg(Enum):
#     run = 1
#     stop = 2
#     new_df = 3


# def send_df(df, queue, shared_array, use_pickle, from_name):
#     if use_pickle:
#         send(df, from_name, queue)
#     else:
#         nd_array = df.values.astype('S' + str(buffer_item_size), copy=False)
#         np.copyto(shared_array, nd_array, casting='no')
#         send(Msg.new_df, from_name, queue)


# def send(msg, queue):
#     if msg.message_type is MessageType.data:
#         nd_array = msg.data.values.astype('S' + str(buffer_item_size), copy=False)
#         np.copyto(queue.buffer, nd_array, casting='no')
#
#         pickled_msg = cPickle.dumps(Message(MessageType.data_header, msg.sender_name, ()), cPickle.HIGHEST_PROTOCOL)
#         queue.put(pickled_msg)
#     else:
#         pickled_msg = cPickle.dumps(msg, cPickle.HIGHEST_PROTOCOL)
#         queue.put(pickled_msg)


# def receive(q):
#     pickled_msg = q.get()
#     return cPickle.loads(pickled_msg)


# def run(use_pickle):
#     df = random_dataframe()
#
#     def sender(q, s, q2, driver_queue, use_pickle):
#
#         print("f1 Started")
#
#         if s is not None:
#             shared_array = np.frombuffer(s, dtype='S' + str(buffer_item_size)).reshape(buffer_num_rows, buffer_num_cols)
#         else:
#             shared_array = None
#
#         done = False
#         while not done:
#             m = receive(q)
#
#             # pr = cProfile.Profile()
#             # pr.enable()
#
#             # print("sender | Received message {}".format(m))
#             if type(m[1]) is Msg and m[1] == Msg.stop:
#                 done = True
#             elif type(m[1]) is Msg and m[1] == Msg.run:
#                 for i in range(0, num_iterations):
#                     send_df(df, q2, shared_array, use_pickle, 'sender')
#                 send(Msg.stop, 'sender', q2)
#                 done = True
#             else:
#                 print("sender | Unrecognised message {}".format(m))
#
#             # pr.disable()
#             # s = StringIO.StringIO()
#             # sortby = 'cumulative'
#             # ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
#             # ps.print_stats()
#             # print (s.getvalue())
#
#         print("f1 Finished")
#
#     def receiver(q, s, driver_queue, use_pickle):
#
#         print("f2 Started")
#
#         if s is not None:
#             shared_array = np.frombuffer(s, dtype='S' + str(buffer_item_size)).reshape(buffer_num_rows, buffer_num_cols)
#         else:
#             shared_array = None
#
#         done = False
#         while not done:
#             m = receive(q)
#             # print("receiver | Received message {}".format(m))
#             if type(m[1]) is Msg and m[1] == Msg.stop:
#                 done = True
#             elif type(m[1]) is pd.DataFrame:
#                 df = m[1]
#                 print("receiver | Received pickled dataframe, length {}".format(len(df)))
#             elif type(m[1]) is Msg and m[1] == Msg.new_df:
#                 df = pd.DataFrame(shared_array, copy=False)
#                 print("receiver | Received shared dataframe {}".format(len(df)))
#             else:
#                 print("receiver | Unrecognised message {}".format(m))
#
#         print("f2 Finished")
#
#     q1 = Queue()
#     q2 = Queue()
#     driver_queue = Queue()
#
#     if not use_pickle:
#         shared_array_base = RawArray(c_char, buffer_num_cols * buffer_num_rows * buffer_item_size)
#     else:
#         shared_array_base = None
#
#     p1 = Process(target=sender, args=(q1, shared_array_base, q2, driver_queue, use_pickle))
#     p2 = Process(target=receiver, args=(q2, shared_array_base, driver_queue, use_pickle))
#
#     p1.start()
#     p2.start()
#
#     # pr = cProfile.Profile()
#     # pr.enable()
#
#     start_time = timeit.default_timer()
#
#     for i in range(1, 10):
#         send(Msg.run, 'driver', q1)
#
#     p1.join()
#     p2.join()
#
#     stop_time = timeit.default_timer()
#
#     # pr.disable()
#     # s = StringIO.StringIO()
#     # sortby = 'cumulative'
#     # ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
#     # ps.print_stats()
#     # print (s.getvalue())
#
#     time = stop_time - start_time
#     bytes = buffer_num_cols * buffer_num_rows * buffer_item_size
#     total_bytes = bytes * num_iterations
#     bytes_sec = total_bytes / time
#
#     print("Time {}, Bytes {}, Throughput {} B/s, {} MB/s".format(time, bytes * num_iterations, bytes_sec,
#                                                                  bytes_sec / 1000 / 1000))


def get_queues(names, queues):
    return map(lambda n: queues[n], names)


def test_fork_join_sharedmem_complex():
    num_messages_to_send = 10
    closure = {'num_messages_received': 0}

    def p(message, worker, consumer_names, workers):

        running = True

        # print("{} | type: {}, sender: {}".format(name, message.message_type, message.sender_name))
        if message.message_type is MessageBaseType.start:
            for i in range(0, num_messages_to_send):
                test_df = random_shaped_dataframe(worker.name)
                map(lambda q: q.put(MessageBase(MessageBaseType.data, worker.name, test_df), worker),
                    get_queues(consumer_names, workers))
        elif message.message_type is MessageBaseType.data:
            closure['num_messages_received'] += 1
            df = message.data
            # print(df)
            if len(consumer_names) > 0:
                map(lambda q: q.put(MessageBase(MessageBaseType.data, worker.name, df), worker),
                    get_queues(consumer_names, workers))
            else:
                if closure['num_messages_received'] >= num_messages_to_send * 2:
                    map(lambda q: q.put(MessageBase(MessageBaseType.stop, worker.name, None), worker), workers.values())
        else:
            raise Exception("Unrecognised message type {}".format(message.message_type))

        return running

    shared_array_size = buffer_num_cols * buffer_num_rows * buffer_item_size

    workers = {}
    workers['p1'] = Worker('p1', p, ['p3'], shared_array_size, buffer_item_size, workers)
    workers['p2'] = Worker('p2', p, ['p3'], shared_array_size, buffer_item_size, workers)
    workers['p3'] = Worker('p3', p, ['p4', 'p5'], shared_array_size, buffer_item_size, workers)
    workers['p4'] = Worker('p4', p, ['p6'], shared_array_size, buffer_item_size, workers)
    workers['p5'] = Worker('p5', p, ['p6'], shared_array_size, buffer_item_size, workers)
    workers['p6'] = Worker('p6', p, [], shared_array_size, buffer_item_size, workers)

    map(lambda p: p.start(), workers.values())

    name = 'driver'

    workers['p1'].put(MessageBase(MessageBaseType.start, name, None), None)
    workers['p2'].put(MessageBase(MessageBaseType.start, name, None), None)

    map(lambda p: p.join(), workers.values())


def test_fork_join_sharedmem_simple():
    num_messages_to_send = 10
    closure = {'num_messages_received': 0}

    def p(message, worker, consumer_names, workers):

        running = True

        # print("{} | type: {}, sender: {}".format(name, message.message_type, message.sender_name))
        if message.message_type is MessageBaseType.start:
            for i in range(0, num_messages_to_send):
                test_df = random_shaped_dataframe(worker.name)
                map(lambda q: q.put(MessageBase(MessageBaseType.data, worker.name, test_df), worker),
                    get_queues(consumer_names, workers))
        elif message.message_type is MessageBaseType.data:
            closure['num_messages_received'] += 1
            df = message.data
            # print(df)
            if len(consumer_names) > 0:
                map(lambda q: q.put(MessageBase(MessageBaseType.data, worker.name, df), worker),
                    get_queues(consumer_names, workers))
            else:
                if closure['num_messages_received'] >= num_messages_to_send * 2:
                    map(lambda q: q.put(MessageBase(MessageBaseType.stop, worker.name, None), worker), workers.values())
        else:
            raise Exception("Unrecognised message type {}".format(message.message_type))

        return running

    shared_array_size = buffer_num_cols * buffer_num_rows * buffer_item_size

    workers = {}
    workers['p1'] = Worker('p1', p, ['p3'], shared_array_size, buffer_item_size, workers)
    workers['p2'] = Worker('p2', p, ['p3'], shared_array_size, buffer_item_size, workers)
    workers['p3'] = Worker('p3', p, [], shared_array_size, buffer_item_size, workers)

    map(lambda p: p.start(), workers.values())

    name = 'driver'

    workers['p1'].put(MessageBase(MessageBaseType.start, name, None), None)
    workers['p2'].put(MessageBase(MessageBaseType.start, name, None), None)

    map(lambda p: p.join(), workers.values())
