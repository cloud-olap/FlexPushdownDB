import StringIO
import cPickle
import cProfile
import multiprocessing
import pstats
import random
import timeit
from ctypes import c_char
from multiprocessing import Process, RawArray, Queue

import numpy as np
import pandas as pd
from enum import Enum

from s3filter.multiprocessing.handler_base import HandlerBase
from s3filter.multiprocessing.message_base import MessageBase
from s3filter.multiprocessing.message_base_type import MessageBaseType
from s3filter.multiprocessing.worker_system import WorkerSystem
from s3filter.multiprocessing.worker import Worker

buffer_num_cols = 4
buffer_num_rows = 10
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
    df = df.add_prefix('_')
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


# def get_queues(names, queues):
#     return map(lambda n: queues[n], names)


def test_fork_join_sharedmem_complex():
    num_messages_to_send = 10
    closure = {'num_messages_received': 0}

    class Handler(HandlerBase):

        def __init__(self, consumer_names):
            super(Handler, self).__init__()
            self.consumer_names = consumer_names

        def on_message(self, message, worker, workers):

            running = True

            # print("{} | type: {}, sender: {}".format(name, message.message_type, message.sender_name))
            if message.message_type is MessageBaseType.start:
                for i in range(0, num_messages_to_send):
                    test_df = random_shaped_dataframe(worker.name)
                    msg = worker.create_message(MessageBaseType.data, test_df)
                    system.put_many(self.consumer_names, msg, worker)
            elif message.message_type is MessageBaseType.data:
                closure['num_messages_received'] += 1
                df = message.data
                # print(df)
                if len(self.consumer_names) > 0:
                    msg = worker.create_message(MessageBaseType.data, df)
                    system.put_many(self.consumer_names, msg, worker)
                else:
                    if closure['num_messages_received'] >= num_messages_to_send * 2:
                        # msg = worker.create_message(MessageBaseType.stop, None)
                        # system.put_all(msg, worker)
                        msg = system.create_message(MessageBaseType.operator_completed, worker.name)
                        system.put('system', msg, worker)
            else:
                raise Exception("Unrecognised message type {}".format(message.message_type))

            return running

    num_elements = buffer_num_cols * buffer_num_rows
    element_size = buffer_item_size

    system = WorkerSystem(num_elements, element_size)

    system.create_worker('p1', num_elements, element_size, Handler(['p3']))
    system.create_worker('p2', num_elements, element_size, Handler(['p3']))
    system.create_worker('p3', num_elements, element_size, Handler(['p4', 'p5']))
    system.create_worker('p4', num_elements, element_size, Handler(['p6']))
    system.create_worker('p5', num_elements, element_size, Handler(['p6']))
    system.create_worker('p6', num_elements, element_size, Handler([]))

    system.start()

    system.put('p1', system.create_message(MessageBaseType.start, None), None)
    system.put('p2', system.create_message(MessageBaseType.start, None), None)

    completed_workers = {worker_name: False for worker_name in system.workers.keys()}
    while completed_workers['p6'] is False:
        msg = system.listen(MessageBaseType.operator_completed)
        completed_workers[msg.data] = True

    msg = system.create_message(MessageBaseType.stop, None)
    system.put_all(msg, system)

    system.join()


def test_fork_join_sharedmem_simple():
    num_messages_to_send = 10
    closure = {'num_messages_received': 0}

    class Handler(HandlerBase):

        def __init__(self, consumer_names):
            super(Handler, self).__init__()
            self.consumer_names = consumer_names

        def on_message(self, message, worker, system):

            running = True

            # print("{} | type: {}, sender: {}".format(name, message.message_type, message.sender_name))
            if message.message_type is MessageBaseType.start:
                for i in range(0, num_messages_to_send):
                    test_df = random_shaped_dataframe(worker.name)
                    msg = worker.create_message(MessageBaseType.data, test_df)
                    system.put_many(self.consumer_names, msg, worker)
            elif message.message_type is MessageBaseType.data:
                closure['num_messages_received'] += 1
                df = message.data
                # print(df)
                if len(self.consumer_names) > 0:
                    msg = worker.create_message(MessageBaseType.data, df)
                    system.put_many(self.consumer_names, msg, worker)
                else:
                    if closure['num_messages_received'] >= num_messages_to_send * 2:
                        # msg = worker.create_message(MessageBaseType.stop, None)
                        # system.put_all(msg, worker)
                        msg = system.create_message(MessageBaseType.operator_completed, worker.name)
                        system.put('system', msg, worker)
            else:
                raise Exception("Unrecognised message type {}".format(message.message_type))

            return running

    num_elements = buffer_num_cols * buffer_num_rows
    element_size = buffer_item_size

    system = WorkerSystem(num_elements=num_elements, element_size=element_size)

    system.create_worker('p1', num_elements, element_size, Handler(['p3']))
    system.create_worker('p2', num_elements, element_size, Handler(['p3']))
    system.create_worker('p3', num_elements, element_size, Handler([]))

    system.start()

    system.put('p1', system.create_message(MessageBaseType.start, None), None)
    system.put('p2', system.create_message(MessageBaseType.start, None), None)

    completed_workers = {worker_name: False for worker_name in system.workers.keys()}
    while completed_workers['p3'] is False:
        msg = system.listen(MessageBaseType.operator_completed)
        completed_workers[msg.data] = True

    msg = system.create_message(MessageBaseType.stop, None)
    system.put_all(msg, system)

    system.join()
