import StringIO
import cProfile
import pstats
import random

import numpy as np
import pandas as pd

from s3filter.multiprocessing.handler_base import HandlerBase
from s3filter.multiprocessing.message import DataFrameMessage, MessageBase, StartMessage, StopMessage
from s3filter.multiprocessing.worker_system import WorkerSystem

buffer_size = 256 * 1024 * 1024
num_iterations = 10


# def random_dataframe():
#     df = pd.DataFrame(np.random.randint(0, 10000, size=(buffer_num_rows, buffer_num_cols)))
#     df = df.applymap(str)
#     return df


def random_shaped_dataframe(field_value, min_rows, max_rows):
    num_cols = random.randint(10, 20)
    num_rows = random.randint(min_rows, max_rows)

    # row = [field_value] * num_cols
    # row = [field_value + '_' + str(i) for i in range(num_cols)]
    # rows2 = [row] * num_rows

    rows = []
    for r in range(num_rows):
        col = []
        for c in range(num_cols):
            field = field_value + '_(' + str(c) + ',' + str(r) + ')'
            col.append(field)
        rows.append(col)

    df = pd.DataFrame(rows)
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


class ProcessCompletedMessage(MessageBase):

    def __init__(self, operator_name):
        super(ProcessCompletedMessage, self).__init__()
        self.operator_name = operator_name

    def __repr__(self):
        return "{}".format(self.__class__.__name__)


class CustomDataFrameMessage(DataFrameMessage):

    def __init__(self, dataframe, custom_data):
        super(CustomDataFrameMessage, self).__init__(dataframe)
        self.custom_data = custom_data

    def __repr__(self):
        return "{}".format(self.__class__.__name__)


class Handler(HandlerBase):

    def __init__(self, dataframe_to_send, num_dataframes_to_send, expected_dataframes, consumer_names):
        super(Handler, self).__init__()
        self.consumer_names = consumer_names
        self.num_dataframes_to_send = num_dataframes_to_send
        self.num_messages_received = 0
        self.dataframe_to_send = dataframe_to_send
        self.expected_dataframes = expected_dataframes
        self.received_dataframes = {}

    def assert_expected(self):
        for sender_name, expected_df in self.expected_dataframes.items():
            assert sender_name in self.received_dataframes
            received_df = self.received_dataframes[sender_name]
            assert received_df.shape == expected_df.shape

    def received_expected(self):
        all_rows_received = True
        for sender_name, expected_df in self.expected_dataframes.items():

            if sender_name in self.received_dataframes:
                received_df = self.received_dataframes[sender_name]

                if len(received_df) < len(expected_df):
                    all_rows_received = False
                    break
            else:
                all_rows_received = False

        return all_rows_received

    def on_message(self, message, worker):

        running = True

        # print("{} | message: {}".format(worker.name, type(message)))

        if isinstance(message, StartMessage):
            self.on_start_message(message, worker)
        elif isinstance(message, DataFrameMessage):
            self.on_dataframe_message(message, worker)
        elif isinstance(message, CustomDataFrameMessage):
            self.on_dataframe_message(message, worker)
        elif isinstance(message, StopMessage):
            pass  # NOOP
        else:
            raise Exception("Unrecognised message '{}'".format(type(message)))

        return running

    def on_dataframe_message(self, message, worker):

        self.num_messages_received += 1

        df = message.dataframe
        # print("{} | Received Message {}".format(worker.name, df))
        # Peek at first element
        first_element = df.loc[0][0]
        originator = first_element[:2]

        if isinstance(message, CustomDataFrameMessage):
            assert message.custom_data == 'custom_data'

        self.received_dataframes.setdefault(originator, pd.DataFrame())
        self.received_dataframes[originator] = self.received_dataframes[originator] \
            .append(df, ignore_index=True)
        if len(self.consumer_names) > 0:
            worker.system.send_many(self.consumer_names, CustomDataFrameMessage(df, 'custom_data'), worker)
        else:
            if self.received_expected():
                self.assert_expected()
                worker.system.send('system', ProcessCompletedMessage(worker.name), worker)

    def on_start_message(self, message, worker):
        for i in range(0, self.num_dataframes_to_send):
            msg = DataFrameMessage(self.dataframe_to_send)
            worker.system.send_many(self.consumer_names, msg, worker)


def test_fork_join_sharedmem_3():
    num_records_to_send = 100000
    num_records_per_batch = 10000
    num_batches = num_records_to_send / num_records_per_batch

    df1, df2 = generate_dataframes_to_send(num_records_per_batch * 0.8, num_records_per_batch * 1.2)

    edf1, edf2 = generate_expected_dataframes(df1, df2, num_batches, 2)

    # print()
    # print("Sending df1 {} times \n{}".format(num_messages_to_send, df1))
    # print("Sending df2 {} times \n{}".format(num_messages_to_send, df2))
    # print("Expecting df1 \n{}".format(edf1))
    # print("Expecting df2 s \n{}".format(edf2))

    h1 = Handler(df1, num_batches, None, ['p3'])
    h2 = Handler(df2, num_batches, None, ['p3'])

    expected_dataframes = {'p1': edf1, 'p2': edf2}

    h3 = Handler(None, 0, None, ['p4', 'p5'])
    h4 = Handler(None, 0, None, ['p6'])
    h5 = Handler(None, 0, None, ['p6'])
    h6 = Handler(None, 0, expected_dataframes, [])

    system = WorkerSystem(buffer_size)

    system.create_worker('p1', h1, buffer_size)
    system.create_worker('p2', h2, buffer_size)
    system.create_worker('p3', h3, buffer_size)
    system.create_worker('p4', h4, buffer_size)
    system.create_worker('p5', h5, buffer_size)
    system.create_worker('p6', h6, buffer_size)

    execute(system, ['p1', 'p2'], ['p6'])

    system.send_all(StopMessage(), None)

    system.join()


def execute(system, workers_to_start, workers_to_wait_on):
    system.start()

    map(lambda w: system.send(w, StartMessage(), None), workers_to_start)

    completed_workers = {worker_name: False for worker_name in workers_to_wait_on}
    while not all(completed_workers.values()):
        msg = system.listen(ProcessCompletedMessage)
        completed_workers[msg.operator_name] = True


def generate_dataframes_to_send(min_rows, max_rows):
    df1 = random_shaped_dataframe('p1', min_rows, max_rows)
    df2 = random_shaped_dataframe('p2', min_rows, max_rows)
    return df1, df2


def generate_expected_dataframes(df1, df2, num_messages_to_send, duplicates):
    edf1 = pd.DataFrame()
    edf2 = pd.DataFrame()
    for _ in range(num_messages_to_send * duplicates):
        edf1 = edf1.append(df1, ignore_index=True)
        edf2 = edf2.append(df2, ignore_index=True)
    return edf1, edf2


def test_fork_join_sharedmem_2():
    num_records_to_send = 100
    num_records_per_batch = 10
    num_batches = num_records_to_send / num_records_per_batch

    df1, df2 = generate_dataframes_to_send(num_records_per_batch * 0.8, num_records_per_batch * 1.2)

    edf1, edf2 = generate_expected_dataframes(df1, df2, num_batches, 1)

    h1 = Handler(df1, num_batches, None, ['p3'])
    h2 = Handler(df2, num_batches, None, ['p3'])

    expected_dataframes = {'p1': edf1, 'p2': edf2}

    h3 = Handler(None, 0, expected_dataframes, [])

    system = WorkerSystem(buffer_size)

    system.create_worker('p1', h1, buffer_size)
    system.create_worker('p2', h2, buffer_size)
    system.create_worker('p3', h3, buffer_size)

    execute(system, ['p1', 'p2'], ['p3'])

    system.send_all(StopMessage(), None)

    system.join()


def test_fork_join_sharedmem_1():
    num_records_to_send = 10
    num_records_per_batch = 2
    num_batches = num_records_to_send / num_records_per_batch

    df1 = random_shaped_dataframe('p1', 8, 12)
    edf1 = pd.DataFrame()
    for _ in range(num_batches * 1):
        edf1 = edf1.append(df1, ignore_index=True)

    h1 = Handler(df1, num_batches, None, ['p2'])

    expected_dataframes = {'p1': edf1}

    h2 = Handler(None, 0, expected_dataframes, [])

    system = WorkerSystem(buffer_size)

    system.create_worker('p1', h1, buffer_size)
    system.create_worker('p2', h2, buffer_size)

    execute(system, ['p1'], ['p2'])

    system.send_all(StopMessage(), None)

    system.join()
