import StringIO
import cPickle
import cProfile
import pstats
import timeit
from ctypes import c_char
from multiprocessing import Process, RawArray, Queue

import numpy as np
import pandas as pd
from enum import Enum

buffer_num_cols = 20
buffer_num_rows = 100000
buffer_item_size = 100
num_iterations = 10


def random_dataframe():
    df = pd.DataFrame(np.random.randint(0, 10000, size=(buffer_num_rows, buffer_num_cols)))
    df = df.applymap(str)
    return df


def test_sharedmem():
    run(False)


def test_pickle():
    run(True)


class Msg(Enum):
    run = 1
    stop = 2
    new_df = 3


def send_df(df, q2, shared_array, use_pickle, from_name):
    if use_pickle:
        send(df, from_name, q2)
    else:
        nd_array = df.values.astype('S' + str(buffer_item_size), copy=False)
        np.copyto(shared_array, nd_array, casting='no')
        send(Msg.new_df, from_name, q2)


def send(msg, from_name, q):
    pickled_msg = cPickle.dumps((from_name, msg), cPickle.HIGHEST_PROTOCOL)
    q.put(pickled_msg)


def receive(q):
    pickled_msg = q.get()
    return cPickle.loads(pickled_msg)


df = random_dataframe()


def run(use_pickle):
    def sender(q, s, q2, driver_queue, use_pickle):

        print("f1 Started")

        if s is not None:
            shared_array = np.frombuffer(s, dtype='S' + str(buffer_item_size)).reshape(buffer_num_rows, buffer_num_cols)
        else:
            shared_array = None

        done = False
        while not done:
            m = receive(q)

            # pr = cProfile.Profile()
            # pr.enable()

            # print("sender | Received message {}".format(m))
            if type(m[1]) is Msg and m[1] == Msg.stop:
                done = True
            elif type(m[1]) is Msg and m[1] == Msg.run:
                for i in range(0, num_iterations):
                    send_df(df, q2, shared_array, use_pickle, 'sender')
                send(Msg.stop, 'sender', q2)
                done = True
            else:
                print("sender | Unrecognised message {}".format(m))

            # pr.disable()
            # s = StringIO.StringIO()
            # sortby = 'cumulative'
            # ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
            # ps.print_stats()
            # print (s.getvalue())

        print("f1 Finished")

    def receiver(q, s, driver_queue, use_pickle):

        print("f2 Started")

        if s is not None:
            shared_array = np.frombuffer(s, dtype='S' + str(buffer_item_size)).reshape(buffer_num_rows, buffer_num_cols)
        else:
            shared_array = None

        done = False
        while not done:
            m = receive(q)
            # print("receiver | Received message {}".format(m))
            if type(m[1]) is Msg and m[1] == Msg.stop:
                done = True
            elif type(m[1]) is pd.DataFrame:
                df = m[1]
                print("receiver | Received pickled dataframe, length {}".format(len(df)))
            elif type(m[1]) is Msg and m[1] == Msg.new_df:
                df = pd.DataFrame(shared_array, copy=False)
                print("receiver | Received shared dataframe {}".format(len(df)))
            else:
                print("receiver | Unrecognised message {}".format(m))

        print("f2 Finished")

    q1 = Queue()
    q2 = Queue()
    driver_queue = Queue()

    if not use_pickle:
        shared_array_base = RawArray(c_char, buffer_num_cols * buffer_num_rows * buffer_item_size)
    else:
        shared_array_base = None

    p1 = Process(target=sender, args=(q1, shared_array_base, q2, driver_queue, use_pickle))
    p2 = Process(target=receiver, args=(q2, shared_array_base, driver_queue, use_pickle))

    p1.start()
    p2.start()

    # pr = cProfile.Profile()
    # pr.enable()

    start_time = timeit.default_timer()

    for i in range(1, 10):
        send(Msg.run, 'driver', q1)

    p1.join()
    p2.join()

    stop_time = timeit.default_timer()

    # pr.disable()
    # s = StringIO.StringIO()
    # sortby = 'cumulative'
    # ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
    # ps.print_stats()
    # print (s.getvalue())

    time = stop_time - start_time
    bytes = buffer_num_cols * buffer_num_rows * buffer_item_size
    total_bytes = bytes * num_iterations
    bytes_sec = total_bytes / time

    print("Time {}, Bytes {}, Throughput {} B/s, {} MB/s".format(time, bytes * num_iterations, bytes_sec,
                                                                 bytes_sec / 1000 / 1000))
