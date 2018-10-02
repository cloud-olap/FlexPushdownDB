# noinspection PyCompatibility
import cPickle
import multiprocessing
from ctypes import c_char
from multiprocessing import RawArray

import numpy as np
import pandas as pd


class Channel(object):
    MAX_BUFFER_SIZE = 16 * 1024 * 1024

    def __init__(self, num_elements, element_size):
        """

        :param buffer_size: Should be sized according to the maximum expected size of the dataframes sent over
        the channel. This equals rows x cols x element size.
        :param max_element_size: Maximum size of the strings stored in the dataframe.
        """
        self.num_elements = num_elements
        self.element_size = element_size
        self.buffer_size = num_elements * element_size

        if self.buffer_size > Channel.MAX_BUFFER_SIZE:
            raise Exception("{} exceeds allowable size ({}) of shared memory to allocate"
                            .format(self.buffer_size, Channel.MAX_BUFFER_SIZE))
        else:
            self.queue = multiprocessing.Queue()

            # Create the shared memory array
            self.shared_array = RawArray(c_char, self.buffer_size)

            # Wrap the shared array
            self.shared_array_wrapper = np.frombuffer(self.shared_array, dtype='S' + str(self.element_size))

            # Tracks who has been given permission to write to the shared array
            self.__owner = None

    def put(self, msg):
        self.__pickle_and_send(msg)

    def get(self):
        return self.__receive_and_unpickle()

    def __pickle_and_send(self, header_msg):
        pickled_msg = cPickle.dumps(header_msg, cPickle.HIGHEST_PROTOCOL)
        self.queue.put(pickled_msg)

    def available(self):
        return self.__owner is None

    def release(self):
        self.__owner = None

    def acquire(self, worker_name):
        if self.__owner is None or self.__owner == worker_name:
            self.__owner = worker_name
        else:
            raise Exception("Shared array is owned by '{}'. '{}' cannot acquire.".format(
                self.__owner, worker_name))

    def copy_dataframe_to_buffer(self, df):

        df_size = df.shape[0] * df.shape[1] * self.element_size
        if df_size > self.buffer_size:
            raise Exception("Dataframe size {} ({} x {} x {}) exceeds capacity of shared memory {}."
                            .format(df_size, df.shape[0], df.shape[1], self.element_size, self.buffer_size))
        else:
            # Make sure the src array are sized strings not objects
            nd_array = df.values.astype('S' + str(self.element_size), copy=False)

            # Reshape the source array into 1 row
            nd_array = nd_array.reshape(1, df.shape[0] * df.shape[1])

            self.shared_array_wrapper[0: df.shape[0] * df.shape[1]] = nd_array

    def copy_dataframe_from_buffer(self, df_shape):

        df_size = df_shape[0] * df_shape[1] * self.element_size
        if df_size > self.buffer_size:
            raise Exception("Dataframe size {} ({} x {} x {}) exceeds capacity of shared memory {}."
                            .format(df_size, df_shape[0], df_shape[1], self.element_size, self.buffer_size))
        else:
            num_items = df_shape[0] * df_shape[1]

            shared_array = np.frombuffer(self.shared_array, dtype='S' + str(self.element_size))

            sliced_array = shared_array[0: num_items]

            shaped_array = sliced_array.reshape(df_shape[0], df_shape[1])

            df = pd.DataFrame(shaped_array, copy=False)

            return df

    def __receive_and_unpickle(self):
        pickled_msg = self.queue.get()
        msg = cPickle.loads(pickled_msg)
        return msg

    def close(self):
        self.queue.close()
