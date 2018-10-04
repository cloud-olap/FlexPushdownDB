# noinspection PyCompatibility
import cPickle
import warnings
from ctypes import c_char
from multiprocessing import RawArray
from multiprocessing.queues import Queue

import numpy as np
import pandas as pd

from s3filter.multiprocessing.message import StartMessage
from s3filter.multiprocessing.packet import PacketBase, MessagePacket


class Channel(object):

    # Maximum size of shared memory array, just a safety check it's not set to something ridiculous
    MAX_BUFFER_SIZE = 256 * 1024 * 1024

    def __init__(self, buffer_size):
        # type: (int) -> None

        self.buffer_size = buffer_size

        if self.buffer_size > Channel.MAX_BUFFER_SIZE:
            raise Exception("{} exceeds allowable size ({}) of shared memory to allocate"
                            .format(self.buffer_size, Channel.MAX_BUFFER_SIZE))
        else:
            self.__queue = Queue()

            # Create the shared memory array
            self.__shared_array = RawArray(c_char, self.buffer_size)

            # Tracks who has been given permission to write to the shared array
            self.__owner = None

    def put(self, sending_worker_name, packet):
        # type: (str, PacketBase) -> None

        # if sending_worker_name is None:
        #     warnings.warn("Put packet with no sender {}".format(packet))

        packet.set_sender(sending_worker_name)

        if not isinstance(packet, PacketBase):
            raise Exception("Packet {} is not of type {}".format(packet, PacketBase))

        self.__pickle_and_put(packet)

    def get(self):
        packet = self.__get_and_unpickle()

        # if packet.sender_name is None:
        #     warnings.warn("Get packet with no sender {}".format(packet))

        return packet

    def __pickle_and_put(self, packet):
        # type: (PacketBase) -> None
        pickled_packet = self.__pickle(packet)
        self.__queue.put(pickled_packet)

    @staticmethod
    def __pickle(packet):
        return cPickle.dumps(packet, cPickle.HIGHEST_PROTOCOL)

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

    def copy_array_to_buffer(self, ndarray):
        # type: (np.ndarray) -> None

        total_bytes = ndarray.nbytes
        if total_bytes > self.buffer_size:
            raise Exception("Numpy array ({} bytes) exceeds capacity of shared memory ({} bytes)."
                            .format(total_bytes, self.buffer_size))
        else:
            # Reshape the source array into 1 row, wrap the shared array and copy the ndarray into the wrapper
            ndarray = ndarray.reshape(1, ndarray.shape[0] * ndarray.shape[1])
            shared_array_wrapper = np.frombuffer(self.__shared_array, dtype=ndarray.dtype, count=ndarray.size)
            shared_array_wrapper[0: ndarray.shape[0] * ndarray.shape[1]] = ndarray

    def copy_array_from_buffer(self, df_shape, dtype):
        # type: (tuple, str) -> pd.DataFrame

        num_elements = df_shape[0] * df_shape[1]
        num_bytes = num_elements * np.dtype(dtype).itemsize
        if num_bytes > self.buffer_size:
            raise Exception("Array size {} exceeds capacity of shared memory {}."
                            .format(num_bytes, self.buffer_size))
        else:
            # Wrap the shared array, reshape the shared array and reconstruct the dataframe
            shared_array_wrapper = np.frombuffer(self.__shared_array, dtype=dtype, count=num_elements)
            shaped_array = shared_array_wrapper.reshape(df_shape[0], df_shape[1])
            df = pd.DataFrame(shaped_array, copy=False)
            return df

    def __get_and_unpickle(self):
        pickled_msg = self.__queue.get()
        msg = self.__unpickle(pickled_msg)
        return msg

    @staticmethod
    def __unpickle(pickled_msg):
        return cPickle.loads(pickled_msg)

    def close(self):
        self.__queue.close()
