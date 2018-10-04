import copy
from collections import deque
from multiprocessing import Process

import numpy as np
import pandas as pd

from s3filter.multiprocessing.channel import Channel
from s3filter.multiprocessing.header import Header
from s3filter.multiprocessing.message import StopMessage, DataFrameMessage, MessageBase
from s3filter.multiprocessing.packet import StopPacket, RequestBufferPacket, GrantBufferPacket, \
    HeaderPacket, MessagePacket, DataPacket, PacketBase


class Worker(object):

    def __init__(self, name, buffer_size, system, handler):
        self.name = name
        self.handler = handler
        self.channel = Channel(buffer_size)
        self.pending_queues = {}
        self.system = system
        self.pending_buffer_requests = deque()
        self.process = Process(target=self.work, args=())
        # self.process = threading.Thread(target=self.work, args=())
        self.running = True
        self.proxy = True

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.name)

    def work(self):

        # This allows us to see whethere an instance is a proxy or the actual remote process
        self.proxy = False

        # pr = cProfile.Profile()
        # pr.enable()

        while self.running:

            packet = self.channel.get()

            assert (isinstance(packet, PacketBase))

            # print("{} {} | Get | packet {}".
            #       format(self.name, 'proxy' if self.proxy else '', packet))

            if isinstance(packet, StopPacket):
                msg = StopMessage()
                msg.set_sender(packet.sender_name)
                self.on_user_defined(msg)
                self.on_stop()
            elif isinstance(packet, RequestBufferPacket):
                self.on_request_buffer(packet)
            elif isinstance(packet, GrantBufferPacket):
                self.on_grant_buffer(packet)
            elif isinstance(packet, HeaderPacket):
                self.on_header(packet)
            elif isinstance(packet, MessagePacket):
                self.on_user_defined(packet.message)
            else:
                raise Exception("Unrecognised packet {}".format(packet))

        # pr.disable()
        # s = StringIO.StringIO()
        # sortby = 'tottime'
        # ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        # ps.print_stats()
        #
        # print("-----------============= {} =============--------------".format(self.name))
        #
        # print(s.getvalue())

    def on_stop(self):
        self.running = False

    def on_request_buffer(self, packet):
        # type: (RequestBufferPacket) -> None

        # If shared array is available then grant access, otherwise store the request in pending requests
        requesting_worker_name = packet.sender_name
        if self.channel.available():
            self.grant_buffer(requesting_worker_name)
        else:
            self.pending_buffer_requests.append(requesting_worker_name)

    def on_grant_buffer(self, packet):
        # type: (GrantBufferPacket) -> None

        # Get any messages that are waiting for the buffer
        data_pkt = self.pending_queues[packet.sender_name].pop()
        pkt_sender = self.system.workers[packet.sender_name]

        # Put the dataframe into shared memory
        pkt_sender.channel.copy_array_to_buffer(data_pkt.ndarray)

        # Send the meta data to the consumer
        header_pkt = HeaderPacket(
            Header(data_pkt.ndarray.shape, data_pkt.ndarray.dtype.str, data_pkt.columns.tolist(), data_pkt.msg))
        pkt_sender.channel.put(data_pkt.sender_name, header_pkt)

        # Drain the pending messages queue until we find another data message
        while True:
            if len(self.pending_queues[packet.sender_name]) > 0:
                m = self.pending_queues[packet.sender_name][-1]
                if isinstance(m, DataPacket):
                    # There are messages waiting on a buffer grant can't send message yet
                    break
                else:
                    # Pop and send
                    m = self.pending_queues[packet.sender_name].pop()
                    pkt_sender.channel.put(m.sender_name, m)
            else:
                break

    def on_user_defined(self, msg):
        self.running = self.handler.on_message(msg, self)

    def on_header(self, header_pkt):
        # type: (HeaderPacket) -> None
        # Get dataframe from buffer
        df = self.channel.copy_array_from_buffer(header_pkt.header.shape, header_pkt.header.dtype)
        df.columns = header_pkt.header.columns

        # Release buffer, and make available to others waiting for it
        self.channel.release()
        if len(self.pending_buffer_requests) > 0:
            buffer_request_worker_name = self.pending_buffer_requests.pop()
            self.grant_buffer(buffer_request_worker_name)

        # Reassemble the message and pass to the user defined handler
        data_msg = header_pkt.header.msg
        data_msg.dataframe = df
        data_msg.set_sender(header_pkt.sender_name)
        self.on_user_defined(data_msg)

    def grant_buffer(self, worker_name):

        # print(
        #     "{} {} | Granting to {}".format(self.name, 'proxy' if self.proxy else '', worker_name))

        # Acquire shared memory
        self.channel.acquire(worker_name)

        # Send the grant message to the given worker
        # msg = self.create_message(PacketType.grant_buffer, None, True)
        self.system.put(worker_name, self.name, GrantBufferPacket())

    def start(self):
        self.process.start()

    def join(self):
        self.process.join()

    def send(self, msg, sender):
        # type: (MessageBase, Worker) -> None

        # print("{} {} | send | {} -> {}, msg: {}"
        #       .format(self.name,
        #               'proxy' if self.proxy else '',
        #               sender.name if sender is not None else 'None',
        #               self.name,
        #               msg))

        sender_name = sender.name if sender is not None else None

        msg.set_sender(sender_name)

        if isinstance(msg, MessageBase):
            if isinstance(msg, DataFrameMessage):

                df = msg.dataframe
                np_array_slices = self.slice_dataframe(df, self.channel.buffer_size)

                # Create a shallow copy of the message, and remove the dataframe,
                # we will put it back when it gets to its destination
                msg_copy = copy.copy(msg)
                msg_copy.dataframe = None

                # Add arrays to pending data queue
                for np_array in np_array_slices:
                    data_pkt = DataPacket(np_array, df.columns.values, msg_copy)
                    data_pkt.set_sender(sender.name)
                    sender.pending_queues.setdefault(self.name, deque())
                    sender.pending_queues[self.name].appendleft(data_pkt)

                    # Request access to shared memory from the consumer
                    request_buffer_pkt = RequestBufferPacket()
                    self.channel.put(sender_name, request_buffer_pkt)

            elif isinstance(msg, StopMessage):
                self.channel.put(sender_name, StopPacket())
            else:
                # If there are packets waiting then add this to the pending queue
                if self.is_pending_packets(sender):
                    msg_pkt = MessagePacket(msg)
                    msg_pkt.set_sender(sender_name)
                    sender.pending_queues[self.name].appendleft(msg_pkt)
                else:
                    self.channel.put(sender_name, MessagePacket(msg))
        else:
            raise Exception("Unrecognised message '{}'".format(msg))

    def is_pending_packets(self, sender):
        return sender is not None and self.name in sender.pending_queues and len(sender.pending_queues[self.name]) > 0

    def calc_max_rows(self, array, max_bytes):
        """Calculate the number of rows from the array that are less than max bytes

        :param array:
        :param max_bytes:
        :return:
        """

        num_bytes_per_row = self.calc_bytes_per_row(array)
        max_rows = max_bytes / num_bytes_per_row

        return max_rows

    @staticmethod
    def calc_bytes_per_row(array):
        num_bytes_per_element = array.dtype.itemsize
        num_cols = array.shape[1]
        num_bytes_per_row = num_cols * num_bytes_per_element
        return num_bytes_per_row

    def slice_dataframe(self, df, max_bytes):
        # type: (pd.DataFrame, int) -> [np.ndarray]

        # Convert pandas dataframe (with dtype object) to numpy array of sized strings (e.g. S12)
        array = df.values.astype(np.str, casting='unsafe', subok=False, copy=False)

        # Get number of bytes consumed
        total_bytes = array.nbytes

        num_bytes_per_row = self.calc_bytes_per_row(array)
        max_rows = self.calc_max_rows(array, max_bytes)

        if max_rows < 1:
            raise Exception("DataFrame rows are too large ({} bytes per row) to fit in buffer ({} bytes)".format(
                num_bytes_per_row, max_bytes))
        else:
            slices = []
            current_row = 0
            num_bytes_sliced = 0
            while num_bytes_sliced < total_bytes:
                slice_ = array[current_row: current_row + max_rows]
                num_bytes_sliced += slice_.nbytes
                slices.append(slice_)
                current_row += max_rows

                assert slice_.nbytes <= max_bytes

            return slices

    def close(self):
        self.channel.close()
