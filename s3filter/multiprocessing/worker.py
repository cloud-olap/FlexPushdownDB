import threading
from collections import deque
from multiprocessing import Process

import pandas as pd

from s3filter.multiprocessing.channel import Channel
from s3filter.multiprocessing.header_message_data import HeaderMessageData
from s3filter.multiprocessing.message_base import MessageBase
from s3filter.multiprocessing.message_base_type import MessageBaseType


class Worker(object):

    def __init__(self, name, num_elements, element_size, system, handler):
        self.name = name
        self.handler = handler
        self.channel = Channel(num_elements, element_size)
        self.pending_queues = {}
        self.system = system
        self.pending_buffer_requests = deque()
        self.process = Process(target=self.work, args=())
        # self.process = threading.Thread(target=self.work, args=())
        self.running = True
        self.proxy = True

    def work(self):

        # This allows us to see whethere an instance is a proxy or the actual remote process
        self.proxy = False

        while self.running:

            msg = self.channel.get()

            # print("{} {} | Get | type: {}, sender: {}, data {}".
            #       format(self.name, 'proxy' if self.proxy else '', msg.message_type, msg.sender_name, msg.data))

            if msg.message_type is MessageBaseType.stop:
                self.on_stop(msg)
            elif msg.message_type is MessageBaseType.request_buffer:
                self.on_request_buffer(msg)
            elif msg.message_type is MessageBaseType.grant_buffer:
                self.on_grant_buffer(msg)
            elif msg.message_type is MessageBaseType.header:
                self.on_header(msg)
            else:
                self.on_user_defined(msg)

    def on_stop(self, _msg):
        self.running = False
        # queues[driver_queue].put(MessageBase(MessageBaseType.stopped, name, None))

    def on_request_buffer(self, msg):
        # If shared array is available then grant access, otherwise store the request in pending requests
        requesting_worker_name = msg.sender_name
        if self.channel.available():
            self.grant_buffer(requesting_worker_name)
        else:
            self.pending_buffer_requests.append(requesting_worker_name)

    def on_grant_buffer(self, msg):
        # Get any messages that are waiting for the buffer
        pending_msg = self.pending_queues[msg.sender_name].pop()
        msg_sender = self.system.workers[msg.sender_name]

        # Put the dataframe into shared memory
        msg_sender.channel.copy_dataframe_to_buffer(pending_msg.data)

        # Send the meta data to the consumer
        header_msg = MessageBase(MessageBaseType.header, pending_msg.sender_name,
                                 HeaderMessageData(pending_msg.data.shape, pending_msg.data.columns.values), True)
        msg_sender.put(header_msg, self)

        # Drain the pending messages queue until we find another data message
        while True:
            if len(self.pending_queues[msg.sender_name]) > 0:
                m = self.pending_queues[msg.sender_name][-1]
                if m.message_type is MessageBaseType.data:
                    # There are messages waiting on a buffer grant can't send message yet
                    break
                else:
                    # Pop and send
                    m = self.pending_queues[msg.sender_name].pop()
                    msg_sender.channel.put(m)
            else:
                break

    def on_user_defined(self, msg):
        self.running = self.handler.on_message(msg, self, self.system)

    def on_header(self, msg):
        # Get dataframe from buffer
        df = self.channel.copy_dataframe_from_buffer(msg.data.shape)
        df.columns = msg.data.columns

        # Release buffer, and make available to others waiting for it
        self.channel.release()
        if len(self.pending_buffer_requests) > 0:
            buffer_request_worker_name = self.pending_buffer_requests.pop()
            self.grant_buffer(buffer_request_worker_name)

        # Reassemble the message and pass to the user defined handler
        data_msg = MessageBase(MessageBaseType.data, msg.sender_name, df, False)
        self.on_user_defined(data_msg)

    def grant_buffer(self, worker_name):

        # print(
        #     "{} {} | Granting to {}".format(self.name, 'proxy' if self.proxy else '', worker_name))

        # Acquire shared memory
        self.channel.acquire(worker_name)

        # Send the grant message to the given worker
        msg = self.create_message(MessageBaseType.grant_buffer, None, True)
        self.system.put(worker_name, msg, self)

    def start(self):
        self.process.start()

    def join(self):
        self.process.join()

    def put(self, msg, sender):

        # print("{} {} | Put | type: {}, sender: {}".format(self.name, 'proxy' if self.proxy else '', msg.message_type,
        #                                                   msg.sender_name))

        if type(msg) is MessageBase and type(msg.data) is pd.DataFrame:

            df = msg.data

            df_slices = self.slice_dataframe(df, self.channel.buffer_size / self.channel.element_size)

            # Add message to pending messages queue
            for df_slice in df_slices:

                assert(df_slice.size * self.channel.element_size <= self.channel.buffer_size)

                pending_msg_slice = MessageBase(msg.message_type, msg.sender_name, df_slice, False)
                sender.pending_queues.setdefault(self.name, deque())
                sender.pending_queues[self.name].appendleft(pending_msg_slice)

                # Request access to shared memory from the consumer
                request_buffer_msg = MessageBase(MessageBaseType.request_buffer, msg.sender_name, None, True)
                self.channel.put(request_buffer_msg)
        elif type(msg) is MessageBase and msg.system_message:
            # If it's a system message let it straight through
            self.channel.put(msg)
        else:
            # If there are messages waiting then add this to the pending queue
            if sender is not None and sender.pending_queues.has_key(self.name) and len(sender.pending_queues[self.name]) > 0:
                sender.pending_queues[self.name].appendleft(msg)
            else:
                self.channel.put(msg)

    def slice_dataframe(self, df, max_elements):
        df_slices = []
        num_cols = df.shape[1]
        max_rows = (max_elements / num_cols)
        counter = 0
        while sum(map(len, df_slices)) < len(df):
            start = counter * max_rows
            end = start + max_rows
            df_slice = df[start:end]
            df_slices.append(df_slice)
            counter += 1

        assert sum(map(len, df_slices)) == len(df)

        return df_slices

    def create_message(self, messsage_type, data, system_message):
        msg = MessageBase(messsage_type, self.name, data, system_message)
        return msg

    def close(self):
        self.channel.close()
