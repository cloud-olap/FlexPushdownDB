from collections import deque
from multiprocessing import Process

import pandas as pd

from s3filter.multiprocessing.channel import Channel
from s3filter.multiprocessing.message_base import MessageBase
from s3filter.multiprocessing.message_base_type import MessageBaseType


class Worker(object):

    def __init__(self, name, handler, consumer_names, buffer_size, max_element_size, workers):
        self.consumer_names = consumer_names
        self.name = name
        self.handler = handler
        self.channel = Channel(buffer_size, max_element_size)
        self.pending_queues = {}
        self.workers = workers
        self.pending_buffer_requests = deque()
        self.process = Process(target=self.work, args=())
        self.running = True
        self.proxy = True

    def work(self):

        # This allows us to see whethere an instance is a proxy or the actual remote process
        self.proxy = False

        while self.running:

            msg = self.channel.get()

            print(
                "{} {} | Get | type: {}, sender: {}".format(self.name, 'proxy' if self.proxy else '', msg.message_type,
                                                            msg.sender_name))

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

        # Put the dataframe into shared memory
        self.workers[msg.sender_name].channel.copy_dataframe_to_buffer(pending_msg.data)

        # Send the meta data to the consumer
        header_msg = MessageBase(MessageBaseType.header, pending_msg.sender_name,
                                 (pending_msg.data.shape[0], pending_msg.data.shape[1]))
        self.workers[msg.sender_name].put(header_msg, self)

    def on_user_defined(self, msg):
        self.running = self.handler(msg, self, self.consumer_names, self.workers)

    def on_header(self, msg):
        # Get dataframe from buffer
        df = self.channel.copy_dataframe_from_buffer(msg.data)

        # Release buffer, and make available to others waiting for it
        self.channel.release()
        if len(self.pending_buffer_requests) > 0:
            buffer_request_worker_name = self.pending_buffer_requests.pop()
            self.grant_buffer(buffer_request_worker_name)

        # Reassemble the message and pass to the user defined handler
        data_msg = MessageBase(MessageBaseType.data, msg.sender_name, df)
        self.on_user_defined(data_msg)

    def grant_buffer(self, worker_name):

        print(
            "{} {} | Granting to {}".format(self.name, 'proxy' if self.proxy else '', worker_name))

        # Acquire shared memory
        self.channel.acquire(worker_name)

        # Send the grant message to the given worker
        msg = MessageBase(MessageBaseType.grant_buffer, self.name, None)
        self.workers[worker_name].put(msg, self)

    def start(self):
        self.process.start()

    def join(self):
        self.process.join()

    def put(self, msg, sender):

        # print("{} {} | Put | type: {}, sender: {}".format(self.name, 'proxy' if self.proxy else '', msg.message_type,
        #                                                   msg.sender_name))

        if type(msg.data) is pd.DataFrame:

            # Add message to pending messages queue
            sender.pending_queues.setdefault(self.name, deque())
            sender.pending_queues[self.name].appendleft(msg)

            # Request access to shared memory from the consumer
            request_buffer_msg = MessageBase(MessageBaseType.request_buffer, msg.sender_name, None)
            self.channel.put(request_buffer_msg)
        else:
            self.channel.put(msg)
