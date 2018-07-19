# -*- coding: utf-8 -*-
"""Ray tests

"""

import time
import timeit

import pytest
import ray

start_message_type = 0
test_message_type = 1
get_message_type = 2

pytest.ray_initialised = False


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


@ray.remote
class Sender(object):

    def __init__(self, name, receiver, num_messages_to_send, batch_size):

        self._name = name
        self._receiver = receiver
        self._num_messages_to_send = num_messages_to_send
        self._batch_size = batch_size

    def handle(self, message):

        # print("{}:receive:{}".format(self._name, message))

        # if Envelope.get_type(message) == start_message_type:
        #
        #     for i in xrange(0, self._num_messages_to_send / self._batch_size):
        #         buffer_ = [Envelope.build_data(test_message_type)] * self._batch_size
        #         # for m in buffer_:
        #         #     print ("{}:send:{}".format(self._name, m))
        #         self._receiver.handle_batch.remote(buffer_)
        #
        # else:
        #     raise Exception("Unrecognized message {} ".format(message))

        try:
            if Envelope.get_type(message) == start_message_type:
                num_messages_remaining = self._num_messages_to_send

                while num_messages_remaining > 0:
                    num_messages_in_batch = min(num_messages_remaining, self._batch_size)
                    buffer_ = [Envelope.build_data(test_message_type)] * num_messages_in_batch
                    # print("send {}".format(num_messages_in_batch))
                    self._receiver.handle_batch.remote(buffer_)
                    num_messages_remaining -= num_messages_in_batch
            else:
                raise Exception("Unrecognized message {} ".format(message))

        except BaseException as e:
            print(e)


@ray.remote
class Receiver(object):

    def __init__(self, name):

        self._name = name
        self._num_messages_received = 0

    def handle(self, message):

        # print("{}:receive:{}".format(self._name, message))

        if Envelope.get_type(message) == test_message_type:
            self._num_messages_received += 1

        elif Envelope.get_type(message) == get_message_type:
            return self._num_messages_received

        else:
            raise Exception("Unrecognized message {} ".format(message))

    def handle_batch(self, batch):
        map(self.handle, batch)


def test_ray_message_throughput():
    """Tests how many messages can be sent from one process to another

    NOTE: This is really slow so only a few messages are sent

    :return:
    """

    num_messages = 10000
    batch_size = 1

    if not pytest.ray_initialised:
        print("Starting...")
        ray.init(driver_mode=ray.SCRIPT_MODE)
        print("Started")
        pytest.ray_initialised = True

    receiver = Receiver.remote("receiver")
    sender = Sender.remote("sender", receiver, num_messages, batch_size)

    start = timeit.default_timer()

    sender.handle.remote(Envelope.build_data(start_message_type))

    while True:
        time.sleep(0.01)
        num_messages_received = ray.get(receiver.handle.remote(Envelope.build_data(get_message_type)))
        if num_messages_received >= num_messages:
            break

    finish = timeit.default_timer()

    elapsed = finish - start

    print("elapsed: {}, messages/sec: {}".format(elapsed, float(num_messages) / elapsed))


def test_ray_message_throughput_batched():
    """Tests how many messages can be sent from one process to another with messages sent in batches

    :return:
    """

    num_messages = 1000000
    batch_size = 64000

    if not pytest.ray_initialised:
        print("Starting...")
        ray.init(driver_mode=ray.PYTHON_MODE)
        print("Started")
        pytest.ray_initialised = True

    receiver = Receiver.remote("receiver")
    sender = Sender.remote("sender", receiver, num_messages, batch_size)

    start = timeit.default_timer()

    sender.handle.remote(Envelope.build_data(start_message_type))

    while True:
        time.sleep(0.01)
        num_messages_received = ray.get(receiver.handle.remote(Envelope.build_data(get_message_type)))
        if num_messages_received >= num_messages:
            break

    finish = timeit.default_timer()

    elapsed = finish - start

    print("elapsed: {}, messages/sec: {}".format(elapsed, float(num_messages) / elapsed))
