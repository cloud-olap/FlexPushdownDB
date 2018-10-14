from typing import Union, TypeVar

import s3filter.multiprocessing.channel as channel
from s3filter.multiprocessing.handler_base import HandlerBase
from s3filter.multiprocessing.message import MessageBase
from s3filter.multiprocessing.packet import PacketBase, MessagePacket
from s3filter.multiprocessing.worker import Worker


class WorkerSystem(object):
    DEFAULT_BUFFER_SIZE = 1 * 1024 * 1024

    def __init__(self, buffer_size=DEFAULT_BUFFER_SIZE):
        self.workers = {}
        self.channel = channel.Channel(buffer_size)

    def create_worker(self, name, handler, buffer_size=DEFAULT_BUFFER_SIZE, is_profiled=False, profile_file_name=None):
        # type: (str, HandlerBase, int, bool, str) -> object
        worker = Worker(name, buffer_size, self, handler, is_profiled, profile_file_name)
        self.workers[name] = worker
        return worker

    def start(self):
        map(lambda p: p.start(), self.workers.values())

    def join(self):
        map(lambda p: p.join(), self.workers.values())

    def put(self, to, sender, packet):
        # type: (str, Worker, PacketBase) -> None

        if to == 'system':
            if isinstance(packet, PacketBase):
                self.channel.put(sender.name, packet)
            else:
                raise NotImplementedError()
        else:
            self.workers[to].channel.put(sender, packet)

    def send(self, to, message, sender):
        # type: (str, MessageBase, Union[Worker, None]) -> None
        if to == 'system':
            if isinstance(message, MessageBase):
                packet = MessagePacket(message)
                self.put(to, sender, packet)
            else:
                raise NotImplementedError()
        else:
            self.workers[to].send(message, sender)

    def send_many(self, names, message, sender=None):
        dest_workers = map(lambda n: self.workers[n], names)
        map(lambda w: self.send(w.name, message, sender), dest_workers)

    def send_all(self, message, sender=None):
        map(lambda w: self.send(w.name, message, sender), self.workers.values())

    def listen(self, message_type):
        # type: (TypeVar[MessageBase]) -> MessageBase
        pkt = self.get()

        if isinstance(pkt, MessagePacket):
            msg = pkt.message
            if isinstance(msg, message_type):
                return msg
            else:
                raise Exception("Received '{}' while listening for '{}'".format(type(msg), message_type))
        else:
            raise Exception("Unrecognised packet '{}'".format(pkt))

    def get(self):
        # type: () -> PacketBase
        pkt = self.channel.get()

        # print("{} | get | type: {}, sender: {}, data: {}".
        #       format('system', msg.message_type, msg.sender_name, msg.data))

        return pkt

    def close(self):
        map(lambda w: w.close(), self.workers.values())
        self.channel.close()
