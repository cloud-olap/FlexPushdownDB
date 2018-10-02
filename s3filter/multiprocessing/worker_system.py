import multiprocessing

from s3filter.multiprocessing.channel import Channel
from s3filter.multiprocessing.message_base import MessageBase
from s3filter.multiprocessing.worker import Worker


class WorkerSystem(object):

    def __init__(self, num_elements, element_size):
        self.workers = {}
        self.channel = Channel(num_elements, element_size)

    def create_worker(self, name, num_elements, element_size, handler):
        worker = Worker(name, num_elements, element_size, self, handler)
        self.workers[name] = worker
        return worker

    def start(self):
        map(lambda p: p.start(), self.workers.values())

    def join(self):
        map(lambda p: p.join(), self.workers.values())

    def put(self, name, message, sender):
        if name == 'system':
            self.channel.put(message)
        else:
            self.workers[name].put(message, sender)

    def put_many(self, names, message, sender=None):
        dest_workers = map(lambda n: self.workers[n], names)
        map(lambda w: w.put(message, sender), dest_workers)

    def put_all(self, message, sender=None):
        map(lambda w: w.put(message, sender), self.workers.values())

    def create_message(self, messsage_type, data, system_message):

        if multiprocessing.current_process().name == 'MainProcess':
            msg = MessageBase(messsage_type, 'system', data, system_message)
            return msg
        else:
            raise Exception("Only the main process can create system messages")

    def listen(self, message_type):
        msg = self.get()
        if msg.message_type is message_type:
            return msg
        else:
            raise Exception("Recieved '{}' while listening for '{}'".format(msg.message_type, message_type))

    def get(self):
        msg = self.channel.get()

        # print("{} | Get | type: {}, sender: {}, data: {}".
        #       format('system', msg.message_type, msg.sender_name, msg.data))

        return msg

    def close(self):
        map(lambda w: w.close(), self.workers.values())
        self.channel.close()
