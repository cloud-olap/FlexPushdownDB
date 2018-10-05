from s3filter.multiprocessing.header import Header
from s3filter.multiprocessing.message import MessageBase


class PacketBase(object):
    sender_name = None  # type: str

    def __init__(self):
        pass

    def set_sender(self, sender_name):
        # type: (str) -> None
        self.sender_name = sender_name

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, {
            'sender_name': self.sender_name
        })


class StopPacket(PacketBase):

    def __init__(self):
        super(StopPacket, self).__init__()


class RequestBufferPacket(PacketBase):

    def __init__(self):
        super(RequestBufferPacket, self).__init__()


class GrantBufferPacket(PacketBase):

    def __init__(self):
        super(GrantBufferPacket, self).__init__()


class HeaderPacket(PacketBase):

    def __init__(self, header):
        # type: (Header) -> None
        super(HeaderPacket, self).__init__()
        self.header = header

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, {
            'sender_name': self.sender_name, 'header': self.header
        })


class DataPacket(PacketBase):

    def __init__(self, ndarray, columns, msg):
        super(DataPacket, self).__init__()
        self.ndarray = ndarray
        self.columns = columns
        self.msg = msg

    def __repr__(self):
        return "{}".format({
            'ndarray': self.ndarray, 'columns': self.columns, 'sender_name': self.sender_name}
        )


class MessagePacket(PacketBase):
    message = None  # type: MessageBase

    def __init__(self, message):
        # type: (MessageBase) -> None
        super(MessagePacket, self).__init__()
        self.message = message

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, {
            'sender_name': self.sender_name, 'message': self.message
        })
