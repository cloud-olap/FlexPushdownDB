class MessageBase(object):
    sender_name = None  # type: str

    def __init__(self):
        pass

    def set_sender(self, sender_name):
        # type: (str) -> None
        self.sender_name = sender_name


class DataFrameMessage(MessageBase):

    def __init__(self, dataframe):
        super(DataFrameMessage, self).__init__()
        self.dataframe = dataframe

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, {
            'dataframe': self.dataframe
        })


class StartMessage(MessageBase):

    def __init__(self):
        super(StartMessage, self).__init__()

    def __repr__(self):
        return "{}".format(self.__class__.__name__)


class StopMessage(MessageBase):

    def __init__(self):
        super(StopMessage, self).__init__()

    def __repr__(self):
        return "{}".format(self.__class__.__name__)
