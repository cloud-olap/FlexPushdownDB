class HandlerBase(object):

    def __init__(self):
        pass

    def on_message(self, message, worker, system):
        raise NotImplementedError()
