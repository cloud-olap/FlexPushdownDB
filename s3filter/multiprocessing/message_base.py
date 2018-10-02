class MessageBase(object):

    def __init__(self, message_type, sender_name, data, system_message):
        self.message_type = message_type
        self.sender_name = sender_name
        self.data = data
        self.system_message = system_message

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, {
            'message_type': self.message_type, 'sender_name': self.sender_name}
        )
