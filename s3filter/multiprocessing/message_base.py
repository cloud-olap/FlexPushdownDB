class MessageBase(object):

    def __init__(self, message_type, sender_name, data):
        self.message_type = message_type
        self.sender_name = sender_name
        self.data = data

    def __repr__(self):
        return "{}".format({
            'message_type': self.message_type, 'sender_name': self.sender_name, 'data': self.data}
        )
