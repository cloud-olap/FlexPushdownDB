class HeaderMessageData(object):

    def __init__(self, shape, columns):
        self.shape = shape
        self.columns = columns

    def __repr__(self):
        return "{}".format({
            'shape': self.shape, 'columns': self.columns}
        )
