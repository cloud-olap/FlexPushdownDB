from s3filter.multiprocessing.message import MessageBase


class Header(object):

    def __init__(self, shape, dtype, columns, msg):
        # type: (tuple, str, [str], MessageBase) -> None
        self.shape = shape
        self.dtype = dtype
        self.columns = columns
        self.msg = msg

    def __repr__(self):
        return "{}".format({
            'shape': self.shape, 'dtype': self.dtype, 'columns': self.columns}
        )
