# -*- coding: utf-8 -*-
"""Operator messages

"""

from s3filter.op.tuple import Tuple
from s3filter.util.bloom_filter import BloomFilter
from s3filter.util.scalable_bloom_filter import ScalableBloomFilter
from s3filter.util.simple_bloom_filter import SimpleBloomFilter


class Message(object):
    """Base Message class

    """

    def __init__(self, content):
        """Creates a new Message

        :param content: The content of the message
        """
        self.content = content


class TupleMessage(Message):
    """Message containing a tuple.

    """

    def __init__(self, tuple_):
        """Creates a new TupleMessage

        :param tuple_: The tuple content of the message
        """

        if type(tuple_) is not Tuple:
            raise Exception("Message content type is {}. Type must be Tuple".format(type(tuple_)))

        self.tuple_ = tuple_
        super(TupleMessage, self).__init__(tuple_)


class BloomMessage(Message):
    """Message containing a bloom filter.

    """

    def __init__(self, bloom_filter):
        """Creates a new BloomMessage

        :param bloom_filter: The bloom filter content of the message
        """

        if type(bloom_filter) is not ScalableBloomFilter and \
                type(bloom_filter) is not BloomFilter and \
                type(bloom_filter) is not SimpleBloomFilter:
            raise Exception("Message content type is {}. Type must be '{}', '{}', or '{}'"
                            .format(type(bloom_filter),
                                    SimpleBloomFilter.__class__.__name__,
                                    BloomFilter.__class__.__name__,
                                    ScalableBloomFilter.__class__.__name__))

        self.bloom_filter = bloom_filter
        super(BloomMessage, self).__init__(bloom_filter)
